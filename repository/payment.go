package repository

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type paymentRepository struct {
	database *gorm.DB
}

func (p *paymentRepository) Refund(ctx context.Context, userId string, amountDeducted uint) error {
	tx := p.database.Model(Payment{}).Begin()
	err := error(nil)
	defer func() {
		if err != nil {
			tx.Rollback()
		}
		tx.Commit()
	}()
	var payment Payment
	if err = tx.Where("user_id = ?", userId).First(&payment).Error; err != nil {
		return err
	}
	payment.Balance += amountDeducted
	if err = tx.Where("user_id = ?", userId).Save(&payment).Error; err != nil {
		return err
	}
	return nil
}

func (p *paymentRepository) Debit(ctx context.Context, userId string, amountDeducted uint) error {
	//time.Sleep(time.Duration(6) * time.Second)
	select {
	case <-ctx.Done():
		return fmt.Errorf("timeout")
	default:
		return func() error {
			tx := p.database.Model(Payment{}).Begin()
			err := error(nil)
			defer func() {
				if err != nil {
					tx.Rollback()
				}
				tx.Commit()
			}()
			var payment Payment
			if err = tx.Where("user_id = ?", userId).First(&payment).Error; err != nil {
				return err
			}
			if payment.Balance < amountDeducted {
				return fmt.Errorf("thieu tien")
			}
			payment.Balance -= amountDeducted
			if err = tx.Where("user_id = ?", userId).Save(&payment).Error; err != nil {
				return err
			}
			return nil
		}()
	}
}

func (p *paymentRepository) GetById(ctx context.Context, userId string) (Payment, error) {
	var (
		payment = Payment{}
	)
	err := p.database.Model(Payment{}).Where("user_id = ?", userId).First(&payment).Error
	return payment, err
}

func (p *paymentRepository) Prepare(ctx context.Context, userId string, amountDeducted uint) TxI {
	//time.Sleep(time.Duration(10) * time.Second)
	select {
	case <-ctx.Done():
		return TxI{
			DB:  nil,
			Err: fmt.Errorf("timeout"),
		}
	default:
		return func() TxI {
			tx := p.database.Model(Payment{}).Begin()
			var payment Payment
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("user_id = ?", userId).First(&payment).Error; err != nil {
				tx.Rollback()
				return TxI{
					DB:  nil,
					Err: err,
				}
			}
			if payment.Balance < amountDeducted {
				return TxI{
					DB:  nil,
					Err: fmt.Errorf("thieu tien"),
				}
			}
			payment.Balance -= amountDeducted
			if err := tx.Where("user_id = ?", userId).Save(&payment).Error; err != nil {
				tx.Rollback()
				return TxI{
					DB:  nil,
					Err: err,
				}
			}

			return TxI{
				DB:  tx,
				Err: nil,
			}
		}()
	}
}

type PaymentRepository interface {
	Prepare(ctx context.Context, userId string, amountDeducted uint) TxI
	GetById(ctx context.Context, userId string) (Payment, error)
	Refund(ctx context.Context, userId string, amountDeducted uint) error
	Debit(ctx context.Context, userId string, amountDeducted uint) error
}

func NewPaymentRepo(db *gorm.DB) PaymentRepository {
	return &paymentRepository{database: db}
}
