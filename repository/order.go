package repository

import (
	"context"
	"fmt"
	"gorm.io/gorm"
)

type orderRepository struct {
	database *gorm.DB
}

func (o *orderRepository) Delete(ctx context.Context, orderID uint) error {
	return o.database.WithContext(ctx).Model(Order{}).Delete(&Order{}, orderID).Error
}

func (o *orderRepository) Create(ctx context.Context, userId, bookId string, number uint) (Order, error) {
	var order = Order{
		UserID: userId,
		BookId: bookId,
		Number: number,
	}
	err := o.database.WithContext(ctx).Model(Order{}).Create(&order).Error
	return order, err
}

func (o *orderRepository) Prepare(ctx context.Context, userId, bookId string, number uint) TxI {
	select {
	case <-ctx.Done():
		return TxI{
			DB:  nil,
			Err: fmt.Errorf("timeout"),
		}
	default:
		return func() TxI {
			tx := o.database.Model(Order{}).Begin()
			var order = Order{
				UserID: userId,
				BookId: bookId,
				Number: number,
			}
			if err := tx.Create(&order).Error; err != nil {
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

type OrderRepository interface {
	Prepare(ctx context.Context, userId, bookId string, number uint) TxI
	Create(ctx context.Context, userId, bookId string, number uint) (Order, error)
	Delete(ctx context.Context, id uint) error
}

func NewOrderRepo(db *gorm.DB) OrderRepository {
	return &orderRepository{database: db}
}
