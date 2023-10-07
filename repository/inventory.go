package repository

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type inventoryRepository struct {
	database *gorm.DB
}

func (i *inventoryRepository) Revert(ctx context.Context, bookId string, number uint) error {
	tx := i.database.WithContext(ctx).Model(Inventory{}).Begin()
	err := error(nil)
	defer func() {
		if err != nil {
			tx.Rollback()
		}
		tx.Commit()
	}()
	var inventory Inventory
	if err = tx.Where("book_id = ?", bookId).First(&inventory).Error; err != nil {
		return err
	}
	inventory.Number += number
	if err = tx.Where("book_id = ?", bookId).Save(&inventory).Error; err != nil {
		return err
	}
	return nil
}

func (i *inventoryRepository) Deduct(ctx context.Context, bookId string, number uint) error {
	tx := i.database.WithContext(ctx).Model(Inventory{}).Begin()
	err := error(nil)
	defer func() {
		if err != nil {
			tx.Rollback()
		}
		tx.Commit()
	}()
	var inventory Inventory
	if err = tx.Where("book_id = ?", bookId).First(&inventory).Error; err != nil {
		return err
	}
	if inventory.Number < number {
		return fmt.Errorf("thieu tien")
	}
	inventory.Number -= number
	if err = tx.Where("book_id = ?", bookId).Save(&inventory).Error; err != nil {
		return err
	}
	return nil
}

func (i *inventoryRepository) GetById(ctx context.Context, bookId string) (Inventory, error) {
	var (
		inventory = Inventory{}
	)
	err := i.database.Model(Inventory{}).Where("book_id = ?", bookId).First(&inventory).Error
	return inventory, err
}

func (i *inventoryRepository) Prepare(ctx context.Context, bookId string, number uint) TxI {
	select {
	case <-ctx.Done():
		return TxI{
			DB:  nil,
			Err: fmt.Errorf("timeout"),
		}
	default:
		return func() TxI {
			tx := i.database.Model(Inventory{}).Begin()
			var inventory Inventory
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("book_id = ?", bookId).First(&inventory).Error; err != nil {
				tx.Rollback()
				return TxI{
					DB:  nil,
					Err: err,
				}
			}
			if inventory.Number < number {
				return TxI{
					DB:  nil,
					Err: fmt.Errorf("thieu tien"),
				}
			}
			inventory.Number -= number
			if err := tx.Where("book_id = ?", bookId).Save(&inventory).Error; err != nil {
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

type InventoryRepository interface {
	Prepare(ctx context.Context, bookId string, number uint) TxI
	GetById(ctx context.Context, bookId string) (Inventory, error)
	Deduct(ctx context.Context, bookId string, number uint) error
	Revert(ctx context.Context, bookId string, number uint) error
}

func NewInventoryRepo(db *gorm.DB) InventoryRepository {
	return &inventoryRepository{database: db}
}
