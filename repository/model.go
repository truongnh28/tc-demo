package repository

import "gorm.io/gorm"

type Inventory struct {
	*gorm.Model
	BookId string `gorm:"type:varchar(255);column:book_id;not null"`
	Number uint   `gorm:"type:int unsigned;column:number;not null"`
}

type Payment struct {
	*gorm.Model
	UserID  string `gorm:"type:varchar(255);column:user_id;not null"`
	Balance uint   `gorm:"type:int unsigned;column:balance;not null"`
}

type Order struct {
	*gorm.Model
	UserID string `gorm:"type:varchar(255);column:user_id;not null"`
	BookId string `gorm:"type:varchar(255);column:book_id;not null"`
	Number uint   `gorm:"type:int unsigned;column:number;not null"`
}
