package main

import (
	"dt/domain"
	"github.com/bytedance/sonic"
)

const (
	PaymentChannel   string = "PaymentChannel"
	OrderChannel     string = "OrderChannel"
	InventoryChannel string = "InventoryChannel"
	ReplyChannel     string = "ReplyChannel"

	ServicePayment   string = "PaymentService"
	ServiceOrder     string = "Order"
	ServiceInventory string = "Inventory"

	ActionStart    string = "Start"
	ActionDone     string = "DoneMsg"
	ActionError    string = "ErrorMsg"
	ActionRollback string = "RollbackMsg"
)

type Message struct {
	ID         string              `json:"id"`
	Service    string              `json:"service"`
	Action     string              `json:"action"`
	ServiceErr string              `json:"service_err"`
	Message    domain.OrderRequest `json:"message"`
}

type OrderRequest struct {
	UserId string `json:"user_id,omitempty"`
	BookId string `json:"book_id,omitempty"`
	Number uint   `json:"number,omitempty"`
}

func (m Message) MarshalBinary() ([]byte, error) {
	return sonic.Marshal(m)
}
