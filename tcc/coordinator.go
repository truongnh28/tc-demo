package tcc

import (
	"context"
	DT "dt/domain"
	"dt/log"
	repo2 "dt/repository"
	"fmt"
	"sync"
)

type Voz struct {
	paymentTx   repo2.PaymentRepository
	orderTx     repo2.OrderRepository
	inventoryTx repo2.InventoryRepository
}

type Coordinator struct {
	client Voz
}

func (c *Coordinator) Run(ctx context.Context, req DT.OrderRequest) error {
	logger := log.GetLogger(ctx)
	logger.Infoln("Try phase!!!")
	orderId, err := c.try(ctx, req)
	if err != nil {
		logger.Infoln("Cancel phase!!!")
		logger.WithError(err).Errorf("Try phase err: %s\n", err)
		return c.cancel(ctx, orderId)
	}
	logger.Infoln("Commit phase!!!")
	err = c.confirm(ctx, req)
	if err != nil {
		logger.WithError(err).Errorf("Comfirm phase err: %s\n", err)
		cerr := c.cancel(ctx, orderId)
		logger.WithError(cerr).Errorf("Cancel err: %s\n", cerr)
		return err
	}
	return nil
}

func (c *Coordinator) try(ctx context.Context, req DT.OrderRequest) (uint, error) {
	var (
		errs   = make(chan error, 2)
		es     = make([]error, 0)
		logger = log.GetLogger(ctx)
	)
	order, err := c.client.orderTx.Create(ctx, req.UserId, req.BookId, req.Number)
	if err != nil {
		logger.WithError(err).Errorf("Try phase: Create Order fail: %s\n", err)
		return 0, err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pay, err := c.client.paymentTx.GetById(ctx, req.UserId)
		if err != nil {
			logger.WithError(err).Errorf("Try phase: Get payment fail: %s\n", err)
			errs <- err
			return
		}
		if pay.Balance < req.Number*10000 {
			err := fmt.Errorf("thieu tien")
			logger.WithError(err).Errorf("Try phase: Thieu tien: %s\n", err)
			errs <- err
			return
		}
		errs <- nil
		return
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		inv, err := c.client.inventoryTx.GetById(ctx, req.BookId)
		if err != nil {
			logger.WithError(err).Errorf("Try phase: Get inventory fail: %s\n", err)
			errs <- err
			return
		}
		if inv.Number < req.Number {
			err := fmt.Errorf("khong du sach ban :(")
			logger.WithError(err).Errorf("Try phase: Het sach: %s\n", err)
			errs <- err
			return
		}
		errs <- nil
		return
	}()
	go func() {
		wg.Wait()
		close(errs)
	}()
	for e := range errs {
		es = append(es, e)
	}
	for _, e := range es {
		if e != nil {
			return order.ID, e
		}
	}
	return order.ID, nil
}

func (c *Coordinator) confirm(ctx context.Context, req DT.OrderRequest) error {
	logger := log.GetLogger(ctx)
	err := c.client.inventoryTx.Deduct(ctx, req.BookId, req.Number)
	if err != nil {
		logger.WithError(err).Errorf("Confirm phase: Inventory Deduct fail: %s\n", err)
		return err
	}
	err = c.client.paymentTx.Debit(ctx, req.UserId, req.Number*1000000)
	if err != nil {
		logger.WithError(err).Errorf("Confirm phase: Payment Debit fail: %s\n", err)
		logger.Infoln("Confirm phase: Payment refund")
		_ = c.client.inventoryTx.Revert(ctx, req.BookId, req.Number)
		return err
	}
	return nil
}

func (c *Coordinator) cancel(ctx context.Context, orderId uint) error {
	return c.client.orderTx.Delete(ctx, orderId)
}

func NewCoordinator(
	paymentTx repo2.PaymentRepository,
	orderTx repo2.OrderRepository,
	inventoryTx repo2.InventoryRepository,
) *Coordinator {
	return &Coordinator{
		client: Voz{
			paymentTx:   paymentTx,
			orderTx:     orderTx,
			inventoryTx: inventoryTx,
		},
	}
}
