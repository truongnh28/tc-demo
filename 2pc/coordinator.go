package _pc

import (
	"context"
	DT "dt/domain"
	repo2 "dt/repository"
	"github.com/samber/lo"
	"gorm.io/gorm"
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
	txs := c.prepare(ctx, req)
	dbs := lo.Map(txs, func(item repo2.TxI, index int) *gorm.DB {
		return item.DB
	})
	for i := 0; i < len(txs); i++ {
		if txs[i].Err == nil {
			continue
		}
		c.rollback(dbs)
		return txs[i].Err
	}
	c.commit(dbs)
	return nil
}

func (c *Coordinator) prepare(ctx context.Context, req DT.OrderRequest) []repo2.TxI {
	var (
		dbs     = make([]repo2.TxI, 0)
		results = make(chan repo2.TxI, 3)
	)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		results <- c.client.orderTx.Prepare(ctx, req.UserId, req.BookId, req.Number)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		results <- c.client.inventoryTx.Prepare(ctx, req.BookId, req.Number)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		results <- c.client.paymentTx.Prepare(ctx, req.UserId, req.Number*10000) // đồng giá :)
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	for result := range results {
		dbs = append(dbs, result)
	}
	return dbs
}

func (c *Coordinator) commit(dbs []*gorm.DB) {
	if len(dbs) == 0 {
		return
	}
	for _, db := range dbs {
		if db == nil {
			continue
		}
		db.Commit()
	}
	return
}

func (c *Coordinator) rollback(dbs []*gorm.DB) {
	if len(dbs) == 0 {
		return
	}
	for _, db := range dbs {
		if db == nil {
			continue
		}
		db.Rollback()
	}
	return
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
