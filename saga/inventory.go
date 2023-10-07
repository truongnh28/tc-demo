package main

import (
	"context"
	log2 "dt/log"
	repo2 "dt/repository"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/go-redis/redis/v8"
)

type InventoryService struct {
	inventoryTx repo2.InventoryRepository
	redisCli    *redis.Client
}

func (p InventoryService) Run(ctx context.Context) {
	var (
		err    error
		logger = log2.GetLogger(ctx)
	)
	pubsub := p.redisCli.Subscribe(ctx, InventoryChannel, ReplyChannel)
	if _, err = pubsub.Receive(ctx); err != nil {
		logger.Fatalf("error subscribing %s", err)
	}
	defer func() { _ = pubsub.Close() }()
	ch := pubsub.Channel()

	logger.Infoln("starting the Inventory service")
	for {
		select {
		case msg := <-ch:
			m := Message{}
			err := sonic.Unmarshal([]byte(msg.Payload), &m)
			if err != nil {
				logger.WithError(err).Errorf("Unmarshal err: %s\n", err)
				continue
			}
			switch msg.Channel {
			case InventoryChannel:
				logger.Infof("received message with id %s ", m.ID)
				if m.Action == ActionStart {
					m.Action = func() string {
						inv, err := p.inventoryTx.GetById(ctx, m.Message.BookId)
						if err != nil {
							logger.WithError(err).Errorf("InventoryService: Get inventory fail: %s\n", err)
							return ActionError
						}
						if inv.Number < m.Message.Number {
							err := fmt.Errorf("khong du sach ban :(")
							logger.WithError(err).Errorf("InventoryService: Het sach: %s\n", err)
							return ActionError
						}
						err = p.inventoryTx.Deduct(ctx, m.Message.BookId, m.Message.Number)
						if err != nil {
							logger.WithError(err).Errorf("InventoryService Debit err: %s\n", err)
							return ActionError
						}
						return ActionDone
					}()
					if m.Action != ActionDone {
						m.ServiceErr = ServiceInventory
					}
					if err = p.redisCli.Publish(ctx, ReplyChannel, m).Err(); err != nil {
						logger.WithError(err).Errorf("error publishing done-message to %s channel", ReplyChannel)
						continue
					}

				}
				if m.Action == ActionRollback {
					logger.Infof("rolling back transaction with ID :%s", m.ID)
					err = p.inventoryTx.Revert(ctx, m.Message.BookId, m.Message.Number)
					if err != nil {
						logger.WithError(err).Errorf("InventoryService Refund err: %s\n", err)
					}
					logger.Infof("rollback inventory success")
				}
			}
		}
	}
}

func NewInventoryService(
	InventoryTx repo2.InventoryRepository,
	redisCli *redis.Client,
) *InventoryService {
	return &InventoryService{
		inventoryTx: InventoryTx,
		redisCli:    redisCli,
	}
}
