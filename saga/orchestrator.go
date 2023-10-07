package main

import (
	"context"
	"dt/domain"
	log2 "dt/log"
	"github.com/bytedance/sonic"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type Orchestrator struct {
	c *redis.Client
}

func (o *Orchestrator) Create(ctx context.Context, req domain.OrderRequest) {
	m := Message{
		ID:      uuid.New().String(),
		Message: req,
	}
	o.next(ctx, OrderChannel, ServiceOrder, m)
}

func (o *Orchestrator) Run(ctx context.Context) {
	var (
		err    error
		logger = log2.GetLogger(ctx)
	)
	pubsub := o.c.Subscribe(ctx, PaymentChannel, ReplyChannel)
	if _, err = pubsub.Receive(ctx); err != nil {
		logger.Fatalf("error subscribing %s", err)
	}
	defer func() { _ = pubsub.Close() }()
	ch := pubsub.Channel()
	logger.Infoln("starting the Orchestrator")
	for {
		select {
		case msg := <-ch:
			m := Message{}
			if err = sonic.Unmarshal([]byte(msg.Payload), &m); err != nil {
				logger.WithError(err).Errorf("Unmarshal err: %s\n", err)
				continue
			}
			switch msg.Channel {
			case ReplyChannel:
				if m.Action != ActionDone {
					logger.Infof("Rolling back transaction with id %s", m.ID)
					o.rollback(ctx, m)
					continue
				}
				switch m.Service {
				case ServiceOrder:
					o.next(ctx, PaymentChannel, ServicePayment, m)
				case ServicePayment:
					o.next(ctx, InventoryChannel, ServiceInventory, m)
				case ServiceInventory:
					logger.Infoln("OK")
				}
			}
		}
	}
}

func (o *Orchestrator) next(ctx context.Context, channel, service string, message Message) {
	var (
		err    error
		logger = log2.GetLogger(ctx)
	)
	message.Action = ActionStart
	message.Service = service
	if err = o.c.Publish(ctx, channel, message).Err(); err != nil {
		logger.WithError(err).Errorf("error publishing Run-message to %s channel", channel)
	}
	logger.Infof("Run message published to channel :%s", channel)
}

func (o *Orchestrator) rollback(ctx context.Context, m Message) {
	var (
		err    error
		logger = log2.GetLogger(ctx)
	)
	message := Message{
		ID:         m.ID,
		Service:    m.Service,
		Action:     ActionRollback,
		Message:    m.Message,
		ServiceErr: m.ServiceErr,
	}
	if m.ServiceErr == OrderChannel {
		return
	}
	if err = o.c.Publish(ctx, OrderChannel, message).Err(); err != nil {
		logger.WithError(err).Errorf("error publishing rollback message to %s channel", OrderChannel)
	} else {
		logger.Infof("Send event Order rollback success :%x", message)
	}
	if m.ServiceErr == PaymentChannel {
		return
	}
	if err = o.c.Publish(ctx, PaymentChannel, message).Err(); err != nil {
		logger.WithError(err).Errorf("error publishing rollback message to %s channel", PaymentChannel)
	} else {
		logger.Infof("Send event Payment rollback success :%x", message)
	}
	if m.ServiceErr == ServiceInventory {
		return
	}
	if err = o.c.Publish(ctx, InventoryChannel, message).Err(); err != nil {
		logger.WithError(err).Errorf("error publishing rollback message to %s channel", InventoryChannel)
	} else {
		logger.Infof("Send event Inventory rollback success :%x", message)
	}
}

func NewOrchestrator(c *redis.Client) *Orchestrator {
	return &Orchestrator{
		c: c,
	}
}
