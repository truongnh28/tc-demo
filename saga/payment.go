package main

import (
	"context"
	log2 "dt/log"
	repo2 "dt/repository"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/go-redis/redis/v8"
)

type PaymentService struct {
	paymentTx repo2.PaymentRepository
	redisCli  *redis.Client
}

func (p PaymentService) Run(ctx context.Context) {
	var (
		err    error
		logger = log2.GetLogger(ctx)
	)
	pubsub := p.redisCli.Subscribe(ctx, PaymentChannel, ReplyChannel)
	if _, err = pubsub.Receive(ctx); err != nil {
		logger.Fatalf("error subscribing %s", err)
	}
	defer func() { _ = pubsub.Close() }()
	ch := pubsub.Channel()

	logger.Infoln("starting the Payment service")
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
			case PaymentChannel:
				logger.Infof("recieved message with id %s ", m.ID)
				if m.Action == ActionStart {
					m.Action = func() string {
						pay, err := p.paymentTx.GetById(ctx, m.Message.UserId)
						if err != nil {
							logger.WithError(err).Errorf("PaymentService: Get payment fail: %s\n", err)
							return ActionError
						}
						if pay.Balance < m.Message.Number*10000 {
							err := fmt.Errorf("thieu tien")
							logger.WithError(err).Errorf("PaymentService: Thieu tien: %s\n", err)
							return ActionError
						}
						err = p.paymentTx.Debit(ctx, m.Message.UserId, m.Message.Number*10000)
						if err != nil {
							logger.WithError(err).Errorf("PaymentService Debit err: %s\n", err)
							return ActionError
						}
						return ActionDone
					}()
					if m.Action != ActionDone {
						m.ServiceErr = ServicePayment
					}
					if err = p.redisCli.Publish(ctx, ReplyChannel, m).Err(); err != nil {
						logger.WithError(err).Errorf("error publishing done-message to %s channel", ReplyChannel)
						continue
					}
					logger.Infof("done message published to channel :%s", ReplyChannel)
				}
				if m.Action == ActionRollback {
					logger.Infof("rolling back transaction with ID :%s", m.ID)
					err = p.paymentTx.Refund(ctx, m.Message.UserId, m.Message.Number*10000)
					if err != nil {
						logger.WithError(err).Errorf("PaymentService Refund err: %s\n", err)
						continue
					}
					logger.Infof("refund success")
				}
			}
		}
	}
}

func NewPaymentService(
	paymentTx repo2.PaymentRepository,
	redisCli *redis.Client,
) *PaymentService {
	return &PaymentService{
		paymentTx: paymentTx,
		redisCli:  redisCli,
	}
}
