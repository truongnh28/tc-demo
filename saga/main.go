package main

import (
	"context"
	"dt/domain"
	repo2 "dt/repository"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"log"
	"net/http"
	"time"
)

func main() {
	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	db := repo2.InitDatabase()
	pTx := repo2.NewPaymentRepo(db)
	oTx := repo2.NewOrderRepo(db)
	iTx := repo2.NewInventoryRepo(db)

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("error creating redis client %s", err)
	}

	paymentService := NewPaymentService(pTx, client)
	orderService := NewOrderService(oTx, client)
	inventoryService := NewInventoryService(iTx, client)
	oo := NewOrchestrator(client)
	go oo.Run(context.Background())
	go paymentService.Run(context.Background())
	go inventoryService.Run(context.Background())
	go orderService.Run(context.Background())

	router.POST("/saga/order", func(c *gin.Context) {
		var orderData domain.OrderRequest
		err := c.BindJSON(&orderData)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		ctx, cancel := context.WithTimeout(c, 1000*time.Second)
		defer cancel()
		oo.Create(ctx, orderData)
		if err != nil {
			c.JSON(500, gin.H{
				"message": fmt.Sprintf("err: %s", err.Error()),
			})
		} else {
			c.JSON(200, gin.H{
				"message": "success",
			})
		}
	})

	router.Run(":8080")
}
