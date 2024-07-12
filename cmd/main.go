package main

import (
	"VKTestTask/config"
	"VKTestTask/internal/processor"
	"VKTestTask/internal/repository"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
)

func main() {
	conf, err := config.LoadConfig("conf/conf.yaml")
	if err != nil {
		log.Fatalf("Failed to load conf: %v", err)
	}

	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=%s TimeZone=%s",
		conf.Database.Host,
		conf.Database.User,
		conf.Database.Password,
		conf.Database.DBName,
		conf.Database.Port,
		conf.Database.SSLMode,
		conf.Database.TimeZone,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Автоматическая миграция базы данных
	if err := db.AutoMigrate(&repository.TDocument{}); err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(conf.Kafka.Brokers, saramaConfig)
	if err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(conf.Kafka.Topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	repo := repository.NewDatabaseRepository(db)
	proc := processor.NewDocumentProcessor(repo)

	go func() {
		for err := range partitionConsumer.Errors() {
			log.Printf("Error consuming messages: %v", err)
		}
	}()

	for msg := range partitionConsumer.Messages() {
		var doc repository.TDocument
		if err := json.Unmarshal(msg.Value, &doc); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}

		processedDoc, err := proc.Process(&doc)
		if err != nil {
			log.Printf("Failed to process document: %v", err)
			continue
		}

		log.Printf("Processed document: %+v", processedDoc)
	}

	producer, err := sarama.NewSyncProducer(conf.Kafka.Brokers, nil)
	if err != nil {
		log.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Close()

	doc := &repository.TDocument{
		Url:       "http://example.com/doc1",
		PubDate:   100,
		FetchTime: 200,
		Text:      "First version",
	}
	docBytes, _ := json.Marshal(doc)
	message := &sarama.ProducerMessage{
		Topic: conf.Kafka.ProcessedTopic,
		Value: sarama.ByteEncoder(docBytes),
	}
	_, _, err = producer.SendMessage(message)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
	} else {
		log.Println("Message sent successfully")
	}
}
