package main

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"

	"github.com/mxsfog/VKTestTask/internal/processor"
	"github.com/mxsfog/VKTestTask/internal/repository"
)

func main() {
	// Database connection
	dsn := "host=localhost user=youruser password=yourpassword dbname=yourdb port=5432 sslmode=disable TimeZone=Asia/Shanghai"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Auto migrate the schema
	db.AutoMigrate(&repository.TDocument{})

	// Kafka consumer setup
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	partitionConsumer, err := consumer.ConsumePartition("documents", 0, sarama.OffsetNewest)
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
		// Here you can send processedDoc to another Kafka topic or handle it as needed
	}

	// Kafka producer example (you might want to send processed documents to another topic)
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
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
		Topic: "processed_documents",
		Value: sarama.ByteEncoder(docBytes),
	}
	_, _, err = producer.SendMessage(message)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
	} else {
		log.Println("Message sent successfully")
	}
}
