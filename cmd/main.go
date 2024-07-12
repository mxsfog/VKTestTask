package main

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/mxsfog/VKTestTask/internal/processor"
	"github.com/mxsfog/VKTestTask/internal/repository"
	"gopkg.in/yaml.v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"io/ioutil"
	"log"
)

type Config struct {
	Database struct {
		Host     string `yaml:"host"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		DBName   string `yaml:"dbname"`
		Port     int    `yaml:"port"`
		SSLMode  string `yaml:"sslmode"`
		TimeZone string `yaml:"timezone"`
	} `yaml:"database"`
	Kafka struct {
		Brokers        []string `yaml:"brokers"`
		Topic          string   `yaml:"topic"`
		ProcessedTopic string   `yaml:"processed_topic"`
	} `yaml:"kafka"`
}

func loadConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return &config, nil
}

func main() {
	config, err := loadConfig("config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	dsn := "host=" + config.Database.Host +
		" user=" + config.Database.User +
		" password=" + config.Database.Password +
		" dbname=" + config.Database.DBName +
		" port=" + string(config.Database.Port) +
		" sslmode=" + config.Database.SSLMode +
		" TimeZone=" + config.Database.TimeZone

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	db.AutoMigrate(&repository.TDocument{})

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(config.Kafka.Brokers, saramaConfig)
	if err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(config.Kafka.Topic, 0, sarama.OffsetNewest)
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

	producer, err := sarama.NewSyncProducer(config.Kafka.Brokers, nil)
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
		Topic: config.Kafka.ProcessedTopic,
		Value: sarama.ByteEncoder(docBytes),
	}
	_, _, err = producer.SendMessage(message)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
	} else {
		log.Println("Message sent successfully")
	}
}
