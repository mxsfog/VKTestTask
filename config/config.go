package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
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

func LoadConfig(path string) (*Config, error) {
	fmt.Println("Loading config from:", path)

	// Проверка существования файла перед чтением
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file does not exist: %s", path)
	} else if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}
