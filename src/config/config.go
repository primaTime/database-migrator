package config

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

type Config struct {
	Source    DatabaseConfig `json:"source"`
	Target    DatabaseConfig `json:"target"`
	BatchSize int            `json:"batchSize"`
	Tables    []Table        `json:"tables"`
}

type DatabaseConfig struct {
	DSN    string `json:"dsn"`
	Schema string `json:"schema"`
	Driver string `json:"driver"`
}

type Table struct {
	Name         string   `json:"name"`
	Columns      []string `json:"columns"`
	Dependencies []string `json:"dependencies"`
}

func ReadConfig(configFile string) (Config, error) {
	config := Config{}
	file, err := os.Open(configFile)
	if err != nil {
		return config, err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return config, err
	}

	err = json.Unmarshal(bytes, &config)
	return config, err
}

func WriteConfig(configPath string, config Config) {
	configFile, err := os.Create(configPath)
	if err != nil {
		log.Fatalf("Error creating config file: %v", err)
	}
	defer configFile.Close()

	configBytes, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		log.Fatalf("Error marshalling config: %v", err)
	}

	_, err = configFile.Write(configBytes)
	if err != nil {
		log.Fatalf("Error writing to config file: %v", err)
	}
}
