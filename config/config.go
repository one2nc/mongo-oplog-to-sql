package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

const (
	MONGO_URI   = "MONGO_URI"
	DB_ADAPTOR  = "DB_ADAPTOR"
	DB_NAME     = "DB_NAME"
	DB_HOST     = "DB_HOST"
	DB_PORT     = "DB_PORT"
	DB_USER     = "DB_USER"
	DB_PASSWORD = "DB_PASSWORD"
	ENV         = ".env"
)

type DBConfig struct {
	Adaptor  string
	Name     string
	Host     string
	UserName string
	Password string
	Port     string
}

type Config struct {
	MongoURI string
	DBConfig DBConfig
}

func Load() Config {
	err := godotenv.Load(ENV)
	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	cfg := Config{
		MongoURI: readFromEnvFile(MONGO_URI),
		DBConfig: DBConfig{
			Adaptor:  readFromEnvFile(DB_ADAPTOR),
			Name:     readFromEnvFile(DB_NAME),
			Host:     readFromEnvFile(DB_HOST),
			Port:     readFromEnvFile(DB_PORT),
			UserName: readFromEnvFile(DB_USER),
			Password: readFromEnvFile(DB_PASSWORD),
		},
	}

	return cfg
}

func readFromEnvFile(key string) string {
	return os.Getenv(key)
}
