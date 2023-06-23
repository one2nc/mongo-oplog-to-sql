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
	DBAdaptor  string
	DBName     string
	DBHost     string
	DBUserName string
	DBPassword string
	DBPort     string
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
			DBAdaptor:  readFromEnvFile(DB_ADAPTOR),
			DBName:     readFromEnvFile(DB_NAME),
			DBHost:     readFromEnvFile(DB_HOST),
			DBPort:     readFromEnvFile(DB_PORT),
			DBUserName: readFromEnvFile(DB_USER),
			DBPassword: readFromEnvFile(DB_PASSWORD),
		},
	}

	return cfg
}

func readFromEnvFile(key string) string {
	return os.Getenv(key)
}
