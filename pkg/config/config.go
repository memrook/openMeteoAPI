package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

// Config содержит настройки приложения
type Config struct {
	// Данные для базы данных
	DbServer   string
	DbLogin    string
	DbPassword string
	DbName     string

	// Интервал сбора данных в минутах
	CollectionInterval int

	// Количество дней для предыдущих данных
	PastDays int
}

// LoadConfig загружает конфигурацию из .env файла и переменных окружения
func LoadConfig() *Config {
	// Попытка загрузить .env файл, если он существует
	_ = godotenv.Load()

	cfg := &Config{
		// Данные базы данных
		DbServer:   getEnv("DB_SERVER", "ACLSDWHODS001.acl.agroconcern.ru"),
		DbLogin:    getEnv("DB_LOGIN", ""),
		DbPassword: getEnv("DB_PASSWORD", ""),
		DbName:     getEnv("DB_NAME", "WeatherData"),

		// Интервал сбора данных (по умолчанию 24 часа)
		CollectionInterval: getEnvAsInt("COLLECTION_INTERVAL", 24*60), // 1 день по умолчанию

		// Количество дней истории (по умолчанию 365 дней)
		PastDays: getEnvAsInt("PAST_DAYS", 365),
	}

	// Проверка обязательных полей
	if cfg.DbLogin == "" || cfg.DbPassword == "" {
		log.Fatal("DB_LOGIN и DB_PASSWORD должны быть указаны")
	}

	return cfg
}

// getEnv получает значение из переменной окружения или возвращает значение по умолчанию
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// getEnvAsInt получает значение из переменной окружения как int или возвращает значение по умолчанию
func getEnvAsInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	intValue := defaultValue
	_, err := fmt.Sscanf(value, "%d", &intValue)
	if err != nil {
		return defaultValue
	}

	return intValue
}
