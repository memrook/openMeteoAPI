package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"openMeteoAPI/pkg/api"
	"openMeteoAPI/pkg/config"
	"openMeteoAPI/pkg/database"
)

// Определение сенсоров для OpenMeteo API
var openMeteoSensors = []string{
	"openmeteo_temperature",
	"openmeteo_humidity",
	"openmeteo_rain",
	"openmeteo_wind_speed",
	"openmeteo_wind_direction",
	"openmeteo_precipitation",
	"openmeteo_soil_temp_0_7",
	"openmeteo_soil_temp_7_28",
	"openmeteo_soil_moisture_0_7",
	"openmeteo_soil_moisture_7_28",
}

func main() {
	// Загружаем конфигурацию
	cfg := config.LoadConfig()

	// Инициализируем API клиент OpenMeteo
	openMeteoAPI := api.NewOpenMeteoAPI(cfg)

	// Инициализируем менеджер БД
	dbManager, err := database.NewDBManager(cfg)
	if err != nil {
		log.Fatalf("Ошибка при подключении к БД: %v", err)
	}
	defer dbManager.Close()

	// Создаем таблицы, если они не существуют
	if err := dbManager.CreateTablesIfNotExists(); err != nil {
		log.Fatalf("Ошибка при создании таблиц: %v", err)
	}

	// Канал для остановки сервиса
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	// Запускаем регулярный сбор данных в отдельной горутине
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Запускаем первый сбор данных немедленно
		collectData(openMeteoAPI, dbManager, cfg)

		// Настраиваем периодический запуск
		ticker := time.NewTicker(time.Duration(cfg.CollectionInterval) * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				collectData(openMeteoAPI, dbManager, cfg)
			case <-stopChan:
				log.Println("Получен сигнал остановки. Завершаем работу...")
				return
			}
		}
	}()

	// Ожидаем сигнал остановки
	<-stopChan
	log.Println("Ожидаем завершения всех задач...")
	wg.Wait()
	log.Println("Сервис остановлен")
}

// collectData выполняет сбор данных со всех метеостанций и их сохранение в БД
func collectData(openMeteoAPI *api.OpenMeteoAPI, dbManager *database.DBManager, cfg *config.Config) {
	log.Println("Начинаем сбор данных из Archive OpenMeteo API...")

	// Получаем список всех координат метеостанций
	stations, err := dbManager.GetAllStationCoordinates()
	if err != nil {
		log.Printf("Ошибка при получении координат станций: %v", err)
		return
	}

	log.Printf("Найдено станций с координатами: %d", len(stations))

	// Если нет станций с координатами, завершаем работу
	if len(stations) == 0 {
		log.Println("Не найдено станций с координатами. Завершаем работу.")
		return
	}

	// Обрабатываем каждую станцию
	for _, station := range stations {
		processStation(openMeteoAPI, dbManager, station, cfg.PastDays)
	}

	log.Println("Сбор данных из Archive OpenMeteo API завершен")
}

// processStation обрабатывает отдельную станцию
func processStation(openMeteoAPI *api.OpenMeteoAPI, dbManager *database.DBManager, station struct {
	ID        string
	Label     string
	Latitude  float64
	Longitude float64
}, pastDays int) {
	stationLabel := station.Label
	if stationLabel == "" {
		stationLabel = station.ID
	}

	log.Printf("Обрабатываем станцию: %s (ID: %s, координаты: %.6f, %.6f)",
		stationLabel, station.ID, station.Latitude, station.Longitude)

	// Проверяем наличие исторических данных для каждого сенсора
	var existingSensors []string
	var newSensors []string
	var minTimestamp int64 = time.Now().UnixNano() / int64(time.Millisecond)

	for _, sensor := range openMeteoSensors {
		// Получаем время последней записи из базы данных
		lastTs, err := dbManager.GetLatestTelemetryTimestamp(station.ID, sensor)
		if err != nil {
			log.Printf("Ошибка при получении последнего timestamp для %s-%s: %v", station.ID, sensor, err)
			// Если ошибка, считаем, что данных нет
			newSensors = append(newSensors, sensor)
			continue
		}

		// Проверяем, есть ли для этого датчика данные в базе
		if lastTs > 0 {
			existingSensors = append(existingSensors, sensor)

			// Определяем минимальный timestamp для всех существующих датчиков
			if lastTs < minTimestamp {
				minTimestamp = lastTs
			}
		} else {
			// Датчик есть в списке, но данных по нему нет
			newSensors = append(newSensors, sensor)
		}
	}

	// Если у нас нет исторических данных, загружаем за предыдущие N дней
	if len(newSensors) > 0 {
		log.Printf("Для станции %s запрашиваем архивные данные за %d дней", station.ID, pastDays)

		// Получаем данные из OpenMeteo API
		weatherData, err := openMeteoAPI.GetWeatherData(station.Latitude, station.Longitude, pastDays)
		if err != nil {
			log.Printf("Ошибка при получении данных из Archive OpenMeteo API для станции %s: %v", station.ID, err)
			return
		}

		// Преобразуем данные в формат телеметрии и сохраняем в базе
		telemetryMap := openMeteoAPI.ConvertToTelemetryMap(weatherData)

		// Сохраняем телеметрию в базу данных
		if err := dbManager.StoreTelemetry(station.ID, telemetryMap); err != nil {
			log.Printf("Ошибка при сохранении телеметрии для станции %s: %v", station.ID, err)
			return
		}

		log.Printf("Архивные данные для станции %s успешно сохранены", station.ID)
	} else if len(existingSensors) > 0 {
		// Если у нас уже есть исторические данные, загружаем только за последний день
		log.Printf("Для станции %s запрашиваем новые данные за последний день", station.ID)

		// Получаем данные из OpenMeteo API только за последний день
		weatherData, err := openMeteoAPI.GetWeatherData(station.Latitude, station.Longitude, 1)
		if err != nil {
			log.Printf("Ошибка при получении данных из Archive OpenMeteo API для станции %s: %v", station.ID, err)
			return
		}

		// Преобразуем данные в формат телеметрии
		telemetryMap := openMeteoAPI.ConvertToTelemetryMap(weatherData)

		// Фильтруем только новые данные
		filteredTelemetryMap := make(map[string][]api.TelemetryPoint)
		for key, points := range telemetryMap {
			var filteredPoints []api.TelemetryPoint
			for _, point := range points {
				if point.Ts > minTimestamp {
					filteredPoints = append(filteredPoints, point)
				}
			}
			if len(filteredPoints) > 0 {
				filteredTelemetryMap[key] = filteredPoints
			}
		}

		// Если есть новые данные, сохраняем их в базу
		if len(filteredTelemetryMap) > 0 {
			// Сохраняем телеметрию в базу данных
			if err := dbManager.StoreTelemetry(station.ID, filteredTelemetryMap); err != nil {
				log.Printf("Ошибка при сохранении новой телеметрии для станции %s: %v", station.ID, err)
				return
			}

			log.Printf("Новые данные для станции %s успешно сохранены", station.ID)
		} else {
			log.Printf("Для станции %s нет новых данных", station.ID)
		}
	}
}
