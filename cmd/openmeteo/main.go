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

// Определение сенсоров архива для OpenMeteo API
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

// Определение сенсоров для прогноза OpenMeteo
var forecastSensors = []string{
	"forecast_temperature",
	"forecast_humidity",
	"forecast_precipitation",
	"forecast_precipitation_probability",
	"forecast_wind_speed",
	"forecast_wind_direction",
	"forecast_wind_gusts",
	"forecast_dew_point",
	"forecast_soil_temp_18cm",
	"forecast_soil_temp_6cm",
	"forecast_soil_temp_0cm",
	"forecast_soil_moisture_1_3cm",
	"forecast_soil_moisture_3_9cm",
	"forecast_soil_moisture_9_27cm",
	"forecast_daily_precipitation_sum",
	"forecast_daily_wind_direction",
	"forecast_daily_temp_min",
	"forecast_daily_temp_max",
	"forecast_daily_wind_gusts_max",
	"forecast_daily_precipitation_hours",
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

	// Создаем группу ожидания для параллельной обработки
	var wg sync.WaitGroup

	// Параллельно обрабатываем архивные данные и прогноз
	wg.Add(2)

	// Обрабатываем каждую станцию для архивных данных
	go func() {
		defer wg.Done()
		for _, station := range stations {
			processStation(openMeteoAPI, dbManager, station, cfg.PastDays)
		}
		log.Println("Сбор архивных данных из Archive OpenMeteo API завершен")
	}()

	// Параллельно обрабатываем прогноз погоды
	go func() {
		defer wg.Done()
		for _, station := range stations {
			processForecast(openMeteoAPI, dbManager, station, cfg.ForecastPastDays)
		}
		log.Println("Сбор данных прогноза из OpenMeteo Forecast API завершен")
	}()

	// Ожидаем завершения всех горутин
	wg.Wait()
	log.Println("Сбор всех данных из OpenMeteo API завершен")
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

	// Проверяем наличие данных для каждого сенсора
	var noDataSensors []string
	sensorLastTimes := make(map[string]time.Time)
	now := time.Now()

	// Старшая дата последней записи среди всех сенсоров
	var latestRecordTime time.Time

	// Получаем последние записи для каждого сенсора
	for _, sensor := range openMeteoSensors {
		// Получаем время последней записи из базы данных
		lastTs, err := dbManager.GetLatestTelemetryTimestamp(station.ID, sensor)
		if err != nil {
			log.Printf("Ошибка при получении последнего timestamp для %s-%s: %v", station.ID, sensor, err)
			noDataSensors = append(noDataSensors, sensor)
			continue
		}

		// Проверяем, есть ли для этого датчика данные в базе
		if lastTs > 0 {
			// Преобразуем timestamp в time.Time
			lastTime := time.Unix(0, lastTs*int64(time.Millisecond))
			sensorLastTimes[sensor] = lastTime

			// Обновляем latestRecordTime, если текущая запись новее
			if lastTime.After(latestRecordTime) {
				latestRecordTime = lastTime
			}
		} else {
			// Датчик есть в списке, но данных по нему нет
			noDataSensors = append(noDataSensors, sensor)
		}
	}

	// Если нет ни одной записи ни по одному сенсору - загружаем данные за весь период (pastDays)
	if len(sensorLastTimes) == 0 {
		log.Printf("Для станции %s нет данных, запрашиваем архивные данные за %d дней", station.ID, pastDays)

		// Получаем данные из OpenMeteo API за весь период
		weatherData, err := openMeteoAPI.GetWeatherData(station.Latitude, station.Longitude, pastDays)
		if err != nil {
			log.Printf("Ошибка при получении данных из Archive OpenMeteo API для станции %s: %v", station.ID, err)
			return
		}

		// Сохраняем все данные
		processTelemetryData(openMeteoAPI, dbManager, station.ID, weatherData, "Архивные данные")
		return
	}

	// Если у нас есть хотя бы одна запись, загружаем данные только с момента последней записи
	// Добавим 1 час к последней записи во избежание дублирования
	startDate := latestRecordTime.Add(time.Hour)

	// Вычисляем разницу между последней записью и текущим временем в днях
	daysDiff := int(now.Sub(startDate).Hours()/24) + 1 // +1 день для надежности

	// Если с момента последней записи прошло менее 1 дня, нет смысла загружать новые данные
	if daysDiff < 1 {
		log.Printf("Для станции %s последние данные были получены менее 1 дня назад (%s), пропускаем обновление",
			station.ID, latestRecordTime.Format("2006-01-02 15:04:05"))
		return
	}

	log.Printf("Для станции %s запрашиваем данные с %s по %s (%d дней)",
		station.ID,
		startDate.Format("2006-01-02"),
		now.Format("2006-01-02"),
		daysDiff)

	// Получаем данные из OpenMeteo API за период с последней записи до текущего времени
	weatherData, err := openMeteoAPI.GetWeatherDataByDates(station.Latitude, station.Longitude, startDate, now)
	if err != nil {
		log.Printf("Ошибка при получении данных из Archive OpenMeteo API для станции %s: %v", station.ID, err)
		return
	}

	// Сохраняем новые данные
	processTelemetryData(openMeteoAPI, dbManager, station.ID, weatherData, "Новые данные")

	// Если есть сенсоры без данных - для них загружаем полную историю отдельно
	if len(noDataSensors) > 0 {
		log.Printf("Для станции %s запрашиваем полную историю для сенсоров без данных: %v",
			station.ID, noDataSensors)

		// Здесь мы уже загрузили данные за период от последней записи до текущего времени
		// Теперь нужно загрузить исторические данные за весь период для сенсоров без записей
		historicalStartDate := now.AddDate(0, 0, -pastDays)

		// Запрашиваем исторические данные от (now - pastDays) до startDate
		if historicalStartDate.Before(startDate) {
			weatherData, err = openMeteoAPI.GetWeatherDataByDates(
				station.Latitude, station.Longitude, historicalStartDate, startDate)
			if err != nil {
				log.Printf("Ошибка при получении исторических данных для станции %s: %v", station.ID, err)
				return
			}

			// Сохраняем исторические данные
			processTelemetryData(openMeteoAPI, dbManager, station.ID, weatherData, "Исторические данные")
		}
	}
}

// processForecast получает и сохраняет данные прогноза погоды для станции
func processForecast(openMeteoAPI *api.OpenMeteoAPI, dbManager *database.DBManager, station struct {
	ID        string
	Label     string
	Latitude  float64
	Longitude float64
}, pastDays int) {
	stationLabel := station.Label
	if stationLabel == "" {
		stationLabel = station.ID
	}

	log.Printf("Обрабатываем прогноз для станции: %s (ID: %s, координаты: %.6f, %.6f)",
		stationLabel, station.ID, station.Latitude, station.Longitude)

	// Проверяем, когда последний раз обновлялись данные прогноза
	var latestUpdateTime time.Time
	for _, sensor := range forecastSensors {
		lastTs, err := dbManager.GetLatestForecastTimestamp(station.ID, sensor)
		if err != nil {
			log.Printf("Ошибка при получении последнего timestamp прогноза для %s-%s: %v", station.ID, sensor, err)
			continue
		}

		if lastTs > 0 {
			lastTime := time.Unix(0, lastTs*int64(time.Millisecond))
			if lastTime.After(latestUpdateTime) {
				latestUpdateTime = lastTime
			}
		}
	}

	// Если прогноз обновлялся менее 3 часов назад, пропускаем обновление
	if !latestUpdateTime.IsZero() && time.Since(latestUpdateTime) < 3*time.Hour {
		log.Printf("Для станции %s прогноз обновлялся менее 3 часов назад (%s), пропускаем обновление",
			station.ID, latestUpdateTime.Format("2006-01-02 15:04:05"))
		return
	}

	// Получаем данные прогноза
	log.Printf("Запрашиваем данные прогноза для станции %s с прошлыми данными за %d дней",
		station.ID, pastDays)

	forecastData, err := openMeteoAPI.GetWeatherForecast(station.Latitude, station.Longitude, pastDays)
	if err != nil {
		log.Printf("Ошибка при получении прогноза для станции %s: %v", station.ID, err)
		return
	}

	// Преобразуем данные в формат телеметрии
	telemetryMap := openMeteoAPI.ConvertForecastToTelemetryMap(forecastData)

	// Подсчитываем общее количество точек данных для логирования
	totalPoints := 0
	for _, points := range telemetryMap {
		totalPoints += len(points)
	}

	// Если данных нет, выходим
	if totalPoints == 0 {
		log.Printf("Для станции %s не получено данных прогноза", station.ID)
		return
	}

	// Сохраняем данные прогноза в базу данных
	if err := dbManager.StoreForecast(station.ID, telemetryMap); err != nil {
		log.Printf("Ошибка при сохранении прогноза для станции %s: %v", station.ID, err)
		return
	}

	log.Printf("Данные прогноза для станции %s успешно сохранены: %d точек данных",
		station.ID, totalPoints)
}

// processTelemetryData обрабатывает и сохраняет данные телеметрии
func processTelemetryData(openMeteoAPI *api.OpenMeteoAPI, dbManager *database.DBManager, stationID string, weatherData *api.WeatherData, logPrefix string) {
	// Преобразуем данные в формат телеметрии
	telemetryMap := openMeteoAPI.ConvertToTelemetryMap(weatherData)

	// Подсчитываем общее количество точек данных для логирования
	totalPoints := 0
	for _, points := range telemetryMap {
		totalPoints += len(points)
	}

	// Если данных нет, выходим
	if totalPoints == 0 {
		log.Printf("%s для станции %s: нет новых данных", logPrefix, stationID)
		return
	}

	// Сохраняем телеметрию в базу данных
	if err := dbManager.StoreTelemetry(stationID, telemetryMap); err != nil {
		log.Printf("Ошибка при сохранении телеметрии для станции %s: %v", stationID, err)
		return
	}

	log.Printf("%s для станции %s успешно сохранены: %d точек данных",
		logPrefix, stationID, totalPoints)
}
