package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"openMeteoAPI/pkg/api"
	"openMeteoAPI/pkg/config"

	_ "github.com/denisenkom/go-mssqldb"
)

// DBManager представляет собой менеджер для работы с базой данных
type DBManager struct {
	Config *config.Config
	DB     *sql.DB
}

// NewDBManager создает новый экземпляр менеджера БД
func NewDBManager(cfg *config.Config) (*DBManager, error) {
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s",
		cfg.DbServer, cfg.DbLogin, cfg.DbPassword, cfg.DbName)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, fmt.Errorf("ошибка подключения к базе данных: %w", err)
	}

	// Проверка соединения
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ошибка при проверке соединения с базой данных: %w", err)
	}

	// Установка параметров пула соединений
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)

	return &DBManager{
		Config: cfg,
		DB:     db,
	}, nil
}

// Close закрывает соединение с базой данных
func (d *DBManager) Close() error {
	return d.DB.Close()
}

// CreateTablesIfNotExists создает необходимые таблицы, если они не существуют
func (d *DBManager) CreateTablesIfNotExists() error {
	// Создаем таблицу для телеметрии OpenMeteo
	_, err := d.DB.Exec(`
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='OpenMeteoTelemetry' AND xtype='U')
	CREATE TABLE OpenMeteoTelemetry (
		ID INT IDENTITY(1,1) PRIMARY KEY,
		StationID NVARCHAR(100) NOT NULL,
		SensorKey NVARCHAR(100) NOT NULL,
		Timestamp BIGINT NOT NULL,
		DateValue DATETIME NOT NULL,
		Value FLOAT,
		CONSTRAINT UQ_OpenMeteoTelemetry_Station_Sensor_Date UNIQUE (StationID, SensorKey, Timestamp)
	)
	`)
	if err != nil {
		return fmt.Errorf("ошибка при создании таблицы OpenMeteoTelemetry: %w", err)
	}

	// Создаем таблицу для прогноза погоды OpenMeteo
	_, err = d.DB.Exec(`
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='OpenMeteoForecast' AND xtype='U')
	CREATE TABLE OpenMeteoForecast (
		ID INT IDENTITY(1,1) PRIMARY KEY,
		StationID NVARCHAR(100) NOT NULL,
		SensorKey NVARCHAR(100) NOT NULL,
		Timestamp BIGINT NOT NULL,
		DateValue DATETIME NOT NULL,
		Value FLOAT,
		CONSTRAINT UQ_OpenMeteoForecast_Station_Sensor_Date UNIQUE (StationID, SensorKey, Timestamp)
	)
	`)
	if err != nil {
		return fmt.Errorf("ошибка при создании таблицы OpenMeteoForecast: %w", err)
	}

	// Создаем индексы для быстрого поиска в таблице телеметрии
	_, err = d.DB.Exec(`
	IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_OpenMeteoTelemetry_StationID_SensorKey_Timestamp' AND object_id = OBJECT_ID('OpenMeteoTelemetry'))
	CREATE INDEX IX_OpenMeteoTelemetry_StationID_SensorKey_Timestamp ON OpenMeteoTelemetry (StationID, SensorKey, Timestamp)
	`)
	if err != nil {
		return fmt.Errorf("ошибка при создании индекса: %w", err)
	}

	_, err = d.DB.Exec(`
	IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_OpenMeteoTelemetry_DateValue' AND object_id = OBJECT_ID('OpenMeteoTelemetry'))
	CREATE INDEX IX_OpenMeteoTelemetry_DateValue ON OpenMeteoTelemetry (DateValue)
	`)
	if err != nil {
		return fmt.Errorf("ошибка при создании индекса: %w", err)
	}

	// Создаем индексы для быстрого поиска в таблице прогноза
	_, err = d.DB.Exec(`
	IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_OpenMeteoForecast_StationID_SensorKey_Timestamp' AND object_id = OBJECT_ID('OpenMeteoForecast'))
	CREATE INDEX IX_OpenMeteoForecast_StationID_SensorKey_Timestamp ON OpenMeteoForecast (StationID, SensorKey, Timestamp)
	`)
	if err != nil {
		return fmt.Errorf("ошибка при создании индекса: %w", err)
	}

	_, err = d.DB.Exec(`
	IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_OpenMeteoForecast_DateValue' AND object_id = OBJECT_ID('OpenMeteoForecast'))
	CREATE INDEX IX_OpenMeteoForecast_DateValue ON OpenMeteoForecast (DateValue)
	`)
	if err != nil {
		return fmt.Errorf("ошибка при создании индекса: %w", err)
	}

	return nil
}

// GetAllStationCoordinates получает все координаты метеостанций из таблицы Stations
func (d *DBManager) GetAllStationCoordinates() ([]struct {
	ID        string
	Label     string
	Latitude  float64
	Longitude float64
}, error) {
	rows, err := d.DB.Query(`
	SELECT ID, Label, Latitude, Longitude 
	FROM Stations 
	WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
	`)
	if err != nil {
		return nil, fmt.Errorf("ошибка при получении координат станций: %w", err)
	}
	defer rows.Close()

	var result []struct {
		ID        string
		Label     string
		Latitude  float64
		Longitude float64
	}

	for rows.Next() {
		var station struct {
			ID        string
			Label     string
			Latitude  float64
			Longitude float64
		}
		if err := rows.Scan(&station.ID, &station.Label, &station.Latitude, &station.Longitude); err != nil {
			return nil, fmt.Errorf("ошибка при чтении данных станции: %w", err)
		}
		result = append(result, station)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при обработке результатов запроса: %w", err)
	}

	return result, nil
}

// StoreTelemetry сохраняет телеметрию в базу данных
func (d *DBManager) StoreTelemetry(stationID string, data map[string][]api.TelemetryPoint) error {
	// Объединим все точки данных в один массив для обработки по пакетам
	var allPoints []struct {
		SensorKey      string
		TelemetryPoint api.TelemetryPoint
	}

	for sensorKey, points := range data {
		for _, point := range points {
			allPoints = append(allPoints, struct {
				SensorKey      string
				TelemetryPoint api.TelemetryPoint
			}{
				SensorKey:      sensorKey,
				TelemetryPoint: point,
			})
		}
	}

	// Размер пакета (чанка) для обработки
	const batchSize = 200
	totalBatches := (len(allPoints) + batchSize - 1) / batchSize

	// Если есть несколько пакетов, выводим информацию
	if totalBatches > 1 {
		log.Printf("Разбиваем %d записей на %d пакетов по %d записей", len(allPoints), totalBatches, batchSize)
	}

	// Обрабатываем данные пакетами
	for i := 0; i < len(allPoints); i += batchSize {
		end := i + batchSize
		if end > len(allPoints) {
			end = len(allPoints)
		}

		// Обрабатываем текущий пакет
		currentBatch := allPoints[i:end]
		batchNum := (i / batchSize) + 1

		// Если много пакетов, выводим информацию о текущем пакете
		if totalBatches > 1 && batchNum%5 == 0 {
			log.Printf("Сохранение пакета %d из %d (%.1f%%)",
				batchNum,
				totalBatches,
				float64(batchNum)/float64(totalBatches)*100)
		}

		if err := d.storeTelemetryBatch(stationID, currentBatch); err != nil {
			return fmt.Errorf("ошибка при сохранении пакета данных телеметрии %d из %d (%d-%d): %w",
				batchNum, totalBatches, i, end, err)
		}
	}

	// Если было несколько пакетов, выводим информацию о завершении
	if totalBatches > 1 {
		log.Printf("Все %d пакетов успешно сохранены в базу данных", totalBatches)
	}

	return nil
}

// storeTelemetryBatch сохраняет пакет данных телеметрии в базу данных
func (d *DBManager) storeTelemetryBatch(stationID string, batch []struct {
	SensorKey      string
	TelemetryPoint api.TelemetryPoint
}) error {
	// Если пакет пустой, ничего не делаем
	if len(batch) == 0 {
		return nil
	}

	// Начинаем транзакцию
	tx, err := d.DB.Begin()
	if err != nil {
		return fmt.Errorf("ошибка при начале транзакции: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // паника после отката
		}
	}()

	// Подготавливаем запрос на вставку с обработкой дубликатов
	stmt, err := tx.Prepare(`
	MERGE INTO OpenMeteoTelemetry AS target
	USING (VALUES (@StationID, @SensorKey, @Timestamp, @DateValue, @Value)) 
	    AS source (StationID, SensorKey, Timestamp, DateValue, Value)
	ON target.StationID = source.StationID
	    AND target.SensorKey = source.SensorKey
	    AND target.Timestamp = source.Timestamp
	WHEN MATCHED THEN
	    UPDATE SET Value = source.Value
	WHEN NOT MATCHED THEN
	    INSERT (StationID, SensorKey, Timestamp, DateValue, Value)
	    VALUES (source.StationID, source.SensorKey, source.Timestamp, source.DateValue, source.Value);
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("ошибка при подготовке запроса: %w", err)
	}
	defer stmt.Close()

	// Вставляем каждую точку данных
	for _, point := range batch {
		// Преобразуем timestamp в дату/время
		dateValue := time.Unix(0, point.TelemetryPoint.Ts*int64(time.Millisecond))

		// Убеждаемся, что значение можно преобразовать в float64
		var value float64
		switch v := point.TelemetryPoint.Value.(type) {
		case float64:
			value = v
		case float32:
			value = float64(v)
		case int:
			value = float64(v)
		case int64:
			value = float64(v)
		default:
			// Если невозможно преобразовать, пропускаем запись
			log.Printf("Невозможно преобразовать значение для %s-%s: %v (тип: %T)",
				stationID, point.SensorKey, point.TelemetryPoint.Value, point.TelemetryPoint.Value)
			continue
		}

		_, err := stmt.Exec(
			sql.Named("StationID", stationID),
			sql.Named("SensorKey", point.SensorKey),
			sql.Named("Timestamp", point.TelemetryPoint.Ts),
			sql.Named("DateValue", dateValue),
			sql.Named("Value", value),
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("ошибка при вставке телеметрии: %w", err)
		}
	}

	// Коммитим транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("ошибка при коммите транзакции: %w", err)
	}

	return nil
}

// GetLatestTelemetryTimestamp получает время последней записи для станции и датчика
func (d *DBManager) GetLatestTelemetryTimestamp(stationID, sensorKey string) (int64, error) {
	var timestamp sql.NullInt64

	err := d.DB.QueryRow(`
	SELECT MAX(Timestamp) 
	FROM OpenMeteoTelemetry 
	WHERE StationID = @StationID AND SensorKey = @SensorKey
	`, sql.Named("StationID", stationID), sql.Named("SensorKey", sensorKey)).Scan(&timestamp)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil // Данных нет, возвращаем 0
		}
		return 0, fmt.Errorf("ошибка при получении последнего timestamp: %w", err)
	}

	if !timestamp.Valid {
		return 0, nil // Значение NULL, возвращаем 0
	}

	return timestamp.Int64, nil
}

// StoreForecast сохраняет данные прогноза в базу данных
func (d *DBManager) StoreForecast(stationID string, data map[string][]api.TelemetryPoint) error {
	// Объединим все точки данных в один массив для обработки по пакетам
	var allPoints []struct {
		SensorKey      string
		TelemetryPoint api.TelemetryPoint
	}

	for sensorKey, points := range data {
		for _, point := range points {
			allPoints = append(allPoints, struct {
				SensorKey      string
				TelemetryPoint api.TelemetryPoint
			}{
				SensorKey:      sensorKey,
				TelemetryPoint: point,
			})
		}
	}

	// Размер пакета (чанка) для обработки
	const batchSize = 200
	totalBatches := (len(allPoints) + batchSize - 1) / batchSize

	// Если есть несколько пакетов, выводим информацию
	if totalBatches > 1 {
		log.Printf("Разбиваем %d записей прогноза на %d пакетов по %d записей", len(allPoints), totalBatches, batchSize)
	}

	// Обрабатываем данные пакетами
	for i := 0; i < len(allPoints); i += batchSize {
		end := i + batchSize
		if end > len(allPoints) {
			end = len(allPoints)
		}

		// Обрабатываем текущий пакет
		currentBatch := allPoints[i:end]
		batchNum := (i / batchSize) + 1

		// Если много пакетов, выводим информацию о текущем пакете
		if totalBatches > 1 && batchNum%5 == 0 {
			log.Printf("Сохранение пакета прогноза %d из %d (%.1f%%)",
				batchNum,
				totalBatches,
				float64(batchNum)/float64(totalBatches)*100)
		}

		if err := d.storeForecastBatch(stationID, currentBatch); err != nil {
			return fmt.Errorf("ошибка при сохранении пакета данных прогноза %d из %d (%d-%d): %w",
				batchNum, totalBatches, i, end, err)
		}
	}

	// Если было несколько пакетов, выводим информацию о завершении
	if totalBatches > 1 {
		log.Printf("Все %d пакетов прогноза успешно сохранены в базу данных", totalBatches)
	}

	return nil
}

// storeForecastBatch сохраняет пакет данных прогноза в базу данных
func (d *DBManager) storeForecastBatch(stationID string, batch []struct {
	SensorKey      string
	TelemetryPoint api.TelemetryPoint
}) error {
	// Если пакет пустой, ничего не делаем
	if len(batch) == 0 {
		return nil
	}

	// Начинаем транзакцию
	tx, err := d.DB.Begin()
	if err != nil {
		return fmt.Errorf("ошибка при начале транзакции: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // паника после отката
		}
	}()

	// Подготавливаем запрос на вставку с обработкой дубликатов
	stmt, err := tx.Prepare(`
	MERGE INTO OpenMeteoForecast AS target
	USING (VALUES (@StationID, @SensorKey, @Timestamp, @DateValue, @Value)) 
	    AS source (StationID, SensorKey, Timestamp, DateValue, Value)
	ON target.StationID = source.StationID
	    AND target.SensorKey = source.SensorKey
	    AND target.Timestamp = source.Timestamp
	WHEN MATCHED THEN
	    UPDATE SET Value = source.Value
	WHEN NOT MATCHED THEN
	    INSERT (StationID, SensorKey, Timestamp, DateValue, Value)
	    VALUES (source.StationID, source.SensorKey, source.Timestamp, source.DateValue, source.Value);
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("ошибка при подготовке запроса: %w", err)
	}
	defer stmt.Close()

	// Вставляем каждую точку данных
	for _, point := range batch {
		// Преобразуем timestamp в дату/время
		dateValue := time.Unix(0, point.TelemetryPoint.Ts*int64(time.Millisecond))

		// Убеждаемся, что значение можно преобразовать в float64
		var value float64
		switch v := point.TelemetryPoint.Value.(type) {
		case float64:
			value = v
		case float32:
			value = float64(v)
		case int:
			value = float64(v)
		case int64:
			value = float64(v)
		default:
			return fmt.Errorf("неподдерживаемый тип значения: %T", point.TelemetryPoint.Value)
		}

		_, err = stmt.Exec(
			sql.Named("StationID", stationID),
			sql.Named("SensorKey", point.SensorKey),
			sql.Named("Timestamp", point.TelemetryPoint.Ts),
			sql.Named("DateValue", dateValue),
			sql.Named("Value", value),
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("ошибка при выполнении запроса: %w", err)
		}
	}

	// Завершаем транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("ошибка при завершении транзакции: %w", err)
	}

	return nil
}

// GetLatestForecastTimestamp получает последний timestamp для указанного датчика станции
func (d *DBManager) GetLatestForecastTimestamp(stationID, sensorKey string) (int64, error) {
	var lastTimestamp sql.NullInt64
	err := d.DB.QueryRow(`
	SELECT TOP 1 Timestamp
	FROM OpenMeteoForecast
	WHERE StationID = @StationID AND SensorKey = @SensorKey
	ORDER BY Timestamp DESC
	`, sql.Named("StationID", stationID), sql.Named("SensorKey", sensorKey)).Scan(&lastTimestamp)

	if err != nil {
		if err == sql.ErrNoRows {
			// Если нет записей, возвращаем 0
			return 0, nil
		}
		return 0, err
	}

	if !lastTimestamp.Valid {
		// Если значение NULL, возвращаем 0
		return 0, nil
	}

	return lastTimestamp.Int64, nil
}
