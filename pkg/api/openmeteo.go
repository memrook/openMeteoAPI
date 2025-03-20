package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"openMeteoAPI/pkg/config"
)

// OpenMeteoAPI представляет API клиент для работы с Open-Meteo API
type OpenMeteoAPI struct {
	Config  *config.Config
	Client  *http.Client
	BaseURL string
}

// WeatherData представляет структуру ответа API Open-Meteo
type WeatherData struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timezone  string  `json:"timezone"`
	Hourly    struct {
		Time                   []string  `json:"time"`
		Temperature2m          []float64 `json:"temperature_2m"`
		RelativeHumidity2m     []float64 `json:"relative_humidity_2m"`
		Rain                   []float64 `json:"rain"`
		WindSpeed10m           []float64 `json:"wind_speed_10m"`
		SoilTemperature0to7cm  []float64 `json:"soil_temperature_0_to_7cm"`
		SoilTemperature7to28cm []float64 `json:"soil_temperature_7_to_28cm"`
		SoilMoisture0to7cm     []float64 `json:"soil_moisture_0_to_7cm"`
		SoilMoisture7to28cm    []float64 `json:"soil_moisture_7_to_28cm"`
		WindDirection10m       []float64 `json:"wind_direction_10m"`
		Precipitation          []float64 `json:"precipitation"`
	} `json:"hourly"`
	HourlyUnits struct {
		Temperature2m          string `json:"temperature_2m"`
		RelativeHumidity2m     string `json:"relative_humidity_2m"`
		Rain                   string `json:"rain"`
		WindSpeed10m           string `json:"wind_speed_10m"`
		SoilTemperature0to7cm  string `json:"soil_temperature_0_to_7cm"`
		SoilTemperature7to28cm string `json:"soil_temperature_7_to_28cm"`
		SoilMoisture0to7cm     string `json:"soil_moisture_0_to_7cm"`
		SoilMoisture7to28cm    string `json:"soil_moisture_7_to_28cm"`
		WindDirection10m       string `json:"wind_direction_10m"`
		Precipitation          string `json:"precipitation"`
	} `json:"hourly_units"`
}

// TelemetryPoint представляет точку данных телеметрии
type TelemetryPoint struct {
	Ts    int64       `json:"ts"`
	Value interface{} `json:"value"`
}

// NewOpenMeteoAPI создает новый экземпляр API клиента Open-Meteo
func NewOpenMeteoAPI(cfg *config.Config) *OpenMeteoAPI {
	return &OpenMeteoAPI{
		Config:  cfg,
		Client:  &http.Client{Timeout: 30 * time.Second},
		BaseURL: "https://archive-api.open-meteo.com/v1/archive",
	}
}

// GetWeatherData получает данные о погоде для указанных координат за указанный период
func (o *OpenMeteoAPI) GetWeatherData(latitude, longitude float64, pastDays int) (*WeatherData, error) {
	// Вычисляем даты начала и конца периода
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -pastDays)

	params := url.Values{}
	params.Add("latitude", strconv.FormatFloat(latitude, 'f', 6, 64))
	params.Add("longitude", strconv.FormatFloat(longitude, 'f', 6, 64))
	params.Add("start_date", startDate.Format("2006-01-02"))
	params.Add("end_date", endDate.Format("2006-01-02"))
	params.Add("hourly", "temperature_2m,relative_humidity_2m,rain,wind_speed_10m,soil_temperature_0_to_7cm,soil_temperature_7_to_28cm,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm,wind_direction_10m,precipitation")
	params.Add("timezone", "Europe/Moscow")

	requestURL := fmt.Sprintf("%s?%s", o.BaseURL, params.Encode())

	resp, err := o.Client.Get(requestURL)
	if err != nil {
		return nil, fmt.Errorf("ошибка при выполнении запроса: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ошибка API: статус %d", resp.StatusCode)
	}

	var weatherData WeatherData
	if err := json.NewDecoder(resp.Body).Decode(&weatherData); err != nil {
		return nil, fmt.Errorf("ошибка при десериализации ответа: %w", err)
	}

	return &weatherData, nil
}

// ConvertToTelemetryMap преобразует данные от Open-Meteo в формат телеметрии
func (o *OpenMeteoAPI) ConvertToTelemetryMap(data *WeatherData) map[string][]TelemetryPoint {
	result := make(map[string][]TelemetryPoint)

	for i, timeStr := range data.Hourly.Time {
		// Парсим время в формате ISO 8601
		t, err := time.Parse("2006-01-02T15:04", timeStr)
		if err != nil {
			fmt.Printf("Ошибка при парсинге времени %s: %v\n", timeStr, err)
			continue
		}

		// Переводим время в миллисекунды
		ts := t.UnixNano() / int64(time.Millisecond)

		// Добавляем данные по каждому показателю
		if i < len(data.Hourly.Temperature2m) {
			addPointToMap(result, "openmeteo_temperature", ts, data.Hourly.Temperature2m[i])
		}
		if i < len(data.Hourly.RelativeHumidity2m) {
			addPointToMap(result, "openmeteo_humidity", ts, data.Hourly.RelativeHumidity2m[i])
		}
		if i < len(data.Hourly.Rain) {
			addPointToMap(result, "openmeteo_rain", ts, data.Hourly.Rain[i])
		}
		if i < len(data.Hourly.WindSpeed10m) {
			addPointToMap(result, "openmeteo_wind_speed", ts, data.Hourly.WindSpeed10m[i])
		}
		if i < len(data.Hourly.WindDirection10m) {
			addPointToMap(result, "openmeteo_wind_direction", ts, data.Hourly.WindDirection10m[i])
		}
		if i < len(data.Hourly.Precipitation) {
			addPointToMap(result, "openmeteo_precipitation", ts, data.Hourly.Precipitation[i])
		}
		if i < len(data.Hourly.SoilTemperature0to7cm) {
			addPointToMap(result, "openmeteo_soil_temp_0_7", ts, data.Hourly.SoilTemperature0to7cm[i])
		}
		if i < len(data.Hourly.SoilTemperature7to28cm) {
			addPointToMap(result, "openmeteo_soil_temp_7_28", ts, data.Hourly.SoilTemperature7to28cm[i])
		}
		if i < len(data.Hourly.SoilMoisture0to7cm) {
			addPointToMap(result, "openmeteo_soil_moisture_0_7", ts, data.Hourly.SoilMoisture0to7cm[i])
		}
		if i < len(data.Hourly.SoilMoisture7to28cm) {
			addPointToMap(result, "openmeteo_soil_moisture_7_28", ts, data.Hourly.SoilMoisture7to28cm[i])
		}
	}

	return result
}

// addPointToMap добавляет точку данных в карту телеметрии
func addPointToMap(m map[string][]TelemetryPoint, key string, ts int64, value float64) {
	// Пропускаем нулевые значения, которые могут означать отсутствие данных
	if value == 0 {
		return
	}

	point := TelemetryPoint{
		Ts:    ts,
		Value: value,
	}
	m[key] = append(m[key], point)
}
