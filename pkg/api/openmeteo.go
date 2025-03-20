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
	Config      *config.Config
	Client      *http.Client
	BaseURL     string
	ForecastURL string
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

// WeatherForecast представляет структуру ответа API Open-Meteo Forecast
type WeatherForecast struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timezone  string  `json:"timezone"`
	Elevation float64 `json:"elevation"`

	// Почасовые данные
	Hourly struct {
		Time                     []string  `json:"time"`
		Temperature2m            []float64 `json:"temperature_2m"`
		RelativeHumidity2m       []float64 `json:"relative_humidity_2m"`
		Precipitation            []float64 `json:"precipitation"`
		PrecipitationProbability []float64 `json:"precipitation_probability"`
		WindSpeed10m             []float64 `json:"wind_speed_10m"`
		WindDirection10m         []float64 `json:"wind_direction_10m"`
		WindGusts10m             []float64 `json:"wind_gusts_10m"`
		DewPoint2m               []float64 `json:"dew_point_2m"`
		SoilTemperature18cm      []float64 `json:"soil_temperature_18cm"`
		SoilTemperature6cm       []float64 `json:"soil_temperature_6cm"`
		SoilTemperature0cm       []float64 `json:"soil_temperature_0cm"`
		SoilMoisture1to3cm       []float64 `json:"soil_moisture_1_to_3cm"`
		SoilMoisture3to9cm       []float64 `json:"soil_moisture_3_to_9cm"`
		SoilMoisture9to27cm      []float64 `json:"soil_moisture_9_to_27cm"`
	} `json:"hourly"`

	// Единицы измерения для почасовых данных
	HourlyUnits struct {
		Temperature2m            string `json:"temperature_2m"`
		RelativeHumidity2m       string `json:"relative_humidity_2m"`
		Precipitation            string `json:"precipitation"`
		PrecipitationProbability string `json:"precipitation_probability"`
		WindSpeed10m             string `json:"wind_speed_10m"`
		WindDirection10m         string `json:"wind_direction_10m"`
		WindGusts10m             string `json:"wind_gusts_10m"`
		DewPoint2m               string `json:"dew_point_2m"`
		SoilTemperature18cm      string `json:"soil_temperature_18cm"`
		SoilTemperature6cm       string `json:"soil_temperature_6cm"`
		SoilTemperature0cm       string `json:"soil_temperature_0cm"`
		SoilMoisture1to3cm       string `json:"soil_moisture_1_to_3cm"`
		SoilMoisture3to9cm       string `json:"soil_moisture_3_to_9cm"`
		SoilMoisture9to27cm      string `json:"soil_moisture_9_to_27cm"`
	} `json:"hourly_units"`

	// Ежедневные данные
	Daily struct {
		Time                     []string  `json:"time"`
		PrecipitationSum         []float64 `json:"precipitation_sum"`
		WindDirection10mDominant []float64 `json:"wind_direction_10m_dominant"`
		Temperature2mMin         []float64 `json:"temperature_2m_min"`
		Temperature2mMax         []float64 `json:"temperature_2m_max"`
		WindGusts10mMax          []float64 `json:"wind_gusts_10m_max"`
		PrecipitationHours       []float64 `json:"precipitation_hours"`
	} `json:"daily"`

	// Единицы измерения для ежедневных данных
	DailyUnits struct {
		PrecipitationSum         string `json:"precipitation_sum"`
		WindDirection10mDominant string `json:"wind_direction_10m_dominant"`
		Temperature2mMin         string `json:"temperature_2m_min"`
		Temperature2mMax         string `json:"temperature_2m_max"`
		WindGusts10mMax          string `json:"wind_gusts_10m_max"`
		PrecipitationHours       string `json:"precipitation_hours"`
	} `json:"daily_units"`
}

// TelemetryPoint представляет точку данных телеметрии
type TelemetryPoint struct {
	Ts    int64       `json:"ts"`
	Value interface{} `json:"value"`
}

// NewOpenMeteoAPI создает новый экземпляр API клиента Open-Meteo
func NewOpenMeteoAPI(cfg *config.Config) *OpenMeteoAPI {
	return &OpenMeteoAPI{
		Config:      cfg,
		Client:      &http.Client{Timeout: 30 * time.Second},
		BaseURL:     "https://archive-api.open-meteo.com/v1/archive",
		ForecastURL: "https://api.open-meteo.com/v1/forecast",
	}
}

// GetWeatherData получает данные о погоде для указанных координат за указанный период
func (o *OpenMeteoAPI) GetWeatherData(latitude, longitude float64, pastDays int) (*WeatherData, error) {
	// Вычисляем даты начала и конца периода
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -pastDays)

	return o.GetWeatherDataByDates(latitude, longitude, startDate, endDate)
}

// GetWeatherDataByDates получает данные о погоде для указанных координат за указанный период между датами
func (o *OpenMeteoAPI) GetWeatherDataByDates(latitude, longitude float64, startDate, endDate time.Time) (*WeatherData, error) {
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

// GetWeatherForecast получает данные прогноза погоды для указанных координат
func (o *OpenMeteoAPI) GetWeatherForecast(latitude, longitude float64, pastDays int) (*WeatherForecast, error) {
	params := url.Values{}
	params.Add("latitude", strconv.FormatFloat(latitude, 'f', 6, 64))
	params.Add("longitude", strconv.FormatFloat(longitude, 'f', 6, 64))
	params.Add("timezone", "Europe/Moscow")
	params.Add("past_days", strconv.Itoa(pastDays))

	// Почасовые параметры
	params.Add("hourly", "soil_temperature_18cm,soil_temperature_6cm,soil_temperature_0cm,soil_moisture_1_to_3cm,soil_moisture_3_to_9cm,soil_moisture_9_to_27cm,wind_speed_10m,temperature_2m,relative_humidity_2m,precipitation_probability,precipitation,dew_point_2m,wind_direction_10m,wind_gusts_10m")

	// Ежедневные параметры
	params.Add("daily", "precipitation_sum,wind_direction_10m_dominant,temperature_2m_min,temperature_2m_max,wind_gusts_10m_max,precipitation_hours")

	requestURL := fmt.Sprintf("%s?%s", o.ForecastURL, params.Encode())

	resp, err := o.Client.Get(requestURL)
	if err != nil {
		return nil, fmt.Errorf("ошибка при выполнении запроса к Forecast API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ошибка Forecast API: статус %d", resp.StatusCode)
	}

	var forecastData WeatherForecast
	if err := json.NewDecoder(resp.Body).Decode(&forecastData); err != nil {
		return nil, fmt.Errorf("ошибка при десериализации ответа Forecast API: %w", err)
	}

	return &forecastData, nil
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

// ConvertForecastToTelemetryMap преобразует данные прогноза от Open-Meteo в формат телеметрии
func (o *OpenMeteoAPI) ConvertForecastToTelemetryMap(data *WeatherForecast) map[string][]TelemetryPoint {
	result := make(map[string][]TelemetryPoint)

	// Преобразуем почасовые данные
	for i, timeStr := range data.Hourly.Time {
		// Парсим время в формате ISO 8601
		t, err := time.Parse("2006-01-02T15:04", timeStr)
		if err != nil {
			fmt.Printf("Ошибка при парсинге времени %s: %v\n", timeStr, err)
			continue
		}

		// Переводим время в миллисекунды
		ts := t.UnixNano() / int64(time.Millisecond)

		// Добавляем почасовые данные
		if i < len(data.Hourly.Temperature2m) {
			addPointToMap(result, "forecast_temperature", ts, data.Hourly.Temperature2m[i])
		}
		if i < len(data.Hourly.RelativeHumidity2m) {
			addPointToMap(result, "forecast_humidity", ts, data.Hourly.RelativeHumidity2m[i])
		}
		if i < len(data.Hourly.Precipitation) {
			addPointToMap(result, "forecast_precipitation", ts, data.Hourly.Precipitation[i])
		}
		if i < len(data.Hourly.PrecipitationProbability) {
			addPointToMap(result, "forecast_precipitation_probability", ts, data.Hourly.PrecipitationProbability[i])
		}
		if i < len(data.Hourly.WindSpeed10m) {
			addPointToMap(result, "forecast_wind_speed", ts, data.Hourly.WindSpeed10m[i])
		}
		if i < len(data.Hourly.WindDirection10m) {
			addPointToMap(result, "forecast_wind_direction", ts, data.Hourly.WindDirection10m[i])
		}
		if i < len(data.Hourly.WindGusts10m) {
			addPointToMap(result, "forecast_wind_gusts", ts, data.Hourly.WindGusts10m[i])
		}
		if i < len(data.Hourly.DewPoint2m) {
			addPointToMap(result, "forecast_dew_point", ts, data.Hourly.DewPoint2m[i])
		}
		if i < len(data.Hourly.SoilTemperature18cm) {
			addPointToMap(result, "forecast_soil_temp_18cm", ts, data.Hourly.SoilTemperature18cm[i])
		}
		if i < len(data.Hourly.SoilTemperature6cm) {
			addPointToMap(result, "forecast_soil_temp_6cm", ts, data.Hourly.SoilTemperature6cm[i])
		}
		if i < len(data.Hourly.SoilTemperature0cm) {
			addPointToMap(result, "forecast_soil_temp_0cm", ts, data.Hourly.SoilTemperature0cm[i])
		}
		if i < len(data.Hourly.SoilMoisture1to3cm) {
			addPointToMap(result, "forecast_soil_moisture_1_3cm", ts, data.Hourly.SoilMoisture1to3cm[i])
		}
		if i < len(data.Hourly.SoilMoisture3to9cm) {
			addPointToMap(result, "forecast_soil_moisture_3_9cm", ts, data.Hourly.SoilMoisture3to9cm[i])
		}
		if i < len(data.Hourly.SoilMoisture9to27cm) {
			addPointToMap(result, "forecast_soil_moisture_9_27cm", ts, data.Hourly.SoilMoisture9to27cm[i])
		}
	}

	// Преобразуем ежедневные данные
	for i, timeStr := range data.Daily.Time {
		// Парсим время в формате ISO 8601 (дата без времени)
		t, err := time.Parse("2006-01-02", timeStr)
		if err != nil {
			fmt.Printf("Ошибка при парсинге даты %s: %v\n", timeStr, err)
			continue
		}

		// Устанавливаем время на 12:00 для ежедневных данных, чтобы отличать их от почасовых
		t = time.Date(t.Year(), t.Month(), t.Day(), 12, 0, 0, 0, t.Location())

		// Переводим время в миллисекунды
		ts := t.UnixNano() / int64(time.Millisecond)

		// Добавляем ежедневные данные
		if i < len(data.Daily.PrecipitationSum) {
			addPointToMap(result, "forecast_daily_precipitation_sum", ts, data.Daily.PrecipitationSum[i])
		}
		if i < len(data.Daily.WindDirection10mDominant) {
			addPointToMap(result, "forecast_daily_wind_direction", ts, data.Daily.WindDirection10mDominant[i])
		}
		if i < len(data.Daily.Temperature2mMin) {
			addPointToMap(result, "forecast_daily_temp_min", ts, data.Daily.Temperature2mMin[i])
		}
		if i < len(data.Daily.Temperature2mMax) {
			addPointToMap(result, "forecast_daily_temp_max", ts, data.Daily.Temperature2mMax[i])
		}
		if i < len(data.Daily.WindGusts10mMax) {
			addPointToMap(result, "forecast_daily_wind_gusts_max", ts, data.Daily.WindGusts10mMax[i])
		}
		if i < len(data.Daily.PrecipitationHours) {
			addPointToMap(result, "forecast_daily_precipitation_hours", ts, data.Daily.PrecipitationHours[i])
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
