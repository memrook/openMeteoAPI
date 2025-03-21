FROM golang:1.23-alpine AS builder

WORKDIR /app

# Копируем файлы проекта
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# Сборка приложения
RUN go build -o openmeteo-service ./cmd/openmeteo

# Создаем финальный образ
FROM alpine:latest

WORKDIR /app

# Устанавливаем зависимости
RUN apk --no-cache add ca-certificates tzdata && \
    mkdir -p /app/data

# Копируем исполняемый файл из builder
COPY --from=builder /app/openmeteo-service /app/
COPY .env.example /app/.env.example

# Задаем пользователя
RUN adduser -D appuser && \
    chown -R appuser:appuser /app
USER appuser

# Запускаем сервис
CMD ["./openmeteo-service"] 