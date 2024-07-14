# Строительный этап
FROM golang:1.20-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Собираем приложение
RUN go build -o main ./cmd

# Финальный этап
FROM alpine:latest

WORKDIR /app

# Копируем скомпилированное приложение из предыдущего этапа
COPY --from=builder /app/main .

# Копируем конфигурационный файл
COPY --from=builder /app/config/config.yaml ./config/config.yaml

RUN chmod +x ./main

# Запускаем приложение
CMD ["./main"]