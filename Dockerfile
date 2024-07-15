# Используем официальный образ golang для сборки
FROM golang:1.20 AS builder

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файлы модулей и загружаем зависимости
COPY go.mod go.sum ./
RUN go mod download

# Копируем остальные файлы проекта
COPY . .

# Сборка приложения с указанием исходного файла main.go
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o /app/main ./cmd/main.go

# Используем минималистичный образ для запуска
FROM alpine:latest

# Устанавливаем необходимые зависимости
RUN apk --no-cache add ca-certificates

# Копируем скомпилированное приложение из стадии сборки
COPY --from=builder /app/main /app/main
COPY --from=builder /app/config/config.yaml /app/config/config.yaml

# Устанавливаем рабочую директорию
WORKDIR /app

# Проверка наличия файлов в конечном образе
RUN ls -la /app

# Запуск приложения
CMD ["./main"]
