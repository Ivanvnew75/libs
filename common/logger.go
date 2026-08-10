package common

import (
	"log/slog"
	"os"
	"strings"
)

// NewLogger собирает структурированный логгер (Фактор 11, Logs).
//
// Три решения и их причины:
//
//  1. Вывод в os.Stdout. Приложение НЕ пишет в файлы, не ротирует логи
//     и не знает, куда они уедут. Лог — это поток событий; собирать его
//     задача среды (в Kubernetes — контейнерного рантайма и агента вроде
//     Promtail/Vector). Приложение, пишущее в файл внутри контейнера,
//     теряет логи при рестарте пода и мешает readOnlyRootFilesystem.
//
//  2. JSON по умолчанию. Человекочитаемый текст удобен ровно один раз —
//     когда смотришь глазами в терминал. Дальше по логам ищут фильтром
//     «покажи ошибки сервиса X с trace_id Y», и для этого нужны поля,
//     а не строка. LOG_FORMAT=text оставлен для локальной разработки.
//
//  3. Уровень из переменной окружения. Поднять детализацию в инциденте
//     должно быть перезапуском пода, а не пересборкой образа.
func NewLogger(service, version, format, level string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}

	var h slog.Handler
	if strings.EqualFold(format, "text") {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}

	// service и version добавляются в КАЖДУЮ запись.
	// Без них логи трёх сервисов в общем хранилище неразличимы, а вопрос
	// «эта ошибка появилась в новой версии?» остаётся без ответа.
	return slog.New(h).With(
		slog.String("service", service),
		slog.String("version", version),
	)
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
