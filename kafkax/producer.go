package kafkax

import (
	"context"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// NewWriter — продюсер с настройками, за которые не стыдно в проде.
//
// Значения по умолчанию у kafka-go «удобные», а не «надёжные»:
// RequiredAcks там RequireNone (не ждать подтверждения вообще).
// Это означает, что WriteMessages возвращает nil, даже если брокер
// сообщение не принял. Приложение считает, что записало, — данных нет.
// Один из самых неприятных классов багов: он не воспроизводится,
// пока всё хорошо.
func NewWriter(brokers []string, topic string, log *slog.Logger) *kafka.Writer {
	return &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,

		// RequireAll = acks=all: ждать подтверждения от всех реплик в ISR.
		// На стенде с одним брокером это то же, что RequireOne, но правильное
		// значение зафиксировано сейчас — при переезде на 3 брокера
		// не придётся вспоминать, что acks надо было поднять.
		RequiredAcks: kafka.RequireAll,

		// Hash-балансировщик: партиция = hash(ключ) % число партиций.
		// Именно он реализует обещание «все события пользователя в одной
		// партиции». Дефолтный балансировщик — round-robin, он это обещание
		// молча нарушает.
		Balancer: &kafka.Hash{},

		// Синхронная запись. Асинхронная (Async: true) вернула бы управление
		// мгновенно, а ошибки уехали бы в фоновый обработчик — то есть
		// вызывающий код не смог бы ответить пользователю «не сохранил».
		Async: false,

		// Топики создаются декларативно через KafkaTopic CR
		// (auto.create.topics.enable=false на брокере). Явное false здесь —
		// защита от иллюзии: с true продюсер молча создал бы топик
		// с дефолтными настройками при опечатке в имени.
		AllowAutoTopicCreation: false,

		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,

		// BatchTimeout при Async: false влияет на задержку одиночной записи:
		// writer ждёт, не наберётся ли батч. Дефолт 1s заметен человеку,
		// который ждёт ответа бота.
		BatchTimeout: 50 * time.Millisecond,

		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...any) {
			log.Error("kafka writer", slog.String("msg", msg), slog.Any("args", args))
		}),
	}
}

// WriteJSON пишет одно сообщение.
func WriteJSON(ctx context.Context, w *kafka.Writer, key, value []byte) error {
	return w.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
}
