// Package kafkax — общая обвязка над Kafka: потребитель с DLQ и продюсер.
//
// Лежит в libs, а не в репозитории сервисов, потому что продюсер
// (telegram-api) и потребители (answers, analytics) живут в разных
// репозиториях, а поведение при ошибке у них должно быть одинаковым.
//
// Вынесено в общий пакет, потому что оба потребителя (answers и analytics)
// нуждаются в одинаковом поведении при ошибке, а «одинаковое поведение,
// написанное дважды» расходится на второй же правке.
package kafkax

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Handler обрабатывает одно сообщение.
//
// Контракт по ошибкам — самая важная часть этого пакета:
//
//	nil            → сообщение обработано, offset коммитится
//	ErrPermanent   → сообщение НЕИСПРАВИМО (битый JSON, чужая схема).
//	                 Уезжает в DLQ, offset коммитится, обработка идёт дальше
//	любая другая   → временная беда (БД недоступна). Ретраим с backoff,
//	                 offset НЕ коммитим
//
// Различение временной и постоянной ошибки — не педантизм. Без него
// возможны ровно два плохих режима, и оба встречаются в проде:
//   - всё считать временным → одно битое сообщение блокирует партицию
//     навсегда («ядовитое сообщение»), потребитель крутится в цикле;
//   - всё считать постоянным → недоступность базы на минуту выбрасывает
//     в DLQ все сообщения этой минуты, то есть тихая потеря данных.
type Handler func(ctx context.Context, msg kafka.Message) error

// ErrPermanent помечает ошибку как неисправимую.
var ErrPermanent = errors.New("permanent error")

// Permanent оборачивает ошибку, помечая её неисправимой.
func Permanent(err error) error { return errors.Join(ErrPermanent, err) }

type ConsumerConfig struct {
	Brokers  []string
	Topic    string
	Group    string
	DLQTopic string

	Log *slog.Logger

	// MaxRetries — сколько раз повторить временную ошибку, прежде чем
	// признать её постоянной и отправить сообщение в DLQ.
	//
	// Ноль означал бы «ретраить вечно»: при долгой недоступности базы
	// потребитель просто ждёт, ничего не теряя. Это безопаснее, но тогда
	// лаг растёт молча. Конечное число + DLQ даёт видимый сигнал:
	// в DLQ что-то приехало — значит, была авария, и вот её след.
	MaxRetries int
}

type Consumer struct {
	reader *kafka.Reader
	dlq    *kafka.Writer
	cfg    ConsumerConfig
	log    *slog.Logger
}

func NewConsumer(cfg ConsumerConfig) *Consumer {
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 5
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		GroupID: cfg.Group,

		// MinBytes/MaxBytes задают, сколько брокер накапливает перед ответом.
		// MinBytes=1 — отдавай сразу, как появилось. Для потока в два
		// сообщения в сутки батчинг ради пропускной способности бессмыслен,
		// а задержка была бы заметна человеку.
		MinBytes: 1,
		MaxBytes: 10e6,

		// StartOffset действует ТОЛЬКО при первом старте consumer group.
		// FirstOffset (earliest) выбран сознательно: новый потребитель
		// должен увидеть всю историю, а не только то, что придёт после
		// его появления. Именно это позволяет восстановить ClickHouse
		// переигрыванием топика — просто сменив имя группы.
		StartOffset: kafka.FirstOffset,

		// Автокоммит выключен (CommitInterval=0 — значение по умолчанию,
		// коммит только явным CommitMessages). С автокоммитом offset
		// уезжает вперёд по таймеру независимо от того, обработали ли мы
		// сообщение, и падение потребителя приводит к ПОТЕРЕ данных —
		// то есть к at-most-once вместо at-least-once.
		CommitInterval: 0,
	})

	var dlq *kafka.Writer
	if cfg.DLQTopic != "" {
		dlq = &kafka.Writer{
			Addr:  kafka.TCP(cfg.Brokers...),
			Topic: cfg.DLQTopic,
			// RequireOne: подтверждение от лидера партиции.
			// RequireNone (fire-and-forget) для DLQ недопустим — мы бы
			// теряли ровно те сообщения, ради сохранения которых DLQ и есть.
			RequiredAcks: kafka.RequireOne,
			Balancer:     &kafka.Hash{},
		}
	}

	return &Consumer{reader: reader, dlq: dlq, cfg: cfg, log: cfg.Log}
}

// Run крутит цикл чтения до отмены контекста.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	c.log.Info("consumer started",
		slog.String("topic", c.cfg.Topic), slog.String("group", c.cfg.Group))

	for {
		// FetchMessage, а не ReadMessage: ReadMessage сам коммитит offset
		// после выдачи сообщения, то есть ДО его обработки. Нам нужен
		// коммит после успеха, иначе падение между чтением и записью
		// в базу теряет сообщение.
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.log.Info("consumer stopped")
				return nil
			}
			c.log.Error("fetch failed", slog.String("error", err.Error()))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}

		if err := c.handleWithRetry(ctx, msg, h); err != nil {
			// Сюда попадаем только при отмене контекста во время ретраев.
			// Offset не коммитим — сообщение придёт заново после рестарта.
			return err
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			// Коммит не прошёл — сообщение придёт повторно. Данные не
			// потеряны, но потребитель обязан быть идемпотентным.
			// Это ровно тот случай, ради которого в событии есть event_id.
			c.log.Error("commit failed", slog.String("error", err.Error()))
		}
	}
}

func (c *Consumer) handleWithRetry(ctx context.Context, msg kafka.Message, h Handler) error {
	backoff := 500 * time.Millisecond

	for attempt := 1; ; attempt++ {
		err := h(ctx, msg)
		if err == nil {
			return nil
		}

		log := c.log.With(
			slog.Int("partition", msg.Partition),
			slog.Int64("offset", msg.Offset),
			slog.String("key", string(msg.Key)),
			slog.Int("attempt", attempt),
			slog.String("error", err.Error()),
		)

		if errors.Is(err, ErrPermanent) {
			log.Error("неисправимая ошибка, сообщение уходит в DLQ")
			c.toDLQ(ctx, msg, err)
			return nil
		}

		if attempt >= c.cfg.MaxRetries {
			log.Error("исчерпаны попытки, сообщение уходит в DLQ")
			c.toDLQ(ctx, msg, err)
			return nil
		}

		log.Warn("временная ошибка, повтор")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

func (c *Consumer) toDLQ(ctx context.Context, msg kafka.Message, cause error) {
	if c.dlq == nil {
		c.log.Error("DLQ не настроен, сообщение будет ПОТЕРЯНО",
			slog.String("key", string(msg.Key)))
		return
	}

	// Отдельный контекст: исходный может быть уже отменён (SIGTERM),
	// а записать в DLQ надо всё равно — иначе сообщение потеряется
	// именно в момент выкатки, когда потребитель перезапускается.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	dead := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		// Причина попадания в DLQ кладётся в ЗАГОЛОВКИ, а не в тело.
		// Тело остаётся байт-в-байт исходным — чтобы после починки
		// сообщение можно было переиграть обратно в основной топик
		// без разбора и пересборки.
		Headers: []kafka.Header{
			{Key: "x-dlq-reason", Value: []byte(cause.Error())},
			{Key: "x-dlq-topic", Value: []byte(msg.Topic)},
			{Key: "x-dlq-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}
	if err := c.dlq.WriteMessages(writeCtx, dead); err != nil {
		c.log.Error("не удалось записать в DLQ — сообщение ПОТЕРЯНО",
			slog.String("error", err.Error()))
	}
}

func (c *Consumer) Close() error {
	if c.dlq != nil {
		_ = c.dlq.Close()
	}
	return c.reader.Close()
}

// Lag возвращает отставание потребителя от конца топика.
// Отдаётся в /metrics: лаг — главная метрика здоровья потребителя,
// куда полезнее, чем «под Running».
//
// ОСТОРОЖНО С ВЫБОРОМ МЕТОДА — здесь легко получить метрику-обманку.
// У kafka-go есть три похожих способа узнать лаг, и два из них
// НЕ РАБОТАЮТ для потребителя в consumer group:
//
//	reader.Lag()            → всегда -1, если задан GroupID (reader.go:1006)
//	reader.ReadLag(ctx)     → ошибка errNotAvailableWithGroup
//	reader.Stats().Lag      → работает: high water mark минус текущий offset
//
// Первая версия этого кода использовала Lag(), и метрика показывала
// ровно -1 при полностью исправном потребителе. Компилятор молчит,
// под Running, /metrics отдаётся — а числа нет. Ровно тот случай,
// когда «мониторинг есть» и «мониторинг работает» — разные вещи;
// увидеть это можно только посмотрев на само значение.
//
// Побочный эффект Stats(): вызов СБРАСЫВАЕТ счётчики (это snapshot).
// Здесь не мешает — счётчики kafka-go мы не используем, у сервиса свои.
// Но если рядом появится второй потребитель Stats(), они начнут
// отбирать друг у друга данные.
func (c *Consumer) Lag() int64 { return c.reader.Stats().Lag }
