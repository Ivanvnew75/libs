package common

import (
	"context"

	"github.com/labstack/echo/v4"
)

// Сквозной идентификатор запроса.
//
// ЗАЧЕМ ОН НУЖЕН В МИКРОСЕРВИСАХ
//
// Пользователь написал боту → telegram-api сходил в users → users сходил
// в базу. Если что-то сломалось, в логах это ТРИ независимых события
// в трёх разных сервисах, и связать их можно только по времени —
// то есть на глаз и с ошибками, когда запросов больше одного в секунду.
//
// Общий идентификатор превращает их в одну историю: фильтр
// `request_id="..."` показывает весь путь запроса через систему.
// Это тот самый минимум, с которого начинается наблюдаемость;
// полноценный distributed tracing (OpenTelemetry) — следующий шаг,
// он добавляет к этому длительности и связи «родитель-потомок».

const HeaderRequestID = "X-Request-ID"

type ctxKey int

const requestIDKey ctxKey = iota

func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey, id)
}

func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// PropagateRequestID кладёт идентификатор из заголовка в context запроса,
// чтобы его подхватил HTTP-клиент при вызове соседнего сервиса.
//
// Ставится ПОСЛЕ RequestID(): к этому моменту заголовок уже гарантированно
// проставлен, даже если клиент его не прислал.
func PropagateRequestID() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			id := c.Response().Header().Get(HeaderRequestID)
			if id == "" {
				id = c.Request().Header.Get(HeaderRequestID)
			}
			req := c.Request()
			c.SetRequest(req.WithContext(WithRequestID(req.Context(), id)))
			return next(c)
		}
	}
}
