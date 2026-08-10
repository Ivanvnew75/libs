package common

import (
	"log/slog"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// RequestLogger — логирование HTTP-запросов через slog (Фактор 11).
//
// ЗАЧЕМ ЗАМЕНЯТЬ ШТАТНЫЙ middleware.Logger()
//
// Штатный логгер Echo пишет своим форматом в свой io.Writer и ничего
// не знает про slog. В результате в одном контейнере получаются ДВА
// разных формата: строки приложения и строки запросов. Любой парсер
// на той стороне (Loki, Elastic, CloudWatch) увидит половину строк
// как «непонятный текст» и не даст по ним искать.
//
// Единый формат — не эстетика. Лог полезен ровно настолько, насколько
// по нему можно фильтровать.
func RequestLogger(logger *slog.Logger) echo.MiddlewareFunc {
	return middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		// Skipper выкидывает из лога пробы kubelet.
		//
		// Это не косметика, а деньги и внимание. При periodSeconds=5
		// и трёх пробах на под получается ~50 000 строк в сутки на под,
		// в которых нет ни одного бита полезной информации. В хранилищах
		// с оплатой за объём это заметная статья, а в инциденте —
		// шум, сквозь который приходится продираться.
		//
		// Если проба начнёт падать, мы узнаем об этом из событий
		// Kubernetes и метрик, а не из access-лога.
		Skipper: func(c echo.Context) bool {
			p := c.Request().URL.Path
			return p == "/health" || p == "/ready" ||
				strings.HasPrefix(c.Request().UserAgent(), "kube-probe/")
		},

		LogRequestID: true,
		LogMethod:    true,
		LogURI:       true,
		LogStatus:    true,
		LogLatency:   true,
		LogError:     true,
		LogRemoteIP:  true,
		// HandleError: true — чтобы в лог попадал статус ПОСЛЕ обработки
		// ошибки хендлером Echo, а не тот, что был до неё.
		HandleError: true,

		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			// Уровень выбирается по коду ответа.
			//
			// Почему это важно: если все запросы пишутся уровнем INFO,
			// то фильтр «покажи ошибки» не работает, и приходится искать
			// по подстроке "status":5 — хрупко и не переносимо.
			level := slog.LevelInfo
			switch {
			case v.Status >= 500:
				level = slog.LevelError
			case v.Status >= 400:
				// 4xx — это ошибка КЛИЕНТА, а не сервиса. WARN, а не ERROR:
				// иначе любой бот, дёргающий несуществующий путь, поднимет
				// алерт по количеству ошибок.
				level = slog.LevelWarn
			}

			attrs := []slog.Attr{
				slog.String("request_id", v.RequestID),
				slog.String("method", v.Method),
				slog.String("uri", v.URI),
				slog.Int("status", v.Status),
				// Миллисекунды числом, а не строкой "1.23ms":
				// по числу можно строить перцентили и пороги в запросе,
				// по строке — нет.
				slog.Float64("latency_ms", float64(v.Latency.Microseconds())/1000),
				slog.String("remote_ip", v.RemoteIP),
			}
			if v.Error != nil {
				attrs = append(attrs, slog.String("error", v.Error.Error()))
			}

			logger.LogAttrs(c.Request().Context(), level, "http request", attrs...)
			return nil
		},
	})
}

// RequestID проставляет заголовок X-Request-ID, если его нет.
//
// Ключевая деталь — «если его НЕТ». Идентификатор, пришедший снаружи,
// нужно пробрасывать дальше, а не перезаписывать: только так один запрос
// пользователя прослеживается через все сервисы. Без этого в логах
// три несвязанных события вместо одной истории.
func RequestID() echo.MiddlewareFunc { return middleware.RequestID() }
