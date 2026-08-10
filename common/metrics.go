package common

import (
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Метрики (бонусный Фактор 13, Telemetry).
//
// ЧЕМ МЕТРИКИ ОТЛИЧАЮТСЯ ОТ ЛОГОВ И ЗАЧЕМ НУЖНЫ ОБА
//
// Лог отвечает на вопрос «что произошло с ЭТИМ запросом» — он подробный
// и дорогой, его объём растёт линейно с трафиком. Метрика отвечает
// на вопрос «как система чувствует себя В ЦЕЛОМ» — она агрегирована,
// её объём зависит от числа временных рядов, а не от числа запросов.
//
// Отсюда практическое разделение: алерты и дашборды строятся на метриках
// (дёшево, быстро, за месяцы истории), а разбор конкретного инцидента
// идёт по логам (подробно, но за последние дни). Пытаться алертить
// по логам — дорого и медленно; пытаться расследовать инцидент
// по метрикам — нечем, в них нет деталей.

// Метрики RED: Rate, Errors, Duration — минимальный набор для сервиса
// на запрос-ответ. Достаточен, чтобы ответить: сколько запросов,
// сколько из них падает, как долго они идут.
type Metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inFlight prometheus.Gauge
	registry *prometheus.Registry
}

// NewMetrics создаёт СВОЙ регистр вместо DefaultRegisterer.
//
// Почему: в глобальный регистр по умолчанию пишут все подключённые
// библиотеки, и состав метрик перестаёт быть предсказуемым. Свой регистр
// делает набор явным, а повторная регистрация в тестах не паникует
// с «duplicate metrics collector registration attempted».
func NewMetrics(service string) *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Количество HTTP-запросов",
			// service в ConstLabels, а не в каждом вызове: иначе его
			// однажды забудут проставить, и метрики двух сервисов
			// сложатся в одну.
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"method", "route", "status"}),

		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "http_request_duration_seconds",
			Help:        "Длительность обработки HTTP-запроса",
			ConstLabels: prometheus.Labels{"service": service},
			// Границы бакетов заданы явно и подобраны под ЭТОТ сервис.
			//
			// Дефолтные бакеты prometheus начинаются с 5 мс и заканчиваются
			// 10 секундами. У нас p99 около 3 мс — все запросы попадали бы
			// в первый бакет, и посчитать по ним перцентиль было бы нельзя:
			// гистограмма даёт разрешение только там, где есть границы.
			Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"method", "route", "status"}),

		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "http_requests_in_flight",
			Help:        "Запросы, обрабатываемые прямо сейчас",
			ConstLabels: prometheus.Labels{"service": service},
		}),
	}

	reg.MustRegister(m.requests, m.duration, m.inFlight)

	// Метрики самого процесса Go: горутины, память, GC, файловые дескрипторы.
	//
	// Их регулярно забывают, а именно они отвечают на вопрос «почему под
	// съел лимит памяти» и «почему растёт задержка» (утечка горутин).
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

// Middleware измеряет каждый запрос.
func (m *Metrics) Middleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// /metrics не измеряем: обращения Prometheus раз в 15 секунд
			// не имеют отношения к пользовательскому трафику и только
			// портят статистику.
			if c.Request().URL.Path == "/metrics" {
				return next(c)
			}

			m.inFlight.Inc()
			start := time.Now()
			err := next(c)
			m.inFlight.Dec()

			status := c.Response().Status
			if err != nil {
				// Echo мог ещё не записать статус, если хендлер вернул ошибку.
				if he, ok := err.(*echo.HTTPError); ok {
					status = he.Code
				} else {
					status = 500
				}
			}

			// route — ШАБЛОН маршрута (/users/:id), а не конкретный URI.
			//
			// Это главная ловушка метрик. Если писать сюда реальный путь,
			// то /users/1, /users/2, /users/3... создадут отдельный
			// временной ряд на каждого пользователя. Prometheus хранит
			// ряды в памяти — миллион рядов кладёт его насмерть.
			// Явление называется cardinality explosion, и оно же было
			// причиной, по которой request_id не попал в метки Loki.
			route := c.Path()
			if route == "" {
				// Запрос, не подошедший ни под один маршрут (404).
				// Пишем константу, а не реальный путь: иначе любой сканер
				// уязвимостей, перебирающий адреса, создаст тысячи рядов.
				route = "unmatched"
			}

			labels := prometheus.Labels{
				"method": c.Request().Method,
				"route":  route,
				"status": strconv.Itoa(status),
			}
			m.requests.With(labels).Inc()
			m.duration.With(labels).Observe(time.Since(start).Seconds())

			return err
		}
	}
}

// Register вешает /metrics.
//
// Отдельный путь, а не отдельный порт — осознанное упрощение.
// В проде метрики часто выносят на второй порт, чтобы не выставлять
// их наружу вместе с публичным API; здесь Service внутрикластерный,
// и разделение не даёт ничего, кроме лишней сущности.
func (m *Metrics) Register(e *echo.Echo) {
	e.GET("/metrics", echo.WrapHandler(promhttp.HandlerFor(
		m.registry,
		promhttp.HandlerOpts{Registry: m.registry},
	)))
}

// Registry — для регистрации собственных метрик сервиса
// (например, «сколько ответов сохранено»).
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }
