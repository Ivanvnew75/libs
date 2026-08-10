package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"time"
)

// Client — HTTP-клиент для походов в соседние сервисы.
//
// Зачем свой, а не http.DefaultClient: у DefaultClient НЕТ таймаута.
// Совсем. Один зависший бэкенд — и горутины копятся, пока сервис
// не упрётся в память. Это самая частая причина «сервис живой,
// но ничего не отвечает» в микросервисных системах.
type Client struct {
	http *http.Client
	// MaxRetries — сколько РАЗ повторить сверх первой попытки.
	MaxRetries int
	BaseDelay  time.Duration
}

func NewClient(timeout time.Duration, maxRetries int) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Пул соединений на хост. Дефолтные 2 (MaxIdleConnsPerHost) означают,
	// что при параллельных запросах к одному сервису соединения будут
	// открываться и закрываться каждый раз — лишний TCP+TLS handshake
	// на каждый вызов.
	transport.MaxIdleConnsPerHost = 32

	return &Client{
		http:       &http.Client{Timeout: timeout, Transport: transport},
		MaxRetries: maxRetries,
		BaseDelay:  100 * time.Millisecond,
	}
}

// DoJSON шлёт JSON и разбирает JSON-ответ.
// out может быть nil, если тело ответа не нужно.
func (c *Client) DoJSON(ctx context.Context, method, url string, in, out any) error {
	var body []byte
	if in != nil {
		var err error
		body, err = json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
	}

	var lastErr error
	for attempt := 0; attempt <= c.MaxRetries; attempt++ {
		if attempt > 0 {
			// Экспоненциальная задержка со джиттером.
			//
			// Джиттер (случайная добавка) — не украшение. Без него все
			// клиенты, получившие ошибку одновременно, повторят запрос
			// тоже одновременно и добьют сервис, который только начал
			// подниматься. Это называется thundering herd.
			delay := c.BaseDelay * (1 << (attempt - 1))
			jitter := time.Duration(rand.Int63n(int64(delay/2 + 1)))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay + jitter):
			}
		}

		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		if in != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		// Прокидываем сквозной идентификатор дальше по цепочке вызовов.
		// Без этой строки request_id обрывается на границе сервиса,
		// и связать логи двух сервисов становится нечем.
		if rid := RequestIDFromContext(ctx); rid != "" {
			req.Header.Set(HeaderRequestID, rid)
		}

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			if retriableNetErr(err) {
				continue
			}
			return err
		}

		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}

		// Повторяем только 5xx и 429.
		//
		// 4xx повторять НЕЛЬЗЯ: 400 останется 400 сколько ни повторяй,
		// а ретраи на 401/403 выглядят для системы защиты как перебор.
		// Отдельно: повторять неидемпотентные POST опасно — можно создать
		// дубль. Здесь это осознанный компромисс: ручки, которые дёргают
		// сервисы, спроектированы идемпотентными (см. telegram-api).
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			lastErr = fmt.Errorf("%s %s: status %d: %s", method, url, resp.StatusCode, truncate(payload))
			continue
		}
		if resp.StatusCode >= 400 {
			return &HTTPError{Code: resp.StatusCode, Body: truncate(payload)}
		}

		if out != nil && len(payload) > 0 {
			if err := json.Unmarshal(payload, out); err != nil {
				return fmt.Errorf("decode response from %s: %w", url, err)
			}
		}
		return nil
	}
	return fmt.Errorf("%s %s failed after %d attempts: %w", method, url, c.MaxRetries+1, lastErr)
}

type HTTPError struct {
	Code int
	Body string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("status %d: %s", e.Code, e.Body) }

// retriableNetErr: таймауты и временные сетевые сбои повторяем.
// В Kubernetes это штатная ситуация — под собеседника мог именно
// в этот момент уехать при выкатке.
func retriableNetErr(err error) bool {
	var netErr net.Error
	if ok := asNetError(err, &netErr); ok {
		return netErr.Timeout()
	}
	return true
}

func asNetError(err error, target *net.Error) bool {
	if ne, ok := err.(net.Error); ok {
		*target = ne
		return true
	}
	return false
}

func truncate(b []byte) string {
	const max = 512
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}
