package common

import (
	"context"
	"errors"
	"os/signal"
	"syscall"
	"time"
)

// SignalContext возвращает контекст, который отменяется по SIGINT/SIGTERM.
//
// Фактор 9 (Disposability). Механика в Kubernetes такая:
//
//	1. под помечается Terminating и СРАЗУ убирается из endpoints Service;
//	2. контейнеру шлётся SIGTERM;
//	3. через terminationGracePeriodSeconds прилетает SIGKILL, который
//	   перехватить невозможно.
//
// Шаги 1 и 2 происходят параллельно и асинхронно: kubelet может доставить
// SIGTERM раньше, чем kube-proxy на всех узлах перепишет правила. Поэтому
// «мгновенно завершиться по SIGTERM» — ошибка: часть запросов уже в пути
// и получит connection refused. Правильно — перестать принимать новые
// соединения, доработать текущие и выйти. Отсюда же практика ставить
// preStop sleep на несколько секунд.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// ShutdownContext — контекст с таймаутом на само завершение.
// Таймаут обязан быть МЕНЬШЕ terminationGracePeriodSeconds, иначе
// приложение не успеет закончить и его добьёт SIGKILL — то есть
// весь graceful shutdown окажется декоративным.
func ShutdownContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// Interrupted сообщает, завершились ли мы по сигналу, а не по ошибке.
func Interrupted(err error) bool {
	return err == nil || errors.Is(err, context.Canceled)
}
