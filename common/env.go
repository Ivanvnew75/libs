// Package common — то, что нужно всем микросервисам приложения.
//
// Почему отдельный модуль, а не копипаста между репозиториями: правку
// в чтении конфигурации или в graceful shutdown иначе пришлось бы вносить
// в трёх местах, и рано или поздно один из сервисов отстанет.
//
// Цена этого решения честная и её надо понимать: общий модуль означает
// цикл «изменил libs → поставил тег → в каждом сервисе go get -u → пересобрал».
// Поэтому в libs кладут только то, что меняется редко. Бизнес-логику
// в общую библиотеку тащить нельзя — так микросервисы превращаются
// в распределённый монолит, где ни один сервис нельзя выкатить отдельно.
package common

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Env читает строку с дефолтом.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// MustEnv читает обязательную переменную. Пустая — ошибка.
//
// Фактор 3: у обязательного параметра не должно быть «умолчания на всякий
// случай». Сервис, стартовавший с подставленным дефолтом вместо адреса
// зависимости, ломается позже и в неочевидном месте.
func MustEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("environment variable %s is required", key)
	}
	return v, nil
}

func EnvInt(key string, def int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer, got %q", key, raw)
	}
	return n, nil
}

// EnvDuration принимает формат Go: 15s, 2m, 1h30m.
//
// Не «число секунд»: 30 — это тридцать секунд или тридцать миллисекунд?
// Единица измерения в самом значении убирает целый класс ошибок конфигурации.
func EnvDuration(key string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration (e.g. 15s, 2m), got %q", key, raw)
	}
	return d, nil
}
