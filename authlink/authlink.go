// Package authlink — вход в веб-кабинет по подписанной ссылке от бота.
//
// ЗАЧЕМ ВООБЩЕ АУТЕНТИФИКАЦИЯ.
// web-admin — единственный сервис, доступный из интернета, и он показывает
// историю настроения конкретного человека. Самый простой вариант —
// /history?user_id=3 — это классический IDOR: любой желающий перебирает
// user_id и читает чужую переписку с ботом. Такая «фича» стабильно входит
// в OWASP Top 10 (Broken Access Control, A01) и находится за минуту.
//
// ПОЧЕМУ ИМЕННО ПОДПИСАННАЯ ССЫЛКА, А НЕ ПАРОЛЬ ИЛИ OAuth.
// У пользователя нет и не должно быть пароля: он пришёл из Telegram,
// где уже аутентифицирован. Варианты:
//
//	пароль          — новая сущность, восстановление, хранение хешей;
//	Telegram Login  — виджет требует публичного домена и привязки к боту,
//	                  плюс зависимость от стороннего JS на странице;
//	подписанная     — бот, который УЖЕ знает, кто это, выдаёт ссылку
//	ссылка (тут)      с криптографической подписью на короткий срок.
//
// Третий вариант даёт настоящую проверку личности без единого нового
// хранилища. Цена: ссылка — это bearer-токен, кто её перехватил, тот
// и вошёл. Отсюда короткий срок жизни и обязательный HTTPS.
package authlink

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	ErrMalformed = errors.New("malformed token")
	ErrSignature = errors.New("bad signature")
	ErrExpired   = errors.New("token expired")
)

// Purpose разделяет токены по назначению.
//
// Зачем: токен входа по ссылке живёт минуты, сессионная кука — дни.
// Без разделения назначений сессионную куку можно было бы подставить
// как ссылку входа и наоборот. Назначение входит в подписываемую
// строку, поэтому подмена ломает подпись.
type Purpose string

const (
	PurposeLogin   Purpose = "login"
	PurposeSession Purpose = "session"
)

// Sign создаёт токен вида <payload>.<подпись>.
//
// payload = base64url("<purpose>:<userID>:<expUnix>")
//
// Подпись считается по payload ЦЕЛИКОМ, а не по отдельным полям:
// иначе возможна путаница границ ("1:23" против "12:3").
func Sign(secret []byte, p Purpose, userID int64, ttl time.Duration) string {
	payload := fmt.Sprintf("%s:%d:%d", p, userID, time.Now().Add(ttl).Unix())
	enc := base64.RawURLEncoding.EncodeToString([]byte(payload))
	return enc + "." + sign(secret, enc)
}

// Verify проверяет подпись и срок, возвращает userID.
func Verify(secret []byte, p Purpose, token string) (int64, error) {
	enc, sig, ok := strings.Cut(token, ".")
	if !ok {
		return 0, ErrMalformed
	}

	// hmac.Equal, а не ==.
	//
	// Обычное сравнение строк выходит на первом различающемся байте,
	// и время ответа зависит от того, сколько байт совпало. По этой
	// разнице подпись подбирается побайтово (timing attack).
	// hmac.Equal сравнивает за постоянное время. Разница в одну строку
	// кода и в наличие/отсутствие уязвимости.
	if !hmac.Equal([]byte(sig), []byte(sign(secret, enc))) {
		return 0, ErrSignature
	}

	raw, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		return 0, ErrMalformed
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 3 {
		return 0, ErrMalformed
	}
	// Назначение проверяется ПОСЛЕ подписи: иначе мы бы отвечали
	// разными ошибками на подделанный и на неподходящий токен,
	// подсказывая атакующему, что подпись он подобрал верно.
	if parts[0] != string(p) {
		return 0, ErrMalformed
	}

	userID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || userID <= 0 {
		return 0, ErrMalformed
	}
	exp, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, ErrMalformed
	}
	if time.Now().Unix() > exp {
		return 0, ErrExpired
	}
	return userID, nil
}

func sign(secret []byte, payload string) string {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}
