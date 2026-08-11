package authlink

import (
	"strings"
	"testing"
	"time"
)

var key = []byte("test-secret-key")

func TestRoundTrip(t *testing.T) {
	tok := Sign(key, PurposeLogin, 42, time.Minute)
	got, err := Verify(key, PurposeLogin, tok)
	if err != nil || got != 42 {
		t.Fatalf("ожидали 42, получили %d, err=%v", got, err)
	}
}

// Токен, выданный для входа, НЕ должен приниматься как сессионный.
// Без разделения назначений короткоживущую ссылку можно было бы
// подставить вместо сессионной куки и наоборот.
func TestPurposeIsolation(t *testing.T) {
	tok := Sign(key, PurposeLogin, 42, time.Minute)
	if _, err := Verify(key, PurposeSession, tok); err == nil {
		t.Fatal("токен входа принят как сессионный")
	}
}

func TestExpired(t *testing.T) {
	tok := Sign(key, PurposeLogin, 42, -time.Second)
	if _, err := Verify(key, PurposeLogin, tok); err != ErrExpired {
		t.Fatalf("ожидали ErrExpired, получили %v", err)
	}
}

// Главный тест: подделка полезной нагрузки без знания ключа
// должна отвергаться. Меняем user_id в payload, подпись оставляем.
func TestTamperedPayload(t *testing.T) {
	tok := Sign(key, PurposeLogin, 42, time.Minute)
	other := Sign(key, PurposeLogin, 43, time.Minute)
	// подпись от токена 42, полезная нагрузка от токена 43
	forged := strings.Split(other, ".")[0] + "." + strings.Split(tok, ".")[1]
	if _, err := Verify(key, PurposeLogin, forged); err != ErrSignature {
		t.Fatalf("подделка прошла, err=%v", err)
	}
}

func TestWrongKey(t *testing.T) {
	tok := Sign(key, PurposeLogin, 42, time.Minute)
	if _, err := Verify([]byte("other-key"), PurposeLogin, tok); err != ErrSignature {
		t.Fatalf("токен принят с чужим ключом, err=%v", err)
	}
}
