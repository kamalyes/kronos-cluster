/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-31 17:51:16
 * @Description: 安全认证管理器测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

func newTestAuthManager(enableAuth bool, secrets []*JoinSecretEntry) *AuthManager {
	config := &MasterConfig{
		EnableAuth:      enableAuth,
		Secret:          "test-secret",
		TokenExpiration: 1 * time.Hour,
		TokenIssuer:     "test-issuer",
		JoinSecrets:     secrets,
	}
	return NewAuthManager(config)
}

func TestNewAuthManager(t *testing.T) {
	am := newTestAuthManager(true, nil)
	assert.NotNil(t, am)
	assert.True(t, am.IsAuthEnabled())
}

func TestNewAuthManagerDisabled(t *testing.T) {
	am := newTestAuthManager(false, nil)
	assert.NotNil(t, am)
	assert.False(t, am.IsAuthEnabled())
}

func TestNewAuthManagerWithJoinSecrets(t *testing.T) {
	secrets := []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 10, Usages: 10},
		{ID: "def456", Secret: "secret456", ExpiresAt: time.Now().Add(2 * time.Hour), MaxUsages: 0, Usages: 0},
	}
	am := newTestAuthManager(true, secrets)
	assert.NotNil(t, am)

	list := am.ListJoinSecrets()
	assert.Len(t, list, 2)
}

func TestValidateJoinSecretDisabled(t *testing.T) {
	am := newTestAuthManager(false, nil)
	err := am.ValidateJoinSecret("")
	assert.NoError(t, err)
}

func TestValidateJoinSecretEmptyToken(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 10, Usages: 10},
	})
	err := am.ValidateJoinSecret("")
	assert.Error(t, err)
}

func TestValidateJoinSecretNoSecrets(t *testing.T) {
	am := newTestAuthManager(true, nil)
	err := am.ValidateJoinSecret("some.token")
	assert.Error(t, err)
}

func TestValidateJoinSecretValidToken(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 10, Usages: 10},
	})
	err := am.ValidateJoinSecret("abc123.secret123")
	assert.NoError(t, err)
}

func TestValidateJoinSecretInvalidToken(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 10, Usages: 10},
	})
	err := am.ValidateJoinSecret("abc123.wrongsecret")
	assert.Error(t, err)
}

func TestValidateJoinSecretExpiredToken(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(-1 * time.Hour), MaxUsages: 10, Usages: 10},
	})
	err := am.ValidateJoinSecret("abc123.secret123")
	assert.Error(t, err)
}

func TestValidateJoinSecretNoUsagesLeft(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 1, Usages: 0},
	})
	err := am.ValidateJoinSecret("abc123.secret123")
	assert.Error(t, err)
}

func TestUseJoinSecret(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 2, Usages: 2},
	})

	ok := am.UseJoinSecret("abc123.secret123")
	assert.True(t, ok)

	secrets := am.ListJoinSecrets()
	for _, s := range secrets {
		if s.ID == "abc123" {
			assert.Equal(t, 1, s.Usages)
		}
	}
}

func TestUseJoinSecretNotFound(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 2, Usages: 2},
	})
	ok := am.UseJoinSecret("nonexistent.token")
	assert.False(t, ok)
}

func TestCreateJoinSecret(t *testing.T) {
	am := newTestAuthManager(true, nil)

	entry := am.CreateJoinSecret(2*time.Hour, "test token", 5, map[string]string{"env": "test"})
	assert.NotNil(t, entry)
	assert.NotEmpty(t, entry.ID)
	assert.NotEmpty(t, entry.Secret)
	assert.Equal(t, "test token", entry.Description)
	assert.Equal(t, 5, entry.MaxUsages)
	assert.False(t, entry.ExpiresAt.IsZero())

	list := am.ListJoinSecrets()
	assert.Len(t, list, 1)
}

func TestCreateJoinSecretDefaultTTL(t *testing.T) {
	am := newTestAuthManager(true, nil)
	entry := am.CreateJoinSecret(0, "", 0, nil)
	assert.NotNil(t, entry)
	assert.Equal(t, 24*time.Hour, entry.TTL)
}

func TestDeleteJoinSecret(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 10, Usages: 10},
	})

	ok := am.DeleteJoinSecret("abc123")
	assert.True(t, ok)

	list := am.ListJoinSecrets()
	assert.Len(t, list, 0)
}

func TestDeleteJoinSecretNotFound(t *testing.T) {
	am := newTestAuthManager(true, nil)
	ok := am.DeleteJoinSecret("nonexistent")
	assert.False(t, ok)
}

func TestCleanupExpiredSecrets(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "expired1", Secret: "s1", ExpiresAt: time.Now().Add(-1 * time.Hour), MaxUsages: 10, Usages: 10},
		{ID: "expired2", Secret: "s2", ExpiresAt: time.Now().Add(-2 * time.Hour), MaxUsages: 10, Usages: 10},
		{ID: "valid1", Secret: "s3", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 10, Usages: 10},
	})

	count := am.CleanupExpiredSecrets()
	assert.Equal(t, 2, count)

	list := am.ListJoinSecrets()
	assert.Len(t, list, 1)
	assert.Equal(t, "valid1", list[0].ID)
}

func TestAuthenticateAdminDisabled(t *testing.T) {
	am := newTestAuthManager(false, nil)
	token, err := am.AuthenticateAdmin("test-secret", "client1")
	assert.Error(t, err)
	assert.Empty(t, token)
}

func TestAuthenticateAdminWrongSecret(t *testing.T) {
	am := newTestAuthManager(true, nil)
	token, err := am.AuthenticateAdmin("wrong-secret", "client1")
	assert.Error(t, err)
	assert.Empty(t, token)
}

func TestAuthenticateAdminSuccess(t *testing.T) {
	am := newTestAuthManager(true, nil)
	token, err := am.AuthenticateAdmin("test-secret", "client1")
	assert.NoError(t, err)
	assert.NotEmpty(t, token)
}

func TestValidateAdminTokenDisabled(t *testing.T) {
	am := newTestAuthManager(false, nil)
	payload, err := am.ValidateAdminToken("some-token")
	assert.NoError(t, err)
	assert.Nil(t, payload)
}

func TestValidateAdminTokenEmpty(t *testing.T) {
	am := newTestAuthManager(true, nil)
	payload, err := am.ValidateAdminToken("")
	assert.Error(t, err)
	assert.Nil(t, payload)
}

func TestValidateAdminTokenInvalid(t *testing.T) {
	am := newTestAuthManager(true, nil)
	payload, err := am.ValidateAdminToken("invalid-token")
	assert.Error(t, err)
	assert.Nil(t, payload)
}

func TestAuthenticateAndValidateAdminToken(t *testing.T) {
	am := newTestAuthManager(true, nil)
	token, err := am.AuthenticateAdmin("test-secret", "client1")
	assert.NoError(t, err)

	payload, err := am.ValidateAdminToken(token)
	assert.NoError(t, err)
	assert.NotNil(t, payload)
	assert.Equal(t, "client1", payload.ClientID)
	assert.Equal(t, "admin", payload.Role)
}

func TestGenerateAndValidateToken(t *testing.T) {
	am := newTestAuthManager(true, nil)
	token, err := am.GenerateToken("node-1")
	assert.NoError(t, err)
	assert.NotEmpty(t, token)

	payload, err := am.ValidateToken(token)
	assert.NoError(t, err)
	assert.NotNil(t, payload)
	assert.Equal(t, "node-1", payload.NodeID)
}

func TestEncryptDecryptSecret(t *testing.T) {
	key := GenerateSecretKey("test-password", 32)
	plainText := "my-secret-value"

	encrypted, err := EncryptSecret(plainText, key)
	assert.NoError(t, err)
	assert.NotEqual(t, plainText, encrypted)

	decrypted, err := DecryptSecret(encrypted, key)
	assert.NoError(t, err)
	assert.Equal(t, plainText, decrypted)
}

func TestGenerateSecretKey(t *testing.T) {
	key := GenerateSecretKey("password", 32)
	assert.Len(t, key, 32)
}

func TestGenerateTOTPSecret(t *testing.T) {
	secret := GenerateTOTPSecret()
	assert.NotEmpty(t, secret)
}

func TestGenerateJoinSecretString(t *testing.T) {
	token := GenerateJoinSecretString()
	assert.NotEmpty(t, token)
	assert.Contains(t, token, ".")
}

func TestParseJoinSecretString(t *testing.T) {
	id, secret, ok := ParseJoinSecretString("abc123.secret456def")
	assert.True(t, ok)
	assert.Equal(t, "abc123", id)
	assert.Equal(t, "secret456def", secret)
}

func TestParseJoinSecretStringInvalid(t *testing.T) {
	_, _, ok := ParseJoinSecretString("invalidtoken")
	assert.False(t, ok)
}

func TestNewDefaultJoinSecretEntry(t *testing.T) {
	entry := NewDefaultJoinSecretEntry(1*time.Hour, "test entry")
	assert.NotNil(t, entry)
	assert.NotEmpty(t, entry.ID)
	assert.NotEmpty(t, entry.Secret)
	assert.Equal(t, "test entry", entry.Description)
	assert.False(t, entry.ExpiresAt.IsZero())
}

func TestNewDefaultJoinSecretEntryDefaultTTL(t *testing.T) {
	entry := NewDefaultJoinSecretEntry(0, "")
	assert.NotNil(t, entry)
	assert.Equal(t, 24*time.Hour, entry.TTL)
	assert.Equal(t, "default join secret", entry.Description)
}

func TestExtractMetadata(t *testing.T) {
	md := metadata.Pairs("authorization", "bearer-token-123")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	carrier, ok := ExtractMetadata(ctx)
	assert.True(t, ok)
	assert.NotNil(t, carrier)
	assert.Equal(t, "bearer-token-123", carrier.Get("authorization"))
}

func TestExtractMetadataNotFound(t *testing.T) {
	ctx := context.Background()
	carrier, ok := ExtractMetadata(ctx)
	assert.False(t, ok)
	assert.Nil(t, carrier)
}

func TestMetadataCarrierGetEmpty(t *testing.T) {
	md := metadata.MD{}
	carrier := &MetadataCarrier{md: md}
	assert.Equal(t, "", carrier.Get("nonexistent"))
}

func TestJoinSecretEntryFullToken(t *testing.T) {
	entry := &JoinSecretEntry{ID: "abc123", Secret: "secret456"}
	assert.Equal(t, "abc123.secret456", entry.FullToken())
}

func TestJoinSecretEntryIsExpired(t *testing.T) {
	entry := &JoinSecretEntry{ExpiresAt: time.Now().Add(-1 * time.Hour)}
	assert.True(t, entry.IsExpired())
}

func TestJoinSecretEntryIsNotExpired(t *testing.T) {
	entry := &JoinSecretEntry{ExpiresAt: time.Now().Add(1 * time.Hour)}
	assert.False(t, entry.IsExpired())
}

func TestJoinSecretEntryIsExpiredZeroTime(t *testing.T) {
	entry := &JoinSecretEntry{ExpiresAt: time.Time{}}
	assert.False(t, entry.IsExpired())
}

func TestJoinSecretEntryIsUsable(t *testing.T) {
	entry := &JoinSecretEntry{
		ExpiresAt: time.Now().Add(1 * time.Hour),
		MaxUsages: 10,
		Usages:    5,
	}
	assert.True(t, entry.IsUsable())
}

func TestJoinSecretEntryIsUsableExpired(t *testing.T) {
	entry := &JoinSecretEntry{
		ExpiresAt: time.Now().Add(-1 * time.Hour),
		MaxUsages: 10,
		Usages:    5,
	}
	assert.False(t, entry.IsUsable())
}

func TestJoinSecretEntryIsUsableNoUsagesLeft(t *testing.T) {
	entry := &JoinSecretEntry{
		ExpiresAt: time.Now().Add(1 * time.Hour),
		MaxUsages: 1,
		Usages:    0,
	}
	assert.False(t, entry.IsUsable())
}

func TestJoinSecretEntryIsUsableUnlimited(t *testing.T) {
	entry := &JoinSecretEntry{
		ExpiresAt: time.Now().Add(1 * time.Hour),
		MaxUsages: 0,
		Usages:    0,
	}
	assert.True(t, entry.IsUsable())
}

func TestJoinSecretEntryUse(t *testing.T) {
	entry := &JoinSecretEntry{
		ExpiresAt: time.Now().Add(1 * time.Hour),
		MaxUsages: 2,
		Usages:    2,
	}
	ok := entry.Use()
	assert.True(t, ok)
	assert.Equal(t, 1, entry.Usages)

	ok = entry.Use()
	assert.True(t, ok)
	assert.Equal(t, 0, entry.Usages)

	ok = entry.Use()
	assert.False(t, ok)
}

func TestJoinSecretEntryUseUnlimited(t *testing.T) {
	entry := &JoinSecretEntry{
		ExpiresAt: time.Now().Add(1 * time.Hour),
		MaxUsages: 0,
		Usages:    0,
	}
	ok := entry.Use()
	assert.True(t, ok)
	assert.Equal(t, 0, entry.Usages)
}

func TestFindJoinSecretEntryDotFormat(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 10, Usages: 10},
	})
	entry := am.findJoinSecretEntry("abc123.secret123")
	assert.NotNil(t, entry)
	assert.Equal(t, "abc123", entry.ID)
}

func TestFindJoinSecretEntryFullTokenFormat(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 10, Usages: 10},
	})
	entry := am.findJoinSecretEntry("abc123.secret123")
	assert.NotNil(t, entry)
}

func TestFindJoinSecretEntryNotFound(t *testing.T) {
	am := newTestAuthManager(true, []*JoinSecretEntry{
		{ID: "abc123", Secret: "secret123", ExpiresAt: time.Now().Add(1 * time.Hour), MaxUsages: 10, Usages: 10},
	})
	entry := am.findJoinSecretEntry("xyz789.nonexistent")
	assert.Nil(t, entry)
}
