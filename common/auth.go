/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-31 17:51:16
 * @Description: 安全认证管理器 - 基于 go-toolbox/sign 实现集群安全认证
 *
 * 提供两种认证机制:
 *   1. JoinSecret: Worker 加入集群时的预共享密钥验证（类似 K8s Bootstrap Token）
 *      - 支持多个令牌，每个有独立的 TTL 和使用次数限制
 *      - 格式: <token-id>.<secret>，如 abcdef.0123456789abcdef
 *   2. AdminToken: CLI 客户端访问 AdminService 的认证令牌
 *
 * 安全特性:
 *   - HMAC-SHA256 签名验证
 *   - AES 对称加密保护密钥传输
 *   - TOTP 时间窗口防重放
 *   - 可通过 EnableAuth 开关控制
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kamalyes/go-toolbox/pkg/mathx"
	"github.com/kamalyes/go-toolbox/pkg/random"
	"github.com/kamalyes/go-toolbox/pkg/sign"
	"google.golang.org/grpc/metadata"
)

// MetadataCarrier gRPC 元数据载体
type MetadataCarrier struct {
	md metadata.MD
}

// Get 从元数据中获取指定键的值
func (m *MetadataCarrier) Get(key string) string {
	values := m.md.Get(key)
	if len(values) > 0 {
		return values[0]
	}
	return ""
}

// ExtractMetadata 从 gRPC 上下文中提取元数据
func ExtractMetadata(ctx context.Context) (*MetadataCarrier, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, false
	}
	return &MetadataCarrier{md: md}, true
}

// AuthManager 安全认证管理器
type AuthManager struct {
	mu           sync.RWMutex                // 保护 joinSecrets 的并发访问
	enableAuth   bool                        // 是否启用安全认证
	joinSecrets  map[string]*JoinSecretEntry // 加入集群的预共享密钥（key 为 token ID）
	adminSecret  string                      // Admin 访问密钥
	signer       sign.Signer                 // HMAC 签名器
	tokenManager *TokenManager               // 令牌管理器
}

// AdminTokenPayload Admin 令牌载荷
type AdminTokenPayload struct {
	ClientID string `json:"client_id"` // 客户端标识
	Role     string `json:"role"`      // 角色（admin/viewer）
	IssuedAt int64  `json:"issued_at"` // 签发时间
}

// NewAuthManager 创建安全认证管理器
func NewAuthManager(config *MasterConfig) *AuthManager {
	signer, err := sign.NewHMACSigner(sign.AlgorithmSHA256)
	if err != nil {
		signer = nil
	}

	tokenManager := NewTokenManager(config.Secret, config.TokenExpiration, config.TokenIssuer)

	joinSecrets := make(map[string]*JoinSecretEntry)
	for _, entry := range config.JoinSecrets {
		if entry != nil {
			joinSecrets[entry.ID] = entry
		}
	}

	return &AuthManager{
		enableAuth:   config.EnableAuth,
		joinSecrets:  joinSecrets,
		adminSecret:  config.Secret,
		signer:       signer,
		tokenManager: tokenManager,
	}
}

// IsAuthEnabled 判断是否启用安全认证
func (am *AuthManager) IsAuthEnabled() bool {
	return am.enableAuth
}

// ValidateJoinSecret 验证 Worker 加入集群的预共享密钥
// 支持两种格式:
//   - 完整格式: <token-id>.<secret>，如 abcdef.0123456789abcdef
//   - 简单格式: 直接传入密钥字符串（向后兼容）
func (am *AuthManager) ValidateJoinSecret(token string) error {
	if !am.enableAuth {
		return nil
	}

	am.mu.RLock()
	defer am.mu.RUnlock()

	if len(am.joinSecrets) == 0 {
		return NewErrorWithCode(CodeJoinSecretRequired, ErrJoinSecretRequired)
	}

	if token == "" {
		return NewErrorWithCode(CodeJoinSecretRequired, ErrJoinSecretRequired)
	}

	entry := am.findJoinSecretEntry(token)
	if entry == nil {
		return NewErrorWithCode(CodeInvalidJoinSecret, ErrInvalidJoinSecret)
	}

	if entry.IsExpired() {
		return NewErrorWithCode(CodeJoinSecretExpired, ErrJoinSecretExpired)
	}

	if !entry.IsUsable() {
		return NewErrorWithCode(CodeJoinSecretExpired, ErrJoinSecretExpired)
	}

	return nil
}

// UseJoinSecret 使用一次加入密钥（验证通过后调用，扣减使用次数）
func (am *AuthManager) UseJoinSecret(token string) bool {
	am.mu.Lock()
	defer am.mu.Unlock()

	entry := am.findJoinSecretEntry(token)
	if entry == nil {
		return false
	}

	return entry.Use()
}

// findJoinSecretEntry 查找匹配的密钥条目
func (am *AuthManager) findJoinSecretEntry(token string) *JoinSecretEntry {
	if strings.Contains(token, ".") {
		parts := strings.SplitN(token, ".", 2)
		if len(parts) == 2 {
			if entry, ok := am.joinSecrets[parts[0]]; ok {
				if entry.Secret == parts[1] {
					return entry
				}
			}
		}
	}

	for _, entry := range am.joinSecrets {
		if entry.FullToken() == token {
			return entry
		}
	}

	return nil
}

// CreateJoinSecret 创建新的加入密钥（类似 kubeadm token create）
func (am *AuthManager) CreateJoinSecret(ttl time.Duration, description string, maxUsages int, labels map[string]string) *JoinSecretEntry {
	am.mu.Lock()
	defer am.mu.Unlock()

	tokenID := random.RandString(6, random.LOWERCASE|random.NUMBER)
	secret := random.RandString(16, random.LOWERCASE|random.NUMBER)

	if ttl == 0 {
		ttl = 24 * time.Hour
	}

	entry := &JoinSecretEntry{
		ID:          tokenID,
		Secret:      secret,
		Description: description,
		ExpiresAt:   time.Now().Add(ttl),
		TTL:         ttl,
		CreatedAt:   time.Now(),
		Usages:      maxUsages,
		MaxUsages:   maxUsages,
		Labels:      labels,
	}

	am.joinSecrets[tokenID] = entry
	return entry
}

// DeleteJoinSecret 删除加入密钥（类似 kubeadm token delete）
func (am *AuthManager) DeleteJoinSecret(tokenID string) bool {
	am.mu.Lock()
	defer am.mu.Unlock()

	if _, ok := am.joinSecrets[tokenID]; ok {
		delete(am.joinSecrets, tokenID)
		return true
	}
	return false
}

// ListJoinSecrets 列出所有加入密钥
func (am *AuthManager) ListJoinSecrets() []*JoinSecretEntry {
	am.mu.RLock()
	defer am.mu.RUnlock()

	result := make([]*JoinSecretEntry, 0, len(am.joinSecrets))
	for _, entry := range am.joinSecrets {
		result = append(result, entry)
	}
	return result
}

// CleanupExpiredSecrets 清理过期的密钥
func (am *AuthManager) CleanupExpiredSecrets() int {
	am.mu.Lock()
	defer am.mu.Unlock()

	count := 0
	for id, entry := range am.joinSecrets {
		if entry.IsExpired() {
			delete(am.joinSecrets, id)
			count++
		}
	}
	return count
}

// AuthenticateAdmin 验证 Admin 客户端凭据并返回认证令牌
func (am *AuthManager) AuthenticateAdmin(secret, clientID string) (string, error) {
	if !am.enableAuth {
		return "", NewErrorWithCode(CodeAuthDisabled, ErrAuthDisabled)
	}

	if secret != am.adminSecret {
		return "", NewErrorWithCode(CodeInvalidCredentials, ErrInvalidCredentials)
	}

	client := sign.NewSignerClient[AdminTokenPayload]().
		WithSecretKey([]byte(am.adminSecret)).
		WithExpiration(am.tokenManager.tokenExpiration).
		WithIssuer(am.tokenManager.tokenIssuer)

	if _, err := client.WithAlgorithm(sign.AlgorithmSHA256); err != nil {
		return fmt.Sprintf("admin-token-%s-%d", clientID, time.Now().Unix()), nil
	}

	token, err := client.Create(AdminTokenPayload{
		ClientID: clientID,
		Role:     "admin",
		IssuedAt: time.Now().Unix(),
	})
	if err != nil {
		return "", NewErrorWithCode(CodeAuthFailed, ErrAuthFailed)
	}

	return token, nil
}

// ValidateAdminToken 验证 Admin 认证令牌
func (am *AuthManager) ValidateAdminToken(token string) (*AdminTokenPayload, error) {
	if !am.enableAuth {
		return nil, nil
	}

	if token == "" {
		return nil, NewErrorWithCode(CodeAuthRequired, ErrAuthRequired)
	}

	client := sign.NewSignerClient[AdminTokenPayload]().
		WithSecretKey([]byte(am.adminSecret)).
		WithIssuer(am.tokenManager.tokenIssuer)

	if _, err := client.WithAlgorithm(sign.AlgorithmSHA256); err != nil {
		return nil, NewErrorWithCode(CodeInvalidAlgorithm, ErrInvalidAlgorithm)
	}

	signedMsg, _, err := client.Validate(token)
	if err != nil {
		return nil, NewErrorWithCode(CodeInvalidToken, ErrInvalidToken)
	}

	return &signedMsg.ExtraData, nil
}

// GenerateToken 为节点生成认证令牌（委托给 TokenManager）
func (am *AuthManager) GenerateToken(nodeID string) (string, error) {
	return am.tokenManager.GenerateToken(nodeID)
}

// ValidateToken 验证节点令牌（委托给 TokenManager）
func (am *AuthManager) ValidateToken(token string) (*TokenPayload, error) {
	return am.tokenManager.ValidateToken(token)
}

// EncryptSecret 使用 AES 加密密钥
func EncryptSecret(plainText string, key []byte) (string, error) {
	return sign.AesEncrypt(plainText, key)
}

// DecryptSecret 使用 AES 解密密钥
func DecryptSecret(cipherText string, key []byte) (string, error) {
	return sign.AesDecrypt(cipherText, key)
}

// GenerateSecretKey 生成指定长度的密钥字节
func GenerateSecretKey(password string, length int) []byte {
	return sign.GenerateByteKey(password, length)
}

// GenerateTOTPSecret 生成 TOTP 密钥
func GenerateTOTPSecret() string {
	return sign.GenerateTOTPSecret(20)
}

// ValidateTOTPCode 验证 TOTP 验证码
func ValidateTOTPCode(secret, code string) bool {
	return sign.ValidateTOTPCode(secret, code, sign.DefaultTOTPConfig())
}

// GenerateJoinSecretString 生成一个简单的加入密钥字符串（向后兼容）
func GenerateJoinSecretString() string {
	return random.RandString(6, random.LOWERCASE|random.NUMBER) + "." + random.RandString(16, random.LOWERCASE|random.NUMBER)
}

// ParseJoinSecretString 解析加入密钥字符串
func ParseJoinSecretString(token string) (id, secret string, ok bool) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// NewDefaultJoinSecretEntry 创建默认的加入密钥条目
func NewDefaultJoinSecretEntry(ttl time.Duration, description string) *JoinSecretEntry {
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	tokenID := random.RandString(6, random.LOWERCASE|random.NUMBER)
	secret := random.RandString(16, random.LOWERCASE|random.NUMBER)
	return &JoinSecretEntry{
		ID:          tokenID,
		Secret:      secret,
		Description: mathx.IfEmpty(description, "default join secret"),
		ExpiresAt:   time.Now().Add(ttl),
		TTL:         ttl,
		CreatedAt:   time.Now(),
		Usages:      0,
		MaxUsages:   0,
	}
}
