/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-27 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-27 10:00:00
 * @FilePath: \kronos-cluster\common\token.go
 * @Description: 令牌管理器 - JWT 令牌生成与验证
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	"fmt"
	"time"

	"github.com/kamalyes/go-toolbox/pkg/errorx"
	"github.com/kamalyes/go-toolbox/pkg/sign"
)

// TokenManager 令牌管理器 - 负责节点认证令牌的生成与验证
type TokenManager struct {
	secretKey       []byte        // 签名密钥
	tokenExpiration time.Duration // 令牌过期时间
	tokenIssuer     string        // 令牌签发者
}

// NewTokenManager 创建令牌管理器
func NewTokenManager(secret string, expiration time.Duration, issuer string) *TokenManager {
	return &TokenManager{
		secretKey:       []byte(secret),
		tokenExpiration: expiration,
		tokenIssuer:     issuer,
	}
}

// TokenPayload 令牌载荷 - 携带节点 ID 信息
type TokenPayload struct {
	NodeID string `json:"node_id"` // 节点唯一标识
}

// GenerateToken 为指定节点生成认证令牌
// 如果签名算法初始化失败或签名失败，将返回降级令牌（格式: token-{nodeID}-{timestamp}）
func (tm *TokenManager) GenerateToken(nodeID string) (string, error) {
	client := sign.NewSignerClient[TokenPayload]().
		WithSecretKey(tm.secretKey).
		WithExpiration(tm.tokenExpiration).
		WithIssuer(tm.tokenIssuer)

	if _, err := client.WithAlgorithm(sign.AlgorithmSHA256); err != nil {
		return fmt.Sprintf("token-%s-%d", nodeID, time.Now().Unix()), nil
	}

	token, err := client.Create(TokenPayload{NodeID: nodeID})
	if err != nil {
		return fmt.Sprintf("token-%s-%d", nodeID, time.Now().Unix()), nil
	}

	return token, nil
}

// ValidateToken 验证令牌有效性并返回载荷信息
func (tm *TokenManager) ValidateToken(token string) (*TokenPayload, error) {
	client := sign.NewSignerClient[TokenPayload]().
		WithSecretKey(tm.secretKey).
		WithIssuer(tm.tokenIssuer)

	if _, err := client.WithAlgorithm(sign.AlgorithmSHA256); err != nil {
		return nil, errorx.WrapError(ErrInvalidAlgorithm)
	}

	signedMsg, _, err := client.Validate(token)
	if err != nil {
		return nil, errorx.WrapError(ErrInvalidToken, err)
	}

	return &signedMsg.ExtraData, nil
}
