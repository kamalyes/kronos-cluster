/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-31 17:51:16
 * @Description: 控制平面配置管理 - 类似 Kubernetes kubeconfig 的配置管理
 *
 * 支持从配置文件加载控制平面配置，CLI 客户端通过配置文件
 * 而非直接指定地址和端口来连接 Master，提高安全性和便捷性
 *
 * 配置文件格式支持 JSON 和 YAML，默认路径:
 *   - $HOME/.go-distributed/config.yaml
 *   - $HOME/.go-distributed/config.json
 *   - /etc/go-distributed/config.yaml
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kamalyes/go-toolbox/pkg/convert"
	"github.com/kamalyes/go-toolbox/pkg/mathx"
)

const (
	DefaultConfigDir  = ".go-distributed"
	DefaultConfigFile = "config.yaml"
)

// ControlPlaneContext 控制平面上下文
type ControlPlaneContext struct {
	Name        string `json:"name" yaml:"name"`                 // 上下文名称
	ClusterName string `json:"cluster_name" yaml:"cluster_name"` // 集群名称
	AuthInfo    string `json:"auth_info" yaml:"auth_info"`       // 认证信息名称
}

// ControlPlaneCluster 集群配置
type ControlPlaneCluster struct {
	Name     string `json:"name" yaml:"name"`         // 集群名称
	Server   string `json:"server" yaml:"server"`     // Master gRPC 地址
	CACert   string `json:"ca_cert" yaml:"ca_cert"`   // CA 证书路径
	Insecure bool   `json:"insecure" yaml:"insecure"` // 是否跳过 TLS 验证
}

// ControlPlaneAuthInfo 认证信息
type ControlPlaneAuthInfo struct {
	Name       string `json:"name" yaml:"name"`               // 认证信息名称
	Secret     string `json:"secret" yaml:"secret"`           // 访问密钥
	Token      string `json:"token" yaml:"token"`             // 已获取的认证令牌
	EnableAuth bool   `json:"enable_auth" yaml:"enable_auth"` // 是否启用认证
}

// ControlPlaneConfigFile 控制平面配置文件
type ControlPlaneConfigFile struct {
	CurrentContext string                 `json:"current_context" yaml:"current_context"` // 当前使用的上下文
	Contexts       []ControlPlaneContext  `json:"contexts" yaml:"contexts"`               // 上下文列表
	Clusters       []ControlPlaneCluster  `json:"clusters" yaml:"clusters"`               // 集群列表
	AuthInfos      []ControlPlaneAuthInfo `json:"auth_infos" yaml:"auth_infos"`           // 认证信息列表
}

// LoadControlPlaneConfig 从文件加载控制平面配置
func LoadControlPlaneConfig(path string) (*ControlPlaneConfigFile, error) {
	if path == "" {
		path = getDefaultConfigPath()
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf(ErrFailedReadConfigFile, err)
	}

	config := &ControlPlaneConfigFile{}
	ext := filepath.Ext(path)
	switch ext {
	case ".yaml", ".yml":
		parsed, err := convert.UnmarshalYAML[ControlPlaneConfigFile](data)
		if err != nil {
			return nil, fmt.Errorf(ErrFailedParseYAMLConfig, err)
		}
		config = parsed
	case ".json":
		parsed, err := convert.UnmarshalJSON[ControlPlaneConfigFile](data)
		if err != nil {
			return nil, fmt.Errorf(ErrFailedParseJSONConfig, err)
		}
		config = parsed
	default:
		parsed, err := convert.UnmarshalYAML[ControlPlaneConfigFile](data)
		if err != nil {
			return nil, fmt.Errorf(ErrFailedParseConfig, err)
		}
		config = parsed
	}

	return config, nil
}

// SaveControlPlaneConfig 保存控制平面配置到文件
func SaveControlPlaneConfig(config *ControlPlaneConfigFile, path string) error {
	if path == "" {
		path = getDefaultConfigPath()
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf(ErrFailedCreateConfigDir, err)
	}

	var data []byte
	var err error
	ext := filepath.Ext(path)
	switch ext {
	case ".json":
		data, err = json.MarshalIndent(config, "", "  ")
	default:
		data, err = convert.MarshalYAML(config)
	}

	if err != nil {
		return fmt.Errorf(ErrFailedMarshalConfig, err)
	}

	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf(ErrFailedWriteConfigFile, err)
	}

	return nil
}

// ResolveCurrentConfig 解析当前上下文的完整配置
func (c *ControlPlaneConfigFile) ResolveCurrentConfig() (*ControlPlaneConfig, error) {
	ctx, err := c.GetCurrentContext()
	if err != nil {
		return nil, err
	}

	cluster, err := c.GetCluster(ctx.ClusterName)
	if err != nil {
		return nil, err
	}

	authInfo, err := c.GetAuthInfo(ctx.AuthInfo)
	if err != nil {
		return nil, err
	}

	return &ControlPlaneConfig{
		ServerAddr:  cluster.Server,
		Secret:      authInfo.Secret,
		EnableAuth:  authInfo.EnableAuth,
		Token:       authInfo.Token,
		CACertFile:  cluster.CACert,
		Insecure:    cluster.Insecure,
		ClusterName: cluster.Name,
	}, nil
}

// GetCurrentContext 获取当前上下文
func (c *ControlPlaneConfigFile) GetCurrentContext() (*ControlPlaneContext, error) {
	for _, ctx := range c.Contexts {
		if ctx.Name == c.CurrentContext {
			return &ctx, nil
		}
	}

	if len(c.Contexts) > 0 {
		return &c.Contexts[0], nil
	}

	return nil, fmt.Errorf(ErrNoContextFound)
}

// GetCluster 根据名称获取集群配置
func (c *ControlPlaneConfigFile) GetCluster(name string) (*ControlPlaneCluster, error) {
	for _, cluster := range c.Clusters {
		if cluster.Name == name {
			return &cluster, nil
		}
	}
	return nil, fmt.Errorf(ErrClusterNotFound, name)
}

// GetAuthInfo 根据名称获取认证信息
func (c *ControlPlaneConfigFile) GetAuthInfo(name string) (*ControlPlaneAuthInfo, error) {
	for _, authInfo := range c.AuthInfos {
		if authInfo.Name == name {
			return &authInfo, nil
		}
	}
	return nil, fmt.Errorf(ErrAuthInfoNotFound, name)
}

// AddContext 添加上下文
func (c *ControlPlaneConfigFile) AddContext(ctx ControlPlaneContext) {
	c.Contexts = append(c.Contexts, ctx)
	if c.CurrentContext == "" {
		c.CurrentContext = ctx.Name
	}
}

// AddCluster 添加集群配置
func (c *ControlPlaneConfigFile) AddCluster(cluster ControlPlaneCluster) {
	c.Clusters = append(c.Clusters, cluster)
}

// AddAuthInfo 添加认证信息
func (c *ControlPlaneConfigFile) AddAuthInfo(authInfo ControlPlaneAuthInfo) {
	c.AuthInfos = append(c.AuthInfos, authInfo)
}

// SetCurrentContext 设置当前上下文
func (c *ControlPlaneConfigFile) SetCurrentContext(name string) error {
	for _, ctx := range c.Contexts {
		if ctx.Name == name {
			c.CurrentContext = name
			return nil
		}
	}
	return fmt.Errorf(ErrContextNotFound, name)
}

// NewDefaultControlPlaneConfig 创建默认控制平面配置
func NewDefaultControlPlaneConfig(serverAddr, secret string, enableAuth bool) *ControlPlaneConfigFile {
	clusterName := mathx.IfEmpty(secret, "default")
	if len(clusterName) > 8 {
		clusterName = clusterName[:8]
	}

	return &ControlPlaneConfigFile{
		CurrentContext: "default",
		Contexts: []ControlPlaneContext{
			{
				Name:        "default",
				ClusterName: clusterName,
				AuthInfo:    "default",
			},
		},
		Clusters: []ControlPlaneCluster{
			{
				Name:     clusterName,
				Server:   serverAddr,
				Insecure: true,
			},
		},
		AuthInfos: []ControlPlaneAuthInfo{
			{
				Name:       "default",
				Secret:     secret,
				EnableAuth: enableAuth,
			},
		},
	}
}

// getDefaultConfigPath 获取默认配置文件路径
func getDefaultConfigPath() string {
	homeDir, err := os.UserHomeDir()
	if err == nil && homeDir != "" {
		return filepath.Join(homeDir, DefaultConfigDir, DefaultConfigFile)
	}
	return filepath.Join("/etc", "go-distributed", DefaultConfigFile)
}

// ControlPlaneConfigFromMasterConfig 从 MasterConfig 创建 ControlPlaneConfig
func ControlPlaneConfigFromMasterConfig(config *MasterConfig) *ControlPlaneConfig {
	cp := &ControlPlaneConfig{
		ServerAddr: fmt.Sprintf("localhost:%d", config.GRPCPort),
		EnableAuth: config.EnableAuth,
		Insecure:   !config.EnableTLS,
	}

	if config.ControlPlane != nil {
		cp = config.ControlPlane
	}

	if cp.ServerAddr == "" {
		cp.ServerAddr = fmt.Sprintf("localhost:%d", config.GRPCPort)
	}

	return cp
}

// TokenRefreshThreshold 令牌刷新阈值
const TokenRefreshThreshold = 5 * time.Minute

// ShouldRefreshToken 判断令牌是否需要刷新
func ShouldRefreshToken(expiresAt time.Time) bool {
	if expiresAt.IsZero() {
		return true
	}
	return time.Until(expiresAt) < TokenRefreshThreshold
}
