/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-31 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-31 00:00:00
 * @FilePath: \go-distributed\common\config_generator.go
 * @Description: 配置文件生成器 - 自动生成控制平面配置文件
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/kamalyes/go-logger"
)

// ConfigFileOptions 配置文件生成选项
type ConfigFileOptions struct {
	CurrentContext string
	ContextName    string
	ClusterName    string
	AuthInfoName   string
	ServerAddr     string
	Secret         string
	EnableAuth     bool
	Insecure       bool
}

// DefaultConfigFileOptions 返回默认的配置文件选项
func DefaultConfigFileOptions(serverAddr, secret string) *ConfigFileOptions {
	return &ConfigFileOptions{
		CurrentContext: "default",
		ContextName:    "default",
		ClusterName:    "local-cluster",
		AuthInfoName:   "local-auth",
		ServerAddr:     serverAddr,
		Secret:         secret,
		EnableAuth:     true,
		Insecure:       true,
	}
}

// GenerateConfigFile 自动生成控制平面配置文件
// joinSecret: Worker 加入集群的密钥
// serverAddr: Master gRPC 服务地址
// log: 日志实例
func GenerateConfigFile(joinSecret, serverAddr string, log logger.ILogger) {
	options := DefaultConfigFileOptions(serverAddr, joinSecret)
	GenerateConfigFileWithOptions(options, log)
}

// GenerateConfigFileWithOptions 使用自定义选项生成控制平面配置文件
// options: 配置文件选项
// log: 日志实例
func GenerateConfigFileWithOptions(options *ConfigFileOptions, log logger.ILogger) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.ErrorKV("Failed to get home directory", "error", err.Error())
		return
	}

	configDir := filepath.Join(homeDir, ".go-distributed")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		log.ErrorKV("Failed to create config directory", "error", err.Error())
		return
	}

	configPath := filepath.Join(configDir, "config.yaml")

	configContent := fmt.Sprintf(`current_context: %s
contexts:
  - name: %s
    cluster_name: %s
    auth_info: %s
clusters:
  - name: %s
    server: %s
    insecure: %t
auth_infos:
  - name: %s
    secret: %s
    enable_auth: %t
`,
		options.CurrentContext,
		options.ContextName,
		options.ClusterName,
		options.AuthInfoName,
		options.ClusterName,
		options.ServerAddr,
		options.Insecure,
		options.AuthInfoName,
		options.Secret,
		options.EnableAuth)

	if err := os.WriteFile(configPath, []byte(configContent), 0600); err != nil {
		log.ErrorKV("Failed to write config file", "path", configPath, "error", err.Error())
		return
	}

	log.InfoKV("Config file generated successfully", "path", configPath)
}
