/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-28 18:09:18
 * @Description: 控制平面配置管理测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestControlPlaneConfigFileGetCurrentContext(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		CurrentContext: "default",
		Contexts: []ControlPlaneContext{
			{Name: "default", ClusterName: "cluster1", AuthInfo: "admin"},
			{Name: "staging", ClusterName: "cluster2", AuthInfo: "viewer"},
		},
	}

	ctx, err := cf.GetCurrentContext()
	assert.NoError(t, err)
	assert.Equal(t, "default", ctx.Name)
	assert.Equal(t, "cluster1", ctx.ClusterName)
}

func TestControlPlaneConfigFileGetCurrentContextFallback(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		CurrentContext: "nonexistent",
		Contexts: []ControlPlaneContext{
			{Name: "default", ClusterName: "cluster1", AuthInfo: "admin"},
		},
	}

	ctx, err := cf.GetCurrentContext()
	assert.NoError(t, err)
	assert.Equal(t, "default", ctx.Name)
}

func TestControlPlaneConfigFileGetCurrentContextEmpty(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		CurrentContext: "nonexistent",
		Contexts:       []ControlPlaneContext{},
	}

	ctx, err := cf.GetCurrentContext()
	assert.Error(t, err)
	assert.Nil(t, ctx)
}

func TestControlPlaneConfigFileGetCluster(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		Clusters: []ControlPlaneCluster{
			{Name: "cluster1", Server: "localhost:9000", Insecure: true},
			{Name: "cluster2", Server: "localhost:9001", Insecure: false},
		},
	}

	cluster, err := cf.GetCluster("cluster1")
	assert.NoError(t, err)
	assert.Equal(t, "localhost:9000", cluster.Server)
	assert.True(t, cluster.Insecure)
}

func TestControlPlaneConfigFileGetClusterNotFound(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		Clusters: []ControlPlaneCluster{},
	}

	cluster, err := cf.GetCluster("nonexistent")
	assert.Error(t, err)
	assert.Nil(t, cluster)
}

func TestControlPlaneConfigFileGetAuthInfo(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		AuthInfos: []ControlPlaneAuthInfo{
			{Name: "admin", Secret: "secret1", EnableAuth: true},
			{Name: "viewer", Secret: "secret2", EnableAuth: false},
		},
	}

	authInfo, err := cf.GetAuthInfo("admin")
	assert.NoError(t, err)
	assert.Equal(t, "secret1", authInfo.Secret)
	assert.True(t, authInfo.EnableAuth)
}

func TestControlPlaneConfigFileGetAuthInfoNotFound(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		AuthInfos: []ControlPlaneAuthInfo{},
	}

	authInfo, err := cf.GetAuthInfo("nonexistent")
	assert.Error(t, err)
	assert.Nil(t, authInfo)
}

func TestControlPlaneConfigFileResolveCurrentConfig(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		CurrentContext: "default",
		Contexts: []ControlPlaneContext{
			{Name: "default", ClusterName: "cluster1", AuthInfo: "admin"},
		},
		Clusters: []ControlPlaneCluster{
			{Name: "cluster1", Server: "localhost:9000", Insecure: true},
		},
		AuthInfos: []ControlPlaneAuthInfo{
			{Name: "admin", Secret: "my-secret", EnableAuth: true},
		},
	}

	config, err := cf.ResolveCurrentConfig()
	assert.NoError(t, err)
	assert.NotNil(t, config)
	assert.Equal(t, "localhost:9000", config.ServerAddr)
	assert.Equal(t, "my-secret", config.Secret)
	assert.True(t, config.EnableAuth)
	assert.True(t, config.Insecure)
}

func TestControlPlaneConfigFileResolveCurrentConfigNoCluster(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		CurrentContext: "default",
		Contexts: []ControlPlaneContext{
			{Name: "default", ClusterName: "nonexistent", AuthInfo: "admin"},
		},
		Clusters: []ControlPlaneCluster{},
		AuthInfos: []ControlPlaneAuthInfo{
			{Name: "admin", Secret: "my-secret", EnableAuth: true},
		},
	}

	config, err := cf.ResolveCurrentConfig()
	assert.Error(t, err)
	assert.Nil(t, config)
}

func TestControlPlaneConfigFileResolveCurrentConfigNoAuthInfo(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		CurrentContext: "default",
		Contexts: []ControlPlaneContext{
			{Name: "default", ClusterName: "cluster1", AuthInfo: "nonexistent"},
		},
		Clusters: []ControlPlaneCluster{
			{Name: "cluster1", Server: "localhost:9000", Insecure: true},
		},
		AuthInfos: []ControlPlaneAuthInfo{},
	}

	config, err := cf.ResolveCurrentConfig()
	assert.Error(t, err)
	assert.Nil(t, config)
}

func TestControlPlaneConfigFileAddContext(t *testing.T) {
	cf := &ControlPlaneConfigFile{}
	cf.AddContext(ControlPlaneContext{Name: "ctx1", ClusterName: "c1", AuthInfo: "a1"})
	assert.Len(t, cf.Contexts, 1)
	assert.Equal(t, "ctx1", cf.CurrentContext)

	cf.AddContext(ControlPlaneContext{Name: "ctx2", ClusterName: "c2", AuthInfo: "a2"})
	assert.Len(t, cf.Contexts, 2)
	assert.Equal(t, "ctx1", cf.CurrentContext)
}

func TestControlPlaneConfigFileAddCluster(t *testing.T) {
	cf := &ControlPlaneConfigFile{}
	cf.AddCluster(ControlPlaneCluster{Name: "c1", Server: "localhost:9000"})
	assert.Len(t, cf.Clusters, 1)
}

func TestControlPlaneConfigFileAddAuthInfo(t *testing.T) {
	cf := &ControlPlaneConfigFile{}
	cf.AddAuthInfo(ControlPlaneAuthInfo{Name: "a1", Secret: "secret1"})
	assert.Len(t, cf.AuthInfos, 1)
}

func TestControlPlaneConfigFileSetCurrentContext(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		Contexts: []ControlPlaneContext{
			{Name: "default", ClusterName: "c1", AuthInfo: "a1"},
			{Name: "staging", ClusterName: "c2", AuthInfo: "a2"},
		},
	}

	err := cf.SetCurrentContext("staging")
	assert.NoError(t, err)
	assert.Equal(t, "staging", cf.CurrentContext)
}

func TestControlPlaneConfigFileSetCurrentContextNotFound(t *testing.T) {
	cf := &ControlPlaneConfigFile{
		Contexts: []ControlPlaneContext{
			{Name: "default", ClusterName: "c1", AuthInfo: "a1"},
		},
	}

	err := cf.SetCurrentContext("nonexistent")
	assert.Error(t, err)
}

func TestNewDefaultControlPlaneConfig(t *testing.T) {
	cf := NewDefaultControlPlaneConfig("localhost:9000", "my-secret", true)
	assert.NotNil(t, cf)
	assert.Equal(t, "default", cf.CurrentContext)
	assert.Len(t, cf.Contexts, 1)
	assert.Len(t, cf.Clusters, 1)
	assert.Len(t, cf.AuthInfos, 1)
	assert.Equal(t, "localhost:9000", cf.Clusters[0].Server)
	assert.Equal(t, "my-secret", cf.AuthInfos[0].Secret)
	assert.True(t, cf.AuthInfos[0].EnableAuth)
}

func TestSaveAndLoadControlPlaneConfigYAML(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.yaml")

	cf := &ControlPlaneConfigFile{
		CurrentContext: "default",
		Contexts: []ControlPlaneContext{
			{Name: "default", ClusterName: "cluster1", AuthInfo: "admin"},
		},
		Clusters: []ControlPlaneCluster{
			{Name: "cluster1", Server: "localhost:9000", Insecure: true},
		},
		AuthInfos: []ControlPlaneAuthInfo{
			{Name: "admin", Secret: "my-secret", EnableAuth: true},
		},
	}

	err := SaveControlPlaneConfig(cf, path)
	require.NoError(t, err)

	loaded, err := LoadControlPlaneConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "default", loaded.CurrentContext)
	assert.Len(t, loaded.Contexts, 1)
	assert.Len(t, loaded.Clusters, 1)
	assert.Len(t, loaded.AuthInfos, 1)
	assert.Equal(t, "localhost:9000", loaded.Clusters[0].Server)
}

func TestSaveAndLoadControlPlaneConfigJSON(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")

	cf := &ControlPlaneConfigFile{
		CurrentContext: "default",
		Contexts: []ControlPlaneContext{
			{Name: "default", ClusterName: "cluster1", AuthInfo: "admin"},
		},
		Clusters: []ControlPlaneCluster{
			{Name: "cluster1", Server: "localhost:9000", Insecure: true},
		},
		AuthInfos: []ControlPlaneAuthInfo{
			{Name: "admin", Secret: "my-secret", EnableAuth: true},
		},
	}

	err := SaveControlPlaneConfig(cf, path)
	require.NoError(t, err)

	loaded, err := LoadControlPlaneConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "default", loaded.CurrentContext)
	assert.Equal(t, "localhost:9000", loaded.Clusters[0].Server)
}

func TestLoadControlPlaneConfigFileNotFound(t *testing.T) {
	_, err := LoadControlPlaneConfig("/nonexistent/path/config.yaml")
	assert.Error(t, err)
}

func TestLoadControlPlaneConfigInvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.yaml")
	err := os.WriteFile(path, []byte("invalid: [yaml: content"), 0644)
	require.NoError(t, err)

	_, err = LoadControlPlaneConfig(path)
	assert.Error(t, err)
}

func TestSaveControlPlaneConfigCreateDir(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "subdir", "nested", "config.yaml")

	cf := NewDefaultControlPlaneConfig("localhost:9000", "secret", false)
	err := SaveControlPlaneConfig(cf, path)
	require.NoError(t, err)

	_, err = os.Stat(path)
	assert.NoError(t, err)
}

func TestControlPlaneConfigFromMasterConfig(t *testing.T) {
	config := &MasterConfig{
		GRPCPort:   9000,
		EnableAuth: true,
		EnableTLS:  false,
	}

	cp := ControlPlaneConfigFromMasterConfig(config)
	assert.NotNil(t, cp)
	assert.Equal(t, "localhost:9000", cp.ServerAddr)
	assert.True(t, cp.EnableAuth)
	assert.True(t, cp.Insecure)
}

func TestControlPlaneConfigFromMasterConfigWithControlPlane(t *testing.T) {
	config := &MasterConfig{
		GRPCPort:   9000,
		EnableAuth: false,
		ControlPlane: &ControlPlaneConfig{
			ServerAddr:  "custom:8080",
			EnableAuth:  true,
			Insecure:    false,
			ClusterName: "custom-cluster",
		},
	}

	cp := ControlPlaneConfigFromMasterConfig(config)
	assert.Equal(t, "custom:8080", cp.ServerAddr)
	assert.True(t, cp.EnableAuth)
	assert.False(t, cp.Insecure)
}

func TestShouldRefreshToken(t *testing.T) {
	assert.True(t, ShouldRefreshToken(time.Time{}))
	assert.True(t, ShouldRefreshToken(time.Now().Add(1*time.Minute)))
	assert.False(t, ShouldRefreshToken(time.Now().Add(10*time.Minute)))
}

func TestGetDefaultConfigPath(t *testing.T) {
	path := getDefaultConfigPath()
	assert.NotEmpty(t, path)
	assert.Contains(t, path, DefaultConfigDir)
}
