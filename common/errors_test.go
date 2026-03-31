/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-03-31 17:51:16
 * @Description: 统一错误常量测试
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */
package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewErrorWithCode(t *testing.T) {
	err := NewErrorWithCode(CodeNodeNotFound, fmt.Sprintf(ErrNodeNotFound, "node-1"))
	assert.NotNil(t, err)
	assert.Equal(t, CodeNodeNotFound, err.Code)
	assert.Contains(t, err.Error(), "node-1")
}

func TestErrorWithCodeError(t *testing.T) {
	err := NewErrorWithCode(CodeConfigCannotNil, ErrConfigCannotNil)
	assert.Equal(t, ErrConfigCannotNil, err.Error())
}

func TestErrorCodeRanges(t *testing.T) {
	nodeCodes := []ErrorCode{CodeNodeNotFound, CodeNodeAlreadyRegistered, CodeNodeAlreadyDisabled, CodeNodeAlreadyEnabled, CodeNodeNotSchedulable, CodeNodeEvictFailed}
	for _, code := range nodeCodes {
		assert.True(t, code >= 10001 && code < 20000, "node code %d should be in 1xxxx range", code)
	}

	configCodes := []ErrorCode{CodeConfigCannotNil, CodeNodeConverterNil, CodeInitInfoCannotNil}
	for _, code := range configCodes {
		assert.True(t, code >= 20001 && code < 30000, "config code %d should be in 2xxxx range", code)
	}

	authCodes := []ErrorCode{CodeJoinSecretRequired, CodeInvalidJoinSecret, CodeJoinSecretExpired, CodeAuthDisabled, CodeInvalidCredentials, CodeAuthFailed, CodeAuthRequired, CodeInvalidToken, CodeInvalidAlgorithm}
	for _, code := range authCodes {
		assert.True(t, code >= 70001 && code < 80000, "auth code %d should be in 7xxxx range", code)
	}

	taskCodes := []ErrorCode{CodeTaskNotFound, CodeTaskTypeInvalid, CodeTaskStateTransition, CodeTaskStateTerminal}
	for _, code := range taskCodes {
		assert.True(t, code >= 80001 && code < 90000, "task code %d should be in 8xxxx range", code)
	}
}

func TestErrorConstantsNotEmpty(t *testing.T) {
	assert.NotEmpty(t, ErrNodeNotFound)
	assert.NotEmpty(t, ErrConfigCannotNil)
	assert.NotEmpty(t, ErrJoinSecretRequired)
	assert.NotEmpty(t, ErrTaskNotFound)
	assert.NotEmpty(t, ErrClientAlreadyRunning)
	assert.NotEmpty(t, ErrFailedMarshalTask)
	assert.NotEmpty(t, ErrInvalidStateTransition)
}

func TestErrorFormatStrings(t *testing.T) {
	formatted := fmt.Sprintf(ErrNodeNotFound, "test-node")
	assert.Contains(t, formatted, "test-node")

	formatted = fmt.Sprintf(ErrClusterNotFound, "test-cluster")
	assert.Contains(t, formatted, "test-cluster")

	formatted = fmt.Errorf(ErrFailedMarshalTask, "task-1", fmt.Errorf("some error")).Error()
	assert.Contains(t, formatted, "task-1")
	assert.Contains(t, formatted, "some error")

	formatted = fmt.Sprintf(ErrInvalidStateTransition, "state1", "state2")
	assert.Contains(t, formatted, "state1")
	assert.Contains(t, formatted, "state2")
}

func TestControlPlaneErrorCodes(t *testing.T) {
	cpCodes := []ErrorCode{
		CodeControlPlaneConfigNil,
		CodeFailedReadConfigFile,
		CodeFailedParseYAMLConfig,
		CodeFailedParseJSONConfig,
		CodeFailedParseConfig,
		CodeFailedCreateConfigDir,
		CodeFailedMarshalConfig,
		CodeFailedWriteConfigFile,
		CodeNoContextFound,
		CodeClusterNotFound,
		CodeAuthInfoNotFound,
		CodeContextNotFound,
		CodeFailedLoadConfig,
		CodeFailedResolveConfig,
		CodeFailedConnectMasterAt,
	}
	for _, code := range cpCodes {
		assert.True(t, code >= 21001 && code < 22000, "control plane code %d should be in 21xxx range", code)
	}
}

func TestControlPlaneErrorConstants(t *testing.T) {
	assert.NotEmpty(t, ErrControlPlaneConfigNil)
	assert.NotEmpty(t, ErrFailedReadConfigFile)
	assert.NotEmpty(t, ErrFailedParseYAMLConfig)
	assert.NotEmpty(t, ErrFailedParseJSONConfig)
	assert.NotEmpty(t, ErrFailedParseConfig)
	assert.NotEmpty(t, ErrFailedCreateConfigDir)
	assert.NotEmpty(t, ErrFailedMarshalConfig)
	assert.NotEmpty(t, ErrFailedWriteConfigFile)
	assert.NotEmpty(t, ErrNoContextFound)
	assert.NotEmpty(t, ErrClusterNotFound)
	assert.NotEmpty(t, ErrAuthInfoNotFound)
	assert.NotEmpty(t, ErrContextNotFound)
	assert.NotEmpty(t, ErrFailedLoadConfig)
	assert.NotEmpty(t, ErrFailedResolveConfig)
	assert.NotEmpty(t, ErrFailedConnectMasterAt)
}
