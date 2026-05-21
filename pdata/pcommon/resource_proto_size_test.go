// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pcommon

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceSizeProtoMatchesInternalResourceSize(t *testing.T) {
	resource := generateTestResource()

	assert.Equal(t, resource.getOrig().SizeProto(), resource.SizeProto())
}

func TestResourceSizeProtoDoesNotAllocate(t *testing.T) {
	resource := generateTestResource()
	var size int

	allocs := testing.AllocsPerRun(50, func() {
		size = resource.SizeProto()
	})

	require.Positive(t, size)
	assert.Zero(t, allocs)
}
