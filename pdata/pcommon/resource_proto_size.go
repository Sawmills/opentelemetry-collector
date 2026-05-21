// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pcommon

// SizeProto returns the protobuf encoded size of this Resource.
func (ms Resource) SizeProto() int {
	return ms.getOrig().SizeProto()
}
