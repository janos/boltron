// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

const yearShift = math.MaxInt16 + 1

// TimeEncodingLen is the length of byte slice time representation returned by
// EncodeTime.
const TimeEncodingLen = 9

// EncodeTime returns slice of bytes that represent provided time in UTC. Slice
// length is always 9, where first 2 bytes represent the year and the rest 7
// bytes nanoseconds since the beginning the year. This representation is
// compact, can represent year range from -math.MaxInt16 to math.MaxInt16 and is
// sortable. Even it is not related to Bolt, it can be used to sort keys by
// timestamp in a compact representation.
func EncodeTime(t time.Time) (b []byte) {
	b = make([]byte, TimeEncodingLen)
	MarshalTime(b, t)
	return b
}

// MarshalTime puts bytes representation if provided time to provide slice. The
// slice must have length of 9 bytes.
func MarshalTime(b []byte, t time.Time) {
	t = t.UTC()
	binary.BigEndian.PutUint16(b[:2], uint16(t.Year()+yearShift))
	putUint48(b[2:9], uint64(t.Sub(time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC))))
}

// DecodeTime converts slice of bytes as described in encodeTime to time.Time.
func DecodeTime(b []byte) (t time.Time, err error) {
	if l := len(b); l != TimeEncodingLen {
		return t, fmt.Errorf("invalid time encoded length %v", l)
	}
	y := int(binary.BigEndian.Uint16(b[:2]))
	if y > 0 {
		y--
	}
	y -= math.MaxInt16
	ns := getUint48(b[2:TimeEncodingLen])
	return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(ns)), nil
}

func putUint48(b []byte, v uint64) {
	_ = b[6]
	b[0] = byte(v >> 48)
	b[1] = byte(v >> 40)
	b[2] = byte(v >> 32)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 16)
	b[5] = byte(v >> 8)
	b[6] = byte(v)
}

func getUint48(b []byte) uint64 {
	_ = b[6]
	return uint64(b[6]) | uint64(b[5])<<8 | uint64(b[4])<<16 |
		uint64(b[3])<<24 | uint64(b[2])<<32 | uint64(b[1])<<40 | uint64(b[0])<<48
}
