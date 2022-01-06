// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"resenje.org/boltron"
)

func TestStringEncoding(t *testing.T) {
	tableTestEncoding(t, boltron.StringEncoding, []struct {
		value   string
		encoded []byte
	}{
		{"", []byte{}},
		{"test", []byte("test")},
		{"💥", []byte("💥")},
	})
}

func TestStringNaturalOrderEncoding(t *testing.T) {

	encodedValue := func(part1, part2 string) []byte {
		var out []byte
		out = append(out, []byte(part1)...)
		out = append(out, 0, 0, 0, 0)
		out = append(out, []byte(base64.StdEncoding.EncodeToString([]byte(part2)))...)
		return out
	}

	encodeBigEndian := func(v uint64) string {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, v)
		return string(b)
	}

	tableTestEncoding(t, boltron.StringNaturalOrderEncoding, []struct {
		value   string
		encoded []byte
	}{
		{"", encodedValue("", "")},
		{"test", encodedValue("test", "test")},
		{"💥", encodedValue("💥", "💥")},
		{"0", encodedValue(encodeBigEndian(0), "0")},
		{"1", encodedValue(encodeBigEndian(1), "1")},
		{"a1", encodedValue("a"+encodeBigEndian(1), "a1")},
		{"1a", encodedValue(encodeBigEndian(1)+"a", "1a")},
		{"00001a", encodedValue(encodeBigEndian(1)+"a", "00001a")},
		{"te2st", encodedValue("te"+encodeBigEndian(2)+"st", "te2st")},
		{"Te 0020世界 st", encodedValue("te "+encodeBigEndian(20)+"世界 st", "Te 0020世界 st")},
	})
}

func TestUint64BinaryEncoding(t *testing.T) {
	tableTestEncoding(t, boltron.Uint64BinaryEncoding, []struct {
		value   uint64
		encoded []byte
	}{
		{0, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{1000, []byte{0, 0, 0, 0, 0, 0, 3, 232}},
		{math.MaxUint64, []byte{255, 255, 255, 255, 255, 255, 255, 255}},
	})
}

func TestIntBase10Encoding(t *testing.T) {
	tableTestEncoding(t, boltron.IntBase10Encoding, []struct {
		value   int
		encoded []byte
	}{
		{0, []byte("0")},
		{1000, []byte("1000")},
		{math.MaxInt, []byte(fmt.Sprint(math.MaxInt))},
		{-1, []byte("-1")},
		{-1234567, []byte("-1234567")},
	})
}

func TestInt64Base36Encoding(t *testing.T) {
	tableTestEncoding(t, boltron.Int64Base36Encoding, []struct {
		value   int64
		encoded []byte
	}{
		{0, []byte("0")},
		{1000, []byte("rs")},
		{math.MaxInt64, []byte("1y2p0ij32e8e7")},
		{-1, []byte("-1")},
		{-1234567, []byte("-qglj")},
	})
}

func TestUint64Base36Encoding(t *testing.T) {
	tableTestEncoding(t, boltron.Uint64Base36Encoding, []struct {
		value   uint64
		encoded []byte
	}{
		{0, []byte("0")},
		{1000, []byte("rs")},
		{math.MaxInt64, []byte("1y2p0ij32e8e7")},
		{math.MaxUint64, []byte("3w5e11264sgsf")},
	})
}

func TestTimeEncoding(t *testing.T) {
	tableTestEncoding(t, boltron.TimeEncoding, []struct {
		value   time.Time
		encoded []byte
	}{
		{time.Time{}.UTC(), []byte{128, 1, 0, 0, 0, 0, 0, 0, 0}},
		{time.Date(-32767, time.January, 1, 0, 0, 0, 0, time.UTC), []byte{0, 1, 0, 0, 0, 0, 0, 0, 0}},
		{time.Date(3000, time.April, 13, 23, 59, 54, 37927935, time.UTC), []byte{139, 184, 31, 157, 197, 19, 106, 255, 255}},
		{time.Unix(0, 0).UTC(), []byte{135, 178, 0, 0, 0, 0, 0, 0, 0}},
	})
}

func TestNullEncoding(t *testing.T) {
	tableTestEncoding(t, boltron.NullEncoding, []struct {
		value   *struct{}
		encoded []byte
	}{
		{nil, nil},
	})
}

func tableTestEncoding[T any](t *testing.T, encoding boltron.Encoding[T], table []struct {
	value   T
	encoded []byte
}) {
	for _, tc := range table {
		testEncoding(t, encoding, tc.value, tc.encoded)
	}
}

func testEncoding[T any](t *testing.T, encoding boltron.Encoding[T], value T, encoded []byte) {
	t.Helper()

	gotEncoded, err := encoding.Encode(value)
	assertFail(t, fmt.Sprintf("%v encode error", value), err, nil)
	assert(t, fmt.Sprintf("%v encoded", value), gotEncoded, encoded)

	gotValue, err := encoding.Decode(encoded)
	assertFail(t, fmt.Sprintf("%v decode error", value), err, nil)
	assert(t, fmt.Sprintf("%v decoded", value), gotValue, value)
}
