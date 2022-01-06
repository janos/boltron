// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"time"
)

// Encoding defines serialization of any type to and from bytes representation.
type Encoding[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

// EncodingFunc is a helper type to construct Encoding from two existing functions.
type EncodingFunc[T any] struct {
	encodeFunc func(T) ([]byte, error)
	decodeFunc func([]byte) (T, error)
}

// NewEncoding returns Encoding from two functions that define it.
func NewEncoding[T any](
	encode func(T) ([]byte, error),
	decode func([]byte) (T, error),
) Encoding[T] {
	return &EncodingFunc[T]{
		encodeFunc: encode,
		decodeFunc: decode,
	}
}

// Encode serializes an instance of certain type to its bytes representation.
func (e *EncodingFunc[T]) Encode(t T) ([]byte, error) {
	return e.encodeFunc(t)
}

// Decode deserializes a slice of bytes into an instance of a specific type.
func (e *EncodingFunc[T]) Decode(b []byte) (T, error) {
	return e.decodeFunc(b)
}

var (
	// StringEncoding encodes string by a simple type conversion to byte slice.
	StringEncoding = NewEncoding(
		func(v string) ([]byte, error) {
			return []byte(v), nil
		},
		func(b []byte) (string, error) {
			return string(b), nil
		},
	)

	// StringNaturalOrderEncoding encodes string to be case insensitive and
	// numerically sorted. It takes more than a double space to store the value
	// as it keeps it in the original form in bas64 encoding alongside the
	// encoded form.
	StringNaturalOrderEncoding = NewEncoding(
		func(v string) ([]byte, error) {
			return encodeNatural(v), nil
		},
		func(b []byte) (string, error) {
			return decodeNatural(b)
		},
	)

	// Uint64BinaryEncoding encodes uint64 number as big endian 8 byte array. It
	// is suitable to be used as OrderBy encoding in lists.
	Uint64BinaryEncoding = NewEncoding(
		func(v uint64) ([]byte, error) {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, v)
			return b, nil
		},
		func(b []byte) (uint64, error) {
			if l := len(b); l != 8 {
				return 0, fmt.Errorf("invalid encoded value length %v", l)
			}
			return binary.BigEndian.Uint64(b), nil
		},
	)

	// IntBase10Encoding encodes integer using strconv.Itoa and strconv.Atoi functions.
	IntBase10Encoding = NewEncoding(
		func(i int) ([]byte, error) {
			return []byte(strconv.Itoa(i)), nil
		},
		func(b []byte) (int, error) {
			return strconv.Atoi(string(b))
		},
	)

	// Int64Base36Encoding encodes int64 using strconv.FormatInt with 36 base
	// string representation.
	Int64Base36Encoding = NewEncoding(
		func(i int64) ([]byte, error) {
			return []byte(strconv.FormatInt(i, 36)), nil
		},
		func(b []byte) (int64, error) {
			return strconv.ParseInt(string(b), 36, 64)
		},
	)

	// Uint64Base36Encoding encodes uint64 using strconv.FormatInt with 36 base
	// string representation.
	Uint64Base36Encoding = NewEncoding(
		func(i uint64) ([]byte, error) {
			return []byte(strconv.FormatUint(i, 36)), nil
		},
		func(b []byte) (uint64, error) {
			return strconv.ParseUint(string(b), 36, 64)
		},
	)

	// TimeEncoding encodes time using EncodeTime and DecodeTime functions. It
	// is suitable to be used as OrderBy encoding in lists.
	TimeEncoding = NewEncoding(
		func(v time.Time) ([]byte, error) {
			return EncodeTime(v), nil
		},
		func(b []byte) (time.Time, error) {
			return DecodeTime(b)
		},
	)

	// NullEncoding always produces a nil byte slice and nul Null value. It is
	// suitable to be used as OrderBy encoding in lists if order is determined
	// by list's values encoding.
	NullEncoding = NewEncoding(
		func(*struct{}) ([]byte, error) {
			return nil, nil
		},
		func([]byte) (*struct{}, error) {
			return nil, nil
		},
	)
)
