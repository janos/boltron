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

// Encoding defines serialization of any type to an from bytes representation.
type Encoding[T any] struct {
	Encode func(T) ([]byte, error)
	Decode func([]byte) (T, error)
}

var (
	// StringEncoding encodes string by a simple type conversion to byte slice.
	StringEncoding = &Encoding[string]{
		Encode: func(v string) ([]byte, error) {
			return []byte(v), nil
		},
		Decode: func(b []byte) (string, error) {
			return string(b), nil
		},
	}

	// Uint64BinaryEncoding encodes uint64 number as big endian 8 byte array. It
	// is suitable to be used as OrderBy encoding in lists.
	Uint64BinaryEncoding = &Encoding[uint64]{
		Encode: func(v uint64) ([]byte, error) {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, v)
			return b, nil
		},
		Decode: func(b []byte) (uint64, error) {
			if l := len(b); l != 8 {
				return 0, fmt.Errorf("invalid encoded value length %v", l)
			}
			return binary.BigEndian.Uint64(b), nil
		},
	}

	// IntBase10Encoding encodes integer using strconv.Itoa and strconv.Atoi functions.
	IntBase10Encoding = &Encoding[int]{
		Encode: func(i int) ([]byte, error) {
			return []byte(strconv.Itoa(i)), nil
		},
		Decode: func(b []byte) (int, error) {
			return strconv.Atoi(string(b))
		},
	}

	// Int64Base36Encoding encodes int64 using strconv.FormatInt with 36 base
	// string representation.
	Int64Base36Encoding = &Encoding[int64]{
		Encode: func(i int64) ([]byte, error) {
			return []byte(strconv.FormatInt(i, 36)), nil
		},
		Decode: func(b []byte) (int64, error) {
			return strconv.ParseInt(string(b), 36, 64)
		},
	}

	// Uint64Base36Encoding encodes uint64 using strconv.FormatInt with 36 base
	// string representation.
	Uint64Base36Encoding = &Encoding[uint64]{
		Encode: func(i uint64) ([]byte, error) {
			return []byte(strconv.FormatUint(i, 36)), nil
		},
		Decode: func(b []byte) (uint64, error) {
			return strconv.ParseUint(string(b), 36, 64)
		},
	}

	// TimeEncoding encodes time using EncodeTime and DecodeTime functions. It
	// is suitable to be used as OrderBy encoding in lists.
	TimeEncoding = &Encoding[time.Time]{
		Encode: func(v time.Time) ([]byte, error) {
			return EncodeTime(v), nil
		},
		Decode: func(b []byte) (time.Time, error) {
			return DecodeTime(b)
		},
	}

	// NullEncoding always produces a nil byte slice and nul Null value. It is
	// suitable to be used as OrderBy encoding in lists if order is determened
	// by list's values encoding.
	NullEncoding = &Encoding[*struct{}]{
		Encode: func(*struct{}) ([]byte, error) {
			return nil, nil
		},
		Decode: func([]byte) (*struct{}, error) {
			return nil, nil
		},
	}
)
