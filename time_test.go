// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"math"
	"testing"
	"time"

	"resenje.org/boltron"
)

func TestEncodeTime(t *testing.T) {
	for _, tc := range []struct {
		time  time.Time
		bytes []byte
	}{
		{
			time:  time.Time{},
			bytes: []byte{128, 1, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			time:  time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC),
			bytes: []byte{128, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			time:  time.Date(-math.MaxInt16, 1, 1, 0, 0, 0, 0, time.UTC),
			bytes: []byte{0, 1, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			time:  time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			bytes: []byte{135, 208, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			time:  time.Date(2000, 1, 1, 0, 0, 0, 1, time.UTC),
			bytes: []byte{135, 208, 0, 0, 0, 0, 0, 0, 1},
		},
		{
			time:  time.Unix(0, 0).UTC(),
			bytes: []byte{135, 178, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			time:  time.Date(2018, 6, 28, 8, 15, 0, 659847297, time.UTC),
			bytes: []byte{135, 226, 54, 190, 80, 66, 53, 160, 129},
		},
		{
			time:  time.Date(2018, 6, 28, 8, 15, 0, 659847298, time.UTC),
			bytes: []byte{135, 226, 54, 190, 80, 66, 53, 160, 130},
		},
		{
			time:  time.Date(2262, 4, 11, 23, 47, 16, 854775807, time.UTC),
			bytes: []byte{136, 214, 30, 255, 235, 165, 42, 255, 255},
		},
		{
			time:  time.Date(math.MaxInt16, 1, 1, 0, 0, 0, 0, time.UTC),
			bytes: []byte{255, 255, 0, 0, 0, 0, 0, 0, 0},
		},
	} {
		bt := boltron.EncodeTime(tc.time)
		assert(t, tc.time.String(), bt, tc.bytes)
		assert(t, tc.time.String(), len(bt), boltron.TimeEncodingLen)

		tm, err := boltron.DecodeTime(tc.bytes)
		assert(t, tc.time.String(), err, nil)
		assert(t, tc.time.String(), tm, tc.time)

		b := make([]byte, boltron.TimeEncodingLen)
		boltron.MarshalTime(b, tc.time)
		assert(t, tc.time.String(), b, tc.bytes)

	}
}
