// Copyright (c) 2022, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strconv"
	"unicode"
)

func encodeNatural(in string) []byte {
	orig := base64.StdEncoding.EncodeToString([]byte(in))
	out := make([]byte, 0, len(in)+len(orig)+4)
	for {
		start, end, number := getFirstNumber(in)
		if start < 0 {
			out = append(out, bytes.ToLower([]byte(in))...)
			break
		}
		out = append(out, bytes.ToLower([]byte(in[:start]))...)
		buff := make([]byte, 8)
		binary.BigEndian.PutUint64(buff, number)
		out = append(out, buff...)
		in = in[end:]
		if in == "" {
			break
		}
	}
	out = append(out, 0, 0, 0, 0)
	out = append(out, []byte(orig)...)
	return out
}

func decodeNatural(in []byte) (string, error) {
	i := bytes.LastIndex(in, []byte{0, 0, 0, 0})
	if i < 0 {
		return "", errors.New("missing natural encoding separator")
	}
	in = in[i+4:]
	dst := make([]byte, base64.StdEncoding.EncodedLen(len(in)))
	n, err := base64.StdEncoding.Decode(dst, in)
	if err != nil {
		return "", err
	}
	return string(dst[:n]), nil
}

func getFirstNumber(in string) (start, end int, number uint64) {
	start = -1
	end = -1
	for i, r := range in {
		if unicode.IsDigit(r) {
			if start < 0 {
				start = i
			}
		} else {
			if start >= 0 && end < 0 {
				end = i
				break
			}
		}
	}
	if start >= 0 {
		if end < 0 {
			end = len(in)
		}
	} else {
		return -1, -1, 0
	}
	number, err := strconv.ParseUint(in[start:end], 10, 64)
	if err != nil {
		return -1, -1, 0
	}
	return start, end, number
}
