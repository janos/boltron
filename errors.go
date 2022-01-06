// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron

import "errors"

var (
	// ErrNotFound is the default error if requested key or value does not exist.
	ErrNotFound = errors.New("boltron: not found")
	// ErrKeyExists is the default error if the key already exists.
	ErrKeyExists = errors.New("boltron: key exists")
	// ErrKeyExists is the default error if the value already exists.
	ErrValueExists = errors.New("boltron: value exists")
	// ErrInvalidPageNumber is returned on on pagination methods where page
	// number is less then 1.
	ErrInvalidPageNumber = errors.New("boltron: invalid page number")
)
