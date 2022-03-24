// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func newDB(t testing.TB) *bolt.DB {
	t.Helper()

	dir := t.TempDir()

	db, err := bolt.Open(filepath.Join(dir, "db"), 0o666, &bolt.Options{
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})

	return db
}

func dbView(t testing.TB, db *bolt.DB, f func(t testing.TB, tx *bolt.Tx)) {
	t.Helper()

	if err := db.View(func(tx *bolt.Tx) error {
		f(t, tx)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func dbUpdate(t testing.TB, db *bolt.DB, f func(t testing.TB, tx *bolt.Tx)) {
	t.Helper()

	if err := db.Update(func(tx *bolt.Tx) error {
		f(t, tx)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assert[T any](t testing.TB, message string, got, want T) {
	t.Helper()

	if message != "" {
		message = message + ": "
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("%sgot %v, want %v", message, got, want)
	}
}

func assertError(t testing.TB, message string, got, want error) {
	t.Helper()

	if message != "" {
		message = message + ": "
	}

	if !errors.Is(got, want) {
		t.Errorf("%sgot %v, want %v", message, got, want)
	}
}

func assertErrorFail(t testing.TB, message string, got, want error) {
	t.Helper()

	if message != "" {
		message = message + ": "
	}

	if !errors.Is(got, want) {
		t.Fatalf("%sgot %v, want %v", message, got, want)
	}
}

func assertTime(t testing.TB, message string, got, want time.Time) {
	t.Helper()

	if message != "" {
		message = message + ": "
	}

	if !got.Equal(want) {
		t.Errorf("%sgot %v, want %v", message, got, want)
	}
}
