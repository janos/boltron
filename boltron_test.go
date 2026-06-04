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
	"resenje.org/boltron"
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

// Concrete types matching the test-package vars.
type electTx = boltron.CollectionsTx[uint64, string, *ballot]
type assocsTx = boltron.AssociationsTx[uint64, string, uint64]
type pdTx = boltron.ListsTx[string, uint64, time.Time]

func assertElectionBallot(t testing.TB, c *electTx, electionID uint64, voter string, want *ballot) {
	t.Helper()
	election, exists, err := c.Collection(electionID)
	assertErrorFail(t, "", err, nil)
	assert(t, "", exists, true)
	got, err := election.Get(voter)
	assertErrorFail(t, "", err, nil)
	assert(t, "ballot for "+voter+" in election", got, want)
}

func assertElectionMissing(t testing.TB, c *electTx, electionID uint64, voter string) {
	t.Helper()
	election, exists, err := c.Collection(electionID)
	assertErrorFail(t, "", err, nil)
	assert(t, "", exists, true)
	has, err := election.Has(voter)
	assertErrorFail(t, "", err, nil)
	assert(t, voter+" should be absent from election", has, false)
}

func assertElectionSize(t testing.TB, c *electTx, electionID uint64, want int) {
	t.Helper()
	election, _, err := c.Collection(electionID)
	assertErrorFail(t, "", err, nil)
	got, err := election.Size()
	assertErrorFail(t, "", err, nil)
	assert(t, "election size", got, want)
}

func assertKeyCollections(t testing.TB, c *electTx, voter string, want []uint64) {
	t.Helper()
	var got []uint64
	_, err := c.IterateCollectionsWithKey(voter, nil, false, func(id uint64) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	assertErrorFail(t, "", err, nil)
	assert(t, "collections for key "+voter, got, want)
}

func assertAssocRight(t testing.TB, b *assocsTx, assocID uint64, left string, want uint64) {
	t.Helper()
	a, _, err := b.Association(assocID)
	assertErrorFail(t, "", err, nil)
	got, err := a.Right(left)
	assertErrorFail(t, "", err, nil)
	assert(t, "right value for "+left+" in assoc", got, want)
}

func assertAssocSize(t testing.TB, b *assocsTx, assocID uint64, want int) {
	t.Helper()
	a, _, err := b.Association(assocID)
	assertErrorFail(t, "", err, nil)
	got, err := a.Size()
	assertErrorFail(t, "", err, nil)
	assert(t, "assoc size", got, want)
}

func assertLeftAssociations(t testing.TB, b *assocsTx, left string, want []uint64) {
	t.Helper()
	var got []uint64
	_, err := b.IterateAssociationsWithLeftValue(left, nil, false, func(id uint64) (bool, error) {
		got = append(got, id)
		return true, nil
	})
	assertErrorFail(t, "", err, nil)
	assert(t, "associations for left "+left, got, want)
}

func assertListOrderBy(t testing.TB, pd *pdTx, listName string, value uint64, want time.Time) {
	t.Helper()
	list, exists, err := pd.List(listName)
	assertErrorFail(t, "", err, nil)
	assert(t, "", exists, true)
	got, err := list.OrderBy(value)
	assertErrorFail(t, "", err, nil)
	assert(t, "orderBy for value in list "+listName, got, want.UTC())
}

func assertListSize(t testing.TB, pd *pdTx, listName string, want int) {
	t.Helper()
	list, _, err := pd.List(listName)
	assertErrorFail(t, "", err, nil)
	got, err := list.Size()
	assertErrorFail(t, "", err, nil)
	assert(t, "list size for "+listName, got, want)
}

func assertValueLists(t testing.TB, pd *pdTx, value uint64, want []string) {
	t.Helper()
	var got []string
	_, err := pd.IterateListsWithValue(value, nil, false, func(name string, _ time.Time) (bool, error) {
		got = append(got, name)
		return true, nil
	})
	assertErrorFail(t, "", err, nil)
	assert(t, "lists for value", got, want)
}
