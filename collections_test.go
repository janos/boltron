// Copyright (c) 2022, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"fmt"
	"testing"

	bolt "go.etcd.io/bbolt"
	"resenje.org/boltron"
)

var (
	electionsDefinition = boltron.NewCollectionsDefinition(
		"elections",
		boltron.Uint64BinaryEncoding,       // election id
		boltron.StringEncoding,             // voter id
		boltron.NewJSONEncoding[*ballot](), // ballot with a vote
		nil,
	)

	testElections = []struct {
		Election uint64
		Voter    string
		Ballot   *ballot
	}{
		{0, "allice", newBallot(1)},
		{0, "bob", newBallot(1)},
		{0, "chriss", newBallot(2)},
		{0, "dave", newBallot(0)},
		{0, "edit", newBallot(2)},

		{5, "allice", newBallot(0)},
		{5, "bob", newBallot(4)},
		{5, "dave", newBallot(2)},
		{5, "mick", newBallot(2)},

		{6, "bob", newBallot(0)},
		{6, "dave", newBallot(0)},
		{6, "edit", newBallot(2)},
		{6, "paul", newBallot(0)},
		{6, "george", newBallot(1)},
		{6, "ringo", newBallot(2)},
		{6, "john", newBallot(1)},
	}

	testElectionsCollections = []uint64{0, 5, 6}

	testElectionsKeys = []string{
		"allice",
		"bob",
		"chriss",
		"dave",
		"edit",
		"george",
		"john",
		"paul",
		"ringo",
	}

	testElectionsCollectionsWithKeyAllice = []uint64{0, 5}
)

type ballot struct {
	Vote int `json:"vote"`
}

func newBallot(vote int) *ballot {
	return &ballot{Vote: vote}
}

func TestCollections(t *testing.T) {
	db := projectsElectionsDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := electionsDefinition.Collections(tx)

		for _, e := range testElections {
			has, err := elections.HasCollection(e.Election)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), has, true)

			_, exists, err := elections.Collection(e.Election)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), exists, true)

			has, err = elections.HasKey(e.Voter)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), has, true)
		}

		has, err := elections.HasCollection(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		has, err = elections.HasKey("unknown")
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		_, exists, err := elections.Collection(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
	})

	deletedKey := "edit"

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := electionsDefinition.Collections(tx)

		err := elections.DeleteKey("unknown", true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = elections.DeleteKey(deletedKey, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := electionsDefinition.Collections(tx)

		for _, e := range testElectionsCollections {
			has, err := elections.HasCollection(e)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), has, true)

			collection, exists, err := elections.Collection(e)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), exists, true)

			has, err = collection.Has(deletedKey)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), has, false)
		}

		for _, e := range testElectionsKeys {
			has, err := elections.HasKey(e)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), has, e != deletedKey)
		}

		has, err := elections.HasKey(deletedKey)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)
	})

	deletedCollection := uint64(5)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := electionsDefinition.Collections(tx)

		err := elections.DeleteCollection(100, true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = elections.DeleteCollection(deletedCollection, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := electionsDefinition.Collections(tx)

		for _, e := range testElections {
			has, err := elections.HasCollection(e.Election)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), has, e.Election != deletedCollection)

			_, exists, err := elections.Collection(e.Election)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), exists, e.Election != deletedCollection)

			has, err = elections.HasKey(e.Voter)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), has, e.Voter != deletedKey && e.Voter != "mick")
		}
	})
}

func projectsElectionsDB(t testing.TB) *bolt.DB {
	t.Helper()

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := electionsDefinition.Collections(tx)

		elections0, exists, err := elections.Collection(0)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		elections5, exists, err := elections.Collection(5)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		elections6, exists, err := elections.Collection(6)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		for _, e := range testElections {
			switch e.Election {
			case 0:
				_, err := elections0.Save(e.Voter, e.Ballot, false)
				assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			case 5:
				_, err := elections5.Save(e.Voter, e.Ballot, false)
				assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			case 6:
				_, err := elections6.Save(e.Voter, e.Ballot, false)
				assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			}
		}
	})

	return db
}
