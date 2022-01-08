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
	ballotsDefinition = boltron.NewAssociationsDefinition(
		"ballots",
		boltron.Uint64BinaryEncoding,       // voting id
		boltron.StringNaturalOrderEncoding, // voter
		boltron.Uint64Base36Encoding,       // ballot serial number
		nil,
	)

	testBallots = []struct {
		Voting   uint64
		Voter    string
		BallotID uint64
	}{
		{1, "alice", 1},
		{1, "bob", 2},
		{1, "chriss", 0},
		{1, "dave", 4},
		{1, "edit", 3},

		{3, "alice", 0},
		{3, "bob", 1},
		{3, "dave", 2},
		{3, "mick", 3},

		{6, "bob", 1},
		{6, "dave", 0},
		{6, "edit", 2},
		{6, "paul", 5},
		{6, "george", 3},
		{6, "ringo", 4},
		{6, "john", 6},

		{7, "alice", 1},
		{7, "dave", 0},
	}

	testBallotsAssociations = []uint64{1, 3, 6, 7}

	testBallotsKeys = []string{
		"alice",
		"bob",
		"chriss",
		"dave",
		"edit",
		"george",
		"john",
		"mick",
		"paul",
		"ringo",
	}

	testBallotsAssociationsWithKeyAlice = []uint64{0, 5, 7}
)

func TestAssociations(t *testing.T) {
	db := ballotsDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballotsDefinition.Associations(tx)

		for _, b := range testBallots {
			has, err := ballots.HasAssociation(b.Voting)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, true)

			_, exists, err := ballots.Association(b.Voting)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), exists, true)

			has, err = ballots.HasKey(b.Voter)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, true)
		}

		has, err := ballots.HasAssociation(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		has, err = ballots.HasKey("unknown")
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		_, exists, err := ballots.Association(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
	})

	deletedKey := "edit"

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballotsDefinition.Associations(tx)

		err := ballots.DeleteKey("unknown", true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = ballots.DeleteKey(deletedKey, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballotsDefinition.Associations(tx)

		for _, b := range testBallotsAssociations {
			has, err := ballots.HasAssociation(b)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, true)

			association, exists, err := ballots.Association(b)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), exists, true)

			has, err = association.HasKey(deletedKey)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, false)
		}

		for _, b := range testBallotsKeys {
			has, err := ballots.HasKey(b)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, b != deletedKey)
		}

		has, err := ballots.HasKey(deletedKey)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)
	})

	deletedAssociation := uint64(3)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballotsDefinition.Associations(tx)

		err := ballots.DeleteAssociation(100, true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = ballots.DeleteAssociation(deletedAssociation, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballotsDefinition.Associations(tx)

		for _, b := range testBallots {
			has, err := ballots.HasAssociation(b.Voting)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, b.Voting != deletedAssociation)

			_, exists, err := ballots.Association(b.Voting)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), exists, b.Voting != deletedAssociation)

			has, err = ballots.HasKey(b.Voter)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, b.Voter != deletedKey && b.Voter != "mick")
		}
	})
}

func ballotsDB(t testing.TB) *bolt.DB {
	t.Helper()

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballotsDefinition.Associations(tx)

		ballots1, exists, err := ballots.Association(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		ballots3, exists, err := ballots.Association(3)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		ballots6, exists, err := ballots.Association(6)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		ballots7, exists, err := ballots.Association(7)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		for _, b := range testBallots {
			switch b.Voting {
			case 1:
				err := ballots1.Set(b.Voter, b.BallotID)
				assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			case 3:
				err := ballots3.Set(b.Voter, b.BallotID)
				assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			case 6:
				err := ballots6.Set(b.Voter, b.BallotID)
				assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			case 7:
				err := ballots7.Set(b.Voter, b.BallotID)
				assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			}
		}
	})

	return db
}
