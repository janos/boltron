// Copyright (c) 2022, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"errors"
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

	testBallotsAssociationsWithKeyAlice = []uint64{1, 3, 7}
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

			has, err = ballots.HasLeft(b.Voter)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, true)
		}

		has, err := ballots.HasAssociation(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		has, err = ballots.HasLeft("unknown")
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		_, exists, err := ballots.Association(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
	})

	deletedLeft := "edit"

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballotsDefinition.Associations(tx)

		err := ballots.DeleteLeft("unknown", true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = ballots.DeleteLeft(deletedLeft, true)
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

			has, err = association.HasLeft(deletedLeft)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, false)
		}

		for _, b := range testBallotsKeys {
			has, err := ballots.HasLeft(b)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, b != deletedLeft)
		}

		has, err := ballots.HasLeft(deletedLeft)
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

			has, err = ballots.HasLeft(b.Voter)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, b.Voter != deletedLeft && b.Voter != "mick")
		}
	})
}

func TestAssociations_iterateAssociations(t *testing.T) {
	db := ballotsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateAssociations(nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociations[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testBallotsAssociations))
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateAssociations(nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociations[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 6)

			next, err = ballots.IterateAssociations(next, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociations[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testBallotsAssociations))
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateAssociations(nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociations[len(testBallotsAssociations)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testBallotsAssociations))
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateAssociations(nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociations[len(testBallotsAssociations)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 3)

			next, err = ballots.IterateAssociations(next, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociations[len(testBallotsAssociations)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testBallotsAssociations))
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var count int
			next, err := ballots.IterateAssociations(nil, false, func(_ uint64) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestAssociations_pageOfAssociations(t *testing.T) {
	db := ballotsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			_, _, _, err := ballots.PageOfAssociations(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = ballots.PageOfAssociations(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := ballots.PageOfAssociations(1, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsAssociations(0, 1))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = ballots.PageOfAssociations(2, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsAssociations(2, 3))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			_, _, _, err := ballots.PageOfAssociations(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = ballots.PageOfAssociations(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := ballots.PageOfAssociations(1, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsAssociations(3, 2))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = ballots.PageOfAssociations(2, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsAssociations(1, 0))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			page, totalElements, totalPages, err := ballots.PageOfAssociations(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestAssociations_iterateAssociationsWithLeftValue(t *testing.T) {
	db := ballotsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateAssociationsWithLeftValue("alice", nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociationsWithKeyAlice[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testBallotsAssociationsWithKeyAlice))
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateAssociationsWithLeftValue("alice", nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociationsWithKeyAlice[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 7)

			next, err = ballots.IterateAssociationsWithLeftValue("alice", next, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociationsWithKeyAlice[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testBallotsAssociationsWithKeyAlice))
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateAssociationsWithLeftValue("alice", nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociationsWithKeyAlice[len(testBallotsAssociationsWithKeyAlice)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testBallotsAssociationsWithKeyAlice))
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateAssociationsWithLeftValue("alice", nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociationsWithKeyAlice[len(testBallotsAssociationsWithKeyAlice)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 1)

			next, err = ballots.IterateAssociationsWithLeftValue("alice", next, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate association #%v", i), v, testBallotsAssociationsWithKeyAlice[len(testBallotsAssociationsWithKeyAlice)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testBallotsAssociationsWithKeyAlice))
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var count int
			next, err := ballots.IterateAssociationsWithLeftValue("alice", nil, false, func(_ uint64) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})

		dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			association, exists, err := ballots.Association(100)
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			err = association.Set("paul", 10000)
			assertErrorFail(t, "", err, nil)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var count int
			next, err := ballots.IterateAssociationsWithLeftValue("alice", nil, false, func(_ uint64) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestAssociations_pageOfAssociationsWithLeftValue(t *testing.T) {
	db := ballotsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			_, _, _, err := ballots.PageOfAssociationsWithLeftValue("alice", -1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = ballots.PageOfAssociationsWithLeftValue("alice", 0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := ballots.PageOfAssociationsWithLeftValue("alice", 1, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsAssociationsWithKeyAlice(0, 1))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = ballots.PageOfAssociationsWithLeftValue("alice", 2, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsAssociationsWithKeyAlice(2))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			_, _, _, err := ballots.PageOfAssociationsWithLeftValue("alice", -1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = ballots.PageOfAssociationsWithLeftValue("alice", 0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := ballots.PageOfAssociationsWithLeftValue("alice", 1, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsAssociationsWithKeyAlice(2, 1))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = ballots.PageOfAssociationsWithLeftValue("alice", 2, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsAssociationsWithKeyAlice(0))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			page, totalElements, totalPages, err := ballots.PageOfAssociationsWithLeftValue("alice", 1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})

		dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			association, exists, err := ballots.Association(0)
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			err = association.Set("paul", 10000)
			assertErrorFail(t, "", err, nil)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			page, totalElements, totalPages, err := ballots.PageOfAssociationsWithLeftValue("alice", 1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestAssociations_iterateLeftValues(t *testing.T) {
	db := ballotsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateLeftValues(nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testBallotsKeys[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateLeftValues(nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testBallotsKeys[i])
				i++
				if i == 3 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "dave")

			next, err = ballots.IterateLeftValues(next, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testBallotsKeys[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateLeftValues(nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testBallotsKeys[len(testBallotsKeys)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var i int
			next, err := ballots.IterateLeftValues(nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testBallotsKeys[len(testBallotsKeys)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "mick")

			next, err = ballots.IterateLeftValues(next, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testBallotsKeys[len(testBallotsKeys)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			var count int
			next, err := ballots.IterateLeftValues(nil, false, func(_ string) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestAssociations_pageOfLeftValues(t *testing.T) {
	db := ballotsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			_, _, _, err := ballots.PageOfLeftValues(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = ballots.PageOfLeftValues(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := ballots.PageOfLeftValues(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsKeys(0, 1, 2))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = ballots.PageOfLeftValues(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsKeys(3, 4, 5))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = ballots.PageOfLeftValues(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsKeys(6, 7, 8))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = ballots.PageOfLeftValues(4, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsKeys(9))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			_, _, _, err := ballots.PageOfLeftValues(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = ballots.PageOfLeftValues(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := ballots.PageOfLeftValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsKeys(9, 8, 7))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = ballots.PageOfLeftValues(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsKeys(6, 5, 4))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = ballots.PageOfLeftValues(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsKeys(3, 2, 1))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = ballots.PageOfLeftValues(4, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, ballotsKeys(0))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballotsDefinition.Associations(tx)

			page, totalElements, totalPages, err := ballots.PageOfLeftValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestAssociations_ErrAssociationNotFound_and_ErrNotFound(t *testing.T) {
	db := newDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballotsDefinition.Associations(tx)

		_, exists, err := ballots.Association(0)
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := ballots.HasAssociation(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = ballots.HasLeft("john")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = ballots.DeleteAssociation(0, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = ballots.DeleteAssociation(0, false)
		assertError(t, "", err, nil)

		err = ballots.DeleteLeft("john", true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = ballots.DeleteLeft("john", false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballotsDefinition.Associations(tx)

		association, exists, err := ballots.Association(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = association.Set("paul", 1000)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballotsDefinition.Associations(tx)

		_, exists, err := ballots.Association(0)
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := ballots.HasAssociation(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = ballots.HasLeft("john")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = ballots.DeleteAssociation(0, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = ballots.DeleteAssociation(0, false)
		assertError(t, "", err, nil)

		err = ballots.DeleteLeft("john", true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = ballots.DeleteLeft("john", false)
		assertError(t, "", err, nil)
	})
}

func TestAssociations_customErrAssociationNotFound_and_customErrNotFound(t *testing.T) {

	errAssociationNotFoundCustom := errors.New("custom association not found error")
	errNotFoundCustom := errors.New("custom not found error")

	customBallotsDefinition := boltron.NewAssociationsDefinition(
		"ballots",
		boltron.Uint64BinaryEncoding,       // voting id
		boltron.StringNaturalOrderEncoding, // voter
		boltron.Uint64Base36Encoding,       // ballot serial number
		&boltron.AssociationsOptions{
			ErrAssociationNotFound: errAssociationNotFoundCustom,
			ErrNotFound:            errNotFoundCustom,
		},
	)

	db := newDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := customBallotsDefinition.Associations(tx)

		_, exists, err := ballots.Association(0)
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := ballots.HasAssociation(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = ballots.HasLeft("john")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = ballots.DeleteAssociation(0, true)
		assertError(t, "", err, errAssociationNotFoundCustom)

		err = ballots.DeleteAssociation(0, false)
		assertError(t, "", err, nil)

		err = ballots.DeleteLeft("john", true)
		assertError(t, "", err, errNotFoundCustom)

		err = ballots.DeleteLeft("john", false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := customBallotsDefinition.Associations(tx)

		association, exists, err := ballots.Association(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = association.Set("paul", 1000)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := customBallotsDefinition.Associations(tx)

		_, exists, err := ballots.Association(0)
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := ballots.HasAssociation(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = ballots.HasLeft("john")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = ballots.DeleteAssociation(0, true)
		assertError(t, "", err, errAssociationNotFoundCustom)

		err = ballots.DeleteAssociation(0, false)
		assertError(t, "", err, nil)

		err = ballots.DeleteLeft("john", true)
		assertError(t, "", err, errNotFoundCustom)

		err = ballots.DeleteLeft("john", false)
		assertError(t, "", err, nil)
	})
}

func TestAssociations_uniqueKeys(t *testing.T) {

	customBallotsDefinition := boltron.NewAssociationsDefinition(
		"ballots",
		boltron.Uint64BinaryEncoding,       // voting id
		boltron.StringNaturalOrderEncoding, // voter
		boltron.Uint64Base36Encoding,       // ballot serial number
		&boltron.AssociationsOptions{
			UniqueLeftValues: true,
		},
	)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := customBallotsDefinition.Associations(tx)

		election0, exists, err := ballots.Association(0)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		election1, exists, err := ballots.Association(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = election0.Set("john", 1000)
		assertErrorFail(t, "", err, nil)

		err = election1.Set("john", 1000)
		assertErrorFail(t, "", err, boltron.ErrLeftExists)
	})
}

func TestAssociations_uniqueKeys_customErrKeyExists(t *testing.T) {

	errLeftExistsCustom := errors.New("custom left exists error")

	customBallotsDefinition := boltron.NewAssociationsDefinition(
		"ballots",
		boltron.Uint64BinaryEncoding,       // voting id
		boltron.StringNaturalOrderEncoding, // voter
		boltron.Uint64Base36Encoding,       // ballot serial number
		&boltron.AssociationsOptions{
			UniqueLeftValues: true,
			ErrLeftExists:    errLeftExistsCustom,
		},
	)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := customBallotsDefinition.Associations(tx)

		election0, exists, err := ballots.Association(0)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		election1, exists, err := ballots.Association(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = election0.Set("john", 1000)
		assertErrorFail(t, "", err, nil)

		err = election1.Set("john", 1000)
		assertErrorFail(t, "", err, errLeftExistsCustom)
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

func ballotsKeys(is ...int) []string {
	s := make([]string, 0, len(is))
	for _, i := range is {
		s = append(s, testBallotsKeys[i])
	}
	return s
}

func ballotsAssociations(is ...int) []uint64 {
	s := make([]uint64, 0, len(is))
	for _, i := range is {
		s = append(s, testBallotsAssociations[i])
	}
	return s
}

func ballotsAssociationsWithKeyAlice(is ...int) []uint64 {
	s := make([]uint64, 0, len(is))
	for _, i := range is {
		s = append(s, testBallotsAssociationsWithKeyAlice[i])
	}
	return s
}
