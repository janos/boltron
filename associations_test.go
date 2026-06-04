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
	ballots = boltron.NewAssociations(
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
		ballots := ballots.Tx(tx)

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
		ballots := ballots.Tx(tx)

		err := ballots.DeleteLeft("unknown", true)
		assertErrorFail(t, "", err, boltron.ErrLeftNotFound)

		err = ballots.DeleteLeft(deletedLeft, true)
		assertErrorFail(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {

		deletedLeftIndirectly := "dave"

		ballots := ballots.Tx(tx)

		ballot, _, err := ballots.Association(1)
		assertErrorFail(t, "", err, nil)

		err = ballot.DeleteByLeft(deletedLeftIndirectly, true)
		assertErrorFail(t, "", err, nil)

		err = ballot.DeleteByLeft(deletedLeftIndirectly, true)
		assertErrorFail(t, "", err, boltron.ErrLeftNotFound)

		has, err := ballots.HasLeft(deletedLeftIndirectly)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, true)

		var associationsWithLeft []uint64
		next, err := ballots.IterateAssociationsWithLeftValue(deletedLeftIndirectly, nil, false, func(a uint64) (bool, error) {
			associationsWithLeft = append(associationsWithLeft, a)
			return true, nil
		})
		assertErrorFail(t, "", err, nil)
		assert(t, "", next, nil)

		assert(t, "", associationsWithLeft, []uint64{3, 6, 7})
	})

	deletedLeftIndirectly6 := "john"

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {

		ballots := ballots.Tx(tx)

		ballot, _, err := ballots.Association(6)
		assertErrorFail(t, "", err, nil)

		err = ballot.DeleteByLeft(deletedLeftIndirectly6, true)
		assertErrorFail(t, "", err, nil)

		err = ballot.DeleteByLeft(deletedLeftIndirectly6, true)
		assertErrorFail(t, "", err, boltron.ErrLeftNotFound)

		has, err := ballots.HasLeft(deletedLeftIndirectly6)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		var associationsWithLeft []uint64
		next, err := ballots.IterateAssociationsWithLeftValue(deletedLeftIndirectly6, nil, false, func(a uint64) (bool, error) {
			associationsWithLeft = append(associationsWithLeft, a)
			return true, nil
		})
		assertErrorFail(t, "", err, nil)
		assert(t, "", next, nil)

		assert(t, "", associationsWithLeft, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballots.Tx(tx)

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
			assert(t, fmt.Sprintf("%+v", b), has, b != deletedLeft && b != deletedLeftIndirectly6)
		}

		has, err := ballots.HasLeft(deletedLeft)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)
	})

	deletedAssociation := uint64(3)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballots.Tx(tx)

		err := ballots.DeleteAssociation(100, true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = ballots.DeleteAssociation(deletedAssociation, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballots.Tx(tx)

		for _, b := range testBallots {
			has, err := ballots.HasAssociation(b.Voting)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, b.Voting != deletedAssociation)

			_, exists, err := ballots.Association(b.Voting)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), exists, b.Voting != deletedAssociation)

			has, err = ballots.HasLeft(b.Voter)
			assertErrorFail(t, fmt.Sprintf("%+v", b), err, nil)
			assert(t, fmt.Sprintf("%+v", b), has, b.Voter != deletedLeft && b.Voter != deletedLeftIndirectly6 && b.Voter != "mick")
		}
	})
}

func TestAssociations_iterateAssociations(t *testing.T) {
	db := ballotsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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

func TestAssociations_size(t *testing.T) {
	db := ballotsDB(t)

	t.Run("full", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballots.Tx(tx)

			size, err := ballots.Size()
			assertErrorFail(t, "", err, nil)
			assert(t, "", size, 4)

		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballots.Tx(tx)

			size, err := ballots.Size()
			assertErrorFail(t, "", err, nil)
			assert(t, "", size, 0)
		})
	})
}

func TestAssociations_pageOfAssociations(t *testing.T) {
	db := ballotsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

			association, exists, err := ballots.Association(100)
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			err = association.Set("paul", 10000)
			assertErrorFail(t, "", err, nil)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

			page, totalElements, totalPages, err := ballots.PageOfAssociationsWithLeftValue("alice", 1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})

		dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballots.Tx(tx)

			association, exists, err := ballots.Association(0)
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			err = association.Set("paul", 10000)
			assertErrorFail(t, "", err, nil)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
			ballots := ballots.Tx(tx)

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
		ballots := ballots.Tx(tx)

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
		assertError(t, "", err, boltron.ErrLeftNotFound)

		err = ballots.DeleteLeft("john", false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballots.Tx(tx)

		association, exists, err := ballots.Association(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = association.Set("paul", 1000)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballots.Tx(tx)

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
		assertError(t, "", err, boltron.ErrLeftNotFound)

		err = ballots.DeleteLeft("john", false)
		assertError(t, "", err, nil)
	})
}

func TestAssociations_customErrAssociationNotFound_and_customErrLeftNotFound_and_customErrRightNotFound(t *testing.T) {

	errAssociationNotFoundCustom := errors.New("custom association not found error")
	errLeftNotFoundCustom := errors.New("custom left not found error")
	errRightNotFoundCustom := errors.New("custom right not found error")

	customBallots := boltron.NewAssociations(
		"ballots",
		boltron.Uint64BinaryEncoding,       // voting id
		boltron.StringNaturalOrderEncoding, // voter
		boltron.Uint64Base36Encoding,       // ballot serial number
		&boltron.AssociationsOptions{
			ErrAssociationNotFound: errAssociationNotFoundCustom,
			ErrLeftNotFound:        errLeftNotFoundCustom,
			ErrRightNotFound:       errRightNotFoundCustom,
		},
	)

	db := newDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := customBallots.Tx(tx)

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
		assertError(t, "", err, errLeftNotFoundCustom)

		err = ballots.DeleteLeft("john", false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := customBallots.Tx(tx)

		association, exists, err := ballots.Association(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = association.Set("paul", 1000)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := customBallots.Tx(tx)

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
		assertError(t, "", err, errLeftNotFoundCustom)

		err = ballots.DeleteLeft("john", false)
		assertError(t, "", err, nil)
	})
}

func TestAssociations_uniqueKeys(t *testing.T) {

	customBallots := boltron.NewAssociations(
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
		ballots := customBallots.Tx(tx)

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

	customBallots := boltron.NewAssociations(
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
		ballots := customBallots.Tx(tx)

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

// TestAssociations_deleteCallback_orphanBucket_multipleLeftValues is the
// critical regression for the wrong-bucket bug in the associations deleteCallback
// (leftIndexBuckets.Stats().KeyN instead of leftIndexBucket.Stats().KeyN).
// With multiple left values, the old check would never fire because the parent
// bucket KeyN was > 1, leaving orphaned leftIndex sub-buckets.
func TestAssociations_deleteCallback_orphanBucket_multipleLeftValues(t *testing.T) {
	db := newDB(t)

	// Set up: association 1 has both "alice" and "bob" as left values.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		assoc, _, err := b.Association(1)
		assertErrorFail(t, "", err, nil)
		err = assoc.Set("alice", 1000)
		assertErrorFail(t, "", err, nil)
		err = assoc.Set("bob", 2000)
		assertErrorFail(t, "", err, nil)
	})

	// Delete the association for right-value 1 by removing alice's mapping.
	// This leaves bob still present. The leftIndex bucket for alice must be
	// cleaned up, while bob's must survive.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		assoc, _, err := b.Association(1)
		assertErrorFail(t, "", err, nil)

		// Delete alice's link to association 1.
		err = assoc.DeleteByLeft("alice", true)
		assertErrorFail(t, "", err, nil)

		// Within same tx: alice's left index must be gone, bob's must survive.
		has, err := b.HasLeft("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice HasLeft gone in same tx", has, false)

		has, err = b.HasLeft("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob HasLeft survives in same tx", has, true)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		has, err := b.HasLeft("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice HasLeft gone after tx", has, false)

		has, err = b.HasLeft("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob HasLeft survives after tx", has, true)
	})
}

// TestAssociations_deleteAssociation_orphanBucket_multipleLeftValues verifies
// that DeleteAssociation cleans up leftIndex sub-buckets for a left value that
// appears in multiple associations, once the last association is deleted.
func TestAssociations_deleteAssociation_orphanBucket_multipleLeftValues(t *testing.T) {
	db := newDB(t)

	// alice maps to both association 1 and 2; bob maps only to 1.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		a1, _, err := b.Association(1)
		assertErrorFail(t, "", err, nil)
		err = a1.Set("alice", 100)
		assertErrorFail(t, "", err, nil)
		err = a1.Set("bob", 200)
		assertErrorFail(t, "", err, nil)

		a2, _, err := b.Association(2)
		assertErrorFail(t, "", err, nil)
		err = a2.Set("alice", 300)
		assertErrorFail(t, "", err, nil)
	})

	// Delete association 1. alice's leftIndex sub-bucket must persist because
	// she still maps to association 2. bob's must be removed (no other assoc).
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		err := b.DeleteAssociation(1, true)
		assertErrorFail(t, "", err, nil)

		has, err := b.HasLeft("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice HasLeft survives (still has assoc 2) in same tx", has, true)

		has, err = b.HasLeft("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob HasLeft gone in same tx", has, false)
	})

	// Delete association 2. Now alice has no associations left; her sub-bucket
	// must be cleaned up. This is the case the old Stats().KeyN == 1 got wrong
	// when multiple left-index sub-buckets existed in the parent.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		err := b.DeleteAssociation(2, true)
		assertErrorFail(t, "", err, nil)

		has, err := b.HasLeft("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice HasLeft gone after last assoc deleted in same tx", has, false)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		has, err := b.HasLeft("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice HasLeft gone after tx", has, false)
	})
}

// TestAssociations_deleteByLeft_dataIntegrity ensures that deleting a left
// value from one association (emptying its leftIndex sub-bucket) does not
// disturb any other left value's right-side data in any other association.
func TestAssociations_deleteByLeft_dataIntegrity(t *testing.T) {
	// Data:
	//   assoc 10: alice→1000, bob→2000, carol→3000
	//   assoc 11: alice→4000, carol→5000
	//   assoc 12: alice→6000, bob→7000
	// Deleting bob from assoc 10: bob's leftIndex sub-bucket maps to {10, 12}.
	// That removes one entry but bob still references assoc 12 → sub-bucket persists.
	// Deleting bob from assoc 12 too: now bob's sub-bucket is empty → deleted.
	// All alice and carol right-values must remain exactly as written.

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		a10, _, err := b.Association(10)
		assertErrorFail(t, "", err, nil)
		a11, _, err := b.Association(11)
		assertErrorFail(t, "", err, nil)
		a12, _, err := b.Association(12)
		assertErrorFail(t, "", err, nil)

		for _, e := range []struct {
			a *boltron.AssociationTx[string, uint64]
			l string
			r uint64
		}{
			{a10, "alice", 1000}, {a10, "bob", 2000}, {a10, "carol", 3000},
			{a11, "alice", 4000}, {a11, "carol", 5000},
			{a12, "alice", 6000}, {a12, "bob", 7000},
		} {
			err := e.a.Set(e.l, e.r)
			assertErrorFail(t, "", err, nil)
		}
	})

	// Remove bob from assoc 10. Bob's index sub-bucket {10,12} → {12}: not deleted.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		a10, _, err := b.Association(10)
		assertErrorFail(t, "", err, nil)
		err = a10.DeleteByLeft("bob", true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)

		// bob still exists via assoc 12.
		has, err := b.HasLeft("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob still exists after partial removal", has, true)

		// All exact right-values must be intact.
		assertAssocRight(t, b, 10, "alice", uint64(1000))
		assertAssocRight(t, b, 10, "carol", uint64(3000))
		assertAssocRight(t, b, 11, "alice", uint64(4000))
		assertAssocRight(t, b, 11, "carol", uint64(5000))
		assertAssocRight(t, b, 12, "alice", uint64(6000))
		assertAssocRight(t, b, 12, "bob", uint64(7000))

		assertAssocSize(t, b, 10, 2) // alice + carol
		assertAssocSize(t, b, 11, 2)
		assertAssocSize(t, b, 12, 2)
	})

	// Remove bob from assoc 12 too. Now bob's sub-bucket is empty → deleted.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		a12, _, err := b.Association(12)
		assertErrorFail(t, "", err, nil)
		err = a12.DeleteByLeft("bob", true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)

		has, err := b.HasLeft("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob is gone after last removal", has, false)

		// Every alice and carol entry must be exactly as stored.
		assertAssocRight(t, b, 10, "alice", uint64(1000))
		assertAssocRight(t, b, 10, "carol", uint64(3000))
		assertAssocRight(t, b, 11, "alice", uint64(4000))
		assertAssocRight(t, b, 11, "carol", uint64(5000))
		assertAssocRight(t, b, 12, "alice", uint64(6000))

		assertAssocSize(t, b, 10, 2)
		assertAssocSize(t, b, 11, 2)
		assertAssocSize(t, b, 12, 1)

		assertLeftAssociations(t, b, "alice", []uint64{10, 11, 12})
		assertLeftAssociations(t, b, "carol", []uint64{10, 11})
	})
}

// TestAssociations_deleteAssociation_dataIntegrity verifies that
// DeleteAssociation removes only the targeted association and its index
// entries. Every left-value that also appears in surviving associations must
// have exactly the right-values that were written for those associations.
func TestAssociations_deleteAssociation_dataIntegrity(t *testing.T) {
	// Data:
	//   assoc 10: alice→1000, bob→2000   ← will be deleted
	//   assoc 11: alice→3000, carol→4000
	//   assoc 12: carol→5000
	// Deleting assoc 10: alice's leftIndex entry for 10 removed (11 survives).
	//                    bob's leftIndex entry for 10 removed → sub-bucket empty → deleted.
	// carol is not in assoc 10 → her sub-bucket completely untouched.

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		a10, _, err := b.Association(10)
		assertErrorFail(t, "", err, nil)
		a11, _, err := b.Association(11)
		assertErrorFail(t, "", err, nil)
		a12, _, err := b.Association(12)
		assertErrorFail(t, "", err, nil)

		for _, e := range []struct {
			a *boltron.AssociationTx[string, uint64]
			l string
			r uint64
		}{
			{a10, "alice", 1000}, {a10, "bob", 2000},
			{a11, "alice", 3000}, {a11, "carol", 4000},
			{a12, "carol", 5000},
		} {
			err := e.a.Set(e.l, e.r)
			assertErrorFail(t, "", err, nil)
		}
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		err := b.DeleteAssociation(10, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)

		has, err := b.HasAssociation(10)
		assertErrorFail(t, "", err, nil)
		assert(t, "assoc 10 is gone", has, false)

		has, err = b.HasLeft("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob is gone (was only in assoc 10)", has, false)

		// alice is still in assoc 11 with the correct right-value.
		assertAssocRight(t, b, 11, "alice", uint64(3000))
		assertAssocRight(t, b, 11, "carol", uint64(4000))
		assertAssocRight(t, b, 12, "carol", uint64(5000))

		assertAssocSize(t, b, 11, 2)
		assertAssocSize(t, b, 12, 1)

		assertLeftAssociations(t, b, "alice", []uint64{11})
		assertLeftAssociations(t, b, "carol", []uint64{11, 12})
	})
}

// TestAssociations_multipleSimultaneousBucketDeletes_dataIntegrity verifies
// that deleting the last left-index entry for multiple left values in one
// transaction each cleans up exactly its own sub-bucket, leaving all other
// left values and exact right-values intact.
func TestAssociations_multipleSimultaneousBucketDeletes_dataIntegrity(t *testing.T) {
	// Data:
	//   assoc 10: alice→1000, bob→2000, carol→3000
	//   assoc 11: alice→4000, carol→5000
	//
	// In ONE transaction on assoc 10:
	//   DeleteByLeft("bob")   → bob's sub-bucket   {10}    → empty → DELETED
	//   DeleteByLeft("carol") → carol's sub-bucket {10,11} → {11}  → survives
	//
	// alice sub-bucket {10,11} must be completely intact with correct rights.

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		a10, _, err := b.Association(10)
		assertErrorFail(t, "", err, nil)
		a11, _, err := b.Association(11)
		assertErrorFail(t, "", err, nil)

		assertErrorFail(t, "", a10.Set("alice", uint64(1000)), nil)
		assertErrorFail(t, "", a10.Set("bob", uint64(2000)), nil)
		assertErrorFail(t, "", a10.Set("carol", uint64(3000)), nil)
		assertErrorFail(t, "", a11.Set("alice", uint64(4000)), nil)
		assertErrorFail(t, "", a11.Set("carol", uint64(5000)), nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)
		a10, _, err := b.Association(10)
		assertErrorFail(t, "", err, nil)

		err = a10.DeleteByLeft("bob", true)
		assertErrorFail(t, "", err, nil)
		err = a10.DeleteByLeft("carol", true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		b := ballots.Tx(tx)

		has, err := b.HasLeft("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob gone", has, false)

		// carol survives via assoc 11.
		has, err = b.HasLeft("carol")
		assertErrorFail(t, "", err, nil)
		assert(t, "carol still present", has, true)

		// All exact right-values intact.
		assertAssocRight(t, b, 10, "alice", uint64(1000))
		assertAssocRight(t, b, 11, "alice", uint64(4000))
		assertAssocRight(t, b, 11, "carol", uint64(5000))

		assertAssocSize(t, b, 10, 1) // alice only
		assertAssocSize(t, b, 11, 2) // alice + carol

		assertLeftAssociations(t, b, "alice", []uint64{10, 11})
		assertLeftAssociations(t, b, "carol", []uint64{11})
	})
}

func ballotsDB(t testing.TB) *bolt.DB {
	t.Helper()

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		ballots := ballots.Tx(tx)

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
