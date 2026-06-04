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
	elections = boltron.NewCollections(
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
		{0, "alice", newBallot(1)},
		{0, "bob", newBallot(1)},
		{0, "chriss", newBallot(2)},
		{0, "dave", newBallot(0)},
		{0, "edit", newBallot(2)},

		{5, "alice", newBallot(0)},
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

		{7, "alice", newBallot(0)},
		{7, "dave", newBallot(0)},
	}

	testElectionsCollections = []uint64{0, 5, 6, 7}

	testElectionsKeys = []string{
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

	testElectionsCollectionsWithKeyAlice = []uint64{0, 5, 7}
)

type ballot struct {
	Vote int `json:"vote"`
}

func newBallot(vote int) *ballot {
	return &ballot{Vote: vote}
}

func TestCollections(t *testing.T) {
	db := electionsDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := elections.Tx(tx)

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
		elections := elections.Tx(tx)

		err := elections.DeleteKey("unknown", true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = elections.DeleteKey(deletedKey, true)
		assertErrorFail(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		deletedKeyIndirectly := "alice"

		elections := elections.Tx(tx)

		election, _, err := elections.Collection(0)
		assertErrorFail(t, "", err, nil)

		err = election.Delete(deletedKeyIndirectly, true)
		assertErrorFail(t, "", err, nil)

		err = election.Delete(deletedKeyIndirectly, true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		has, err := elections.HasKey(deletedKeyIndirectly)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, true)

		var collections []uint64
		next, err := elections.IterateCollectionsWithKey(deletedKeyIndirectly, nil, false, func(c uint64) (bool, error) {
			collections = append(collections, c)
			return true, nil
		})
		assertErrorFail(t, "", err, nil)
		assert(t, "", next, nil)

		assert(t, "", collections, []uint64{5, 7})
	})

	deletedKeyIndirectly6 := "george"

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {

		elections := elections.Tx(tx)

		election, _, err := elections.Collection(6)
		assertErrorFail(t, "", err, nil)

		err = election.Delete(deletedKeyIndirectly6, true)
		assertErrorFail(t, "", err, nil)

		err = election.Delete(deletedKeyIndirectly6, true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		has, err := elections.HasKey(deletedKeyIndirectly6)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		var collections []uint64
		next, err := elections.IterateCollectionsWithKey(deletedKeyIndirectly6, nil, false, func(c uint64) (bool, error) {
			collections = append(collections, c)
			return true, nil
		})
		assertErrorFail(t, "", err, nil)
		assert(t, "", next, nil)

		assert(t, "", collections, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := elections.Tx(tx)

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
			assert(t, fmt.Sprintf("%+v", e), has, e != deletedKey && e != deletedKeyIndirectly6)
		}

		has, err := elections.HasKey(deletedKey)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)
	})

	deletedCollection := uint64(5)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := elections.Tx(tx)

		err := elections.DeleteCollection(100, true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = elections.DeleteCollection(deletedCollection, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := elections.Tx(tx)

		for _, e := range testElections {
			has, err := elections.HasCollection(e.Election)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), has, e.Election != deletedCollection)

			_, exists, err := elections.Collection(e.Election)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), exists, e.Election != deletedCollection)

			has, err = elections.HasKey(e.Voter)
			assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			assert(t, fmt.Sprintf("%+v", e), has, e.Voter != deletedKey && e.Voter != deletedKeyIndirectly6 && e.Voter != "mick")
		}
	})
}

func TestCollections_iterateCollections(t *testing.T) {
	db := electionsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateCollections(nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollections[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollections))
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateCollections(nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollections[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 6)

			next, err = elections.IterateCollections(next, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollections[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollections))
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateCollections(nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollections[len(testElectionsCollections)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollections))
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateCollections(nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollections[len(testElectionsCollections)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 5)

			next, err = elections.IterateCollections(next, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollections[len(testElectionsCollections)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollections))
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var count int
			next, err := elections.IterateCollections(nil, false, func(_ uint64) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestCollections_size(t *testing.T) {
	db := electionsDB(t)

	t.Run("full", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			size, err := elections.Size()
			assertErrorFail(t, "", err, nil)
			assert(t, "", size, 4)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			size, err := elections.Size()
			assertErrorFail(t, "", err, nil)
			assert(t, "", size, 0)
		})
	})
}

func TestCollections_pageOfCollections(t *testing.T) {
	db := electionsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			_, _, _, err := elections.PageOfCollections(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = elections.PageOfCollections(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := elections.PageOfCollections(1, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollections(0, 1))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = elections.PageOfCollections(2, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollections(2, 3))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			_, _, _, err := elections.PageOfCollections(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = elections.PageOfCollections(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := elections.PageOfCollections(1, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollections(3, 2))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = elections.PageOfCollections(2, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollections(1, 0))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			page, totalElements, totalPages, err := elections.PageOfCollections(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestCollections_iterateCollectionsWithKey(t *testing.T) {
	db := electionsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateCollectionsWithKey("alice", nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAlice[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollectionsWithKeyAlice))
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateCollectionsWithKey("alice", nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAlice[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 7)

			next, err = elections.IterateCollectionsWithKey("alice", next, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAlice[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollectionsWithKeyAlice))
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateCollectionsWithKey("alice", nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAlice[len(testElectionsCollectionsWithKeyAlice)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollectionsWithKeyAlice))
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateCollectionsWithKey("alice", nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAlice[len(testElectionsCollectionsWithKeyAlice)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 0)

			next, err = elections.IterateCollectionsWithKey("alice", next, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAlice[len(testElectionsCollectionsWithKeyAlice)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollectionsWithKeyAlice))
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var count int
			next, err := elections.IterateCollectionsWithKey("alice", nil, false, func(_ uint64) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})

		dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			collection, exists, err := elections.Collection(0)
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			overwritten, err := collection.Save("mick", newBallot(0), false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", overwritten, false)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var count int
			next, err := elections.IterateCollectionsWithKey("alice", nil, false, func(_ uint64) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestCollections_pageOfCollectionsWithKey(t *testing.T) {
	db := electionsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			_, _, _, err := elections.PageOfCollectionsWithKey("alice", -1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = elections.PageOfCollectionsWithKey("alice", 0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := elections.PageOfCollectionsWithKey("alice", 1, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollectionsWithKeyAlice(0, 1))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = elections.PageOfCollectionsWithKey("alice", 2, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollectionsWithKeyAlice(2))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			_, _, _, err := elections.PageOfCollectionsWithKey("alice", -1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = elections.PageOfCollectionsWithKey("alice", 0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := elections.PageOfCollectionsWithKey("alice", 1, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollectionsWithKeyAlice(2, 1))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = elections.PageOfCollectionsWithKey("alice", 2, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollectionsWithKeyAlice(0))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			page, totalElements, totalPages, err := elections.PageOfCollectionsWithKey("alice", 1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})

		dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			collection, exists, err := elections.Collection(0)
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			overwritten, err := collection.Save("mick", newBallot(0), false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", overwritten, false)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			page, totalElements, totalPages, err := elections.PageOfCollectionsWithKey("alice", 1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestCollections_iterateKeys(t *testing.T) {
	db := electionsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateKeys(nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testElectionsKeys[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateKeys(nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testElectionsKeys[i])
				i++
				if i == 3 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "dave")

			next, err = elections.IterateKeys(next, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testElectionsKeys[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateKeys(nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testElectionsKeys[len(testElectionsKeys)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			var i int
			next, err := elections.IterateKeys(nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testElectionsKeys[len(testElectionsKeys)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "mick")

			next, err = elections.IterateKeys(next, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate key #%v", i), v, testElectionsKeys[len(testElectionsKeys)-1-i])
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
			elections := elections.Tx(tx)

			var count int
			next, err := elections.IterateKeys(nil, false, func(_ string) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestCollections_pageOfKeys(t *testing.T) {
	db := electionsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			_, _, _, err := elections.PageOfKeys(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = elections.PageOfKeys(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := elections.PageOfKeys(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsKeys(0, 1, 2))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = elections.PageOfKeys(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsKeys(3, 4, 5))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = elections.PageOfKeys(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsKeys(6, 7, 8))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = elections.PageOfKeys(4, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsKeys(9))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			_, _, _, err := elections.PageOfKeys(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = elections.PageOfKeys(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := elections.PageOfKeys(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsKeys(9, 8, 7))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = elections.PageOfKeys(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsKeys(6, 5, 4))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = elections.PageOfKeys(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsKeys(3, 2, 1))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)

			page, totalElements, totalPages, err = elections.PageOfKeys(4, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsKeys(0))
			assert(t, "", totalElements, 10)
			assert(t, "", totalPages, 4)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := elections.Tx(tx)

			page, totalElements, totalPages, err := elections.PageOfKeys(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestCollections_ErrCollectionNotFound_and_ErrKeyNotFound(t *testing.T) {
	db := newDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := elections.Tx(tx)

		_, exists, err := elections.Collection(0)
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := elections.HasCollection(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = elections.HasKey("john")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = elections.DeleteCollection(0, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = elections.DeleteCollection(0, false)
		assertError(t, "", err, nil)

		err = elections.DeleteKey("john", true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = elections.DeleteKey("john", false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := elections.Tx(tx)

		collection, exists, err := elections.Collection(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		overwritten, err := collection.Save("paul", newBallot(0), false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := elections.Tx(tx)

		_, exists, err := elections.Collection(0)
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := elections.HasCollection(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = elections.HasKey("john")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = elections.DeleteCollection(0, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = elections.DeleteCollection(0, false)
		assertError(t, "", err, nil)

		err = elections.DeleteKey("john", true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = elections.DeleteKey("john", false)
		assertError(t, "", err, nil)
	})
}

func TestCollections_customErrCollectionNotFound_and_customErrKeyNotFound(t *testing.T) {

	errCollectionNotFoundCustom := errors.New("custom collection not found error")
	errKeyNotFoundCustom := errors.New("custom key not found error")

	customElections := boltron.NewCollections(
		"elections",
		boltron.Uint64BinaryEncoding,       // election id
		boltron.StringEncoding,             // voter id
		boltron.NewJSONEncoding[*ballot](), // ballot with a vote
		&boltron.CollectionsOptions{
			ErrCollectionNotFound: errCollectionNotFoundCustom,
			ErrKeyNotFound:        errKeyNotFoundCustom,
		},
	)

	db := newDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := customElections.Tx(tx)

		_, exists, err := elections.Collection(0)
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := elections.HasCollection(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = elections.HasKey("john")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = elections.DeleteCollection(0, true)
		assertError(t, "", err, errCollectionNotFoundCustom)

		err = elections.DeleteCollection(0, false)
		assertError(t, "", err, nil)

		err = elections.DeleteKey("john", true)
		assertError(t, "", err, errKeyNotFoundCustom)

		err = elections.DeleteKey("john", false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := customElections.Tx(tx)

		collection, exists, err := elections.Collection(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		overwritten, err := collection.Save("paul", newBallot(0), false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := customElections.Tx(tx)

		_, exists, err := elections.Collection(0)
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := elections.HasCollection(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = elections.HasKey("john")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = elections.DeleteCollection(0, true)
		assertError(t, "", err, errCollectionNotFoundCustom)

		err = elections.DeleteCollection(0, false)
		assertError(t, "", err, nil)

		err = elections.DeleteKey("john", true)
		assertError(t, "", err, errKeyNotFoundCustom)

		err = elections.DeleteKey("john", false)
		assertError(t, "", err, nil)
	})
}

func TestCollections_uniqueKeys(t *testing.T) {

	customElections := boltron.NewCollections(
		"elections",
		boltron.Uint64BinaryEncoding,       // election id
		boltron.StringEncoding,             // voter id
		boltron.NewJSONEncoding[*ballot](), // ballot with a vote
		&boltron.CollectionsOptions{
			UniqueKeys: true,
		},
	)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := customElections.Tx(tx)

		election0, exists, err := elections.Collection(0)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		election1, exists, err := elections.Collection(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		overwritten, err := election0.Save("john", newBallot(0), false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)

		overwritten, err = election1.Save("john", newBallot(0), false)
		assertErrorFail(t, "", err, boltron.ErrKeyExists)
		assert(t, "", overwritten, false)
	})
}

func TestCollections_uniqueKeys_customErrKeyExists(t *testing.T) {

	errKeyExistsCustom := errors.New("custom key exists error")

	customElections := boltron.NewCollections(
		"elections",
		boltron.Uint64BinaryEncoding,       // election id
		boltron.StringEncoding,             // voter id
		boltron.NewJSONEncoding[*ballot](), // ballot with a vote
		&boltron.CollectionsOptions{
			UniqueKeys:   true,
			ErrKeyExists: errKeyExistsCustom,
		},
	)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := customElections.Tx(tx)

		election0, exists, err := elections.Collection(0)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		election1, exists, err := elections.Collection(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		overwritten, err := election0.Save("john", newBallot(0), false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)

		overwritten, err = election1.Save("john", newBallot(0), false)
		assertErrorFail(t, "", err, errKeyExistsCustom)
		assert(t, "", overwritten, false)
	})
}

// TestCollections_deleteCallback_orphanBucket_singleKey exercises the
// deleteCallback in Collection() when only one key sub-bucket exists.
// Previously this worked by accident because Stats().KeyN happened to be 1.
func TestCollections_deleteCallback_orphanBucket_singleKey(t *testing.T) {
	db := newDB(t)

	// Set up: one collection, one voter → one key sub-bucket ("alice").
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		election, _, err := c.Collection(0)
		assertErrorFail(t, "", err, nil)
		_, err = election.Save("alice", newBallot(1), false)
		assertErrorFail(t, "", err, nil)
	})

	// Delete alice from election 0 — alice's key sub-bucket should disappear.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		election, _, err := c.Collection(0)
		assertErrorFail(t, "", err, nil)
		err = election.Delete("alice", true)
		assertErrorFail(t, "", err, nil)

		// Within the same transaction, HasKey must already return false.
		has, err := c.HasKey("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "HasKey within same tx", has, false)
	})

	// After the transaction, alice must not appear in any index.
	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		has, err := c.HasKey("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "HasKey after tx", has, false)

		var count int
		_, err = c.IterateCollectionsWithKey("alice", nil, false, func(_ uint64) (bool, error) {
			count++
			return true, nil
		})
		assertErrorFail(t, "", err, nil)
		assert(t, "IterateCollectionsWithKey count after delete", count, 0)
	})
}

// TestCollections_deleteCallback_orphanBucket_multipleKeys is the critical
// regression for the wrong-bucket bug. It ensures the key sub-bucket for the
// deleted key is cleaned up even when *other* key sub-buckets still exist in
// the same parent bucket (old code would leave it orphaned).
func TestCollections_deleteCallback_orphanBucket_multipleKeys(t *testing.T) {
	db := newDB(t)

	// Set up: two collections sharing voters "alice" and "bob".
	// After deleting alice from election 0, election 5 still has alice →
	// alice's key bucket must survive. But when we also delete alice from
	// election 5, alice's key bucket must be removed while bob's stays.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		e0, _, err := c.Collection(0)
		assertErrorFail(t, "", err, nil)
		e5, _, err := c.Collection(5)
		assertErrorFail(t, "", err, nil)

		_, err = e0.Save("alice", newBallot(1), false)
		assertErrorFail(t, "", err, nil)
		_, err = e0.Save("bob", newBallot(2), false)
		assertErrorFail(t, "", err, nil)
		_, err = e5.Save("alice", newBallot(3), false)
		assertErrorFail(t, "", err, nil)
		_, err = e5.Save("bob", newBallot(4), false)
		assertErrorFail(t, "", err, nil)
	})

	// Delete alice from election 0 only. "alice" key bucket must persist
	// (she still belongs to election 5); "bob" key bucket must also persist.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		e0, _, err := c.Collection(0)
		assertErrorFail(t, "", err, nil)
		err = e0.Delete("alice", true)
		assertErrorFail(t, "", err, nil)

		has, err := c.HasKey("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice still in election 5 within same tx", has, true)

		has, err = c.HasKey("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob still present within same tx", has, true)
	})

	// Now delete alice from election 5 too. Now her key bucket must vanish,
	// but bob's must remain. This is the case the old wrong-bucket check
	// (keysBucket.Stats().KeyN == 1) missed: the parent bucket had 2 children
	// ("alice", "bob"), so KeyN was 2, not 1, and the cleanup never ran.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		e5, _, err := c.Collection(5)
		assertErrorFail(t, "", err, nil)
		err = e5.Delete("alice", true)
		assertErrorFail(t, "", err, nil)

		// alice must be gone, bob must survive — within the same transaction.
		has, err := c.HasKey("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice gone in same tx", has, false)

		has, err = c.HasKey("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob survives in same tx", has, true)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		has, err := c.HasKey("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice gone after tx", has, false)

		has, err = c.HasKey("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob survives after tx", has, true)
	})
}

// TestCollections_deleteCollection_orphanBucket verifies that DeleteCollection
// cleans up key sub-buckets for a key that appears in multiple collections
// when the deleted collection is the last one referencing it.
func TestCollections_deleteCollection_orphanBucket_multipleKeys(t *testing.T) {
	db := newDB(t)

	// alice appears in election 0 only; bob appears in elections 0 and 5.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		e0, _, err := c.Collection(0)
		assertErrorFail(t, "", err, nil)
		e5, _, err := c.Collection(5)
		assertErrorFail(t, "", err, nil)

		_, err = e0.Save("alice", newBallot(1), false)
		assertErrorFail(t, "", err, nil)
		_, err = e0.Save("bob", newBallot(2), false)
		assertErrorFail(t, "", err, nil)
		_, err = e5.Save("bob", newBallot(3), false)
		assertErrorFail(t, "", err, nil)
	})

	// Delete election 0. alice's key bucket must vanish (no other reference).
	// bob's key bucket must survive (still in election 5).
	// Old code: keyBucket.Stats().KeyN == 1 checked *keyBucket* which still
	// had KeyN==1 (stale). But the wrong-bucket bug in the parent iterator
	// could cause the bucket to persist when multiple key types existed.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		err := c.DeleteCollection(0, true)
		assertErrorFail(t, "", err, nil)

		has, err := c.HasKey("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice gone in same tx after DeleteCollection", has, false)

		has, err = c.HasKey("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob survives in same tx after DeleteCollection", has, true)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		has, err := c.HasKey("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice gone after tx", has, false)

		has, err = c.HasKey("bob")
		assertErrorFail(t, "", err, nil)
		assert(t, "bob survives after tx", has, true)
	})
}

// TestCollections_deleteKey_dataIntegrity verifies that deleting a key (voter)
// that is present in only one collection causes cleanup of exactly that key's
// index sub-bucket and nothing else. Every ballot value in every surviving
// collection must be byte-for-byte identical to what was stored.
func TestCollections_deleteKey_dataIntegrity(t *testing.T) {
	// Data layout:
	//   election 10: alice→ballot{1}, bob→ballot{2}, carol→ballot{3}
	//   election 11: alice→ballot{4}, bob→ballot{5}
	//   election 12: alice→ballot{6}, bob→ballot{7}, carol→ballot{8}
	// carol's key index sub-bucket maps her to elections {10, 12}.
	// Deleting carol from election 10 leaves {12}; sub-bucket must NOT be deleted.
	// Deleting carol from election 12 empties her sub-bucket → sub-bucket deleted.
	// After both deletions every alice and bob ballot must be intact.

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		e10, _, err := c.Collection(10)
		assertErrorFail(t, "", err, nil)
		e11, _, err := c.Collection(11)
		assertErrorFail(t, "", err, nil)
		e12, _, err := c.Collection(12)
		assertErrorFail(t, "", err, nil)

		for _, entry := range []struct {
			e *boltron.CollectionTx[string, *ballot]
			k string
			v int
		}{
			{e10, "alice", 1}, {e10, "bob", 2}, {e10, "carol", 3},
			{e11, "alice", 4}, {e11, "bob", 5},
			{e12, "alice", 6}, {e12, "bob", 7}, {e12, "carol", 8},
		} {
			_, err := entry.e.Save(entry.k, newBallot(entry.v), false)
			assertErrorFail(t, "", err, nil)
		}
	})

	// Phase 1: remove carol from election 10. Her sub-bucket must NOT be deleted
	// (she is still in election 12). All other data must be untouched.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		e10, _, err := c.Collection(10)
		assertErrorFail(t, "", err, nil)
		err = e10.Delete("carol", true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)

		assertElectionBallot(t, c, 10, "alice", newBallot(1))
		assertElectionBallot(t, c, 10, "bob", newBallot(2))
		assertElectionMissing(t, c, 10, "carol")

		assertElectionBallot(t, c, 11, "alice", newBallot(4))
		assertElectionBallot(t, c, 11, "bob", newBallot(5))

		assertElectionBallot(t, c, 12, "alice", newBallot(6))
		assertElectionBallot(t, c, 12, "bob", newBallot(7))
		assertElectionBallot(t, c, 12, "carol", newBallot(8)) // still in 12

		assertElectionSize(t, c, 10, 2) // alice + bob
		assertElectionSize(t, c, 11, 2)
		assertElectionSize(t, c, 12, 3)

		// carol still has one collection entry; alice and bob have all theirs.
		assertKeyCollections(t, c, "alice", []uint64{10, 11, 12})
		assertKeyCollections(t, c, "bob", []uint64{10, 11, 12})
		assertKeyCollections(t, c, "carol", []uint64{12})
	})

	// Phase 2: remove carol from election 12. Her sub-bucket is now empty → deleted.
	// Every alice and bob ballot must still be the same.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		e12, _, err := c.Collection(12)
		assertErrorFail(t, "", err, nil)
		err = e12.Delete("carol", true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)

		// carol is completely gone — her sub-bucket was deleted.
		has, err := c.HasKey("carol")
		assertErrorFail(t, "", err, nil)
		assert(t, "carol HasKey is false", has, false)

		// All alice and bob data is exactly as written.
		assertElectionBallot(t, c, 10, "alice", newBallot(1))
		assertElectionBallot(t, c, 10, "bob", newBallot(2))
		assertElectionBallot(t, c, 11, "alice", newBallot(4))
		assertElectionBallot(t, c, 11, "bob", newBallot(5))
		assertElectionBallot(t, c, 12, "alice", newBallot(6))
		assertElectionBallot(t, c, 12, "bob", newBallot(7))

		assertElectionSize(t, c, 10, 2)
		assertElectionSize(t, c, 11, 2)
		assertElectionSize(t, c, 12, 2)

		assertKeyCollections(t, c, "alice", []uint64{10, 11, 12})
		assertKeyCollections(t, c, "bob", []uint64{10, 11, 12})
	})
}

// TestCollections_deleteCollection_dataIntegrity verifies that DeleteCollection
// removes exactly the targeted collection and its index entries, leaving every
// other collection and every key's remaining index entries fully intact.
func TestCollections_deleteCollection_dataIntegrity(t *testing.T) {
	// Data:
	//   election 10: alice→ballot{1}, bob→ballot{2}  ← will be deleted
	//   election 11: alice→ballot{3}, bob→ballot{4}, carol→ballot{5}
	//   election 12: carol→ballot{6}
	// Deleting election 10: alice/bob key sub-buckets must survive (still in 11).
	// carol is not in election 10, so her bucket is untouched.

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		e10, _, err := c.Collection(10)
		assertErrorFail(t, "", err, nil)
		e11, _, err := c.Collection(11)
		assertErrorFail(t, "", err, nil)
		e12, _, err := c.Collection(12)
		assertErrorFail(t, "", err, nil)

		_, err = e10.Save("alice", newBallot(1), false)
		assertErrorFail(t, "", err, nil)
		_, err = e10.Save("bob", newBallot(2), false)
		assertErrorFail(t, "", err, nil)
		_, err = e11.Save("alice", newBallot(3), false)
		assertErrorFail(t, "", err, nil)
		_, err = e11.Save("bob", newBallot(4), false)
		assertErrorFail(t, "", err, nil)
		_, err = e11.Save("carol", newBallot(5), false)
		assertErrorFail(t, "", err, nil)
		_, err = e12.Save("carol", newBallot(6), false)
		assertErrorFail(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		err := c.DeleteCollection(10, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)

		has, err := c.HasCollection(10)
		assertErrorFail(t, "", err, nil)
		assert(t, "election 10 is gone", has, false)

		// alice and bob still exist via election 11.
		assertElectionBallot(t, c, 11, "alice", newBallot(3))
		assertElectionBallot(t, c, 11, "bob", newBallot(4))
		assertElectionBallot(t, c, 11, "carol", newBallot(5))
		assertElectionBallot(t, c, 12, "carol", newBallot(6))

		assertElectionSize(t, c, 11, 3)
		assertElectionSize(t, c, 12, 1)

		assertKeyCollections(t, c, "alice", []uint64{11})
		assertKeyCollections(t, c, "bob", []uint64{11})
		assertKeyCollections(t, c, "carol", []uint64{11, 12})
	})
}

// TestCollections_deleteKeyBulk_dataIntegrity tests CollectionsTx.DeleteKey,
// which removes a voter from ALL collections in one call and deletes their
// key index sub-bucket. Every other voter's ballot in every other collection
// must survive exactly as written.
func TestCollections_deleteKeyBulk_dataIntegrity(t *testing.T) {
	// Data:
	//   election 10: alice→ballot{1}, bob→ballot{2}, carol→ballot{3}
	//   election 11: alice→ballot{4}, bob→ballot{5}
	//   election 12: bob→ballot{6}, carol→ballot{7}
	// DeleteKey("alice"): alice sub-bucket deleted; bob and carol untouched.

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		e10, _, err := c.Collection(10)
		assertErrorFail(t, "", err, nil)
		e11, _, err := c.Collection(11)
		assertErrorFail(t, "", err, nil)
		e12, _, err := c.Collection(12)
		assertErrorFail(t, "", err, nil)

		_, err = e10.Save("alice", newBallot(1), false)
		assertErrorFail(t, "", err, nil)
		_, err = e10.Save("bob", newBallot(2), false)
		assertErrorFail(t, "", err, nil)
		_, err = e10.Save("carol", newBallot(3), false)
		assertErrorFail(t, "", err, nil)
		_, err = e11.Save("alice", newBallot(4), false)
		assertErrorFail(t, "", err, nil)
		_, err = e11.Save("bob", newBallot(5), false)
		assertErrorFail(t, "", err, nil)
		_, err = e12.Save("bob", newBallot(6), false)
		assertErrorFail(t, "", err, nil)
		_, err = e12.Save("carol", newBallot(7), false)
		assertErrorFail(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		err := c.DeleteKey("alice", true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)

		has, err := c.HasKey("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice completely gone", has, false)

		assertElectionBallot(t, c, 10, "bob", newBallot(2))
		assertElectionBallot(t, c, 10, "carol", newBallot(3))
		assertElectionBallot(t, c, 11, "bob", newBallot(5))
		assertElectionBallot(t, c, 12, "bob", newBallot(6))
		assertElectionBallot(t, c, 12, "carol", newBallot(7))

		assertElectionMissing(t, c, 10, "alice")
		assertElectionMissing(t, c, 11, "alice")
		assertElectionMissing(t, c, 12, "alice")

		assertElectionSize(t, c, 10, 2) // bob + carol
		assertElectionSize(t, c, 11, 1) // bob
		assertElectionSize(t, c, 12, 2) // bob + carol

		assertKeyCollections(t, c, "bob", []uint64{10, 11, 12})
		assertKeyCollections(t, c, "carol", []uint64{10, 12})
	})
}

// Multiple simultaneous sub-bucket cleanups in one transaction
//
// The highest-risk scenario: a single write tx empties and deletes MULTIPLE
// different index sub-buckets. If the cursor or bbolt page state is disturbed
// by the first DeleteBucket, subsequent deletions could corrupt unrelated data.

// TestCollections_multipleSimultaneousBucketDeletes_dataIntegrity verifies
// that deleting entries from multiple keys in a single transaction — where each
// deletion empties a different index sub-bucket — does not disturb any
// surviving entry in any remaining sub-bucket.
func TestCollections_multipleSimultaneousBucketDeletes_dataIntegrity(t *testing.T) {
	// Data:
	//   election 10: alice→ballot{1}, bob→ballot{2}, carol→ballot{3}
	//   election 11: alice→ballot{4}, carol→ballot{5}
	//   election 12: bob→ballot{6}
	//
	// In ONE transaction:
	//   e11.Delete("carol") → carol's sub-bucket {10,11} → {10}: survives
	//   e12.Delete("bob")   → bob's sub-bucket   {10,12} → {10}: survives
	//
	// alice sub-bucket {10,11} must be completely intact.
	// election 10 must still have alice, bob, carol with exact ballot values.

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		e10, _, err := c.Collection(10)
		assertErrorFail(t, "", err, nil)
		e11, _, err := c.Collection(11)
		assertErrorFail(t, "", err, nil)
		e12, _, err := c.Collection(12)
		assertErrorFail(t, "", err, nil)

		_, err = e10.Save("alice", newBallot(1), false)
		assertErrorFail(t, "", err, nil)
		_, err = e10.Save("bob", newBallot(2), false)
		assertErrorFail(t, "", err, nil)
		_, err = e10.Save("carol", newBallot(3), false)
		assertErrorFail(t, "", err, nil)
		_, err = e11.Save("alice", newBallot(4), false)
		assertErrorFail(t, "", err, nil)
		_, err = e11.Save("carol", newBallot(5), false)
		assertErrorFail(t, "", err, nil)
		_, err = e12.Save("bob", newBallot(6), false)
		assertErrorFail(t, "", err, nil)
	})

	// Two sub-bucket operations in a single transaction.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)

		e11, _, err := c.Collection(11)
		assertErrorFail(t, "", err, nil)
		err = e11.Delete("carol", true)
		assertErrorFail(t, "", err, nil)

		e12, _, err := c.Collection(12)
		assertErrorFail(t, "", err, nil)
		err = e12.Delete("bob", true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)

		// alice's key sub-bucket is intact with her election 10 and 11 entries.
		assertKeyCollections(t, c, "alice", []uint64{10, 11})
		assertElectionBallot(t, c, 10, "alice", newBallot(1))
		assertElectionBallot(t, c, 11, "alice", newBallot(4))

		// election 10 retains all three voters with exact ballot values.
		assertElectionBallot(t, c, 10, "bob", newBallot(2))
		assertElectionBallot(t, c, 10, "carol", newBallot(3))
		assertElectionSize(t, c, 10, 3)

		// carol and bob each lost one election but their election 10 entries remain.
		assertKeyCollections(t, c, "carol", []uint64{10})
		assertKeyCollections(t, c, "bob", []uint64{10})
		assertElectionSize(t, c, 11, 1) // alice only
		assertElectionSize(t, c, 12, 0) // bob removed, empty
	})
}

// TestCollections_deleteKey_txFromBucket is the regression guard for
// CollectionsTx.DeleteKey. It puts alice in 5 elections, then calls DeleteKey
// which internally creates a fresh CollectionTx via txFromBucket for each of
// the 5 ForEach iterations. Bob and carol's data must be completely intact.
func TestCollections_deleteKey_txFromBucket(t *testing.T) {
	// alice: elections 10–14 (5 iterations through the internal ForEach)
	// bob:   elections 10–12 (3 elections, with exact ballot values)
	// carol: elections 13–14 (2 elections, with exact ballot values)
	//
	// DeleteKey("alice") triggers 5 separate txFromBucket calls.
	// bob and carol must survive with byte-exact ballot data.

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)

		for _, entry := range []struct {
			electionID uint64
			voter      string
			val        int
		}{
			// alice — 5 elections
			{10, "alice", 1}, {11, "alice", 2}, {12, "alice", 3},
			{13, "alice", 4}, {14, "alice", 5},
			// bob — 3 elections
			{10, "bob", 101}, {11, "bob", 102}, {12, "bob", 103},
			// carol — 2 elections
			{13, "carol", 201}, {14, "carol", 202},
		} {
			e, _, err := c.Collection(entry.electionID)
			assertErrorFail(t, "", err, nil)
			_, err = e.Save(entry.voter, newBallot(entry.val), false)
			assertErrorFail(t, "", err, nil)
		}
	})

	// DeleteKey("alice") — 5 ForEach iterations, each via txFromBucket.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)
		err := c.DeleteKey("alice", true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		c := elections.Tx(tx)

		// alice is completely gone.
		has, err := c.HasKey("alice")
		assertErrorFail(t, "", err, nil)
		assert(t, "alice gone", has, false)

		for _, id := range []uint64{10, 11, 12, 13, 14} {
			assertElectionMissing(t, c, id, "alice")
		}

		// bob: exact ballot values in all 3 elections, unchanged.
		assertElectionBallot(t, c, 10, "bob", newBallot(101))
		assertElectionBallot(t, c, 11, "bob", newBallot(102))
		assertElectionBallot(t, c, 12, "bob", newBallot(103))
		assertElectionSize(t, c, 10, 1)
		assertElectionSize(t, c, 11, 1)
		assertElectionSize(t, c, 12, 1)
		assertKeyCollections(t, c, "bob", []uint64{10, 11, 12})

		// carol: exact ballot values in both elections, unchanged.
		assertElectionBallot(t, c, 13, "carol", newBallot(201))
		assertElectionBallot(t, c, 14, "carol", newBallot(202))
		assertElectionSize(t, c, 13, 1)
		assertElectionSize(t, c, 14, 1)
		assertKeyCollections(t, c, "carol", []uint64{13, 14})
	})
}

func electionsDB(t testing.TB) *bolt.DB {
	t.Helper()

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := elections.Tx(tx)

		elections0, exists, err := elections.Collection(0)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		elections5, exists, err := elections.Collection(5)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		elections6, exists, err := elections.Collection(6)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		elections7, exists, err := elections.Collection(7)
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
			case 7:
				_, err := elections7.Save(e.Voter, e.Ballot, false)
				assertErrorFail(t, fmt.Sprintf("%+v", e), err, nil)
			}
		}
	})

	return db
}

func electionsKeys(is ...int) []string {
	s := make([]string, 0, len(is))
	for _, i := range is {
		s = append(s, testElectionsKeys[i])
	}
	return s
}

func electionsCollections(is ...int) []uint64 {
	s := make([]uint64, 0, len(is))
	for _, i := range is {
		s = append(s, testElectionsCollections[i])
	}
	return s
}

func electionsCollectionsWithKeyAlice(is ...int) []uint64 {
	s := make([]uint64, 0, len(is))
	for _, i := range is {
		s = append(s, testElectionsCollectionsWithKeyAlice[i])
	}
	return s
}
