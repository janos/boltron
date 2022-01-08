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

		{7, "allice", newBallot(0)},
		{7, "dave", newBallot(0)},
	}

	testElectionsCollections = []uint64{0, 5, 6, 7}

	testElectionsKeys = []string{
		"allice",
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

	testElectionsCollectionsWithKeyAllice = []uint64{0, 5, 7}
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

func TestCollections_iterateCollections(t *testing.T) {
	db := electionsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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

func TestCollections_pageOfCollections(t *testing.T) {
	db := electionsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

			var i int
			next, err := elections.IterateCollectionsWithKey("allice", nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAllice[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollectionsWithKeyAllice))
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

			var i int
			next, err := elections.IterateCollectionsWithKey("allice", nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAllice[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 7)

			next, err = elections.IterateCollectionsWithKey("allice", next, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAllice[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollectionsWithKeyAllice))
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

			var i int
			next, err := elections.IterateCollectionsWithKey("allice", nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAllice[len(testElectionsCollectionsWithKeyAllice)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollectionsWithKeyAllice))
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

			var i int
			next, err := elections.IterateCollectionsWithKey("allice", nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAllice[len(testElectionsCollectionsWithKeyAllice)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 0)

			next, err = elections.IterateCollectionsWithKey("allice", next, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate collection #%v", i), v, testElectionsCollectionsWithKeyAllice[len(testElectionsCollectionsWithKeyAllice)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testElectionsCollectionsWithKeyAllice))
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

			var count int
			next, err := elections.IterateCollectionsWithKey("allice", nil, false, func(_ uint64) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})

		dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

			collection, exists, err := elections.Collection(0)
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			overwritten, err := collection.Save("mick", newBallot(0), false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", overwritten, false)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

			var count int
			next, err := elections.IterateCollectionsWithKey("allice", nil, false, func(_ uint64) (bool, error) {
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
			elections := electionsDefinition.Collections(tx)

			_, _, _, err := elections.PageOfCollectionsWithKey("allice", -1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = elections.PageOfCollectionsWithKey("allice", 0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := elections.PageOfCollectionsWithKey("allice", 1, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollectionsWithKeyAllice(0, 1))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = elections.PageOfCollectionsWithKey("allice", 2, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollectionsWithKeyAllice(2))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

			_, _, _, err := elections.PageOfCollectionsWithKey("allice", -1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = elections.PageOfCollectionsWithKey("allice", 0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := elections.PageOfCollectionsWithKey("allice", 1, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollectionsWithKeyAllice(2, 1))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = elections.PageOfCollectionsWithKey("allice", 2, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, electionsCollectionsWithKeyAllice(0))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

			page, totalElements, totalPages, err := elections.PageOfCollectionsWithKey("allice", 1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})

		dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

			collection, exists, err := elections.Collection(0)
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			overwritten, err := collection.Save("mick", newBallot(0), false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", overwritten, false)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			elections := electionsDefinition.Collections(tx)

			page, totalElements, totalPages, err := elections.PageOfCollectionsWithKey("allice", 1, 3, true)
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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
			elections := electionsDefinition.Collections(tx)

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
		elections := electionsDefinition.Collections(tx)

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
		elections := electionsDefinition.Collections(tx)

		collection, exists, err := elections.Collection(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		overwritten, err := collection.Save("paul", newBallot(0), false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := electionsDefinition.Collections(tx)

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

func TestCollections_customErrNotFound(t *testing.T) {

	errNotFoundCustom := errors.New("custom not found error")

	customElectionsDefinition := boltron.NewCollectionsDefinition(
		"elections",
		boltron.Uint64BinaryEncoding,       // election id
		boltron.StringEncoding,             // voter id
		boltron.NewJSONEncoding[*ballot](), // ballot with a vote
		&boltron.CollectionsOptions{
			ErrNotFound: errNotFoundCustom,
		},
	)

	db := newDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := customElectionsDefinition.Collections(tx)

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
		assertError(t, "", err, errNotFoundCustom)

		err = elections.DeleteCollection(0, false)
		assertError(t, "", err, nil)

		err = elections.DeleteKey("john", true)
		assertError(t, "", err, errNotFoundCustom)

		err = elections.DeleteKey("john", false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := customElectionsDefinition.Collections(tx)

		collection, exists, err := elections.Collection(1)
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		overwritten, err := collection.Save("paul", newBallot(0), false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		elections := customElectionsDefinition.Collections(tx)

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
		assertError(t, "", err, errNotFoundCustom)

		err = elections.DeleteCollection(0, false)
		assertError(t, "", err, nil)

		err = elections.DeleteKey("john", true)
		assertError(t, "", err, errNotFoundCustom)

		err = elections.DeleteKey("john", false)
		assertError(t, "", err, nil)
	})
}

func TestCollections_uniqueKeys(t *testing.T) {

	customElectionsDefinition := boltron.NewCollectionsDefinition(
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
		elections := customElectionsDefinition.Collections(tx)

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

	customElectionsDefinition := boltron.NewCollectionsDefinition(
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
		elections := customElectionsDefinition.Collections(tx)

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

func electionsDB(t testing.TB) *bolt.DB {
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

func electionsCollectionsWithKeyAllice(is ...int) []uint64 {
	s := make([]uint64, 0, len(is))
	for _, i := range is {
		s = append(s, testElectionsCollectionsWithKeyAllice[i])
	}
	return s
}
