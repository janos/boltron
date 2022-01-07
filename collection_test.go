// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	bolt "go.etcd.io/bbolt"
	"resenje.org/boltron"
)

type Record struct {
	ID      int
	Message string
}

var (
	recordsDefinition = boltron.NewCollectionDefinition(
		"records",
		boltron.IntBase10Encoding,
		recordEncoding,
		nil,
	)

	recordEncoding = boltron.NewEncoding(
		func(r *Record) ([]byte, error) {
			return json.Marshal(r)
		},
		func(b []byte) (*Record, error) {
			var r Record
			if err := json.Unmarshal(b, &r); err != nil {
				return nil, err
			}
			return &r, nil
		},
	)

	testRecords = []*Record{
		{
			ID:      1,
			Message: "test one",
		},
		{
			ID:      10,
			Message: "test ten",
		},
		{
			ID:      2,
			Message: "test two",
		},
		{
			ID:      3,
			Message: "test three",
		},
		{
			ID:      31,
			Message: "test 😇",
		},
		{
			ID:      42,
			Message: "test 🦖",
		},
		{
			ID:      43,
			Message: "last one",
		},
	}
)

func TestCollection_singleRecord(t *testing.T) {
	db := newDB(t)

	r := testRecords[0]

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinition.Collection(tx)

		overwritten, err := records.Save(r.ID, r, false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)

		v, err := records.Get(r.ID)
		assertErrorFail(t, "", err, nil)
		assert(t, "", v, r)

		has, err := records.Has(r.ID)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, true)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinition.Collection(tx)

		v, err := records.Get(r.ID)
		assertErrorFail(t, "", err, nil)
		assert(t, "", v, r)

		has, err := records.Has(r.ID)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, true)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinition.Collection(tx)

		err := records.Delete(r.ID, true)
		assertErrorFail(t, "", err, nil)

		v, err := records.Get(r.ID)
		assertErrorFail(t, "", err, boltron.ErrNotFound)
		assert(t, "", v, nil)

		has, err := records.Has(r.ID)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinition.Collection(tx)

		v, err := records.Get(r.ID)
		assertErrorFail(t, "", err, boltron.ErrNotFound)
		assert(t, "", v, nil)

		has, err := records.Has(r.ID)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinition.Collection(tx)

		overwritten, err := records.Save(r.ID, r, false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)

		overwritten, err = records.Save(r.ID, r, false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)

		overwritten, err = records.Save(r.ID, r, true)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)

		overwritten, err = records.Save(r.ID, testRecords[1], false)
		assertErrorFail(t, "", err, boltron.ErrKeyExists)
		assert(t, "", overwritten, false)
	})
}

func TestCollection_iterate(t *testing.T) {
	db := newRecordsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.Iterate(nil, false, func(id int, r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[i].ID)
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.Iterate(nil, false, func(id int, r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[i].ID)
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 2)

			next, err = records.Iterate(next, false, func(id int, r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[i].ID)
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.Iterate(nil, true, func(id int, r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[len(testRecords)-1-i].ID)
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[len(testRecords)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.Iterate(nil, true, func(id int, r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[len(testRecords)-1-i].ID)
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[len(testRecords)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 31)

			next, err = records.Iterate(next, true, func(id int, r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[len(testRecords)-1-i].ID)
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[len(testRecords)-1-i])
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
			records := recordsDefinition.Collection(tx)

			var count int
			next, err := records.Iterate(nil, false, func(_ int, _ *Record) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestCollection_iterateKeys(t *testing.T) {
	db := newRecordsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.IterateKeys(nil, false, func(id int) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[i].ID)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.IterateKeys(nil, false, func(id int) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[i].ID)
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 2)

			next, err = records.IterateKeys(next, false, func(id int) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[i].ID)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.IterateKeys(nil, true, func(id int) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[len(testRecords)-1-i].ID)
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.IterateKeys(nil, true, func(id int) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[len(testRecords)-1-i].ID)
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 31)

			next, err = records.IterateKeys(next, true, func(id int) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), id, testRecords[len(testRecords)-1-i].ID)
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
			records := recordsDefinition.Collection(tx)

			var count int
			next, err := records.IterateKeys(nil, false, func(_ int) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestCollection_iterateValues(t *testing.T) {
	db := newRecordsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.IterateValues(nil, false, func(r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.IterateValues(nil, false, func(r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 2)

			next, err = records.IterateValues(next, false, func(r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.IterateValues(nil, true, func(r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[len(testRecords)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			var i int
			next, err := records.IterateValues(nil, true, func(r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[len(testRecords)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 31)

			next, err = records.IterateValues(next, true, func(r *Record) (bool, error) {
				assert(t, fmt.Sprintf("iterate record #%v", i), r, testRecords[len(testRecords)-1-i])
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
			records := recordsDefinition.Collection(tx)

			var count int
			next, err := records.IterateValues(nil, false, func(_ *Record) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestCollection_page(t *testing.T) {
	db := newRecordsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			_, _, _, err := records.Page(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = records.Page(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := records.Page(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordElements(0, 1, 2))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.Page(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordElements(3, 4, 5))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.Page(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordElements(6))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			_, _, _, err := records.Page(-1, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = records.Page(0, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := records.Page(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordElements(6, 5, 4))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.Page(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordElements(3, 2, 1))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.Page(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordElements(0))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			page, totalElements, totalPages, err := records.Page(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestCollection_pageOfKeys(t *testing.T) {
	db := newRecordsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			_, _, _, err := records.PageOfKeys(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = records.PageOfKeys(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := records.PageOfKeys(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordKeys(0, 1, 2))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.PageOfKeys(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordKeys(3, 4, 5))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.PageOfKeys(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordKeys(6))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			_, _, _, err := records.PageOfKeys(-1, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = records.PageOfKeys(0, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := records.PageOfKeys(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordKeys(6, 5, 4))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.PageOfKeys(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordKeys(3, 2, 1))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.PageOfKeys(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordKeys(0))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			page, totalElements, totalPages, err := records.PageOfKeys(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestCollection_pageOfValues(t *testing.T) {
	db := newRecordsDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			_, _, _, err := records.PageOfValues(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = records.PageOfValues(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := records.PageOfValues(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordValues(0, 1, 2))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.PageOfValues(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordValues(3, 4, 5))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.PageOfValues(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordValues(6))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			_, _, _, err := records.PageOfValues(-1, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = records.PageOfValues(0, 3, true)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := records.PageOfValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordValues(6, 5, 4))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.PageOfValues(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordValues(3, 2, 1))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = records.PageOfValues(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, recordValues(0))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			records := recordsDefinition.Collection(tx)

			page, totalElements, totalPages, err := records.PageOfValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestCollection_ErrNotFound(t *testing.T) {
	db := newDB(t)

	notFoundID := testRecords[0].ID

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinition.Collection(tx)

		v, err := records.Get(notFoundID)
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", v, nil)

		has, err := records.Has(notFoundID)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = records.Delete(notFoundID, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = records.Delete(notFoundID, false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinition.Collection(tx)

		r := testRecords[1]

		overwritten, err := records.Save(r.ID, r, false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinition.Collection(tx)

		v, err := records.Get(notFoundID)
		assertError(t, "", err, boltron.ErrNotFound)
		assert(t, "", v, nil)

		err = records.Delete(notFoundID, true)
		assertError(t, "", err, boltron.ErrNotFound)
	})
}

func TestCollection_customErrNotFound(t *testing.T) {

	errNotFoundCustom := errors.New("custom not found error")

	recordsDefinitionCustom := boltron.NewCollectionDefinition(
		"records",
		boltron.IntBase10Encoding,
		recordEncoding,
		&boltron.CollectionOptions{
			ErrNotFound: errNotFoundCustom,
		},
	)

	db := newDB(t)

	notFoundID := testRecords[0].ID

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinitionCustom.Collection(tx)

		v, err := records.Get(notFoundID)
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", v, nil)

		has, err := records.Has(notFoundID)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = records.Delete(notFoundID, true)
		assertError(t, "", err, errNotFoundCustom)

		err = records.Delete(notFoundID, false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinitionCustom.Collection(tx)

		r := testRecords[1]

		overwritten, err := records.Save(r.ID, r, false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinitionCustom.Collection(tx)

		v, err := records.Get(notFoundID)
		assertError(t, "", err, errNotFoundCustom)
		assert(t, "", v, nil)

		err = records.Delete(notFoundID, true)
		assertError(t, "", err, errNotFoundCustom)
	})
}

func TestCollection_customErrKeyExists(t *testing.T) {

	errKeyExistsCustom := errors.New("custom exists error")

	recordsDefinitionCustom := boltron.NewCollectionDefinition(
		"records",
		boltron.IntBase10Encoding,
		recordEncoding,
		&boltron.CollectionOptions{
			ErrKeyExists: errKeyExistsCustom,
		},
	)

	db := newDB(t)

	r := testRecords[0]

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinitionCustom.Collection(tx)

		overwritten, err := records.Save(r.ID, r, false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)

		// both key and value are the same
		// it is not overwritten
		overwritten, err = records.Save(r.ID, r, false)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, false)

		// do not overwrite
		overwritten, err = records.Save(r.ID, testRecords[1], false)
		assertErrorFail(t, "", err, errKeyExistsCustom)
		assert(t, "", overwritten, false)

		// do overwrite
		overwritten, err = records.Save(r.ID, testRecords[1], true)
		assertErrorFail(t, "", err, nil)
		assert(t, "", overwritten, true)
	})
}

func newRecordsDB(t testing.TB) *bolt.DB {
	t.Helper()

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		records := recordsDefinition.Collection(tx)

		for _, r := range testRecords {
			overwritten, err := records.Save(r.ID, r, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", overwritten, false)
		}
	})

	return db
}

func recordElements(is ...int) []boltron.Element[int, *Record] {
	s := make([]boltron.Element[int, *Record], 0, len(is))
	for _, i := range is {
		s = append(s, boltron.Element[int, *Record]{
			Key:   testRecords[i].ID,
			Value: testRecords[i],
		})
	}
	return s
}

func recordKeys(is ...int) []int {
	s := make([]int, 0, len(is))
	for _, i := range is {
		s = append(s, testRecords[i].ID)
	}
	return s
}

func recordValues(is ...int) []*Record {
	s := make([]*Record, 0, len(is))
	for _, i := range is {
		s = append(s, testRecords[i])
	}
	return s
}
