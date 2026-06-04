// Copyright (c) 2021, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"resenje.org/boltron"
)

var (
	projectDependencies = boltron.NewLists(
		"project dependencies",
		boltron.StringEncoding,
		boltron.Uint64Base36Encoding, // dependency id in another collection
		boltron.TimeEncoding,
		nil,
	)

	testProjectDependencies = []struct {
		ProjectName  string
		DependencyID uint64
		UpdateTime   time.Time
	}{
		{"resenje.org/boltron", 382, time.Unix(1640731016, 0)},
		{"resenje.org/boltron", 121, time.Unix(1640730983, 0)},
		{"resenje.org/boltron", 122, time.Unix(1640731310, 0)},
		{"resenje.org/schulze", 501, time.Unix(1640732181, 0)},
		{"resenje.org/schulze", 121, time.Unix(1640732188, 0)},
		{"resenje.org/schulze", 398, time.Unix(1640732192, 0)},
		{"resenje.org/schulze", 125, time.Unix(1640732205, 0)},
		{"resenje.org/schulze", 881, time.Unix(1640732216, 0)},
		{"resenje.org/web", 398, time.Unix(1640732358, 0)},
		{"resenje.org/web", 121, time.Unix(1640732362, 0)},
		{"resenje.org/web", 125, time.Unix(1640732381, 0)},
		{"resenje.org/web", 881, time.Unix(1640732390, 0)},
		{"resenje.org/pool", 121, time.Unix(1640732487, 0)},
		{"resenje.org/pool", 125, time.Unix(1640732500, 0)},
		{"resenje.org/pool", 881, time.Unix(1640732508, 0)},
	}

	testProjectDependenciesValues = []uint64{
		121,
		122,
		125,
		382,
		398,
		501,
		881,
	}

	testProjectDependenciesLists = []string{
		"resenje.org/boltron",
		"resenje.org/pool",
		"resenje.org/schulze",
		"resenje.org/web",
	}

	testProjectDependenciesListsWithValue125 = []string{
		"resenje.org/pool",
		"resenje.org/schulze",
		"resenje.org/web",
	}
	testProjectDependenciesListsWithValue125Times = []time.Time{
		time.Unix(1640732500, 0).UTC(),
		time.Unix(1640732205, 0).UTC(),
		time.Unix(1640732381, 0).UTC(),
	}
)

func TestLists(t *testing.T) {
	db := projectsDependenciesDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependencies.Tx(tx)

		for _, d := range testProjectDependencies {
			has, err := projectDependencies.HasList(d.ProjectName)
			assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, true)

			_, exists, err := projectDependencies.List(d.ProjectName)
			assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), exists, true)

			has, err = projectDependencies.HasValue(d.DependencyID)
			assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, true)
		}

		has, err := projectDependencies.HasList("resenje.org/missing")
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		has, err = projectDependencies.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		_, exists, err := projectDependencies.List("resenje.org/missing")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
	})

	deletedValue := uint64(121)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependencies.Tx(tx)

		err := projectDependencies.DeleteValue(120, true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = projectDependencies.DeleteValue(deletedValue, true)
		assertErrorFail(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		deletedValueFromList := uint64(125)

		projectDependencies := projectDependencies.Tx(tx)

		list, exists, err := projectDependencies.List("resenje.org/schulze")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, true)

		err = list.Remove(deletedValueFromList, true)
		assertErrorFail(t, "", err, nil)

		err = list.Remove(deletedValueFromList, true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		has, err := projectDependencies.HasValue(deletedValueFromList)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, true)

		var listsWithDeletedValue []string
		next, err := projectDependencies.IterateListsWithValue(deletedValueFromList, nil, false, func(l string, _ time.Time) (bool, error) {
			listsWithDeletedValue = append(listsWithDeletedValue, l)
			return true, nil
		})
		assertErrorFail(t, "", err, nil)
		assert(t, "", next, nil)

		assert(t, "", listsWithDeletedValue, []string{"resenje.org/pool", "resenje.org/web"})
	})

	deletedValueFromListBoltron := uint64(382)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependencies.Tx(tx)

		list, exists, err := projectDependencies.List("resenje.org/boltron")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, true)

		err = list.Remove(deletedValueFromListBoltron, true)
		assertErrorFail(t, "", err, nil)

		err = list.Remove(deletedValueFromListBoltron, true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		has, err := projectDependencies.HasValue(deletedValueFromListBoltron)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)

		var listsWithDeletedValue []string
		next, err := projectDependencies.IterateListsWithValue(deletedValueFromListBoltron, nil, false, func(l string, _ time.Time) (bool, error) {
			listsWithDeletedValue = append(listsWithDeletedValue, l)
			return true, nil
		})
		assertErrorFail(t, "", err, nil)
		assert(t, "", next, nil)

		assert(t, "", listsWithDeletedValue, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependencies.Tx(tx)

		for _, d := range testProjectDependenciesLists {
			has, err := projectDependencies.HasList(d)
			assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, true)

			list, exists, err := projectDependencies.List(d)
			assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), exists, true)

			has, err = list.Has(deletedValue)
			assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, false)
		}

		for _, d := range testProjectDependenciesValues {
			has, err := projectDependencies.HasValue(d)
			assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, d != deletedValue && d != deletedValueFromListBoltron)
		}

		has, err := projectDependencies.HasValue(deletedValue)
		assertErrorFail(t, "", err, nil)
		assert(t, "", has, false)
	})

	deletedList := "resenje.org/schulze"

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependencies.Tx(tx)

		err := projectDependencies.DeleteList("resenje.org/missing", true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = projectDependencies.DeleteList(deletedList, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependencies.Tx(tx)

		for _, d := range testProjectDependencies {
			has, err := projectDependencies.HasList(d.ProjectName)
			assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, d.ProjectName != deletedList)

			_, exists, err := projectDependencies.List(d.ProjectName)
			assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), exists, d.ProjectName != deletedList)

			has, err = projectDependencies.HasValue(d.DependencyID)
			assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			assert(t, fmt.Sprintf("%+v", d), has, d.DependencyID != deletedValue && d.DependencyID != 501 && d.DependencyID != deletedValueFromListBoltron)
		}
	})
}

func TestLists_iterateLists(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateLists(nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesLists[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesLists))
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateLists(nil, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesLists[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "resenje.org/schulze")

			next, err = projectDependencies.IterateLists(next, false, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesLists[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesLists))
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateLists(nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesLists[len(testProjectDependenciesLists)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesLists))
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateLists(nil, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesLists[len(testProjectDependenciesLists)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "resenje.org/pool")

			next, err = projectDependencies.IterateLists(next, true, func(v string) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesLists[len(testProjectDependenciesLists)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesLists))
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var count int
			next, err := projectDependencies.IterateLists(nil, false, func(_ string) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestLists_size(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("full", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			size, err := projectDependencies.Size()
			assertErrorFail(t, "", err, nil)
			assert(t, "", size, 4)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			size, err := projectDependencies.Size()
			assertErrorFail(t, "", err, nil)
			assert(t, "", size, 0)
		})
	})
}

func TestLists_pageOfLists(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			_, _, _, err := projectDependencies.PageOfLists(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfLists(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfLists(1, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesLists(0, 1))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = projectDependencies.PageOfLists(2, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesLists(2, 3))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			_, _, _, err := projectDependencies.PageOfLists(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfLists(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfLists(1, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesLists(3, 2))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = projectDependencies.PageOfLists(2, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesLists(1, 0))
			assert(t, "", totalElements, 4)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			page, totalElements, totalPages, err := projectDependencies.PageOfLists(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestLists_iterateListsWithValue(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateListsWithValue(125, nil, false, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesListsWithValue125[i])
				assert(t, fmt.Sprintf("iterate list #%v", i), o, testProjectDependenciesListsWithValue125Times[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesListsWithValue125))
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateListsWithValue(125, nil, false, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesListsWithValue125[i])
				assert(t, fmt.Sprintf("iterate list #%v", i), o, testProjectDependenciesListsWithValue125Times[i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "resenje.org/web")

			next, err = projectDependencies.IterateListsWithValue(125, next, false, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesListsWithValue125[i])
				assert(t, fmt.Sprintf("iterate list #%v", i), o, testProjectDependenciesListsWithValue125Times[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesListsWithValue125))
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateListsWithValue(125, nil, true, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesListsWithValue125[len(testProjectDependenciesListsWithValue125)-1-i])
				assert(t, fmt.Sprintf("iterate list #%v", i), o, testProjectDependenciesListsWithValue125Times[len(testProjectDependenciesListsWithValue125)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesListsWithValue125))
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateListsWithValue(125, nil, true, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesListsWithValue125[len(testProjectDependenciesListsWithValue125)-1-i])
				assert(t, fmt.Sprintf("iterate list #%v", i), o, testProjectDependenciesListsWithValue125Times[len(testProjectDependenciesListsWithValue125)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, "resenje.org/pool")

			next, err = projectDependencies.IterateListsWithValue(125, next, true, func(v string, o time.Time) (bool, error) {
				assert(t, fmt.Sprintf("iterate list #%v", i), v, testProjectDependenciesListsWithValue125[len(testProjectDependenciesListsWithValue125)-1-i])
				assert(t, fmt.Sprintf("iterate list #%v", i), o, testProjectDependenciesListsWithValue125Times[len(testProjectDependenciesListsWithValue125)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesListsWithValue125))
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var count int
			next, err := projectDependencies.IterateListsWithValue(125, nil, false, func(_ string, _ time.Time) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})

		dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			list, exists, err := projectDependencies.List("resenje.org/daemon")
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			err = list.Add(10000, time.Now())
			assertErrorFail(t, "", err, nil)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var count int
			next, err := projectDependencies.IterateListsWithValue(125, nil, false, func(_ string, _ time.Time) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestLists_pageOfListsWithValue(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			_, _, _, err := projectDependencies.PageOfListsWithValue(125, -1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfListsWithValue(125, 0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfListsWithValue(125, 1, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesListsWithValue125(0, 1))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = projectDependencies.PageOfListsWithValue(125, 2, 2, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesListsWithValue125(2))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			_, _, _, err := projectDependencies.PageOfListsWithValue(125, -1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfListsWithValue(125, 0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfListsWithValue(125, 1, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesListsWithValue125(2, 1))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)

			page, totalElements, totalPages, err = projectDependencies.PageOfListsWithValue(125, 2, 2, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesListsWithValue125(0))
			assert(t, "", totalElements, 3)
			assert(t, "", totalPages, 2)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			page, totalElements, totalPages, err := projectDependencies.PageOfListsWithValue(125, 1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})

		dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			list, exists, err := projectDependencies.List("resenje.org/daemon")
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			err = list.Add(10000, time.Now())
			assertErrorFail(t, "", err, nil)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			page, totalElements, totalPages, err := projectDependencies.PageOfListsWithValue(125, 1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestLists_iterateValues(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateValues(nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesValues))
		})
	})

	t.Run("forward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateValues(nil, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[i])
				i++
				if i == 3 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 382)

			next, err = projectDependencies.IterateValues(next, false, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesValues))
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateValues(nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[len(testProjectDependenciesValues)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesValues))
		})
	})

	t.Run("backward partial", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var i int
			next, err := projectDependencies.IterateValues(nil, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[len(testProjectDependenciesValues)-1-i])
				i++
				if i == 2 {
					return false, nil
				}
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", *next, 398)

			next, err = projectDependencies.IterateValues(next, true, func(v uint64) (bool, error) {
				assert(t, fmt.Sprintf("iterate value #%v", i), v, testProjectDependenciesValues[len(testProjectDependenciesValues)-1-i])
				i++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", i, len(testProjectDependenciesValues))
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			var count int
			next, err := projectDependencies.IterateValues(nil, false, func(_ uint64) (bool, error) {
				count++
				return true, nil
			})
			assertErrorFail(t, "", err, nil)
			assert(t, "", next, nil)
			assert(t, "", count, 0)
		})
	})
}

func TestLists_pageOfValues(t *testing.T) {
	db := projectsDependenciesDB(t)

	t.Run("forward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			_, _, _, err := projectDependencies.PageOfValues(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfValues(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfValues(1, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(0, 1, 2))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = projectDependencies.PageOfValues(2, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(3, 4, 5))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = projectDependencies.PageOfValues(3, 3, false)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(6))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("backward", func(t *testing.T) {
		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			_, _, _, err := projectDependencies.PageOfValues(-1, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			_, _, _, err = projectDependencies.PageOfValues(0, 3, false)
			assertErrorFail(t, "", err, boltron.ErrInvalidPageNumber)

			page, totalElements, totalPages, err := projectDependencies.PageOfValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(6, 5, 4))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = projectDependencies.PageOfValues(2, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(3, 2, 1))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)

			page, totalElements, totalPages, err = projectDependencies.PageOfValues(3, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, projectDependenciesValues(0))
			assert(t, "", totalElements, 7)
			assert(t, "", totalPages, 3)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependencies.Tx(tx)

			page, totalElements, totalPages, err := projectDependencies.PageOfValues(1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})
	})
}

func TestLists_ErrListNotFound_and_ErrValueNotFound(t *testing.T) {
	db := newDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependencies.Tx(tx)

		_, exists, err := projectDependencies.List("missing")
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := projectDependencies.HasList("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = projectDependencies.HasValue(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = projectDependencies.DeleteList("missing", true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = projectDependencies.DeleteList("missing", false)
		assertError(t, "", err, nil)

		err = projectDependencies.DeleteValue(0, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = projectDependencies.DeleteValue(0, false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependencies.Tx(tx)

		list, exists, err := projectDependencies.List("one")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = list.Add(1, time.Now())
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependencies.Tx(tx)

		_, exists, err := projectDependencies.List("missing")
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := projectDependencies.HasList("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = projectDependencies.HasValue(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = projectDependencies.DeleteList("missing", true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = projectDependencies.DeleteList("missing", false)
		assertError(t, "", err, nil)

		err = projectDependencies.DeleteValue(0, true)
		assertError(t, "", err, boltron.ErrNotFound)

		err = projectDependencies.DeleteValue(0, false)
		assertError(t, "", err, nil)
	})
}

func TestLists_customErrListNotFound_and_customErrValueNotFound(t *testing.T) {

	errListNotFoundCustom := errors.New("custom list not found error")
	errValueNotFoundCustom := errors.New("custom value not found error")

	customProjectDependencies := boltron.NewLists(
		"project dependencies",
		boltron.StringEncoding,
		boltron.Uint64Base36Encoding, // dependency id in another collection
		boltron.TimeEncoding,
		&boltron.ListsOptions{
			ErrListNotFound:  errListNotFoundCustom,
			ErrValueNotFound: errValueNotFoundCustom,
		},
	)

	db := newDB(t)

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := customProjectDependencies.Tx(tx)

		_, exists, err := projectDependencies.List("missing")
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := projectDependencies.HasList("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = projectDependencies.HasValue(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = projectDependencies.DeleteList("missing", true)
		assertError(t, "", err, errListNotFoundCustom)

		err = projectDependencies.DeleteList("missing", false)
		assertError(t, "", err, nil)

		err = projectDependencies.DeleteValue(0, true)
		assertError(t, "", err, errValueNotFoundCustom)

		err = projectDependencies.DeleteValue(0, false)
		assertError(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := customProjectDependencies.Tx(tx)

		list, exists, err := projectDependencies.List("one")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = list.Add(1, time.Now())
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := customProjectDependencies.Tx(tx)

		_, exists, err := projectDependencies.List("missing")
		assertError(t, "", err, nil)
		assert(t, "", exists, false)

		has, err := projectDependencies.HasList("missing")
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		has, err = projectDependencies.HasValue(0)
		assertError(t, "", err, nil)
		assert(t, "", has, false)

		err = projectDependencies.DeleteList("missing", true)
		assertError(t, "", err, errListNotFoundCustom)

		err = projectDependencies.DeleteList("missing", false)
		assertError(t, "", err, nil)

		err = projectDependencies.DeleteValue(0, true)
		assertError(t, "", err, errValueNotFoundCustom)

		err = projectDependencies.DeleteValue(0, false)
		assertError(t, "", err, nil)
	})
}

func TestLists_uniqueValues(t *testing.T) {
	customProjectDependencies := boltron.NewLists(
		"project dependencies",
		boltron.StringEncoding,
		boltron.Uint64Base36Encoding, // dependency id in another collection
		boltron.TimeEncoding,
		&boltron.ListsOptions{
			UniqueValues: true,
		},
	)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := customProjectDependencies.Tx(tx)

		boltronProjectDependencies, exists, err := projectDependencies.List("resenje.org/boltron")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		schulzeProjectDependencies, exists, err := projectDependencies.List("resenje.org/schulze")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = boltronProjectDependencies.Add(100, time.Now())
		assertErrorFail(t, "", err, nil)

		err = schulzeProjectDependencies.Add(100, time.Now())
		assertErrorFail(t, "", err, boltron.ErrValueExists)
	})
}

func TestLists_uniqueValues_customErrValueExists(t *testing.T) {

	errValueExistsCustom := errors.New("custom value exists error")

	customProjectDependencies := boltron.NewLists(
		"project dependencies",
		boltron.StringEncoding,
		boltron.Uint64Base36Encoding, // dependency id in another collection
		boltron.TimeEncoding,
		&boltron.ListsOptions{
			UniqueValues:   true,
			ErrValueExists: errValueExistsCustom,
		},
	)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := customProjectDependencies.Tx(tx)

		boltronProjectDependencies, exists, err := projectDependencies.List("resenje.org/boltron")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		schulzeProjectDependencies, exists, err := projectDependencies.List("resenje.org/schulze")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = boltronProjectDependencies.Add(100, time.Now())
		assertErrorFail(t, "", err, nil)

		err = schulzeProjectDependencies.Add(100, time.Now())
		assertErrorFail(t, "", err, errValueExistsCustom)
	})
}

// TestLists_removeCallback_orphanBucket_multipleValues is the critical
// regression for the wrong-bucket bug in the lists removeCallback
// (valuesBucket.Stats().KeyN instead of valueBucket.Stats().KeyN).
// With multiple value sub-buckets, the old check would never fire, leaving
// orphaned value sub-buckets.
func TestLists_removeCallback_orphanBucket_multipleValues(t *testing.T) {
	db := newDB(t)

	// Set up: two projects sharing values 100 and 200 (dep IDs).
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		boltron, _, err := pd.List("resenje.org/boltron")
		assertErrorFail(t, "", err, nil)
		schulze, _, err := pd.List("resenje.org/schulze")
		assertErrorFail(t, "", err, nil)

		err = boltron.Add(100, time.Unix(1000, 0))
		assertErrorFail(t, "", err, nil)
		err = boltron.Add(200, time.Unix(2000, 0))
		assertErrorFail(t, "", err, nil)
		err = schulze.Add(100, time.Unix(3000, 0))
		assertErrorFail(t, "", err, nil)
		err = schulze.Add(200, time.Unix(4000, 0))
		assertErrorFail(t, "", err, nil)
	})

	// Remove value 100 from resenje.org/boltron. Value 100's index sub-bucket
	// must survive (schulze still uses it). Value 200's sub-bucket must survive.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		boltronList, _, err := pd.List("resenje.org/boltron")
		assertErrorFail(t, "", err, nil)
		err = boltronList.Remove(100, true)
		assertErrorFail(t, "", err, nil)

		// 100 still used by schulze, so HasValue(100) must be true.
		has, err := pd.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 100 still has schulze in same tx", has, true)

		has, err = pd.HasValue(200)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 200 still present in same tx", has, true)
	})

	// Remove value 100 from schulze too. Now 100's index sub-bucket must
	// vanish, but 200's must still be there. This is the case the old
	// wrong-bucket check (valuesBucket.Stats().KeyN == 1) missed: the parent
	// bucket had 2 children so KeyN was 2, and the cleanup never ran.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		schulzeList, _, err := pd.List("resenje.org/schulze")
		assertErrorFail(t, "", err, nil)
		err = schulzeList.Remove(100, true)
		assertErrorFail(t, "", err, nil)

		has, err := pd.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 100 gone in same tx", has, false)

		has, err = pd.HasValue(200)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 200 survives in same tx", has, true)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		has, err := pd.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 100 gone after tx", has, false)

		has, err = pd.HasValue(200)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 200 survives after tx", has, true)
	})
}

// TestLists_deleteList_orphanBucket_multipleValues verifies that DeleteList
// cleans up value sub-buckets for a value that appears in multiple lists,
// when the deleted list is the last reference.
func TestLists_deleteList_orphanBucket_multipleValues(t *testing.T) {
	db := newDB(t)

	// boltron uses values 100 and 200; schulze uses value 100 only.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		boltronList, _, err := pd.List("resenje.org/boltron")
		assertErrorFail(t, "", err, nil)
		schulzeList, _, err := pd.List("resenje.org/schulze")
		assertErrorFail(t, "", err, nil)

		err = boltronList.Add(100, time.Unix(1000, 0))
		assertErrorFail(t, "", err, nil)
		err = boltronList.Add(200, time.Unix(2000, 0))
		assertErrorFail(t, "", err, nil)
		err = schulzeList.Add(100, time.Unix(3000, 0))
		assertErrorFail(t, "", err, nil)
	})

	// Delete resenje.org/boltron. Value 100's index sub-bucket must survive
	// (schulze still uses it). Value 200's index sub-bucket must be removed
	// (boltron was its only reference).
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		err := pd.DeleteList("resenje.org/boltron", true)
		assertErrorFail(t, "", err, nil)

		has, err := pd.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 100 survives (still in schulze) in same tx", has, true)

		has, err = pd.HasValue(200)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 200 gone (boltron was sole reference) in same tx", has, false)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		has, err := pd.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 100 survives after tx", has, true)

		has, err = pd.HasValue(200)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 200 gone after tx", has, false)
	})
}

// TestLists_existsCheck_emptyListBucket tests the List() exists check
// (lists.go:142) which used Stats().KeyN != 0 to determine if a list has any
// entries. Stats are stale, so an empty-but-existing bucket would incorrectly
// report exists=true. The fix uses Cursor().First() which reflects current state.
func TestLists_existsCheck_emptyListBucket(t *testing.T) {
	db := newDB(t)

	// Create a list with one entry, then remove that entry in the same tx.
	// Immediately check exists — must be false.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		list, exists, err := pd.List("resenje.org/boltron")
		assertErrorFail(t, "", err, nil)
		assert(t, "list does not exist initially", exists, false)

		err = list.Add(100, time.Unix(1000, 0))
		assertErrorFail(t, "", err, nil)

		err = list.Remove(100, true)
		assertErrorFail(t, "", err, nil)

		// Re-open the list; it exists in the bucket but is now empty.
		// With the stale Stats() check this returned exists=true.
		_, exists, err = pd.List("resenje.org/boltron")
		assertErrorFail(t, "", err, nil)
		assert(t, "list exists=false after emptying within same tx", exists, false)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		_, exists, err := pd.List("resenje.org/boltron")
		assertErrorFail(t, "", err, nil)
		assert(t, "list exists=false after tx", exists, false)
	})
}

// TestLists_remove_dataIntegrity verifies that removing a value from a list
// (emptying its value index sub-bucket) does not disturb other values' index
// entries or the orderBy timestamps stored alongside them.
func TestLists_remove_dataIntegrity(t *testing.T) {
	// Data:
	//   list "alpha": value 100 @ t1, value 200 @ t2, value 300 @ t3
	//   list "beta":  value 100 @ t4, value 200 @ t5
	//   list "gamma": value 100 @ t6, value 300 @ t7
	// Removing 100 from "alpha": value 100 index sub-bucket {alpha,beta,gamma} → {beta,gamma}.
	// Removing 100 from "beta":  index sub-bucket {beta,gamma} → {gamma}.
	// Removing 100 from "gamma": index sub-bucket empty → deleted.
	// All 200 and 300 timestamps must survive exactly.

	t1 := time.Unix(1001, 0)
	t2 := time.Unix(1002, 0)
	t3 := time.Unix(1003, 0)
	t4 := time.Unix(2001, 0)
	t5 := time.Unix(2002, 0)
	t6 := time.Unix(3001, 0)
	t7 := time.Unix(3002, 0)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		alpha, _, err := pd.List("alpha")
		assertErrorFail(t, "", err, nil)
		beta, _, err := pd.List("beta")
		assertErrorFail(t, "", err, nil)
		gamma, _, err := pd.List("gamma")
		assertErrorFail(t, "", err, nil)

		assertErrorFail(t, "", alpha.Add(100, t1), nil)
		assertErrorFail(t, "", alpha.Add(200, t2), nil)
		assertErrorFail(t, "", alpha.Add(300, t3), nil)
		assertErrorFail(t, "", beta.Add(100, t4), nil)
		assertErrorFail(t, "", beta.Add(200, t5), nil)
		assertErrorFail(t, "", gamma.Add(100, t6), nil)
		assertErrorFail(t, "", gamma.Add(300, t7), nil)
	})

	// Remove 100 from alpha.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		alpha, _, err := pd.List("alpha")
		assertErrorFail(t, "", err, nil)
		assertErrorFail(t, "", alpha.Remove(100, true), nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)

		// 100 still in beta and gamma.
		has, err := pd.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 100 still present via beta/gamma", has, true)

		// All exact orderBy timestamps are intact.
		assertListOrderBy(t, pd, "alpha", 200, t2)
		assertListOrderBy(t, pd, "alpha", 300, t3)
		assertListOrderBy(t, pd, "beta", 100, t4)
		assertListOrderBy(t, pd, "beta", 200, t5)
		assertListOrderBy(t, pd, "gamma", 100, t6)
		assertListOrderBy(t, pd, "gamma", 300, t7)

		assertListSize(t, pd, "alpha", 2) // 200 + 300
		assertListSize(t, pd, "beta", 2)
		assertListSize(t, pd, "gamma", 2)

		assertValueLists(t, pd, 200, []string{"alpha", "beta"})
		assertValueLists(t, pd, 300, []string{"alpha", "gamma"})
		assertValueLists(t, pd, 100, []string{"beta", "gamma"})
	})

	// Remove 100 from beta and gamma — triggers sub-bucket deletion for 100.
	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		beta, _, err := pd.List("beta")
		assertErrorFail(t, "", err, nil)
		gamma, _, err := pd.List("gamma")
		assertErrorFail(t, "", err, nil)
		assertErrorFail(t, "", beta.Remove(100, true), nil)
		assertErrorFail(t, "", gamma.Remove(100, true), nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)

		has, err := pd.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 100 is gone after last removal", has, false)

		// All 200 and 300 timestamps are byte-for-byte unchanged.
		assertListOrderBy(t, pd, "alpha", 200, t2)
		assertListOrderBy(t, pd, "alpha", 300, t3)
		assertListOrderBy(t, pd, "beta", 200, t5)
		assertListOrderBy(t, pd, "gamma", 300, t7)

		assertListSize(t, pd, "alpha", 2)
		assertListSize(t, pd, "beta", 1)  // 200 only
		assertListSize(t, pd, "gamma", 1) // 300 only

		assertValueLists(t, pd, 200, []string{"alpha", "beta"})
		assertValueLists(t, pd, 300, []string{"alpha", "gamma"})
	})
}

// TestLists_deleteList_dataIntegrity verifies that DeleteList removes only the
// targeted list and the targeted list's index entries from value sub-buckets.
// Other lists and their exact orderBy timestamps must be completely intact.
func TestLists_deleteList_dataIntegrity(t *testing.T) {
	// Data:
	//   list "alpha": value 100 @ t1, value 200 @ t2   ← will be deleted
	//   list "beta":  value 100 @ t3, value 300 @ t4
	//   list "gamma": value 200 @ t5, value 300 @ t6
	// Deleting "alpha": 100 sub-bucket {alpha,beta} → {beta} (survives).
	//                   200 sub-bucket {alpha,gamma} → {gamma} (survives).
	// 300 sub-bucket only references beta and gamma — completely untouched.

	t1 := time.Unix(1001, 0)
	t2 := time.Unix(1002, 0)
	t3 := time.Unix(2001, 0)
	t4 := time.Unix(2002, 0)
	t5 := time.Unix(3001, 0)
	t6 := time.Unix(3002, 0)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		alpha, _, err := pd.List("alpha")
		assertErrorFail(t, "", err, nil)
		beta, _, err := pd.List("beta")
		assertErrorFail(t, "", err, nil)
		gamma, _, err := pd.List("gamma")
		assertErrorFail(t, "", err, nil)

		assertErrorFail(t, "", alpha.Add(100, t1), nil)
		assertErrorFail(t, "", alpha.Add(200, t2), nil)
		assertErrorFail(t, "", beta.Add(100, t3), nil)
		assertErrorFail(t, "", beta.Add(300, t4), nil)
		assertErrorFail(t, "", gamma.Add(200, t5), nil)
		assertErrorFail(t, "", gamma.Add(300, t6), nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		err := pd.DeleteList("alpha", true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)

		has, err := pd.HasList("alpha")
		assertErrorFail(t, "", err, nil)
		assert(t, "list alpha is gone", has, false)

		// 100 and 200 survived (still referenced by beta/gamma).
		has, err = pd.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 100 survives (still in beta)", has, true)

		has, err = pd.HasValue(200)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 200 survives (still in gamma)", has, true)

		// 300 sub-bucket was never touched.
		has, err = pd.HasValue(300)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 300 completely untouched", has, true)

		// All exact orderBy timestamps are intact.
		assertListOrderBy(t, pd, "beta", 100, t3)
		assertListOrderBy(t, pd, "beta", 300, t4)
		assertListOrderBy(t, pd, "gamma", 200, t5)
		assertListOrderBy(t, pd, "gamma", 300, t6)

		assertListSize(t, pd, "beta", 2)
		assertListSize(t, pd, "gamma", 2)

		assertValueLists(t, pd, 100, []string{"beta"})
		assertValueLists(t, pd, 200, []string{"gamma"})
		assertValueLists(t, pd, 300, []string{"beta", "gamma"})
	})
}

// TestLists_deleteValue_dataIntegrity tests ListsTx.DeleteValue, which removes
// a value from ALL lists in one call and deletes its index sub-bucket.
// Every other value's orderBy timestamps must survive byte-for-byte unchanged.
func TestLists_deleteValue_dataIntegrity(t *testing.T) {
	// Data:
	//   list "alpha": value 100 @ t1, value 200 @ t2, value 300 @ t3
	//   list "beta":  value 100 @ t4, value 200 @ t5
	//   list "gamma": value 100 @ t6, value 300 @ t7
	// DeleteValue(100): removes from alpha, beta, gamma; 100 sub-bucket deleted.
	// Timestamps for 200 and 300 must be exactly as stored.

	t1 := time.Unix(1001, 0)
	t2 := time.Unix(1002, 0)
	t3 := time.Unix(1003, 0)
	t4 := time.Unix(2001, 0)
	t5 := time.Unix(2002, 0)
	t6 := time.Unix(3001, 0)
	t7 := time.Unix(3002, 0)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		alpha, _, err := pd.List("alpha")
		assertErrorFail(t, "", err, nil)
		beta, _, err := pd.List("beta")
		assertErrorFail(t, "", err, nil)
		gamma, _, err := pd.List("gamma")
		assertErrorFail(t, "", err, nil)

		assertErrorFail(t, "", alpha.Add(100, t1), nil)
		assertErrorFail(t, "", alpha.Add(200, t2), nil)
		assertErrorFail(t, "", alpha.Add(300, t3), nil)
		assertErrorFail(t, "", beta.Add(100, t4), nil)
		assertErrorFail(t, "", beta.Add(200, t5), nil)
		assertErrorFail(t, "", gamma.Add(100, t6), nil)
		assertErrorFail(t, "", gamma.Add(300, t7), nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		err := pd.DeleteValue(100, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)

		has, err := pd.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 100 completely gone", has, false)

		assertListSize(t, pd, "alpha", 2) // 200 + 300
		assertListSize(t, pd, "beta", 1)  // 200
		assertListSize(t, pd, "gamma", 1) // 300

		assertListOrderBy(t, pd, "alpha", 200, t2)
		assertListOrderBy(t, pd, "alpha", 300, t3)
		assertListOrderBy(t, pd, "beta", 200, t5)
		assertListOrderBy(t, pd, "gamma", 300, t7)

		assertValueLists(t, pd, 200, []string{"alpha", "beta"})
		assertValueLists(t, pd, 300, []string{"alpha", "gamma"})
	})
}

// TestLists_multipleSimultaneousBucketDeletes_dataIntegrity verifies that
// removing the last reference for multiple different values in a single
// transaction — some triggering sub-bucket deletion, some not — leaves all
// other values and their exact orderBy timestamps completely intact.
func TestLists_multipleSimultaneousBucketDeletes_dataIntegrity(t *testing.T) {
	// Data:
	//   list "alpha": value 100 @ t1, value 200 @ t2, value 300 @ t3
	//   list "beta":  value 100 @ t4, value 300 @ t5
	//
	// In ONE transaction on "alpha":
	//   Remove(100) → 100 sub-bucket {alpha,beta} → {beta}: survives
	//   Remove(200) → 200 sub-bucket {alpha}      → {}:     DELETED
	//
	// 300 sub-bucket {alpha,beta} must be completely untouched.
	// beta entry for 100 (t4) must survive exactly.

	t1 := time.Unix(1001, 0)
	t2 := time.Unix(1002, 0)
	t3 := time.Unix(1003, 0)
	t4 := time.Unix(2001, 0)
	t5 := time.Unix(2002, 0)

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		alpha, _, err := pd.List("alpha")
		assertErrorFail(t, "", err, nil)
		beta, _, err := pd.List("beta")
		assertErrorFail(t, "", err, nil)

		assertErrorFail(t, "", alpha.Add(100, t1), nil)
		assertErrorFail(t, "", alpha.Add(200, t2), nil)
		assertErrorFail(t, "", alpha.Add(300, t3), nil)
		assertErrorFail(t, "", beta.Add(100, t4), nil)
		assertErrorFail(t, "", beta.Add(300, t5), nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)
		alpha, _, err := pd.List("alpha")
		assertErrorFail(t, "", err, nil)

		err = alpha.Remove(100, true)
		assertErrorFail(t, "", err, nil)
		err = alpha.Remove(200, true) // 200 sub-bucket deleted here
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		pd := projectDependencies.Tx(tx)

		has, err := pd.HasValue(200)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 200 gone", has, false)

		has, err = pd.HasValue(100)
		assertErrorFail(t, "", err, nil)
		assert(t, "value 100 survives via beta", has, true)

		// All remaining timestamps are exactly as written.
		assertListOrderBy(t, pd, "alpha", 300, t3)
		assertListOrderBy(t, pd, "beta", 100, t4)
		assertListOrderBy(t, pd, "beta", 300, t5)

		assertListSize(t, pd, "alpha", 1) // 300 only
		assertListSize(t, pd, "beta", 2)  // 100 + 300

		assertValueLists(t, pd, 100, []string{"beta"})
		assertValueLists(t, pd, 300, []string{"alpha", "beta"})
	})
}

func projectsDependenciesDB(t testing.TB) *bolt.DB {
	t.Helper()

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependencies.Tx(tx)

		boltronProjectDependencies, exists, err := projectDependencies.List("resenje.org/boltron")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		schulzeProjectDependencies, exists, err := projectDependencies.List("resenje.org/schulze")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		webProjectDependencies, exists, err := projectDependencies.List("resenje.org/web")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)
		poolProjectDependencies, exists, err := projectDependencies.List("resenje.org/pool")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		for _, d := range testProjectDependencies {
			switch d.ProjectName {
			case "resenje.org/boltron":
				err := boltronProjectDependencies.Add(d.DependencyID, d.UpdateTime)
				assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			case "resenje.org/schulze":
				err := schulzeProjectDependencies.Add(d.DependencyID, d.UpdateTime)
				assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			case "resenje.org/web":
				err := webProjectDependencies.Add(d.DependencyID, d.UpdateTime)
				assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			case "resenje.org/pool":
				err := poolProjectDependencies.Add(d.DependencyID, d.UpdateTime)
				assertErrorFail(t, fmt.Sprintf("%+v", d), err, nil)
			}
		}
	})

	return db
}

func projectDependenciesValues(is ...int) []uint64 {
	s := make([]uint64, 0, len(is))
	for _, i := range is {
		s = append(s, testProjectDependenciesValues[i])
	}
	return s
}

func projectDependenciesLists(is ...int) []string {
	s := make([]string, 0, len(is))
	for _, i := range is {
		s = append(s, testProjectDependenciesLists[i])
	}
	return s
}

func projectDependenciesListsWithValue125(is ...int) []boltron.ListsElement[string, time.Time] {
	s := make([]boltron.ListsElement[string, time.Time], 0, len(is))
	for _, i := range is {
		s = append(s, boltron.ListsElement[string, time.Time]{
			Key:     testProjectDependenciesListsWithValue125[i],
			OrderBy: testProjectDependenciesListsWithValue125Times[i],
		})
	}
	return s
}
