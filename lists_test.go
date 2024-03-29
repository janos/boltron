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
	projectDependenciesDefinition = boltron.NewListsDefinition(
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
		projectDependencies := projectDependenciesDefinition.Lists(tx)

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
		projectDependencies := projectDependenciesDefinition.Lists(tx)

		err := projectDependencies.DeleteValue(120, true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = projectDependencies.DeleteValue(deletedValue, true)
		assertErrorFail(t, "", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		deletedValueFromList := uint64(125)

		projectDependencies := projectDependenciesDefinition.Lists(tx)

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
		projectDependencies := projectDependenciesDefinition.Lists(tx)

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
		projectDependencies := projectDependenciesDefinition.Lists(tx)

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
		projectDependencies := projectDependenciesDefinition.Lists(tx)

		err := projectDependencies.DeleteList("resenje.org/missing", true)
		assertErrorFail(t, "", err, boltron.ErrNotFound)

		err = projectDependencies.DeleteList(deletedList, true)
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			size, err := projectDependencies.Size()
			assertErrorFail(t, "", err, nil)
			assert(t, "", size, 4)
		})
	})

	t.Run("empty", func(t *testing.T) {
		db := newDB(t)

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			list, exists, err := projectDependencies.List("resenje.org/daemon")
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			err = list.Add(10000, time.Now())
			assertErrorFail(t, "", err, nil)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			page, totalElements, totalPages, err := projectDependencies.PageOfListsWithValue(125, 1, 3, true)
			assertErrorFail(t, "", err, nil)
			assert(t, "", page, nil)
			assert(t, "", totalElements, 0)
			assert(t, "", totalPages, 0)
		})

		dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

			list, exists, err := projectDependencies.List("resenje.org/daemon")
			assertErrorFail(t, "", err, nil)
			assert(t, "", exists, false)

			err = list.Add(10000, time.Now())
			assertErrorFail(t, "", err, nil)
		})

		dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
			projectDependencies := projectDependenciesDefinition.Lists(tx)

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
		projectDependencies := projectDependenciesDefinition.Lists(tx)

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
		projectDependencies := projectDependenciesDefinition.Lists(tx)

		list, exists, err := projectDependencies.List("one")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = list.Add(1, time.Now())
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependenciesDefinition.Lists(tx)

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

	customProjectDependenciesDefinition := boltron.NewListsDefinition(
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
		projectDependencies := customProjectDependenciesDefinition.Lists(tx)

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
		projectDependencies := customProjectDependenciesDefinition.Lists(tx)

		list, exists, err := projectDependencies.List("one")
		assertErrorFail(t, "", err, nil)
		assert(t, "", exists, false)

		err = list.Add(1, time.Now())
		assertErrorFail(t, "", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := customProjectDependenciesDefinition.Lists(tx)

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
	customProjectDependenciesDefinition := boltron.NewListsDefinition(
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
		projectDependencies := customProjectDependenciesDefinition.Lists(tx)

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

	customProjectDependenciesDefinition := boltron.NewListsDefinition(
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
		projectDependencies := customProjectDependenciesDefinition.Lists(tx)

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

func projectsDependenciesDB(t testing.TB) *bolt.DB {
	t.Helper()

	db := newDB(t)

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		projectDependencies := projectDependenciesDefinition.Lists(tx)

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
