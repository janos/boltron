// Copyright (c) 2026, Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltron_test

import (
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"resenje.org/boltron"
)

func TestDeepNesting_UniqueKeys_And_DecodedLookups(t *testing.T) {
	db := newDB(t)

	companiesDef := boltron.NewCollectionDefinition("companies", boltron.StringEncoding, boltron.StringEncoding, nil)
	departmentsDef := boltron.NewCollectionDefinition("departments", boltron.StringEncoding, boltron.StringEncoding, &boltron.CollectionOptions{
		UniqueKeys: true,
	})
	employeesDef := boltron.NewListDefinition("employees", boltron.StringEncoding, boltron.TimeEncoding, &boltron.ListOptions{
		UniqueValues: true,
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		companies := companiesDef.Collection(tx)

		deptsA, err := companies.Collection("company-A", departmentsDef)
		assertErrorFail(t, "get deptsA", err, nil)

		_, err = deptsA.Save("engineering", "active", true)
		assertErrorFail(t, "save dept", err, nil)

		employeesA, err := deptsA.List("engineering", employeesDef)
		assertErrorFail(t, "get employeesA", err, nil)

		err = employeesA.Add("alice", time.Now())
		assertErrorFail(t, "add employee to company-A", err, nil)

		deptsB, err := companies.Collection("company-B", departmentsDef)
		assertErrorFail(t, "get deptsB", err, nil)

		_, err = deptsB.Save("engineering", "active", true)
		assertError(t, "save duplicate dept should fail", err, boltron.ErrKeyExists)

		_, err = deptsB.Save("hr", "active", true)
		assertErrorFail(t, "save hr dept", err, nil)

		employeesB, err := deptsB.List("hr", employeesDef)
		assertErrorFail(t, "get employeesB", err, nil)

		err = employeesB.Add("alice", time.Now())
		assertError(t, "add duplicate employee should fail", err, boltron.ErrValueExists)

		err = employeesB.Add("bob", time.Now())
		assertErrorFail(t, "add bob", err, nil)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		parentDept, err := employeesDef.ParentKey(tx, "alice", boltron.StringEncoding)
		assertErrorFail(t, "find parent dept for alice", err, nil)
		assert(t, "parent dept name", parentDept, "engineering")

		parentCompany, err := departmentsDef.ParentKey(tx, "engineering", boltron.StringEncoding)
		assertErrorFail(t, "find parent company for engineering", err, nil)
		assert(t, "parent company name", parentCompany, "company-A")

		_, err = companiesDef.ParentKey(tx, "company-A", boltron.StringEncoding)
		assertError(t, "root container find parent", err, boltron.ErrNoParent)
	})
}

func TestCascadingDeletes(t *testing.T) {
	db := newDB(t)

	companiesDef := boltron.NewCollectionDefinition("companies", boltron.StringEncoding, boltron.StringEncoding, nil)
	departmentsDef := boltron.NewCollectionDefinition("departments", boltron.StringEncoding, boltron.StringEncoding, &boltron.CollectionOptions{
		UniqueKeys: true,
	})
	employeesDef := boltron.NewListDefinition("employees", boltron.StringEncoding, boltron.TimeEncoding, &boltron.ListOptions{
		UniqueValues: true,
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		companies := companiesDef.Collection(tx)
		depts, err := companies.Collection("company-A", departmentsDef)
		assertErrorFail(t, "get depts", err, nil)

		_, err = depts.Save("engineering", "active", true)
		assertErrorFail(t, "save engineering", err, nil)

		employees, err := depts.List("engineering", employeesDef)
		assertErrorFail(t, "get employees", err, nil)

		err = employees.Add("alice", time.Now())
		assertErrorFail(t, "add alice", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		companies := companiesDef.Collection(tx)
		depts, err := companies.Collection("company-A", departmentsDef)
		assertErrorFail(t, "get depts", err, nil)

		err = depts.Delete("engineering", true)
		assertErrorFail(t, "delete engineering department", err, nil)
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		companies := companiesDef.Collection(tx)
		depts, err := companies.Collection("company-B", departmentsDef)
		assertErrorFail(t, "get depts B", err, nil)

		_, err = depts.Save("engineering", "active", true)
		assertErrorFail(t, "recreate engineering under B", err, nil)

		employees, err := depts.List("engineering", employeesDef)
		assertErrorFail(t, "get employees", err, nil)

		err = employees.Add("alice", time.Now())
		assertErrorFail(t, "add alice to B", err, nil)
	})
}

func TestDeepNesting_Association(t *testing.T) {
	db := newDB(t)

	companiesDef := boltron.NewCollectionDefinition("companies", boltron.StringEncoding, boltron.StringEncoding, nil)
	tagsDef := boltron.NewAssociationDefinition("tags", boltron.StringEncoding, boltron.StringEncoding, &boltron.AssociationOptions{
		UniqueLeft:  true,
		UniqueRight: true,
	})

	dbUpdate(t, db, func(t testing.TB, tx *bolt.Tx) {
		companies := companiesDef.Collection(tx)
		tagsA, err := companies.Association("company-A", tagsDef)
		assertErrorFail(t, "get tagsA", err, nil)

		err = tagsA.Set("tag-golang", "item-1")
		assertErrorFail(t, "set tag", err, nil)

		tagsB, err := companies.Association("company-B", tagsDef)
		assertErrorFail(t, "get tagsB", err, nil)

		err = tagsB.Set("tag-golang", "item-2")
		assertError(t, "set duplicate left", err, boltron.ErrLeftExists)

		err = tagsB.Set("tag-rust", "item-1")
		assertError(t, "set duplicate right", err, boltron.ErrRightExists)
	})

	dbView(t, db, func(t testing.TB, tx *bolt.Tx) {
		parentLeft, err := tagsDef.ParentKeyByLeft(tx, "tag-golang", boltron.StringEncoding)
		assertErrorFail(t, "find parent left", err, nil)
		assert(t, "parent key left", parentLeft, "company-A")

		parentRight, err := tagsDef.ParentKeyByRight(tx, "item-1", boltron.StringEncoding)
		assertErrorFail(t, "find parent right", err, nil)
		assert(t, "parent key right", parentRight, "company-A")
	})
}
