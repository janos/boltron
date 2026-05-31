# boltron

[![Go](https://github.com/janos/boltron/workflows/Go/badge.svg)](https://github.com/janos/boltron/actions)
[![PkgGoDev](https://pkg.go.dev/badge/resenje.org/boltron)](https://pkg.go.dev/resenje.org/boltron)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue.svg)](LICENSE)

`boltron` is a lightweight, type-safe, generic data-modeling library for [BoltDB](https://go.etcd.io/bbolt). It leverages Go Type Parameters (Generics) to eliminate repetitive serialization boilerplate and adds support for **arbitrary deep container nesting**, **global uniqueness indexing**, and **cascading deletes**.

Instead of working with raw `[]byte` keys and values in BoltDB, `boltron` lets you define clean, type-safe Go structs and handle them through three core container types: **Collection**, **Association**, and **List**.

---

## Key Features

* 🎯 **Type-Safe Generics**: Interact with native Go types. Key and value serialization/deserialization is managed completely under the hood by configurable encodings.
* 🌳 **Arbitrary Deep Nesting**: Dynamically nest collections, associations, and lists inside each other (e.g. `companies -> departments -> employees`) without exposing database path internals.
* 🔍 **Global Uniqueness Indexing**: Automatically enforce unique constraints on keys or values across all instances of a nested or root-level container.
* 🗑️ **Cascading Deletes**: Deleting a parent key automatically and recursively deletes all nested sub-containers and cleans up their global index entries.
* 🔗 **Type-Safe Parent Resolution**: Look up the parent key of any item in a nested container with decoded, type-safe APIs (`ParentKey`, `ParentKeyByLeft`, `ParentKeyByRight`).

---

## Installation

```bash
go get -u resenje.org/boltron
```

*Note: `boltron` requires Go 1.27 or later for generics support.*

---

## Core Containers

### 1. Collection

A `Collection` is a classic key-value store where keys are unique. Elements are stored sorted lexicographically by their encoded keys.

#### Defining a Collection

```go
type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Statically define the collection structure
var usersDef = boltron.NewCollectionDefinition(
	"users",
	boltron.StringEncoding,              // Key: string
	boltron.NewJSONEncoding[*User](),    // Value: User struct encoded as JSON
	nil,                                 // Options (optional)
)
```

#### Usage in a Bolt Transaction

```go
err := db.Update(func(tx *bolt.Tx) error {
	users := usersDef.Collection(tx)

	// Save a user. The last boolean parameter indicates whether overwrites are allowed.
	overwritten, err := users.Save("user-123", &User{Name: "Alice", Email: "alice@example.com"}, true)
	if err != nil {
		return err
	}

	// Retrieve a user
	user, err := users.Get("user-123")
	if err != nil {
		return err
	}
	fmt.Printf("Fetched user: %s (%s)\n", user.Name, user.Email)
	return nil
})
```

---

### 2. List

A `List` stores unique values ordered by a separate ordering key. If the order is defined by the values themselves (or is not important), you can use `boltron.NullEncoding` for the order.

#### Defining a List

```go
// Statically define a list of unique strings ordered by a timestamp
var tasksDef = boltron.NewListDefinition(
	"tasks",
	boltron.StringEncoding,  // Value: string (e.g., task details)
	boltron.TimeEncoding,    // OrderBy: time.Time
	nil,                     // Options
)
```

#### List Usage

```go
err := db.Update(func(tx *bolt.Tx) error {
	tasks := tasksDef.List(tx)

	// Add unique values with their sort orders
	err := tasks.Add("Write documentation", time.Now())
	if err != nil {
		return err
	}

	// Iterate through the list in chronological order
	_, err = tasks.Iterate(nil, false, func(value string, orderBy time.Time) (bool, error) {
		fmt.Printf("[%v] Task: %s\n", orderBy, value)
		return true, nil
	})
	return err
})
```

---

### 3. Association

An `Association` manages one-to-one relations between two domains (Left and Right). It is highly optimized for fast bi-directional lookups, pagination, and range scans.

#### Defining an Association

```go
// Map unique user IDs to session IDs
var sessionsDef = boltron.NewAssociationDefinition(
	"user_sessions",
	boltron.StringEncoding, // Left: User ID
	boltron.StringEncoding, // Right: Session ID
	nil,
)
```

#### Association Usage

```go
err := db.Update(func(tx *bolt.Tx) error {
	sessions := sessionsDef.Association(tx)

	// Establish a relation
	err := sessions.Set("user-123", "session-xyz")
	if err != nil {
		return err
	}

	// Fast bi-directional lookups
	sessionID, err := sessions.Right("user-123") // Left-to-Right
	if err == nil {
		fmt.Println("Session for user:", sessionID)
	}

	userID, err := sessions.Left("session-xyz")   // Right-to-Left
	if err == nil {
		fmt.Println("User for session:", userID)
	}
	return nil
})
```

---

## Advanced Features

### 1. Arbitrary Deep Nesting

`boltron` allows you to model highly complex, hierarchical data structures by nesting containers inside one another as deeply as your domain requires. For example, a root-level `Collection` of companies can contain a nested `Collection` of departments, which can itself contain a nested `List` of employees.

#### How It Works Under the Hood
Whenever you retrieve a nested container from a parent instance, `boltron` dynamically constructs a unique sub-bucket path. 
* **For Collection & List**: The nested container's bucket path is constructed by copying the parent's path and appending the key or value. Suffixes like `" values"` and `" index"` are used for lists to keep records and internal metadata separated.
* **For Association**: Because associations are bi-directional, they maintain separate left and right buckets. Nesting is supported under the left-side value, appending `" left"` and `" right"` suffixes to construct distinct sub-buckets for the nested container.

This hierarchical path building guarantees that all data remains perfectly isolated under parent buckets, preventing namespace collisions and allowing BoltDB's built-in bucket structure to naturally reflect your application's domain model.

#### Available Nesting Accessors

Each transaction-bound container provides methods to initialize child containers nested under a specific key or value:

| Parent Container | Target Placement | Method Signature |
| :--- | :--- | :--- |
| **`Collection[K, V]`** | Nested under key `key K` | `Collection[K2, V2](key, definition)` <br> `Association[L2, R2](key, definition)` <br> `List[V2, O2](key, definition)` |
| **`List[V, O]`** | Nested under value `value V` | `Collection[K2, V2](value, definition)` <br> `Association[L2, R2](value, definition)` <br> `List[V2, O2](value, definition)` |
| **`Association[L, R]`** | Nested under left value `left L` | `CollectionByLeft[K2, V2](left, definition)` <br> `AssociationByLeft[L2, R2](left, definition)` <br> `ListByLeft[V2, O2](left, definition)` |

#### Complete Nesting Example

```go
// 1. Declare the container definitions (templates)
var companiesDef = boltron.NewCollectionDefinition(
	"companies", 
	boltron.StringEncoding, 
	boltron.StringEncoding, 
	nil,
)

var departmentsDef = boltron.NewCollectionDefinition(
	"departments", 
	boltron.StringEncoding, 
	boltron.StringEncoding, 
	nil,
)

var employeesDef = boltron.NewListDefinition(
	"employees", 
	boltron.StringEncoding, 
	boltron.TimeEncoding, 
	nil,
)

// 2. Perform nested operations inside a transaction
err := db.Update(func(tx *bolt.Tx) error {
	companies := companiesDef.Collection(tx)

	// Get the "departments" collection nested under the company "company-A"
	depts, err := companies.Collection("company-A", departmentsDef)
	if err != nil {
		return err
	}

	// Save a department ("engineering") inside "company-A"
	_, err = depts.Save("engineering", "active", true)
	if err != nil {
		return err
	}

	// Get the "employees" list nested under the department "engineering" (which is inside "company-A")
	employees, err := depts.List("engineering", employeesDef)
	if err != nil {
		return err
	}

	// Add an employee to the engineering department list
	return employees.Add("Alice", time.Now())
})
```

---

### 2. Global Uniqueness Constraints

When nesting containers, you might want to guarantee that keys or values are globally unique across *all* instances of that container in the database (e.g. guaranteeing that an email address or username is unique across all companies or departments).

You can enable this by passing options to the definitions:

```go
// Departments must have globally unique keys
var departmentsDef = boltron.NewCollectionDefinition("departments", boltron.StringEncoding, boltron.StringEncoding, &boltron.CollectionOptions{
	UniqueKeys: true,
})

// Employees must have globally unique values
var employeesDef = boltron.NewListDefinition("employees", boltron.StringEncoding, boltron.TimeEncoding, &boltron.ListOptions{
	UniqueValues: true,
})

// Associations can enforce left, right, or both sides unique globally
var tagsDef = boltron.NewAssociationDefinition("tags", boltron.StringEncoding, boltron.StringEncoding, &boltron.AssociationOptions{
	UniqueLeft:  true,
	UniqueRight: true,
})
```

With `UniqueKeys` enabled, attempting to create a department named `"engineering"` under a different company will fail and return `boltron.ErrKeyExists`, even though they are inside separate company buckets.

---

### 3. Cascading Deletes

If a parent key is deleted, `boltron` recursively deletes all sub-buckets and automatically purges all corresponding entries from the global uniqueness indexes.

```go
err := db.Update(func(tx *bolt.Tx) error {
	companies := companiesDef.Collection(tx)

	// Deleting "company-A" recursively deletes:
	// - All its departments ("engineering", "hr", etc.)
	// - All employees inside those departments ("Alice", "Bob", etc.)
	// - All index entries in the departments and employees uniqueness indexes!
	return companies.Delete("company-A", true)
})
```

---

### 4. Type-Safe Parent Key Resolution

If uniqueness constraints are enabled, you can find the parent key of any nested container element starting from the global definition:

```go
err := db.View(func(tx *bolt.Tx) error {
	// Look up which department Alice belongs to
	parentDept, err := employeesDef.ParentKey(tx, "Alice", boltron.StringEncoding)
	if err != nil {
		return err
	}
	fmt.Println("Alice belongs to department:", parentDept) // e.g. "engineering"

	// Look up which company engineering belongs to
	parentCompany, err := departmentsDef.ParentKey(tx, parentDept, boltron.StringEncoding)
	if err != nil {
		return err
	}
	fmt.Println("Engineering belongs to company:", parentCompany) // e.g. "company-A"

	// Root containers will return boltron.ErrNoParent
	_, err = companiesDef.ParentKey(tx, parentCompany, boltron.StringEncoding)
	if errors.Is(err, boltron.ErrNoParent) {
		fmt.Println("Companies are at the root level")
	}
	return nil
})
```

---

## Built-In Encodings

`boltron` ships with several ready-to-use, optimized encodings:

* `boltron.StringEncoding`: UTF-8 string encoding.
* `boltron.BytesEncoding`: Raw `[]byte` slice encoding (returns a copy of the slice).
* `boltron.BytesUnsafeEncoding`: Raw `[]byte` slice encoding without copies (zero-allocation; returned slices are read-only and only valid during transaction lifetime).
* `boltron.StringNaturalOrderEncoding`: String encoding sorted in human-friendly "natural" alphanumeric order.
* `boltron.Uint64BinaryEncoding`: Integer encoding using big-endian byte order.
* `boltron.IntBase10Encoding`: Integer encoding represented in base-10 string format.
* `boltron.TimeEncoding`: `time.Time` values stored in RFC3339Nano string format.
* `boltron.NewJSONEncoding[T]()`: Custom structured type serialization utilizing JSON format.
* `boltron.NullEncoding`: Empty encoding (useful for order-agnostic lists).

You can easily implement your own encodings by implementing the `boltron.Encoding[T]` interface:

```go
type Encoding[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
}
```

---

## License

This application is distributed under the BSD 3-Clause license. See the [LICENSE](LICENSE) file for details.
