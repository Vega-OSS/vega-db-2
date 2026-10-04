package engine_test

import (
	"fmt"
	"os"

	"vegadb/internal/engine"
)

func Example() {
	dir, err := os.MkdirTemp("", "vegadb-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	opts := engine.DefaultOptions()
	opts.Dir = dir

	db, err := engine.Open(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("hello"), nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	// Output:
	// world
}
