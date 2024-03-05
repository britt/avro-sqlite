package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	avrosqlite "github.com/britt/avro-sqlite"
	"github.com/hamba/avro"
	_ "github.com/mattn/go-sqlite3"
)

// FIXME: replace with real tests
func main() {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS  foo (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO foo (name) VALUES ('bar')")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO foo (name) VALUES ('beef')")
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO foo (name) VALUES ('meatballs')")
	if err != nil {
		log.Fatal(err)
	}

	schema, err := avrosqlite.ReadSchema(db, "foo")
	if err != nil {
		log.Fatal(err)
	}

	a, err := schema.ToAvro()
	if err != nil {
		log.Fatal(err)
	}

	j, err := json.Marshal(a)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(j))

	avro.MustParse(string(j))

	ab, err := avrosqlite.ReadData(db, "foo", a)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(ab)

	out, err := avrosqlite.ReadAvro[struct {
		ID   int64  `avro:"id"`
		Name string `avro:"name"`
	}](a, bytes.NewBuffer(ab))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(out)
}
