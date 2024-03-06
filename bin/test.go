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
	avro.MustParse(`{"name":"com.github.britt.avrosqlite.reminders","type":"record","fields":[{"name":"id","type":"string"},{"name":"calendar_id","type":"string","default":""},{"name":"completed","type":"boolean","default":false},{"name":"title","type":"string","default":""},{"name":"notes","type":"string"},{"name":"duration","type":"long"},{"name":"url","type":"string"},{"name":"due_date","type":"string"},{"name":"start_date","type":"string"},{"name":"completion_date","type":"string"},{"name":"updated_at","type":"string","default":""}]}`)
	db, err := sql.Open("sqlite3", "/Users/brittcrawford/Downloads/maitri.v2.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	outDb, err := sql.Open("sqlite3", "/Users/brittcrawford/Downloads/out.test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer outDb.Close()

	// _, err = db.Exec("CREATE TABLE IF NOT EXISTS  foo (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// _, err = db.Exec("INSERT INTO foo (name) VALUES ('bar')")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// _, err = db.Exec("INSERT INTO foo (name) VALUES ('beef')")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// _, err = db.Exec("INSERT INTO foo (name) VALUES ('meatballs')")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	tables, err := avrosqlite.ListTables(db)
	if err != nil {
		log.Fatal(err)
	}

	for _, table := range tables {
		fmt.Println("Loading table ", table)
		schema, err := avrosqlite.ReadSchema(db, table)
		if err != nil {
			log.Fatal(err)
		}

		sj, _ := json.Marshal(schema)
		fmt.Println(string(sj))

		a, err := schema.ToAvro()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(a)
		j, err := json.Marshal(a)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(j))

		fmt.Println("Parsing schema")

		fmt.Println("Reading data", table)
		ab, err := avrosqlite.ReadData(db, table, a)
		if err != nil {
			log.Fatal("reading data fatal ", err)
		}
		fmt.Println(ab)

		fmt.Println("Reading AVRO", table)
		out, err := avrosqlite.ReadAvro(a, bytes.NewBuffer(ab))
		if err != nil {
			log.Fatal("reading avro fatal", err)
		}
		fmt.Println("read avro", out)

		n, err := avrosqlite.LoadAvro(outDb, schema, bytes.NewBuffer(ab))
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("loaded", n, "records")
	}
}
