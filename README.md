# avro-sqlite

`avro-sqlite` is a Go package that provides functionality to interact with SQLite databases and convert their schemas and data to Apache Avro format. It allows you to export SQLite table schemas and data to Avro Object Container Files (OCF) and JSON formats.

## Features

- List tables in a SQLite database
- Read SQLite table schemas
- Convert SQLite schemas to Avro schemas
- Export SQLite table data to Avro OCF files
- Export SQLite table schemas to JSON files
- Load Avro data into SQLite tables

## Installation

```bash
go get github.com/yourusername/avro-sqlite
```

## Usage Example

Here's a simple example of how to use the `avro-sqlite` package to export a SQLite database to Avro format:

```go
package main

import (
    "database/sql"
    "log"

    "github.com/yourusername/avro-sqlite"
    _ "github.com/mattn/go-sqlite3"
)

func main() {
    // Open SQLite database
    db, err := sql.Open("sqlite3", "path/to/your/database.sqlite")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Export all tables to Avro OCF files
    files, err := avrosqlite.SqliteToAvro(db, "output_directory", "prefix_", true, nil)
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Exported files: %v", files)
}
```

This example opens a SQLite database, exports all tables to Avro OCF files, and also generates JSON schema files for each table.

## License

[Your chosen license]

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
