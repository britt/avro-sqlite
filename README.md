# avro-sqlite

[![Go Reference](https://pkg.go.dev/badge/github.com/britt/avro-sqlite.svg)](https://pkg.go.dev/github.com/britt/avro-sqlite)
[![Go Report Card](https://goreportcard.com/badge/github.com/britt/avro-sqlite)](https://goreportcard.com/report/github.com/britt/avro-sqlite)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`avro-sqlite` is a Go package that converts SQLite database schemas and data to Apache Avro format and vice versa. It allows for exporting SQLite tables to Avro Object Container Files (OCF) and importing Avro data back into SQLite tables.

## Features

- **SQLite to Avro Conversion**: Export SQLite table schemas and data to Avro Object Container Files (OCF).
- **Schema Management**: Convert SQLite schemas to Avro schemas and export them as JSON.
- **Data Import**: Load Avro data back into SQLite tables.
- **Flexible Configuration**: Customize export options, including table selection and file naming.
- **Database Introspection**: List and analyze tables in a SQLite database.

## Installation

To install `avro-sqlite`, use the `go get` command:

```bash
go get -u github.com/britt/avro-sqlite
```

## Usage

Here are some examples of how to use the `avro-sqlite` package:

### Exporting SQLite to Avro

```go
package main

import (
    "database/sql"
    "log"

    "github.com/britt/avro-sqlite"
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

This example demonstrates how to export all tables from a SQLite database to Avro OCF files, including JSON schema files for each table.

### Reading Schema and Data

```go
// Read schema for a specific table
schema, err := avrosqlite.ReadSchema(db, "table_name")
if err != nil {
    log.Fatal(err)
}

// Load data from a table
data, err := avrosqlite.LoadData(db, "table_name")
if err != nil {
    log.Fatal(err)
}
```

### Converting SQLite Schema to Avro Schema

```go
sqliteSchema, err := avrosqlite.ReadSchema(db, "table_name")
if err != nil {
    log.Fatal(err)
}

avroSchema, err := sqliteSchema.ToAvro()
if err != nil {
    log.Fatal(err)
}

// Use the Avro schema...
```

### Loading Avro Data into SQLite

```go
import (
    "os"
    "github.com/hamba/avro"
)

// Open the Avro file
file, err := os.Open("path/to/avro/file.avro")
if err != nil {
    log.Fatal(err)
}
defer file.Close()

// Read the SQLite schema (assuming you have it)
schema, err := avrosqlite.ReadSchema(db, "table_name")
if err != nil {
    log.Fatal(err)
}

// Load the Avro data into SQLite
count, err := avrosqlite.LoadAvro(db, schema, file)
if err != nil {
    log.Fatal(err)
}

log.Printf("Inserted %d records", count)
```

These examples provide a more accurate representation of how to use the `avro-sqlite` package based on the actual implementation in the provided source files.

## Contributing

Contributions to `avro-sqlite` are welcome! Here's how you can contribute:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

Please ensure your code adheres to the project's coding standards and includes appropriate tests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Thanks to all contributors who have helped shape `avro-sqlite`.
- Special thanks to the Go community for providing excellent tools and libraries.

## Support

If you encounter any issues or have questions, please file an issue on the [GitHub issue tracker](https://github.com/britt/avro-sqlite/issues).

---

Happy coding with `avro-sqlite`!
