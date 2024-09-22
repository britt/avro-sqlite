# avro-sqlite

[![Go Reference](https://pkg.go.dev/badge/github.com/britt/avro-sqlite.svg)](https://pkg.go.dev/github.com/britt/avro-sqlite)
[![Go Report Card](https://goreportcard.com/badge/github.com/britt/avro-sqlite)](https://goreportcard.com/report/github.com/britt/avro-sqlite)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

`avro-sqlite` is a powerful Go package that bridges the gap between SQLite databases and Apache Avro format. It provides a seamless way to convert SQLite schemas and data to Avro, enabling efficient data serialization and interoperability.

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

Here's a comprehensive guide on how to use the `avro-sqlite` package:

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

### Customizing Export Options

You can customize the export process by providing specific options:

```go
options := &avrosqlite.ExportOptions{
    Tables:        []string{"users", "orders"},  // Export only specific tables
    IncludeSchema: true,                         // Include schema in OCF files
    Compression:   "snappy",                     // Use Snappy compression
}

files, err := avrosqlite.SqliteToAvro(db, "output_directory", "prefix_", true, options)
```

### Importing Avro Data to SQLite

To import Avro data back into SQLite:

```go
err := avrosqlite.AvroToSqlite(db, "path/to/avro/file.avro", "table_name")
if err != nil {
    log.Fatal(err)
}
```

This function reads an Avro OCF file and inserts its data into the specified SQLite table.

## Advanced Usage

### Working with Schemas

You can directly work with schemas:

```go
// Get Avro schema for a SQLite table
schema, err := avrosqlite.GetAvroSchemaForTable(db, "table_name")

// Convert SQLite schema to Avro schema
avroSchema, err := avrosqlite.SqliteSchemaToAvroSchema(sqliteSchema)
```

### Database Introspection

Analyze your SQLite database:

```go
// List all tables
tables, err := avrosqlite.ListTables(db)

// Get schema for a specific table
schema, err := avrosqlite.GetTableSchema(db, "table_name")
```

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
