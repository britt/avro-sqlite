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
go get github.com/britt/avro-sqlite
```

## Usage Example

Here's a simple example of how to use the `avro-sqlite` package to export a SQLite database to Avro format:

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

This example opens a SQLite database, exports all tables to Avro OCF files, and also generates JSON schema files for each table.

## License

MIT License

Copyright (c) 2024 Britt Crawford

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Contributing

Contributions are welcome. Please feel free to submit a Pull Request.
