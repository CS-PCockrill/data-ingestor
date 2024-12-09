# XML-JSON Loader

XML-JSON Loader is a Go-based application designed to parse XML and JSON files containing structured data, and efficiently load the records into a SQL database using Map-Reduce for batch processing. The system supports PostgreSQL and includes robust error handling for data integrity and transaction safety.

##**Features**
- Supports XML and JSON file formats. 
- Dynamically parses data structures with nested and flattened records.
- Uses Map-Reduce to distribute tasks across workers for scalable performance.
- Ensures data integrity with transaction-based inserts.
- Compatible with PostgreSQL for modern database management. 

##**Table of Contents**

- [Installation](#installation) 
- [Usage](#usage)
- [Configuration](#configuration)
- [Dependencies](#dependencies)
  - [Dependency Management](#dependency-management)


##**Installation**

Clone the repository:
```shell
git clone https://github.com/yourusername/xml-json-loader.git
cd xml-json-loader
```
Install dependencies:
```shell
go mod tidy
go mod vendor
```
Build the application:
```shell
go build -o loader
```

##**Usage**

Command-Line Options
-file: Specify the path to the input file (XML or JSON).
Example Command
To load a JSON file:
```shell
./loader -file path/to/test-loader.json
````
To load an XML file:

```shell
./loader -file path/to/test-loader.xml
````
Sample Output
Processing file: test-loader.json
Parsed 10 records successfully.
All records inserted into the database.

##**Configuration**

### Run a Postgres Container
```shell
# Pull the latest PostgreSQL image from Docker Hub:
docker pull postgres
# Start a new container using the pulled image:
docker run --name postgres-container \
  -e POSTGRES_USER=root \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 \
  -d postgres

# You can connect to the database using the psql CLI from within the container:
docker exec -it postgres-container psql -U root -d testdb
```

Create a table using the following SQL:
```sql
CREATE TABLE SFLW_RECS (
    user VARCHAR(255) NOT NULL,
    dt_created BIGINT NOT NULL,
    dt_submitted BIGINT NOT NULL,
    ast_name VARCHAR(255),
    location VARCHAR(255) NOT NULL,
    status VARCHAR(255) NOT NULL,
    json_hash VARCHAR(255) NOT NULL,
    local_id VARCHAR(255),
    filename VARCHAR(255) NOT NULL,
    fnumber VARCHAR(255) NOT NULL,
    scan_time VARCHAR(255) NOT NULL
);
```
### Connect to Database in Go
Update the dsn (Data Source Name) in the main.go file:
```go
dsn := "postgres://root:password@localhost:5432/testdb"
```

## Dependencies

This project leverages Go's standard library and a minimal set of external dependencies to ensure simplicity, security, and portability.

### Standard Dependencies
The following packages from Go's standard library are used:
- [`database/sql`](https://pkg.go.dev/database/sql): Provides a generic interface for SQL databases.
- [`encoding/json`](https://pkg.go.dev/encoding/json): Handles parsing and encoding JSON data.
- [`encoding/xml`](https://pkg.go.dev/encoding/xml): Handles parsing and encoding XML data.
- [`flag`](https://pkg.go.dev/flag): Provides command-line flag parsing.
- [`fmt`](https://pkg.go.dev/fmt): Used for formatted I/O operations.
- [`log`](https://pkg.go.dev/log): Used for logging errors and application behavior.
- [`os`](https://pkg.go.dev/os): Manages file system operations and environment variables.
- [`sync`](https://pkg.go.dev/sync): Provides basic synchronization primitives (e.g., `sync.WaitGroup`).

### External Dependencies
The project includes the following external dependencies:

1. **[`pgx`](https://github.com/jackc/pgx)**:
    - PostgreSQL driver for Go with support for modern PostgreSQL features.
    - Provides compatibility with `database/sql` while offering advanced functionality for PostgreSQL-specific use cases.
    - Installation:
      ```bash
      go get github.com/jackc/pgx/v5
      ```

2. **[`godror`](https://github.com/godror/godror)**:
    - Oracle database driver for Go, fully compatible with `database/sql`.
    - Enables direct connections to Oracle databases and supports modern Oracle features.
    - Installation:
      ```bash
      go get github.com/godror/godror
      ```

### Dependency Management
To ensure all dependencies are included in the project for offline builds or deployment to secure environments:
1. Use the `go mod vendor` command to create a `vendor/` directory with all required dependencies:
    ```shell
   go mod vendor
    ````
   2. Use the `-mod=vendor` flag during builds and runs to prioritize the `vendor/` directory:
   ```shell
   go build -mod=vendor
   go run -mod=vendor main.go
   ```