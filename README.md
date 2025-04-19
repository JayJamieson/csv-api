# CSV API

A API server for creating ephemeral or persistent endpoints from CSV files.

Ephemeral endpoints use DuckDB for initial CSV import. Persistence can be achieved by exporting to configured Turso database to keep CSV data between server restarts.

## Features

- Uses Turso DB
- Import CSV files from URLs or direct uploads
- Query imported CSV data with filtering, sorting, and pagination
- Support for different output formats (objects or arrays)
- DuckDB for ephemeral storage and CSV import

## Setup and Installation

### Prerequisites

- Go 1.24 or later
- Turso CLI (for local development with Turso DB)

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/JayJamieson/csv-api.git
   cd csv-api
   ```

2. Install dependencies:

   ```bash
   go mod download
   ```

3. Build:

   ```bash
   go build ./...
   ```

## Running the Server

To run the server with default settings:

```bash
go run ./cmd/server/main.go
```

The server will start on port 8001 by default.

### Configuration

You can configure the server using command-line flags:

```bash
go run ./cmd/server/main.go \
  --port=3000 \
  --db-url="http://127.0.0.1:8080" # turso dev
```

Or using environment variables:

```bash
PORT=3000 DATABASE_URL="http://127.0.0.1:8080" go run ./cmd/server/main.go
```

## Using the API

### Import a CSV File

#### From a URL

```bash
curl -X POST "http://localhost:3000/load?url=https://example.com/data.csv"
```

#### From a file upload

```bash
curl -X POST "http://localhost:3000/load?name=animals.csv" \
  --data-binary @./samples/animals.csv \
  -H "Content-Type: text/csv"
```

### Query CSV Data from ephemeral storage

```bash
curl "http://localhost:3000/api/memory/{uuid}?_size=10&_sort=column_name&_shape=objects"
```

### Query CSV Data from persitent storage

First perist to persistent storage from in memory storage:

```bash
curl -X POST "http://localhost:3000/api/{uuid}/persist"
```

Now you can query from persistent storage:

```bash
curl "http://localhost:3000/api/{uuid}?_size=10&_sort=column_name&_shape=objects"
```

Query parameters:

- `_size`: Limit the number of rows returned
- `_offset`: Offset for pagination
- `_sort`: Column name to sort ascending
- `_sort_desc`: Column name to sort descending
- `_shape`: Output format (`objects` or `array`)
- `_rowid`: Show or hide the rowid field (`show` or `hide`)
- `_total`: Show or hide the total row count (`show` or `hide`)

## Development

## License

MIT
