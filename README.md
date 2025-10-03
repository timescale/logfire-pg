# logfire-pg

A wrapper around the [Logfire](https://pydantic.dev/logfire) API, exposing it as a PostgreSQL server.

## Usage

```text
Usage of bin/logfire_pg:
  -port int
    	Port to listen on (default 5432)
  -token string
    	Logfire read token
```

## Development

### Building from Source

```bash
git clone git@github.com:timescale/logfire-pg.git
cd logfire-pg
go build -o bin/logfire_pg ./cmd/logfire_pg
```
