# pg_logfire

A wrapper around the [Logfire](https://pydantic.dev/logfire) API, exposing it as a PostgreSQL server.

## Usage

```text
Usage of bin/pg_logfire:
  -port int
    	Port to listen on (default 5432)
  -token string
    	Logfire read token
```

## Development

### Building from Source

```bash
git clone git@github.com:timescale/pg_logfire.git
cd pg_logfire
go build -o bin/pg_logfire ./cmd/pg_logfire
```
