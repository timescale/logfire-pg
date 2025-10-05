# logfire-pg

A wrapper around the [Logfire](https://pydantic.dev/logfire) API, exposing it as a PostgreSQL server.

## Usage

### Docker

```bash
docker run --name logfire-pg -p 5432:5432 ghcr.io/timescale/logfire-pg
```

### Binary

#### Installation

For Linux and Windows, go to the [Releases](https://github.com/timescale/logfire-pg/releases) page
and download the binary for your system. Place the binary onto somewhere in your PATH.

For macOS, you will need to clone the repo and build from source.

#### Running the Binary

To start the server, simply do:

```bash
logfire_pg
```

For the full available options on running the server, see `--help`:

```text
Usage of ./bin/logfire_pg:
      --help          Print this help message and exit
      --host string   Host to listen on (default "127.0.0.1")
      --port int      Port to listen on (default 5432)
      --version       Print version and exit
```

### Connecting to logfire-pg

After starting the server via one of the above methods, you can then use a PostgreSQL client, like
`psql` to connect. You will need to first obtain a read token for your Logfire project, see
[here](https://logfire.pydantic.dev/docs/how-to-guides/query-api/#how-to-create-a-read-token) on how
to create a read token.

If using `psql`, you could then do the following to connect:

```bash
psql -d "postgres://token:${LOGFIRE_READ_TOKEN}$@localhost:5432/logfire"
```

## Development

### Building from Source

```bash
git clone git@github.com:timescale/logfire-pg.git
cd logfire-pg
go build -o bin/logfire_pg ./cmd/logfire_pg
```
