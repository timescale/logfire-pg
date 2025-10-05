# logfire-pg

A wrapper around the [Logfire](https://pydantic.dev/logfire) API, exposing it as a PostgreSQL server.

## Installation

For Linux and Windows, go to the [Releases](https://github.com/timescale/logfire-pg/releases) page
and download the binary for your system. Place the binary onto somewhere in your PATH.

For macOS, you will need to clone the repo and build from source.

## Usage

To start the server, simply do:

```bash
logfire_pg
```

Where this starts a server that accepts PostgreSQL connections, where the username should be `token`
and the password is your Logfire read token, see
[here](https://logfire.pydantic.dev/docs/how-to-guides/query-api/#how-to-create-a-read-token) on how
to create the token.

Then to connect, you can use something like `psql` as follows:

```bash
psql -d "postgres://token:${LOGFIRE_READ_TOKEN}$@localhost:5432/logfire"
```

For the full available options on running the server, see `--help`:

```text
Usage of ./bin/logfire_pg:
      --help          Print this help message and exit
      --host string   Host to listen on (default "127.0.0.1")
      --port int      Port to listen on (default 5432)
      --version       Print version and exit
```

## Development

### Building from Source

```bash
git clone git@github.com:timescale/logfire-pg.git
cd logfire-pg
go build -o bin/logfire_pg ./cmd/logfire_pg
```
