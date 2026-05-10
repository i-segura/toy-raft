# Toy Raft

This is a toy implementation of the Raft consensus algorithm. Its main purpose is as a learning exercise for me to understand the Raft algorithm and how it works.

## Requirements

[Go](https://go.dev/) 1.26 or newer.

## Build

From the repository root:

```bash
go build -o toy-raft .
go build -o toy-client ./cmd/toy-client
```

## Running a cluster

Each process listens on **two ports**: one for Raft RPCs and one for the **HTTP data API**. Every peer must know the full static membership list as a comma-separated list of triples `host:raftPort:httpPort`. Include **all** peers (including this node).

```bash
./toy-raft <raft_port> <http_port> '<peer_triples>'
```

Example for three nodes on localhost (match the Raft and HTTP ports you choose):

```bash
HOST=127.0.0.1
PEERS="${HOST}:8888:18088,${HOST}:8889:18089,${HOST}:8890:18090"

./toy-raft 8888 18088 "${PEERS}" &
./toy-raft 8889 18089 "${PEERS}" &
./toy-raft 8890 18090 "${PEERS}" &
```

- **Election** uses a timeout on the order of ~10 seconds (plus jitter), so wait briefly after startup before expecting a stable leader.
- **Persistence**: each peer writes `state_peer_<raft_port>.json` in the working directory.
- **Shutdown**: `SIGINT` or `SIGTERM` stops Raft and HTTP servers cleanly.

## Demo script

`scripts/demo-cluster.sh` builds into `./bin`, starts three nodes with ports `8888/18088`, `8889/18089`, `8890/18090`, waits for election, then runs `toy-client`. Override the bind host with `HOST` (default `127.0.0.1`):

```bash
./scripts/demo-cluster.sh
```

Press **Ctrl+C** to stop every child process.

## HTTP API (clients)

Clients talk to the **HTTP** port (`<http_port>`), not the Raft port.

- **Endpoint**: `POST /` with `Content-Type: application/json`.
- **Body**: a JSON object. The toy client sends commands like `{"op":"set","key":"k0","value":123}`; the payload is replicated as opaque JSON through Raft once a leader commits it.
- **Behavior**: only the leader proposes to the log; followers **proxy** the same request to the current leader’s HTTP base URL.

Example with `curl` (adjust the URL to a node’s HTTP port):

```bash
curl -sS -X POST http://127.0.0.1:18088/ \
  -H 'Content-Type: application/json' \
  -d '{"op":"set","key":"hello","value":"world"}'
```

The load generator loops random `POST`s against a comma-separated list of peer base URLs:

```bash
./toy-client -peers 'http://127.0.0.1:18088,http://127.0.0.1:18089,http://127.0.0.1:18090'
```

Flags: `-max-interval` (sleep cap between requests), `-timeout` (HTTP client timeout).
