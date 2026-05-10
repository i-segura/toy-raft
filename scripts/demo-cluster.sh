#!/usr/bin/env bash
# Start three toy-raft nodes and the toy-client. Ctrl+C sends SIGTERM to every child.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${ROOT}/bin"
mkdir -p "${BIN_DIR}"

TOY_RAFT="${BIN_DIR}/toy-raft"
TOY_CLIENT="${BIN_DIR}/toy-client"

build() {
  (cd "${ROOT}" && go build -o "${TOY_RAFT}" .) || exit 1
  (cd "${ROOT}" && go build -o "${TOY_CLIENT}" ./cmd/toy-client) || exit 1
}

HOST="${HOST:-127.0.0.1}"

# Raft ports and matching HTTP (data) ports — must match peer triples below.
RAFT_PORTS=(8888 8889 8890)
HTTP_PORTS=(18088 18089 18090)

PEERS="${HOST}:${RAFT_PORTS[0]}:${HTTP_PORTS[0]},${HOST}:${RAFT_PORTS[1]}:${HTTP_PORTS[1]},${HOST}:${RAFT_PORTS[2]}:${HTTP_PORTS[2]}"
HTTP_PEERS="http://${HOST}:${HTTP_PORTS[0]},http://${HOST}:${HTTP_PORTS[1]},http://${HOST}:${HTTP_PORTS[2]}"

pids=()

cleanup() {
  local pid
  for pid in "${pids[@]:-}"; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill -TERM "${pid}" 2>/dev/null || true
    fi
  done
  for pid in "${pids[@]:-}"; do
    wait "${pid}" 2>/dev/null || true
  done
}

trap 'cleanup; exit 0' INT TERM

build

for i in 0 1 2; do
  "${TOY_RAFT}" "${RAFT_PORTS[$i]}" "${HTTP_PORTS[$i]}" "${PEERS}" &
  pids+=($!)
done

# Allow time for leader election (election timeout is ~10s + jitter).
sleep 12

"${TOY_CLIENT}" -peers "${HTTP_PEERS}" &
pids+=($!)

wait
