#!/usr/bin/env bash
# Fetch Go modules from the index, write protobufs to sync/, and generate index.txt.
# Usage: ./scripts/sync.sh [--server localhost:50051] [--limit 2000]
set -euo pipefail

cd "$(dirname "$0")/.."
go run ./cmd/sync "$@"
