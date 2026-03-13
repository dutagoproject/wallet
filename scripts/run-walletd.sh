#!/usr/bin/env bash
set -euo pipefail
DATA_DIR="${1:-./data/mainnet}"
cargo run -- --datadir "$DATA_DIR"
