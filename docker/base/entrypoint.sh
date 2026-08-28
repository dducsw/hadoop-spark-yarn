#!/usr/bin/env bash
set -e

# Start SSH daemon in background
service ssh start > /dev/null 2>&1 || true

# Execute the passed command or default to bash
exec "$@"
