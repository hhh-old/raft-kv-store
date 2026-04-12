#!/bin/bash
# Start Node 3 (Follower)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Starting Raft KV Store Node 3..."
echo "Project dir: $PROJECT_DIR"

cd "$PROJECT_DIR"

# Use node3 configuration
java -jar \
    -Dspring.config.location=file:$PROJECT_DIR/config/node3.yml \
    -Dserver.port=8083 \
    target/raft-kv-store-1.0.0.jar
