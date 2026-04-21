#!/bin/bash
# Start Node 2 (Follower)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Starting Raft KV Store Node 2..."
echo "Project dir: $PROJECT_DIR"

cd "$PROJECT_DIR"

# Use node2 configuration
java -jar \
    -Dspring.config.location=file:$PROJECT_DIR/config/node2.yml \
    -Dserver.port=8082 \
    raft-kv-server/target/raft-kv-server-1.0.0.jar
