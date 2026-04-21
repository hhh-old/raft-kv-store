#!/bin/bash
# Start Node 1 (Leader/Initializer)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Starting Raft KV Store Node 1..."
echo "Project dir: $PROJECT_DIR"

cd "$PROJECT_DIR"

# Use node1 configuration
java -jar \
    -Dspring.config.location=file:$PROJECT_DIR/config/node1.yml \
    -Dserver.port=8081 \
    raft-kv-server/target/raft-kv-server-1.0.0.jar
