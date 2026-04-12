#!/bin/bash
# Failover Demo Script
# This script demonstrates high availability failover in the Raft cluster

echo "=========================================="
echo "  Raft KV Store - Failover Demo"
echo "=========================================="
echo ""

# Function to get cluster stats from a node
get_stats() {
    local port=$1
    curl -s "http://localhost:$port/kv/stats" 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(f\"  Role: {data.get('role', 'N/A')}\")
    print(f\"  Endpoint: {data.get('endpoint', 'N/A')}\")
    print(f\"  Term: {data.get('currentTerm', 'N/A')}\")
    print(f\"  Leader: {data.get('leaderEndpoint', 'N/A')}\")
    print(f\"  Keys: {data.get('keyCount', 'N/A')}\")
except:
    print('  (node not available)')
" 2>/dev/null || echo "  (node not available)"
}

# Function to find current leader
find_leader() {
    for port in 8081 8082 8083; do
        response=$(curl -s "http://localhost:$port/kv/leader" 2>/dev/null)
        if echo "$response" | grep -q "I_AM_LEADER"; then
            echo "$port"
            return 0
        fi
    done
    echo "unknown"
}

# Check cluster status
echo "Step 0: Checking initial cluster status..."
echo ""
echo "  Node 1 (port 8081):"
get_stats 8081
echo ""
echo "  Node 2 (port 8082):"
get_stats 8082
echo ""
echo "  Node 3 (port 8083):"
get_stats 8083
echo ""

# Find current leader
LEADER_PORT=$(find_leader)
echo "Current leader: Node $LEADER_PORT (port 808$LEADER_PORT)"
echo ""

# Write some data
echo "=========================================="
echo "Step 1: Writing data to cluster..."
echo "=========================================="
echo ""

# Find leader and write data
for i in 1 2 3; do
    response=$(curl -s -X PUT "http://localhost:808$LEADER_PORT/kv/key$i" \
        -H "Content-Type: application/json" \
        -d "{\"value\": \"value$i\"}" 2>/dev/null)
    echo "PUT key$i=value$i: $(echo $response | python3 -c "import sys,json; d=json.load(sys.stdin); print('SUCCESS' if d.get('success') else 'FAILED: ' + str(d.get('error')))" 2>/dev/null || echo 'FAILED')"
done
echo ""

# Verify all nodes have the data
echo "=========================================="
echo "Step 2: Verifying data on all nodes..."
echo "=========================================="
echo ""

for port in 8081 8082 8083; do
    echo "  Node $port:"
    for i in 1 2 3; do
        response=$(curl -s "http://localhost:$port/kv/key$i" 2>/dev/null)
        value=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('value', 'N/A'))" 2>/dev/null || echo 'N/A')
        echo "    key$i = $value"
    done
done
echo ""

# Kill leader
echo "=========================================="
echo "Step 3: Simulating leader failure..."
echo "=========================================="
echo ""
echo "  Killing leader (Node $LEADER_PORT on port 808$LEADER_PORT)..."
echo "  Press Ctrl+C in the leader's terminal window to kill it."
echo ""
read -p "  Press Enter after you have killed the leader..."

# Wait for new election
echo ""
echo "Waiting for new leader election (10-15 seconds)..."
echo ""

sleep 5

# Check cluster status after kill
echo "Cluster status after leader failure:"
echo ""
for port in 8081 8082 8083; do
    if [ "$port" != "808$LEADER_PORT" ]; then
        echo "  Node $port:"
        get_stats $port
        echo ""
    fi
done

# Find new leader
sleep 5
NEW_LEADER=$(find_leader)
echo "New leader elected: Node $NEW_LEADER (port 808$NEW_LEADER)"
echo ""

# Verify data still accessible
echo "=========================================="
echo "Step 4: Verifying data after failover..."
echo "=========================================="
echo ""

for i in 1 2 3; do
    # Try on all surviving nodes
    for port in 8081 8082 8083; do
        if [ "$port" != "808$LEADER_PORT" ]; then
            response=$(curl -s "http://localhost:$port/kv/key$i" 2>/dev/null)
            value=$(echo "$response" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('value', 'N/A'))" 2>/dev/null || echo 'N/A')
            if [ "$value" != "N/A" ]; then
                echo "  key$i = $value (from Node $port)"
                break
            fi
        fi
    done
done
echo ""

# Try writing to new leader
echo "=========================================="
echo "Step 5: Writing new data to new leader..."
echo "=========================================="
echo ""

if [ "$NEW_LEADER" != "unknown" ]; then
    response=$(curl -s -X PUT "http://localhost:808$NEW_LEADER/kv/newkey" \
        -H "Content-Type: application/json" \
        -d "{\"value\": \"newvalue\"}" 2>/dev/null)
    echo "PUT newkey=newvalue to Node $NEW_LEADER: $(echo $response | python3 -c "import sys,json; d=json.load(sys.stdin); print('SUCCESS' if d.get('success') else 'FAILED: ' + str(d.get('error')))" 2>/dev/null || echo 'FAILED')"
fi
echo ""

echo "=========================================="
echo "  Demo Complete!"
echo "=========================================="
