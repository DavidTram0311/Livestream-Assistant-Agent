#!/bin/bash
# ============================================
# ksqlDB Initialization Script
# ============================================
# This script initializes ksqlDB with the required streams and tables.
# Run this after ksqlDB server is up and running.
#
# Usage:
#   ./init_ksql.sh [KSQL_SERVER_URL]
#
# Example:
#   ./init_ksql.sh http://localhost:8088

KSQL_SERVER="${1:-http://localhost:8088}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================"
echo "Initializing ksqlDB at ${KSQL_SERVER}"
echo "============================================"

# Wait for ksqlDB to be ready
echo "Waiting for ksqlDB server to be ready..."
until curl -s "${KSQL_SERVER}/info" > /dev/null 2>&1; do
    echo "  ksqlDB not ready yet, waiting..."
    sleep 5
done
echo "ksqlDB server is ready!"

# Function to execute ksql statement
execute_ksql() {
    local statement="$1"
    local description="$2"
    
    echo ""
    echo "Executing: ${description}"
    echo "----------------------------------------"
    
    # Escape backslashes and quotes for JSON
    local escaped_statement=$(echo "${statement}" | sed 's/\\/\\\\/g' | sed 's/"/\\"/g')
    
    response=$(curl -s -X POST "${KSQL_SERVER}/ksql" \
        -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
        -d "{
            \"ksql\": \"${escaped_statement}\",
            \"streamsProperties\": {}
        }")
    
    echo "${response}" | python3 -m json.tool 2>/dev/null || echo "${response}"
}

# Execute SQL files in order
echo ""
echo "Creating enriched_events stream..."
STREAM_SQL=$(grep -v '^--' "${SCRIPT_DIR}/01_create_streams.sql" | tr '\n' ' ')
execute_ksql "${STREAM_SQL}" "Create enriched_events stream"

echo ""
echo "Creating combined_stats table..."
COMBINED_SQL=$(grep -v '^--' "${SCRIPT_DIR}/02_combined_agg.sql" | tr '\n' ' ')
execute_ksql "${COMBINED_SQL}" "Create combined_stats aggregation table"

echo ""
echo "============================================"
echo "ksqlDB initialization complete!"
echo "============================================"
echo ""
echo "You can verify the setup by running:"
echo "  curl ${KSQL_SERVER}/ksql -d '{\"ksql\": \"SHOW STREAMS;\"}' -H 'Content-Type: application/vnd.ksql.v1+json'"
echo "  curl ${KSQL_SERVER}/ksql -d '{\"ksql\": \"SHOW TABLES;\"}' -H 'Content-Type: application/vnd.ksql.v1+json'"
