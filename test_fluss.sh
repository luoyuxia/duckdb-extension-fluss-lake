#!/bin/bash

# Test script for Fluss DuckDB Extension

EXTENSION_PATH="/Users/yuxia/Projects/cpp-projects/duckdb-extension-fluss-lake/build/fluss.duckdb_extension"
WAREHOUSE_PATH="/Users/yuxia/Desktop/paimon-warehouse"
DATABASE="default"
TABLE="T"

echo "=== Testing Fluss DuckDB Extension ==="
echo ""

echo "1. Loading extension..."
duckdb -unsigned -c "LOAD '${EXTENSION_PATH}'; SELECT 'Extension loaded successfully' as status;" 2>&1

echo ""
echo "2. Checking if fluss_read function is registered..."
duckdb -unsigned -c "LOAD '${EXTENSION_PATH}'; SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'fluss%';" 2>&1

echo ""
echo "3. Testing fluss_read function..."
echo "   Warehouse: ${WAREHOUSE_PATH}"
echo "   Database: ${DATABASE}"
echo "   Table: ${TABLE}"
duckdb -unsigned << EOF
LOAD '${EXTENSION_PATH}';
SELECT * FROM fluss_read('${WAREHOUSE_PATH}', '${DATABASE}', '${TABLE}') LIMIT 5;
EOF

echo ""
echo "=== Test completed ==="
