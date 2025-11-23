-- Test script for Paimon DuckDB Extension
-- Warehouse path: /Users/yuxia/Desktop/paimon-warehouse
-- Database: default
-- Table: T

-- Load the extension
LOAD 'build/paimon.duckdb_extension';

-- Test reading Paimon table
-- Syntax: paimon_read(warehouse_path, database, table)
SELECT * FROM paimon_read('/Users/yuxia/Desktop/paimon-warehouse', 'default', 'T') LIMIT 10;

