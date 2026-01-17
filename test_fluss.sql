-- Test script for Fluss DuckDB Extension
-- Warehouse path: /Users/yuxia/Desktop/paimon-warehouse
-- Database: default
-- Table: T

-- Load the extension
LOAD 'build/fluss.duckdb_extension';

-- Test reading Paimon table
-- Syntax: fluss_read(warehouse_path, database, table)
SELECT * FROM fluss_read('/Users/yuxia/Desktop/paimon-warehouse', 'default', 'T') LIMIT 10;
