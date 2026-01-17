# Quick Start Guide - Apache Paimon

This guide will walk you through setting up a complete Fluss + Paimon + DuckDB environment to query data with second-level freshness using Apache Paimon as the lakehouse storage format.

> **Note**: This is the Quick Start guide for Apache Paimon. A Quick Start guide for Apache Iceberg will be available in the future.

## Overview

This quick start guide covers:
1. Setting up a Fluss cluster
2. Configuring Lakehouse Storage (Paimon)
3. Starting the Datalake Tiering Service
4. Querying data through DuckDB with the Fluss extension

## Prerequisites

Before starting, ensure you have:

- **Java 11+** installed
- **Apache Flink 1.20+** installed and configured
- **DuckDB 1.4.2+** installed
- **DuckDB Fluss Extension** built or downloaded (see [README.md](../README.md) for build instructions)

## Step 1: Set Up Fluss Cluster

### 1.1 Download Fluss

Download the latest Fluss release from the [Apache Fluss website](https://fluss.apache.org/downloads.html) or build from source.

```bash
# Example: Download and extract Fluss 0.8
wget https://dlcdn.apache.org/incubator/fluss/fluss-0.8.0-incubating/fluss-0.8.0-incubating-bin.tgz
tar -xzf fluss-0.8.0-incubating-bin.tgz
cd fluss-0.8.0-incubating
export FLUSS_HOME=$(pwd)
```

### 1.2 Configure Fluss Server

Edit `${FLUSS_HOME}/conf/server.yaml` to configure the Fluss server:

```yaml
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /tmp/paimon
```

### 1.3 Start Fluss Cluster

Start the Fluss server:

```bash
./bin/local-cluster.sh start
```

Verify the server is running:

```bash
# Check if Fluss is listening on the configured port
netstat -an | grep 9123
```

The Fluss bootstrap server address will be `localhost:9123`

## Step 2: Set Up Flink Cluster

The Datalake Tiering Service requires a running Flink cluster to tier data from Fluss to Paimon storage.

### 2.1 Download and Install Flink

Download Apache Flink 1.20 or later from the [Apache Flink website](https://flink.apache.org/downloads.html):

```bash
# Example: Download and extract Flink 1.20
wget https://dlcdn.apache.org/flink/flink-1.20.3/flink-1.20.3-bin-scala_2.12.tgz
tar -xzf flink-1.20.3-bin-scala_2.12.tgz
cd flink-1.20.3
export FLINK_HOME=$(pwd)
```

### 2.2 Configure Flink (Optional)

For basic local setup, Flink's default configuration should work. You can optionally configure Flink in `${FLINK_HOME}/conf/flink-conf.yaml`:

```yaml
# Configure task manager slots (default is 1)
taskmanager.numberOfTaskSlots: 5
```

### 2.3 Prepare JARs Required

Before starting the Flink cluster, you need to download and prepare the required JAR files for the tiering service.

#### Download and Copy Required JARs

Download and copy all required JARs directly to Flink's `lib` directory:

```bash
# Download Fluss Flink connector (match your Flink version)
wget -P ${FLINK_HOME}/lib/ \
  https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/0.8.0-incubating/fluss-flink-1.20-0.8.0-incubating.jar

# Download Fluss Paimon integration
wget -P ${FLINK_HOME}/lib/ \
  https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-paimon/0.8.0-incubating/fluss-lake-paimon-0.8.0-incubating.jar

# Download Flink Hadoop JAR
wget -P ${FLINK_HOME}/lib/ \
  https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# Download Fluss Flink tiering service JAR (for submitting tiering job)
wget -P ${FLINK_HOME}/opt/ \
  https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/0.8.0-incubating/fluss-flink-tiering-0.8.0-incubating.jar
```

### 2.4 Start Flink Cluster

Start the Flink cluster:

```bash
${FLINK_HOME}/bin/start-cluster.sh
```

### 2.5 Verify Flink Cluster

Verify that Flink is running:

```bash
# Check Flink processes
jps | grep -E "StandaloneSessionClusterEntrypoint|TaskManagerRunner"

# Check Flink web UI (usually at http://localhost:8081)
curl http://localhost:8081/overview

# Or open in browser
# http://localhost:8081
```

You should see the Flink web UI showing the cluster is running with at least one TaskManager.

## Step 3: Start Datalake Tiering Service

The tiering service continuously moves data from Fluss tables to Paimon lake storage.

### 3.1 Start Tiering Service

Start the datalake tiering service using Flink (the tiering JAR should already be downloaded in Step 2.3):

```bash
${FLINK_HOME}/bin/flink run ${FLINK_HOME}/opt/fluss-flink-tiering-0.8.0-incubating.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon
```

### 3.2 Verify Tiering Service

Check that the tiering service is running:

```bash
# List Flink jobs
${FLINK_HOME}/bin/flink list

# Check Flink web UI (usually at http://localhost:8081)
```

## Step 4: Create a Fluss Table with Lakehouse Storage

### 4.1 Start Flink SQL Client

Start the Flink SQL Client to create tables:

```bash
${FLINK_HOME}/bin/sql-client.sh embedded
```

### 4.2 Create Flink Catalog

First, create a Fluss catalog in Flink SQL Client:

```sql
-- Create Fluss catalog
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

-- Use the Fluss catalog
USE CATALOG fluss_catalog;

USE fluss;
```

### 4.3 Create Table with Lakehouse Enabled

Create a Fluss table with lakehouse storage enabled:

```sql
-- Create a Fluss table with lakehouse storage enabled
CREATE TABLE logTable (
    a INT,
    b INT
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

**Table Options:**
- `table.datalake.enabled=true`: Enables lakehouse storage for this table
- `table.datalake.freshness=3min`: Sets target data freshness (default is 3 minutes)

### 4.4 Create Data Generation Job

Create a Flink job that generates data using DataGen connector and writes to the Fluss table:

```sql
-- Create a DataGen source table
CREATE TEMPORARY TABLE datagen_source (
    a INT,
    b INT
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10'
);

-- Insert data from DataGen to Fluss table
INSERT INTO logTable SELECT * FROM datagen_source;
```

**Note**: The INSERT statement will start a Flink job that continuously generates data and writes it to the Fluss table. The tiering service will automatically move this data to Paimon storage.
You can use `tree /tmp/paimon` to see the paimon warehouse directory structure.

## Step 5: Query Data with DuckDB

### 5.1 Download and Prepare DuckDB Fluss Extension

Download the pre-built Fluss extension:

```bash
# Download the extension
wget https://github.com/luoyuxia/duckdb-extension-fluss-lake/releases/download/0.1.1-beta/fluss.duckdb_extension.gz

# Extract the extension
gunzip fluss.duckdb_extension.gz

# Verify the extension file exists
ls -lh fluss.duckdb_extension
```

### 5.2 Load DuckDB Fluss Extension

Start DuckDB and load the Fluss extension:

```bash
# Start DuckDB with unsigned extensions allowed
duckdb -unsigned
```

In DuckDB, load the extension:

```sql
-- Load the Fluss extension (use the path where you downloaded/extracted it)
LOAD '/path/to/fluss.duckdb_extension';

-- Verify extension is loaded
SELECT extension_name FROM duckdb_extensions() WHERE extension_name = 'fluss';
```

**Note**: Replace `/path/to/fluss.duckdb_extension` with the actual path where you downloaded and extracted the extension file.

### 5.3 Query Fluss Table

Query the Fluss table using the `fluss_read` table function:

```sql
-- Basic query: fluss_read(bootstrap_server, database, table)
SELECT * FROM fluss_read('127.0.0.1:9123', 'fluss', 'logTable');

-- Query with LIMIT
SELECT * FROM fluss_read('127.0.0.1:9123', 'fluss', 'logTable') LIMIT 10;

-- Query with WHERE clause
SELECT * FROM fluss_read('127.0.0.1:9123', 'fluss', 'logTable') WHERE a > 100;

-- Aggregate queries
SELECT 
    a,
    COUNT(*) as count,
    SUM(b) as sum_b
FROM fluss_read('127.0.0.1:9123', 'fluss', 'logTable')
GROUP BY a
ORDER BY count DESC;

-- Using CTE (Common Table Expression)
WITH fluss_data AS (
    SELECT * FROM fluss_read('127.0.0.1:9123', 'fluss', 'logTable')
)
SELECT COUNT(*) as total_rows FROM fluss_data;
```

**Function Syntax:**
- `fluss_read(bootstrap_server, database, table)` - Full syntax
- `fluss_read(bootstrap_server, table)` - Database defaults to "fluss"

**Parameters:**
- `bootstrap_server`: Fluss bootstrap server address (e.g., `'127.0.0.1:9123'`)
- `database`: Database name (defaults to `'fluss'` if omitted)
- `table`: Table name (must be a Paimon append-only table with filesystem catalog)

### 5.4 Verify Data Freshness

The extension automatically combines:
1. **Historical data** from Paimon lake storage (up to the last snapshot)
2. **Real-time data** from Fluss streaming (from snapshot offset to latest)

This provides **second-level data freshness** instead of the traditional minute-level freshness.

## Troubleshooting

### Fluss Server Issues

**Problem**: Fluss server fails to start
- **Solution**: Check `${FLUSS_HOME}/logs/` for error messages
- Verify Java version (requires Java 11+)
- Check port availability

**Problem**: Cannot connect to Fluss
- **Solution**: Verify bootstrap server address and port
- Check firewall settings
- Ensure Fluss server is running

### Tiering Service Issues

**Problem**: Tiering service fails to start
- **Solution**: 
  - Verify Flink cluster is running
  - Check all required JARs are in `${FLINK_HOME}/lib/`
  - Verify Paimon configuration matches server.yaml

**Problem**: Data not appearing in Paimon
- **Solution**:
  - Check tiering service logs in Flink UI
  - Verify table has `table.datalake.enabled=true`
  - Check Paimon warehouse path is accessible

### DuckDB Extension Issues

**Problem**: Extension fails to load
- **Solution**: 
  - Ensure extension was built for correct DuckDB version
  - Use `duckdb -unsigned` if extension is unsigned
  - Check extension file path

**Problem**: `Table not found` error
- **Solution**:
  - Verify table exists in Fluss
  - Check database and table names are correct
  - Ensure table uses Paimon filesystem catalog
  - Verify table is an append-only table

**Problem**: `Unsupported table type` error
- **Solution**: 
  - Currently only Paimon append-only tables (log tables) are supported
  - Ensure table is created as a log table, not a primary key table

## Next Steps

- Explore more complex queries with joins and aggregations
- Set up multiple tables and databases
- Configure data freshness per table using `table.datalake.freshness`
- Monitor tiering service performance through Flink UI
- Scale tiering service by running multiple instances

## Additional Resources

- [Fluss Documentation](https://fluss.apache.org/docs/)
- [Paimon Documentation](https://paimon.apache.org/docs/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Extension README](../README.md) - For building and advanced usage

## Summary

You've successfully:
1. ✅ Set up a Fluss cluster
2. ✅ Configured Paimon as lakehouse storage
3. ✅ Started the datalake tiering service
4. ✅ Created a table with lakehouse storage enabled
5. ✅ Queried data through DuckDB with second-level freshness

The extension automatically combines historical data from Paimon with real-time data from Fluss, providing you with a unified view of your data with minimal latency.
