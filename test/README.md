# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:

## On macOS/Linux (non-Docker)

By default, tests are skipped outside Docker. To run tests, you need to:

1. **Build with unittest enabled:**
```bash
EXT_FLAGS="-DENABLE_UNITTEST_CPP_TESTS=TRUE" make release
```

2. **Run tests with SKIP_TESTS override:**
```bash
SKIP_TESTS=0 make test
```

Or run the unittest directly:
```bash
./build/release/test/unittest "test/*"
```

## Standard commands (may be skipped outside Docker)
```bash
make test
```
or 
```bash
make test_debug
```