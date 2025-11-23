#pragma once

#include <cstdint>

// Forward declarations for Arrow C Data Interface
struct ArrowSchema;
struct ArrowArray;

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations
struct PaimonCatalog;
struct PaimonTable;
struct PaimonScan;
struct PaimonError;

// Catalog functions
PaimonCatalog* paimon_catalog_new(const char* warehouse_path);
void paimon_catalog_free(PaimonCatalog* catalog);

// Table functions (without scanning)
PaimonTable* get_paimon_table(PaimonCatalog* catalog, const char* database, const char* table, PaimonError** error_out);
int32_t paimon_table_get_column_count(PaimonTable* table);
const char* paimon_table_get_column_name(PaimonTable* table, int32_t index);
const char* paimon_table_get_column_type(PaimonTable* table, int32_t index);
int32_t paimon_table_export_schema(PaimonTable* table, struct ArrowSchema* out_schema);
void paimon_table_free(PaimonTable* table);

// Table scan functions
// Uses the Table object stored in PaimonTable
PaimonScan* paimon_table_scan(PaimonTable* paimon_table, PaimonError** error_out);

// Arrow C Data Interface export functions
int32_t paimon_scan_export_batch(PaimonScan* scan, struct ArrowArray* out_array);

// Cleanup functions
void paimon_scan_free(PaimonScan* scan);
void paimon_error_free(PaimonError* error);
const char* paimon_error_get_message(const PaimonError* error);

#ifdef __cplusplus
}
#endif

