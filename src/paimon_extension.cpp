#define DUCKDB_EXTENSION_MAIN

#include "paimon_extension.hpp"
#include "paimon_ffi.h"
#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include <memory>
#include <string>

namespace duckdb {

// Bind data for Paimon table function
struct PaimonBindData : public TableFunctionData {
	std::string warehouse_path;
	std::string database;
	std::string table;
	PaimonCatalog* catalog = nullptr;
	PaimonTable* table_schema = nullptr;  // Store table schema (no scan)

	~PaimonBindData() override {
		if (table_schema) {
			paimon_table_free(table_schema);
			table_schema = nullptr;
		}
		if (catalog) {
			paimon_catalog_free(catalog);
			catalog = nullptr;
		}
	}
};

// Global state for Paimon table function
struct PaimonGlobalState : public GlobalTableFunctionState {
	PaimonScan* scan = nullptr;
	int32_t current_batch = 0;
	int32_t total_batches = 0;
	ArrowSchema arrow_schema;
	duckdb_arrow_converted_schema converted_schema = nullptr;
	Connection* connection = nullptr;
	bool schema_exported = false;

	~PaimonGlobalState() override {
		if (converted_schema) {
			duckdb_destroy_arrow_converted_schema(&converted_schema);
		}
		if (schema_exported && arrow_schema.release) {
			arrow_schema.release(&arrow_schema);
		}
		if (connection) {
			delete connection;
			connection = nullptr;
		}
		if (scan) {
			paimon_scan_free(scan);
			scan = nullptr;
		}
	}
};

// Local state for Paimon table function
struct PaimonLocalState : public LocalTableFunctionState {
	int32_t current_row_in_batch = 0;
};

// Bind function for Paimon table function
static unique_ptr<FunctionData> PaimonBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<PaimonBindData>();

	// Get warehouse path (required)
	if (input.inputs.size() < 1) {
		throw InvalidInputException("paimon_read requires at least warehouse_path parameter");
	}
	result->warehouse_path = input.inputs[0].GetValue<string>();

	// Get database (optional, defaults to "default")
	if (input.inputs.size() >= 2) {
		result->database = input.inputs[1].GetValue<string>();
	} else {
		result->database = "default";
	}

	// Get table (required)
	if (input.inputs.size() < 3) {
		throw InvalidInputException("paimon_read requires table parameter");
	}
	result->table = input.inputs[2].GetValue<string>();

	// Create catalog
	result->catalog = paimon_catalog_new(result->warehouse_path.c_str());
	if (!result->catalog) {
		throw InvalidInputException("Failed to create Paimon catalog");
	}

	// Get table schema without scanning
	PaimonError* error = nullptr;
	result->table_schema = paimon_table_get_schema(result->catalog, result->database.c_str(), result->table.c_str(), &error);
	if (!result->table_schema) {
		std::string error_msg = "Failed to get Paimon table schema";
		if (error) {
			const char* msg = paimon_error_get_message(error);
			if (msg) {
				error_msg += ": ";
				error_msg += msg;
			}
			paimon_error_free(error);
		}
		throw InvalidInputException(error_msg);
	}

	// Get schema and populate return types and names
	int32_t column_count = paimon_table_get_column_count(result->table_schema);
	if (column_count <= 0) {
		throw InvalidInputException("Paimon table has no columns");
	}

	for (int32_t i = 0; i < column_count; i++) {
		const char* col_name = paimon_table_get_column_name(result->table_schema, i);
		const char* col_type = paimon_table_get_column_type(result->table_schema, i);
		
		if (!col_name) {
			throw InvalidInputException("Failed to get column name");
		}
		names.push_back(col_name);

		// Map Arrow types to DuckDB types
		// This is a simplified mapping - you may need to enhance this
		std::string type_str = col_type ? col_type : "VARCHAR";
		LogicalType duckdb_type;
		
		if (type_str.find("Int32") != std::string::npos || type_str.find("Int") != std::string::npos) {
			duckdb_type = LogicalType::INTEGER;
		} else if (type_str.find("Int64") != std::string::npos || type_str.find("Long") != std::string::npos) {
			duckdb_type = LogicalType::BIGINT;
		} else if (type_str.find("Float") != std::string::npos || type_str.find("Double") != std::string::npos) {
			duckdb_type = LogicalType::DOUBLE;
		} else if (type_str.find("Boolean") != std::string::npos || type_str.find("Bool") != std::string::npos) {
			duckdb_type = LogicalType::BOOLEAN;
		} else if (type_str.find("String") != std::string::npos || type_str.find("Utf8") != std::string::npos) {
			duckdb_type = LogicalType::VARCHAR;
		} else {
			// Default to VARCHAR for unknown types
			duckdb_type = LogicalType::VARCHAR;
		}
		
		return_types.push_back(duckdb_type);
	}

	return std::move(result);
}

// Initialize global state
static unique_ptr<GlobalTableFunctionState> PaimonInitGlobal(ClientContext &context,
                                                              TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PaimonBindData>();
	auto result = make_uniq<PaimonGlobalState>();

	// Now perform the actual scan
	PaimonError* paimon_error = nullptr;
	result->scan = paimon_table_scan(bind_data.catalog, bind_data.database.c_str(), bind_data.table.c_str(), &paimon_error);
	if (!result->scan) {
		std::string error_msg = "Failed to scan Paimon table";
		if (paimon_error) {
			const char* msg = paimon_error_get_message(paimon_error);
			if (msg) {
				error_msg += ": ";
				error_msg += msg;
			}
			paimon_error_free(paimon_error);
		}
		throw InvalidInputException(error_msg);
	}
	
	result->total_batches = paimon_scan_get_batch_count(result->scan);
	result->current_batch = 0;

	// Create a connection for Arrow conversion
	// Connection constructor needs DatabaseInstance&, not shared_ptr
	result->connection = new Connection(*context.db);
	auto conn_handle = reinterpret_cast<duckdb_connection>(result->connection);

	// Export Arrow schema
	result->arrow_schema.Init();
	if (paimon_scan_export_schema(result->scan, &result->arrow_schema) != 0) {
		throw InvalidInputException("Failed to export Arrow schema from Paimon scan");
	}
	result->schema_exported = true;

	// Convert Arrow schema to DuckDB schema
	duckdb_error_data error_data = duckdb_schema_from_arrow(conn_handle, &result->arrow_schema, &result->converted_schema);
	if (error_data) {
		const char* error_msg = duckdb_error_data_message(error_data);
		std::string msg = error_msg ? error_msg : "Failed to convert Arrow schema";
		duckdb_destroy_error_data(&error_data);
		throw InvalidInputException(msg);
	}

	return std::move(result);
}

// Initialize local state
static unique_ptr<LocalTableFunctionState> PaimonInitLocal(ExecutionContext &context,
                                                           TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state) {
	return make_uniq<PaimonLocalState>();
}

// Main table function
static void PaimonFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<PaimonGlobalState>();

	output.Reset();

	// Check if we have more batches
	if (global_state.current_batch >= global_state.total_batches) {
		return;
	}

	// Export Arrow array from current batch
	ArrowArray arrow_array;
	arrow_array.Init();
	if (paimon_scan_export_batch(global_state.scan, global_state.current_batch, &arrow_array) != 0) {
		throw InvalidInputException("Failed to export Arrow array from Paimon batch");
	}

	// Convert Arrow array to DuckDB DataChunk using C API
	auto conn_handle = reinterpret_cast<duckdb_connection>(global_state.connection);
	duckdb_data_chunk chunk = nullptr;
	auto error = duckdb_data_chunk_from_arrow(
		conn_handle,
		&arrow_array,
		global_state.converted_schema,
		&chunk);
	
	if (error) {
		const char* error_msg = duckdb_error_data_message(error);
		std::string msg = error_msg ? error_msg : "Failed to convert Arrow array to DataChunk";
		duckdb_destroy_error_data(&error);
		if (arrow_array.release) {
			arrow_array.release(&arrow_array);
		}
		throw InvalidInputException(msg);
	}

	// Copy data from converted chunk to output
	if (chunk) {
		auto* converted_chunk = reinterpret_cast<DataChunk*>(chunk);
		output.Move(*converted_chunk);
		duckdb_destroy_data_chunk(&chunk);
	}

	// Advance to next batch
	global_state.current_batch++;
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register table function: paimon_read(warehouse_path, database, table)
	TableFunction paimon_read("paimon_read",
	                          {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                          PaimonFunction, PaimonBind, PaimonInitGlobal, PaimonInitLocal);
	loader.RegisterFunction(paimon_read);
}

void PaimonExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string PaimonExtension::Name() {
	return "paimon";
}

std::string PaimonExtension::Version() const {
#ifdef EXT_VERSION_PAIMON
	return EXT_VERSION_PAIMON;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(paimon, loader) {
	duckdb::LoadInternal(loader);
}

}

