#define DUCKDB_EXTENSION_MAIN

#include "fluss_extension.hpp"
#include "fluss_ffi.h"
#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/config.hpp"
#include <memory>
#include <string>

namespace duckdb {

// Bind data for Fluss table function
struct FlussBindData : public TableFunctionData {
	std::string warehouse_path;
	std::string database;
	std::string table;
	PaimonCatalog* catalog = nullptr;
	PaimonTable* paimon_table = nullptr;  // Store Paimon table
	ArrowSchema arrow_schema;  // Arrow schema exported from table

	FlussBindData() {
		arrow_schema.Init();
	}

	~FlussBindData() override {
		if (arrow_schema.release) {
			arrow_schema.release(&arrow_schema);
		}
		if (paimon_table) {
			paimon_table_free(paimon_table);
			paimon_table = nullptr;
		}
		if (catalog) {
			paimon_catalog_free(catalog);
			catalog = nullptr;
		}
	}
};

// Global state for Fluss table function
struct FlussGlobalState : public GlobalTableFunctionState {
	PaimonScan* scan = nullptr;
	duckdb_arrow_converted_schema converted_schema = nullptr;
	Connection* connection = nullptr;

	~FlussGlobalState() override {
		if (converted_schema) {
			duckdb_destroy_arrow_converted_schema(&converted_schema);
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

// Local state for Fluss table function (currently unused, but required by DuckDB API)
struct FlussLocalState : public LocalTableFunctionState {
};

// Bind function for Fluss table function
static unique_ptr<FunctionData> FlussBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FlussBindData>();

	// Get warehouse path (required)
	if (input.inputs.empty()) {
		throw InvalidInputException("fluss_read requires at least warehouse_path parameter");
	}
	result->warehouse_path = input.inputs[0].GetValue<string>();

	// Parse parameters:
	// - 2 parameters: warehouse_path, table (database defaults to "default")
	// - 3 parameters: warehouse_path, database, table
	if (input.inputs.size() < 2) {
		throw InvalidInputException("fluss_read requires at least warehouse_path and table parameters");
	}
	
	if (input.inputs.size() == 2) {
		// 2 parameters: warehouse_path, table
		result->database = "default";
		result->table = input.inputs[1].GetValue<string>();
	} else if (input.inputs.size() >= 3) {
		// 3+ parameters: warehouse_path, database, table
		result->database = input.inputs[1].GetValue<string>();
		result->table = input.inputs[2].GetValue<string>();
	}

	// Create Paimon catalog
	result->catalog = paimon_catalog_new(result->warehouse_path.c_str());
	if (!result->catalog) {
		throw InvalidInputException("Failed to create Paimon catalog");
	}

	// Get Paimon table handle without scanning
	PaimonError* error = nullptr;
	result->paimon_table = get_paimon_table(result->catalog, result->database.c_str(), result->table.c_str(), &error);
	if (!result->paimon_table) {
		std::string error_msg = "Failed to get Paimon table schema";
		if (error) {
			if (const char *msg = paimon_error_get_message(error)) {
				error_msg += ": ";
				error_msg += msg;
			}
			paimon_error_free(error);
		}
		throw InvalidInputException(error_msg);
	}

	// Export Arrow schema from Paimon table and save it in bind_data
	if (paimon_table_export_schema(result->paimon_table, &result->arrow_schema) != 0) {
		throw InvalidInputException("Failed to export Arrow schema from Paimon table");
	}

	// Use DuckDB's Arrow conversion utilities to convert Arrow schema to DuckDB types
	ArrowTableSchema arrow_table_schema;
	auto &config = DBConfig::GetConfig(context);
	try {
		ArrowTableFunction::PopulateArrowTableSchema(config, arrow_table_schema, result->arrow_schema);
	} catch (const std::exception &ex) {
		throw InvalidInputException(std::string("Failed to convert Arrow schema: ") + ex.what());
	}

	// Get types and names from ArrowTableSchema
	return_types = arrow_table_schema.GetTypes();
	names = arrow_table_schema.GetNames();

	return std::move(result);
}

// Initialize global state
static unique_ptr<GlobalTableFunctionState> FlussInitGlobal(ClientContext &context,
                                                              TableFunctionInitInput &input) {
	auto &bind_data = const_cast<FlussBindData&>(input.bind_data->Cast<FlussBindData>());
	auto result = make_uniq<FlussGlobalState>();

	// Now perform the actual scan using PaimonTable
	PaimonError* paimon_error = nullptr;
	result->scan = paimon_table_scan(bind_data.paimon_table, &paimon_error);
	if (!result->scan) {
		std::string error_msg = "Failed to scan Paimon table";
		if (paimon_error) {
			if (const char *msg = paimon_error_get_message(paimon_error)) {
				error_msg += ": ";
				error_msg += msg;
			}
			paimon_error_free(paimon_error);
		}
		throw InvalidInputException(error_msg);
	}

	// Create a connection for Arrow conversion
	// Connection constructor needs DatabaseInstance&, not shared_ptr
	result->connection = new Connection(*context.db);
	auto conn_handle = reinterpret_cast<duckdb_connection>(result->connection);

	if (!bind_data.arrow_schema.release) {
		throw InvalidInputException("Arrow schema not available in bind_data");
	}

	// Convert Arrow schema to DuckDB schema using the schema from bind_data
	duckdb_error_data error_data = duckdb_schema_from_arrow(conn_handle, &bind_data.arrow_schema, &result->converted_schema);
	if (error_data) {
		const char* error_msg = duckdb_error_data_message(error_data);
		const std::string msg = error_msg ? error_msg : "Failed to convert Arrow schema";
		duckdb_destroy_error_data(&error_data);
		throw InvalidInputException(msg);
	}

	return std::move(result);
}

// Initialize local state
static unique_ptr<LocalTableFunctionState> FlussInitLocal(ExecutionContext &context,
                                                           TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state) {
	return make_uniq<FlussLocalState>();
}

// Main table function
static void FlussFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<FlussGlobalState>();

	output.Reset();

	// Export Arrow array from next batch
	ArrowArray arrow_array;
	arrow_array.Init();
	int result = paimon_scan_export_batch(global_state.scan, &arrow_array);
	if (result != 0) {
		// No more batches available or error occurred
		return;
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
		const std::string msg = error_msg ? error_msg : "Failed to convert Arrow array to DataChunk";
		duckdb_destroy_error_data(&error);
		if (arrow_array.release) {
			arrow_array.release(&arrow_array);
		}
		throw InvalidInputException(msg);
	}

	// Copy data from converted chunk to output
	// Note: arrow_array ownership was transferred to DuckDB's DataChunk by duckdb_data_chunk_from_arrow,
	// so we don't need to release it manually. The DataChunk will handle cleanup.
	if (chunk) {
		auto* converted_chunk = reinterpret_cast<DataChunk*>(chunk);
		output.Move(*converted_chunk);
		duckdb_destroy_data_chunk(&chunk);
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register table function: fluss_read(warehouse_path, table) or fluss_read(warehouse_path, database, table)
	TableFunctionSet fluss_read_set("fluss_read");
	
	// 2 parameters: warehouse_path, table (database defaults to "default")
	TableFunction fluss_read_2("fluss_read",
	                             {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                             FlussFunction, FlussBind, FlussInitGlobal, FlussInitLocal);
	fluss_read_set.AddFunction(fluss_read_2);
	
	// 3 parameters: warehouse_path, database, table
	TableFunction fluss_read_3("fluss_read",
	                             {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                             FlussFunction, FlussBind, FlussInitGlobal, FlussInitLocal);
	fluss_read_set.AddFunction(fluss_read_3);
	
	loader.RegisterFunction(fluss_read_set);
}

void FlussExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string FlussExtension::Name() {
	return "fluss";
}

std::string FlussExtension::Version() const {
#ifdef EXT_VERSION_FLUSS
	return EXT_VERSION_FLUSS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(fluss, loader) {
	duckdb::LoadInternal(loader);
}

}
