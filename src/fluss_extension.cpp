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
#include <vector>
#include <map>
#include <set>

namespace duckdb {

// Bind data for Fluss table function
struct FlussBindData : public TableFunctionData {
	std::string bootstrap_server;  // Fluss bootstrap server
	std::string database;
	std::string table;

	// Fluss handles
	FlussConnectionHandle* fluss_conn = nullptr;
	FlussAdminHandle* fluss_admin = nullptr;
	FlussTableInfoHandle* fluss_table_info = nullptr;
	LakeSnapshotHandle* lake_snapshot = nullptr;
	FlussOffsetMap* latest_offsets = nullptr;

	// Paimon handles (created from Fluss table properties)
	PaimonCatalog* paimon_catalog = nullptr;
	PaimonTable* paimon_table = nullptr;

	// Schema
	ArrowSchema arrow_schema;  // Arrow schema exported from table
	int64_t table_id = -1;  // Fluss table ID

	// Bucket information
	std::vector<int32_t> bucket_ids;  // All bucket IDs
	std::map<int32_t, int64_t> snapshot_offsets;  // Snapshot offsets per bucket
	std::map<int32_t, int64_t> end_offsets;  // End offsets per bucket (from latest offsets)

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
		if (paimon_catalog) {
			paimon_catalog_free(paimon_catalog);
			paimon_catalog = nullptr;
		}
		if (latest_offsets) {
			fluss_offset_map_free(latest_offsets);
			latest_offsets = nullptr;
		}
		if (lake_snapshot) {
			fluss_lake_snapshot_free(lake_snapshot);
			lake_snapshot = nullptr;
		}
		if (fluss_table_info) {
			fluss_table_info_free(fluss_table_info);
			fluss_table_info = nullptr;
		}
		if (fluss_admin) {
			fluss_admin_free(fluss_admin);
			fluss_admin = nullptr;
		}
		if (fluss_conn) {
			fluss_connection_free(fluss_conn);
			fluss_conn = nullptr;
		}
	}
};

// Reading phase enum
enum class ReadingPhase {
	PAIMON,  // Reading from Paimon using snapshot
	FLUSS    // Reading from Fluss from snapshot offset to end offset
};

// Global state for Fluss table function
struct FlussGlobalState : public GlobalTableFunctionState {
	ReadingPhase phase = ReadingPhase::PAIMON;

	// Paimon reading state
	PaimonScan* paimon_scan = nullptr;

	// Fluss reading state
	FlussTableHandle* fluss_table = nullptr;
	FlussScannerHandle* fluss_scanner = nullptr;
	FlussScanRecordBatchesHandle* current_batches = nullptr;  // Current batches from poll
	std::map<int32_t, int64_t> fluss_rows_read_per_bucket;  // Track rows read per bucket
	std::set<int32_t> active_buckets;  // Buckets that are still being read

	// Common state
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
		if (current_batches) {
			fluss_scan_record_batches_free(current_batches);
			current_batches = nullptr;
		}
		if (paimon_scan) {
			paimon_scan_free(paimon_scan);
			paimon_scan = nullptr;
		}
		if (fluss_scanner) {
			fluss_scanner_free(fluss_scanner);
			fluss_scanner = nullptr;
		}
		if (fluss_table) {
			fluss_table_free(fluss_table);
			fluss_table = nullptr;
		}
	}
};

// Local state for Fluss table function (currently unused, but required by DuckDB API)
struct FlussLocalState : public LocalTableFunctionState {
};

// Helper function to extract paimon catalog properties from Fluss table properties
static std::map<std::string, std::string> ExtractPaimonCatalogProperties(FlussPropertyMap* property_map) {
	std::map<std::string, std::string> paimon_props;
	if (!property_map) {
		return paimon_props;
	}

	size_t count = fluss_property_map_get_count(property_map);
	for (size_t i = 0; i < count; i++) {
		const char* key = fluss_property_map_get_key(property_map, i);
		const char* value = fluss_property_map_get_value(property_map, i);
		if (key && value) {
			paimon_props[std::string(key)] = std::string(value);
		}
	}
	return paimon_props;
}

// Bind function for Fluss table function
// Parameters: fluss_read(bootstrap_server, database, table)
// or: fluss_read(bootstrap_server, table)  // database defaults to "default"
static unique_ptr<FunctionData> FlussBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FlussBindData>();

	// Get bootstrap server (required)
	if (input.inputs.empty()) {
		throw InvalidInputException("fluss_read requires at least bootstrap_server parameter");
	}
	result->bootstrap_server = input.inputs[0].GetValue<string>();

	// Parse parameters:
	// - 2 parameters: bootstrap_server, table (database defaults to "default")
	// - 3 parameters: bootstrap_server, database, table
	if (input.inputs.size() < 2) {
		throw InvalidInputException("fluss_read requires at least bootstrap_server and table parameters");
	}

	if (input.inputs.size() == 2) {
		// 2 parameters: bootstrap_server, table
		result->database = "fluss";
		result->table = input.inputs[1].GetValue<string>();
	} else if (input.inputs.size() >= 3) {
		// 3+ parameters: bootstrap_server, database, table
		result->database = input.inputs[1].GetValue<string>();
		result->table = input.inputs[2].GetValue<string>();
	}

	// Step 1: Connect to Fluss and get table info
	result->fluss_conn = fluss_connection_new(result->bootstrap_server.c_str());
	if (!result->fluss_conn) {
		throw InvalidInputException("Failed to create Fluss connection");
	}

	FlussError* fluss_error = nullptr;
	result->fluss_admin = fluss_connection_get_admin(result->fluss_conn, &fluss_error);
	if (!result->fluss_admin) {
		std::string error_msg = "Failed to get Fluss admin";
		if (fluss_error) {
			if (const char *msg = fluss_error_get_message(fluss_error)) {
				error_msg += ": ";
				error_msg += msg;
			}
			fluss_error_free(fluss_error);
		}
		throw InvalidInputException(error_msg);
	}

	// Get table info
	result->fluss_table_info = fluss_admin_get_table_info(
		result->fluss_admin,
		result->database.c_str(),
		result->table.c_str(),
		&fluss_error
	);
	if (!result->fluss_table_info) {
		std::string error_msg = "Failed to get Fluss table info";
		if (fluss_error) {
			if (const char *msg = fluss_error_get_message(fluss_error)) {
				error_msg += ": ";
				error_msg += msg;
			}
			fluss_error_free(fluss_error);
		}
		throw InvalidInputException(error_msg);
	}

	// Get table ID
	result->table_id = fluss_table_info_get_table_id(result->fluss_table_info);

	// Step 2: Extract paimon catalog properties from table properties
	// Look for properties with prefix "table.datalake.paimon."
	FlussPropertyMap* paimon_props_map = fluss_table_info_get_properties_with_prefix(
		result->fluss_table_info,
		"table.datalake.paimon.",
		&fluss_error
	);
	if (fluss_error) {
		const char *msg = fluss_error_get_message(fluss_error);
		std::string error_msg = msg ? std::string("Failed to get paimon properties: ") + msg : "Failed to get paimon properties";
		fluss_error_free(fluss_error);
		throw InvalidInputException(error_msg);
	}

	std::map<std::string, std::string> paimon_props = ExtractPaimonCatalogProperties(paimon_props_map);
	if (paimon_props_map) {
		fluss_property_map_free(paimon_props_map);
	}

	// Extract warehouse path from paimon properties
	std::string warehouse_path;
	if (paimon_props.find("warehouse") != paimon_props.end()) {
		warehouse_path = paimon_props["warehouse"];
	} else {
		throw InvalidInputException("Paimon warehouse path not found in table properties (table.datalake.paimon.warehouse)");
	}

	// Step 3: Create Paimon catalog
	result->paimon_catalog = paimon_catalog_new(warehouse_path.c_str());
	if (!result->paimon_catalog) {
		throw InvalidInputException("Failed to create Paimon catalog");
	}

	// Step 4: Get Paimon table handle
	PaimonError* paimon_error = nullptr;
	result->paimon_table = get_paimon_table(
		result->paimon_catalog,
		result->database.c_str(),
		result->table.c_str(),
		&paimon_error
	);
	if (!result->paimon_table) {
		std::string error_msg = "Failed to get Paimon table schema";
		if (paimon_error) {
			if (const char *msg = paimon_error_get_message(paimon_error)) {
				error_msg += ": ";
				error_msg += msg;
			}
			paimon_error_free(paimon_error);
		}
		throw InvalidInputException(error_msg);
	}

	// Step 5: Get latest lake snapshot
	result->lake_snapshot = fluss_admin_get_latest_lake_snapshot(
		result->fluss_admin,
		result->database.c_str(),
		result->table.c_str(),
		&fluss_error
	);
	if (!result->lake_snapshot) {
		std::string error_msg = "Failed to get latest lake snapshot";
		if (fluss_error) {
			if (const char *msg = fluss_error_get_message(fluss_error)) {
				error_msg += ": ";
				error_msg += msg;
			}
			fluss_error_free(fluss_error);
		}
		throw InvalidInputException(error_msg);
	}

	// Step 6: Get latest offsets for all buckets
	// Get number of buckets from table info
	int32_t num_buckets = fluss_table_info_get_num_buckets(result->fluss_table_info);
	if (num_buckets <= 0) {
		throw InvalidInputException("Invalid number of buckets from table info");
	}

	// Query offsets for all buckets (0 to num_buckets-1)
	std::vector<int32_t> all_bucket_ids;
	all_bucket_ids.reserve(num_buckets);
	for (int32_t i = 0; i < num_buckets; i++) {
		all_bucket_ids.push_back(i);
	}

	result->latest_offsets = fluss_admin_list_offsets(
		result->fluss_admin,
		result->database.c_str(),
		result->table.c_str(),
		all_bucket_ids.data(),
		static_cast<int32_t>(all_bucket_ids.size()),
		1,  // Latest offset
		0,  // Not used for Latest
		&fluss_error
	);
	if (!result->latest_offsets) {
		std::string error_msg = "Failed to list latest offsets";
		if (fluss_error) {
			if (const char *msg = fluss_error_get_message(fluss_error)) {
				error_msg += ": ";
				error_msg += msg;
			}
			fluss_error_free(fluss_error);
		}
		throw InvalidInputException(error_msg);
	}

	// Extract bucket IDs and offsets
	size_t bucket_count = fluss_offset_map_get_count(result->latest_offsets);
	result->bucket_ids.resize(bucket_count);

	// Store snapshot offsets and end offsets for each bucket
	// Filter out buckets that don't need to read Fluss data (snapshot_offset >= end_offset)
	std::vector<int32_t> buckets_to_read;
	for (int32_t bucket_id : result->bucket_ids) {
		int64_t snapshot_offset = fluss_lake_snapshot_get_bucket_offset(
			result->lake_snapshot,
			result->table_id,
			bucket_id
		);
		int64_t end_offset = fluss_offset_map_get_offset(result->latest_offsets, bucket_id);

		if (snapshot_offset >= 0) {
			result->snapshot_offsets[bucket_id] = snapshot_offset;
		}
		if (end_offset >= 0) {
			result->end_offsets[bucket_id] = end_offset;
		}

		// Only include buckets that need to read Fluss data
		// If snapshot_offset >= end_offset, this bucket doesn't need to read Fluss data
		if (snapshot_offset >= 0 && end_offset >= 0 && snapshot_offset < end_offset) {
			buckets_to_read.push_back(bucket_id);
		}
	}

	// Update bucket_ids to only include buckets that need to read Fluss data
	result->bucket_ids = std::move(buckets_to_read);

	// Step 7: Export Arrow schema from Fluss table info
	if (fluss_table_info_export_schema(result->fluss_table_info, &result->arrow_schema, &fluss_error) != 0) {
		std::string error_msg = "Failed to export Arrow schema from Fluss table";
		if (fluss_error) {
			if (const char *msg = fluss_error_get_message(fluss_error)) {
				error_msg += ": ";
				error_msg += msg;
			}
			fluss_error_free(fluss_error);
		}
		throw InvalidInputException(error_msg);
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

	// Start with Paimon reading phase
	result->phase = ReadingPhase::PAIMON;

	// Initialize Paimon scan for reading snapshot data
	PaimonError* paimon_error = nullptr;
	result->paimon_scan = paimon_table_scan(bind_data.paimon_table, &paimon_error);
	if (!result->paimon_scan) {
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

	// Initialize Fluss reading state (will be used in Fluss phase)
	// Get Fluss table handle for creating scanners
	FlussError* fluss_error = nullptr;
	result->fluss_table = fluss_connection_get_table(
		bind_data.fluss_conn,
		bind_data.database.c_str(),
		bind_data.table.c_str(),
		&fluss_error
	);
	if (!result->fluss_table) {
		std::string error_msg = "Failed to get Fluss table";
		if (fluss_error) {
			if (const char *msg = fluss_error_get_message(fluss_error)) {
				error_msg += ": ";
				error_msg += msg;
			}
			fluss_error_free(fluss_error);
		}
		throw InvalidInputException(error_msg);
	}

	// Initialize rows read tracking and active buckets
	// Only include buckets that need to read Fluss data (already filtered in FlussBind)
	for (int32_t bucket_id : bind_data.bucket_ids) {
		result->fluss_rows_read_per_bucket[bucket_id] = 0;
		result->active_buckets.insert(bucket_id);
	}

	// Create scanner and subscribe to all buckets that need to read Fluss data
	fluss_error = nullptr;
	result->fluss_scanner = fluss_table_create_record_batch_log_scanner(
		result->fluss_table,
		&fluss_error
	);
	if (!result->fluss_scanner) {
		std::string error_msg = "Failed to create Fluss scanner";
		if (fluss_error) {
			if (const char *msg = fluss_error_get_message(fluss_error)) {
				error_msg += ": ";
				error_msg += msg;
			}
			fluss_error_free(fluss_error);
		}
		throw InvalidInputException(error_msg);
	}

	// Subscribe to all buckets with their snapshot offsets
	// Note: bind_data.bucket_ids already contains only buckets that need to read Fluss data
	std::vector<int32_t> bucket_ids;
	std::vector<int64_t> offsets;
	for (int32_t bucket_id : bind_data.bucket_ids) {
		auto snapshot_offset_it = bind_data.snapshot_offsets.find(bucket_id);
		if (snapshot_offset_it != bind_data.snapshot_offsets.end()) {
			int64_t snapshot_offset = snapshot_offset_it->second;
			bucket_ids.push_back(bucket_id);
			offsets.push_back(snapshot_offset);
		}
	}

	if (!bucket_ids.empty()) {
		int subscribe_result = fluss_scanner_subscribe_batch(
			result->fluss_scanner,
			bucket_ids.data(),
			offsets.data(),
			static_cast<int32_t>(bucket_ids.size()),
			&fluss_error
		);
		if (subscribe_result != 0) {
			std::string error_msg = "Failed to subscribe to buckets";
			if (fluss_error) {
				if (const char *msg = fluss_error_get_message(fluss_error)) {
					error_msg += ": ";
					error_msg += msg;
				}
				fluss_error_free(fluss_error);
			}
			throw InvalidInputException(error_msg);
		}
	}

	// Create a connection for Arrow conversion
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

// Main table function - implements two-phase reading (Paimon Snapshot -> Fluss Log)
static void FlussFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &global_state = data_p.global_state->Cast<FlussGlobalState>();
    auto &bind_data = const_cast<FlussBindData&>(data_p.bind_data->Cast<FlussBindData>());

    // Connection handle for Arrow-to-DataChunk conversion
    auto conn_handle = reinterpret_cast<duckdb_connection>(global_state.connection);

    // Initialize output as empty
    output.SetCardinality(0);

    // --- Phase 1: Paimon (Static Snapshot Data) ---
    if (global_state.phase == ReadingPhase::PAIMON) {
        ArrowArray paimon_array;
        paimon_array.release = nullptr;

        int res = paimon_scan_export_batch(global_state.paimon_scan, &paimon_array);

        if (res == 0 && paimon_array.release != nullptr) {
            duckdb_data_chunk chunk_handle = nullptr;
            // Convert Paimon Arrow data to DuckDB DataChunk
            auto err = duckdb_data_chunk_from_arrow(conn_handle, &paimon_array, global_state.converted_schema, &chunk_handle);
            if (!err && chunk_handle) {
                output.Move(*(reinterpret_cast<DataChunk*>(chunk_handle)));
                duckdb_destroy_data_chunk(&chunk_handle);
                return; // Return with data; DuckDB will call this function again
            }
        }

        // Paimon data exhausted, transition to Fluss phase
        global_state.phase = ReadingPhase::FLUSS;

        // If there are no buckets to stream, return empty (signals actual EOF)
        if (bind_data.bucket_ids.empty()) {
            return;
        }
        // FALLTHROUGH: Don't return here! Immediately try to read from Fluss
    }

    // --- Phase 2: Fluss (Real-time Log Data) ---
    if (global_state.phase == ReadingPhase::FLUSS) {
        // If all buckets have reached their target offsets, signal EOF
        if (global_state.active_buckets.empty()) {
            return;
        }

        // Loop until we have data in output OR all buckets are finished
        // This prevents returning 0 rows prematurely, which would stop the scan
        while (output.size() == 0 && !global_state.active_buckets.empty()) {

            // 1. Poll for new batches if we don't have a pending result
            if (global_state.current_batches == nullptr) {
                // Check for user interruption (e.g., query cancellation)
                if (context.interrupted) {
                    throw InterruptException();
                }

                FlussError* fluss_error = nullptr;
                global_state.current_batches = fluss_scanner_poll_batches(global_state.fluss_scanner, 100, &fluss_error);

                if (fluss_error) {
                    std::string msg = fluss_error_get_message(fluss_error);
                    fluss_error_free(fluss_error);
                    throw InvalidInputException("Fluss poll error: " + msg);
                }

                // If poll returned nothing but buckets are still active, loop again
                if (global_state.current_batches == nullptr) {
                    continue;
                }
            }

            // 2. Process the polled batches
            size_t b_count = fluss_scan_record_batches_get_bucket_count(global_state.current_batches);
            std::vector<int32_t> polled_ids(b_count);
            b_count = fluss_scan_record_batches_get_bucket_ids(global_state.current_batches, polled_ids.data(), b_count);
            polled_ids.resize(b_count);

            for (int32_t bid : polled_ids) {
                if (global_state.active_buckets.find(bid) == global_state.active_buckets.end()) {
                    continue;
                }

                ArrowArray bucket_array;
                bucket_array.release = nullptr;
                if (fluss_scan_record_batches_get_batch_for_bucket(global_state.current_batches, bid, &bucket_array) == 0
                    && bucket_array.release != nullptr) {

                    // Convert and Append data to the output DataChunk
                    duckdb_data_chunk temp_handle = nullptr;
                    auto err = duckdb_data_chunk_from_arrow(conn_handle, &bucket_array, global_state.converted_schema, &temp_handle);

                    if (!err && temp_handle) {
                        auto* temp_chunk = reinterpret_cast<DataChunk*>(temp_handle);
                        output.Append(*temp_chunk);
                        duckdb_destroy_data_chunk(&temp_handle);
                    }

                    // Update progress and check for completion
                    global_state.fluss_rows_read_per_bucket[bid] += bucket_array.length;
                    int64_t limit = bind_data.end_offsets[bid] - bind_data.snapshot_offsets[bid];

                    if (global_state.fluss_rows_read_per_bucket[bid] >= limit) {
                        FlussError* un_error = nullptr;
                        // Corrected API call based on typical Fluss C-FFI
                        fluss_scanner_unsubscribe_batch(global_state.fluss_scanner, bid, bind_data.table_id, &un_error);

                        if (un_error) {
	                        fluss_error_free(un_error);
                        }
                        global_state.active_buckets.erase(bid);
                    }

                    if (bucket_array.release) {
                        bucket_array.release(&bucket_array);
                    }
                }
            }

            // Clean up the current poll result
            if (global_state.current_batches) {
                fluss_scan_record_batches_free(global_state.current_batches);
                global_state.current_batches = nullptr;
            }
        }
    }
}
static void LoadInternal(ExtensionLoader &loader) {
	// Register table function: fluss_read(bootstrap_server, table) or fluss_read(bootstrap_server, database, table)
	TableFunctionSet fluss_read_set("fluss_read");

	// 2 parameters: bootstrap_server, table (database defaults to "default")
	TableFunction fluss_read_2("fluss_read",
	                             {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                             FlussFunction, FlussBind, FlussInitGlobal, FlussInitLocal);
	fluss_read_set.AddFunction(fluss_read_2);

	// 3 parameters: bootstrap_server, database, table
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
