#include "postgres.h"
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <dirent.h>

/* Header for arrow parquet */
#include <parquet-glib/parquet-glib.h>
#include <arrow-glib/arrow-glib.h>
#include <gio/gio.h>

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "export_entry.h"
#include "file_utils.h"
#include "constants.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(ingestor_launch);

PGDLLEXPORT void ingestor_main(void) pg_attribute_noreturn();

#define MAX_COLUMN_NAME_CHARS 100
#define MAX_SUPPORTED_COLUMNS 100
#define MAX_EXPORT_ENTRIES 10
#define MAX_COLUMN_EXPORT_BATCH 10000

/** Arrow functionality */
#define ASSIGN_IF_NOT_NULL(check_ptr, dest_ptr)                                \
  {                                                                            \
    if (check_ptr != NULL) {                                                   \
      *dest_ptr = *check_ptr;                                                  \
    }                                                                          \
  }

#define LOG_ARROW_ERROR(error)                                                 \
  {                                                                            \
    if (error != NULL) {                                                       \
      elog(LOG, error->message);                                               \
    }                                                                          \
  }

static void list_current_directories() {
	DIR *dir;
	struct dirent *entry;

	// Open the current directory (".")
	dir = opendir(".");
	if (dir == NULL) {
		perror("opendir");
		return;
	}

	// Read entries from the directory
	while ((entry = readdir(dir)) != NULL) {
		// Check for "." and ".." entries
		if (strcmp(entry->d_name, ".") == 0 || 
			strcmp(entry->d_name, "..") == 0) {
			continue;
		}
		elog(LOG, "Found directory with path %s", entry->d_name);
	}
	closedir(dir);
}

GArrowArray* create_int64_array(const int64 *data, int num_values, GError *error) {
    GError *inner_error = NULL;
    GArrowInt32ArrayBuilder *int_array_builder;
    gboolean success = TRUE;
    int_array_builder = garrow_int64_array_builder_new();
    
    for (int i = 0; i < num_values; i++) {
        int64 value = data[i];
        elog(LOG, "Adding value %d to arrow array", value);
        garrow_int64_array_builder_append_value(int_array_builder, value, &inner_error);
		LOG_ARROW_ERROR(inner_error);
        ASSIGN_IF_NOT_NULL(inner_error, error);
    }
    GArrowArray *int_array = garrow_array_builder_finish(
		GARROW_ARRAY_BUILDER(int_array_builder), &inner_error);
	LOG_ARROW_ERROR(inner_error);
    ASSIGN_IF_NOT_NULL(inner_error, error);
    g_object_unref(int_array_builder);

    return int_array;
}

GArrowArray* create_string_array(const char **data, int num_values, GError *error) {
    GError *inner_error = NULL;
    GArrowStringArrayBuilder *string_array_builder;
    string_array_builder = garrow_string_array_builder_new();

    for (int i = 0; i < num_values; i++) {
        char* value = data[i];
        if (value == 0) {
          value = "";
        }
		elog(LOG, "Adding value %s to arrow array", value);
        garrow_string_array_builder_append_value(string_array_builder, value, &inner_error);
        ASSIGN_IF_NOT_NULL(inner_error, error);
    }

    GArrowArray *string_array = garrow_array_builder_finish(
		GARROW_ARRAY_BUILDER(string_array_builder), &inner_error);
	LOG_ARROW_ERROR(inner_error);
    ASSIGN_IF_NOT_NULL(inner_error, error);
    g_object_unref(string_array_builder);
    return string_array;
}

/* Stores information about column names and column types. */
typedef struct _ColumnInfo {
	char column_name[MAX_COLUMN_NAME_CHARS];
	Oid column_type;
} ColumnInfo;

/** 
 * Returns list of columns and type of each column.
 * num_of_columns is populated with columns in the table. 
 * Caller should free the returned pointer.
 */
static ColumnInfo* get_column_types(const char *table_name, int* num_of_columns) {
	StringInfoData buf;
	initStringInfo(&buf);
	// TODO - ensure column schema is deterministic even after new columns are added
	appendStringInfo(&buf, "SELECT attname, atttypid FROM pg_attribute WHERE attrelid = '%s'::regclass AND attnum > 0;", table_name);
	int status = SPI_execute(buf.data, true, 0);
	elog(LOG, "Executed SPI_execute query %s with status %d", buf.data, status);
	if (status != SPI_OK_SELECT) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Failed to deduce column types")));
	}
	elog(LOG, "Found %d processed rows", SPI_processed);
	elog(LOG, "Found %d values in result", SPI_tuptable->numvals);
	elog(LOG, "Type of column 1 is %d", SPI_tuptable->tupdesc->attrs[0].atttypid);
	elog(LOG, "Type of column 2 is %d", SPI_tuptable->tupdesc->attrs[1].atttypid);
	bool isnull;
	ColumnInfo* columns = palloc_array(ColumnInfo, SPI_processed);
	for (int i = 0; i < SPI_processed; i += 1) {
		ColumnInfo column_info;
		Datum column_name_data = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
		Name column_name = DatumGetName(column_name_data);
		char* column_name_value = column_name->data;
		elog(LOG, "Acquired column name as %s", column_name_value);

		Datum column_type_data = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull);
		Oid column_type_value = DatumGetObjectId(column_type_data);
		elog(LOG, "Acquired column type as %d", column_type_value);

		strcpy(column_info.column_name, column_name_value);
		column_info.column_type =  column_type_value;

		columns[i] = column_info;
	}
	*num_of_columns = SPI_processed;
	elog(LOG, "Finished extracting column types");
	return columns;
}

/*
* Creates Arrow schema for the table for basic types. 
* Caller is reponsible for freeing schema memory.
*/
static GArrowSchema* create_table_schema(
	const ColumnInfo *column_info,
	const ExportEntry *entry, int overall_num_columns) {
	GError *error = NULL;
	GList *fields = NULL;
	GArrowSchema *temp = NULL;

	// Define data schema
	elog(LOG, "Creating arrow schema");
    temp = garrow_schema_new(fields);
	for (int i = 0; i < entry->num_of_columns; i += 1) {
		const char *column_name = entry->columns_to_export[i];
		// TODO - move column type deduction to util
		Oid column_type;
		bool column_type_found = false;
		for (int j = 0; j < overall_num_columns; j += 1) {
			if (strcmp(column_info[j].column_name, column_name) == 0) {
				column_type = column_info[j].column_type;
				column_type_found = true;
				break;
			}
		}
		Assert(column_type_found);
		switch (column_type) {
			case INT2OID:
			case INT4OID:
			case INT8OID:
			{
				GArrowDataType *int_type = garrow_int64_data_type_new();
				GArrowField *int_field = garrow_field_new(column_name, int_type);
				temp = garrow_schema_add_field(temp, i, int_field, &error);
				g_object_unref(int_field);
				g_object_unref(int_type);
				elog(LOG, "Added field for int type to arrow schema for column %s", column_name);
				elog(LOG, "Created schema with string %s", garrow_schema_to_string(temp));
				break;
			}
			case VARCHAROID:
			{
				GArrowDataType *string_type = garrow_string_data_type_new();
    			GArrowField *string_field = garrow_field_new(column_name, string_type);
				temp = garrow_schema_add_field(temp, i, string_field, &error);
				g_object_unref(string_field);
				g_object_unref(string_type);
				elog(LOG, "Added field for string type to arrow schema for column %s", column_name);
				elog(LOG, "Created schema with string %s", garrow_schema_to_string(temp));
				break;
			}
			default:
				elog(LOG, "Column type %s not yet supported in columnar schema", column_type);
				break;
		}
	}
	const char *schema_str = garrow_schema_to_string(temp);
	elog(LOG, "Created schema with string %s", schema_str);
	return temp;
}

static int64* create_int64_data_array(Datum* data, int num_values) {
	int64 *int_data = (int64*)palloc(num_values * sizeof(int64));
	for (int i = 0; i < num_values; i += 1) {
		int_data[i] = DatumGetInt64(data[i]);
		elog(LOG, "Added value %d to array", int_data[i]);
	}
	return int_data;
}

static char** create_string_data_array(Datum* data, int num_values) {
	char **string_data = (char**)palloc(num_values * sizeof(char*));
	for (int i = 0; i < num_values; i += 1) {
		char* cstring = TextDatumGetCString(data[i]);
		string_data[i] = (char*)palloc(sizeof(char) * strlen(cstring) + 1);
		strcpy(string_data[i], cstring);
		elog(LOG, "Added value %s to array", string_data[i]);
	}
	return string_data;
}

/**
 * Returns Arrow table pointer with data populated from SPI query.
 * Expects SPI query to read desired table rows has been executed.
 * Caller is reponsible for freeing table memory.
 */
static GArrowTable* create_arrow_table(
	GArrowSchema* schema,
	const ColumnInfo *column_info,
	const ExportEntry * entry, 
	int overall_num_columns
) {
	Assert(SPI_processed > 0);
	elog(LOG, "Creating arrow table");
	int num_export_columns = entry->num_of_columns;
	GArrowArray **arrow_arrays = (GArrowArray **)palloc(num_export_columns * sizeof(GArrowArray));
	Datum **table_data = (Datum**)palloc(num_export_columns * sizeof(Datum*));
	// Iterate over all rows for each column and create Datum Arays for each column
	for (int i = 0; i < num_export_columns; i += 1) {
		// Copy datums to temp variable to access all datums for a column
		table_data[i] = (Datum*)palloc(SPI_processed * sizeof(Datum));
		for (int j = 0; j < SPI_processed; j += 1) {
			bool isnull;
			Datum datum = SPI_getbinval(SPI_tuptable->vals[j], SPI_tuptable->tupdesc, i + 1, &isnull);
			if (!isnull) {
				table_data[i][j] = datum;
			} else {
				table_data[i][j] = 0;
			}
		}
	}
	elog(LOG, "Populated datums in temp memory");
	// TODO - handle error
	GError *error = NULL;
	// Populate arrow arrays for each column
	for (int i = 0; i < num_export_columns; i += 1) {
		// TODO - move column type deduction to util
		const char *column_name = entry->columns_to_export[i];
		Oid column_type;
		bool column_type_found = false;
		for (int j = 0; j < overall_num_columns; j += 1) {
			if (strcmp(column_info[j].column_name, column_name) == 0) {
				column_type = column_info[j].column_type;
				column_type_found = true;
				break;
			}
		}
		Assert(column_type_found);
		switch (column_type) {
			case INT2OID:
			case INT4OID:
			case INT8OID:
			{
				const int64* int_data = create_int64_data_array(table_data[i], SPI_processed);
				arrow_arrays[i] = create_int64_array(int_data, SPI_processed, error);
				pfree(int_data);
				elog(LOG, "Created int data array");
				LOG_ARROW_ERROR(error);
				break;
			}
			case VARCHAROID:
			{
				const char** string_data = create_string_data_array(table_data[i], SPI_processed);
				arrow_arrays[i] = create_string_array(string_data, SPI_processed, error);
				pfree(string_data);
				elog(LOG, "Created string data array");
				LOG_ARROW_ERROR(error);
				break;
			}
			default:
				elog(LOG, "Column type %s not yet supported in columnar schema", column_type);
				break;
		}
	}
	GArrowTable *table = garrow_table_new_arrays(schema, arrow_arrays, num_export_columns, &error);
	LOG_ARROW_ERROR(error);

	for (int i = 0; i < num_export_columns; i += 1) {
		g_object_unref(arrow_arrays[i]);
		pfree(table_data[i]);
	}
	pfree(arrow_arrays);
	pfree(table_data);

	const char *table_definition = garrow_table_to_string(table, &error);
	elog(LOG, "Created arrow table with definition %s", table_definition);
	return table;
}


static void write_arrow_table(
	const char *table_name,
	int chunk_num,
	GArrowSchema *schema,
	GArrowTable* table) {
	char path[PATH_MAX];
	char file_name[PATH_MAX];
	populate_temp_path_for_table(table_name, path, /*relative=*/true);
	sprintf(file_name, "/%d.parquet", chunk_num);
	strcat(path, file_name);
	elog(LOG, "Attempting to write file %s", path);

	GError *error = NULL;
	GParquetWriterProperties *writer_properties = gparquet_writer_properties_new();
	elog(LOG, "Create arrow writer properties");
    GParquetArrowFileWriter *writer = gparquet_arrow_file_writer_new_path(schema, path, writer_properties, &error);
	LOG_ARROW_ERROR(error);
	elog(LOG, "Created arrow file writer.");

	gboolean is_write_successfull = 
        gparquet_arrow_file_writer_write_table(writer, table, 10000, &error);
	LOG_ARROW_ERROR(error);
	elog(LOG, "Wrote arrow table to disk");

    gboolean file_closed = gparquet_arrow_file_writer_close(writer, &error);
	LOG_ARROW_ERROR(error);

	Assert(file_closed);

	g_object_unref(table);
	g_object_unref(writer);
	g_object_unref(writer_properties);

	elog(LOG, "Sucessfully wrote columnar file to disk.");
}

void delete_export_entry(const char *table_name) {
	StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(
		&buf, 
        "DELETE FROM analytica_exports WHERE table_name = '%s';",
		table_name
	);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	int connection = SPI_connect();
    if (connection < 0) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Failed to connect to database")));
	}
    elog(LOG, "Executing SPI_execute query %s", buf.data);
    int status = SPI_execute(buf.data, /*read_only=*/false, /*count=*/0);
    elog(LOG, "Executed SPI_execute command with status %d", status);	
	if (status != SPI_OK_DELETE) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Failed to delete export entry.")));
	}
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}

static void get_tables_to_process_in_order(ExportEntry *entries, int *num_of_tables) {
	StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, 
	"SELECT                     \
		table_name,             \
		columns_to_export,      \
		last_run_completed,     \
		export_frequency_hours,  \
		export_status, \
		now() \
	FROM analytica_exports      \
	ORDER BY last_run_completed \
	LIMIT %d;" , MAX_EXPORT_ENTRIES);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	int connection = SPI_connect();
    if (connection < 0) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Failed to connect to database")));
	}
	elog(LOG, "Created connection for query");
    elog(LOG, "Executing SPI_execute query %s", buf.data);
    int status = SPI_execute(buf.data, /*read_only=*/true, /*count=*/0);
    elog(LOG, "Executed SPI_execute command with status %d", status);	
	if (status < 0) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Failed to fetch tables to export.")));
	}

	if (!SPI_processed) {
		*num_of_tables = 0;
		return;
	}
	elog(LOG, "Beginning processing of %d rows", SPI_processed);
	int valid_entries = 0;

	// Calaculate number of vvalid entries to export.
	for (int i = 0; i < SPI_processed; i += 1) {
		bool isnull;
		bool is_valid_entry = false;
		Datum last_completed_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3, &isnull);
		elog(LOG, "Read last completed datum");
		if (isnull) {
			// Table is newly scheduled for export so last run completed is null
			is_valid_entry = true;
			elog(LOG, "last completed datum is null");
		} else {
			elog(LOG, "Evaluating if older export entry is past threshold");
			Datum export_frequency_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4, &isnull);
			int export_frequency = DatumGetInt16(export_frequency_datum);
			
			int64 last_completed = DatumGetTimestampTz(last_completed_datum);
			int64 last_run_pg_time = timestamptz_to_time_t(last_completed);

			Datum current_time_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 6, &isnull);
			TimestampTz current_time = DatumGetTimestampTz(current_time_datum);
			int64 current_time_pg_time = timestamptz_to_time_t(current_time);

			elog(LOG, "last run time %lld", last_run_pg_time);
			elog(LOG, "current time %lld", current_time_pg_time);
			
			int64 seconds_elapsed = (current_time_pg_time - last_run_pg_time);
			elog(LOG, "seconds lapsed are %lld", seconds_elapsed);
			uint64 hours_elapsed = (seconds_elapsed) / 3600;
			elog(LOG, "%lld hours elapsed since last export", hours_elapsed);
			if (hours_elapsed > export_frequency) {
				is_valid_entry = true;
				elog(LOG, "Marking older entry for export again");
			} else {
				elog(LOG, "Older export entry threshold han;t passed");
			}
		}

		/**
		 * Surface inactive entries for cleanup later.
		 */
		Datum export_status_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 5, &isnull);
		int export_status = DatumGetInt32(export_status_datum);
		if (export_status == INACTIVE) {
			is_valid_entry = true;
		}

		if (is_valid_entry) {
			valid_entries += 1;

			Datum name_datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
			char *table_name = TextDatumGetCString(name_datum);

			ArrayType* arr = DatumGetArrayTypeP(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2, &isnull));
			Datum *column_datums;
			int num_of_columns;
			deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT, &column_datums, NULL, &num_of_columns);

			ExportEntry entry;
			initialize_export_entry(table_name, num_of_columns, &entry);
			entry.export_status = export_status;

			for (int j = 0; j < num_of_columns; j += 1) {
				char *column_name = TextDatumGetCString(column_datums[j]);
				export_entry_add_column(&entry, column_name, j);
			}
			entries[i] = entry;
		}
	}
	*num_of_tables = valid_entries;
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}

static void setup_data_directories(const char *table_name) {
	
	char root_path[PATH_MAX];
	char base_path[PATH_MAX];
	char temp_path[PATH_MAX];


	populate_root_path(root_path, /*relative=*/false);
	populate_data_path_for_table(table_name, base_path, /*relative=*/false);
	populate_temp_path_for_table(table_name, temp_path, /*relative=*/false);

	elog(LOG, "Setting up root directory %s", root_path);
	elog(LOG, "Setting up base directory %s", base_path);
	elog(LOG, "Setting up temp directory %s", temp_path);

	// Create root data directory if not exists.
	if (mkdir(root_path, 0755) == -1) {
		perror("mkdir");
		elog(LOG, "Failed to create root directoy %s", root_path);
	}

	
	// Create base data directory if not exists.
	if (mkdir(base_path, 0755) == -1) {
		perror("mkdir");
		elog(LOG, "Failed to create data directoy %s", base_path);
	}

	
	// Create temp directory if not exists.
	if (mkdir(temp_path, 0755) == -1) {
		perror("mkdir");
		elog(LOG, "Failed to create data directoy %s", temp_path);
	}
		
	
	// Delete content in temp directory.
	DIR *dir = opendir(temp_path);
	struct dirent *entry;
	while ((entry = readdir(dir)) != NULL) {
		char filepath[PATH_MAX];
		snprintf(filepath, sizeof(filepath), "%s/%s", temp_path, entry->d_name);

		// Skip special entries (".", "..")
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
			continue;
		}
		if (unlink(filepath) == -1) {
			elog(LOG, "Failed to delete temp file %s", filepath);
		}
		
	}
	closedir(dir);
	elog(LOG, "Data directory setup complete for %s", table_name);
}

/**
 * Moves columnar files from temp directory to data directory for table.
 * Cuurently this moves files one by one so it isn't atomic.
 */
void move_temp_files(const char *table_name) {
	DIR *dir;
	struct dirent *entry;

	char temp_path[PATH_MAX];
	char data_path[PATH_MAX];

	populate_data_path_for_table(table_name, data_path, false);
	populate_temp_path_for_table(table_name, temp_path, false);

	dir = opendir(temp_path);
	if (dir == NULL) {
		perror("opendir");
		return;
	}
	while ((entry = readdir(dir)) != NULL) {
		if (strcmp(entry->d_name, ".") == 0 || 
			strcmp(entry->d_name, "..") == 0) {
			continue;
		}
		elog(LOG, "Found entry with path %s", entry->d_name);
		char src_path[PATH_MAX];
		char dest_path[PATH_MAX];

		// temp columnar file
		strcpy(src_path, temp_path);
		strcat(src_path, "/");
		strcat(src_path, entry->d_name);

		// Final path
		strcpy(dest_path, data_path);
		strcat(dest_path, "/");
		strcat(dest_path, entry->d_name);

		elog(LOG, "Temp path %s", src_path);
		elog(LOG, "Data path %s", dest_path);

		if (rename(src_path, dest_path) == 0) {
			elog(LOG, "File '%s' renamed to '%s' successfully.\n", src_path, dest_path);
		} else {
			perror("rename"); // Print error message if rename fails
		}
	}
	closedir(dir);
}

char* get_columns_string(char **columns, int num_of_columns) {
  int total_size = 0;
  elog(LOG, "Creating stringified columns");
  for (int i = 0; i < num_of_columns; i++) {
    char *column_name = columns[i];
    total_size += strlen(column_name) + 1;
    elog(LOG, "Size of column %s is %d", column_name, strlen(column_name));
  }
  char *combined_string = (char*)palloc(total_size * sizeof(char));
  elog(LOG, "Allocated memory");
  combined_string[0] = '\0'; 
  for (int i = 0; i < num_of_columns; i++) {
    strcat(combined_string, columns[i]);
    elog(LOG, "Creating combined string %s", combined_string);
    if (i < num_of_columns - 1) {
      strcat(combined_string, ",");
    }
  }
  return combined_string;
}

void export_table_data(ExportEntry entry) {
	int offset = 0;
	int64 processed_count;
	int select;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	int connection = SPI_connect();
	if (connection == SPI_ERROR_CONNECT) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Failed to connect to database")));
	}
	elog(LOG, "Created connection for query");
	PushActiveSnapshot(GetTransactionSnapshot());

	int total_columns;
	elog(LOG, "Trying to extract column types");
	ColumnInfo *column_info = get_column_types(entry.table_name, &total_columns);
	elog(LOG, "Extracted %d column types", total_columns);

	GArrowSchema *arrow_schema = create_table_schema(column_info, &entry, total_columns);
	
	int chunk = 0;
	char* column_str = get_columns_string(entry.columns_to_export, entry.num_of_columns);

	do {
		StringInfoData buf;
		initStringInfo(&buf);
		// TODO - order of columns in result should match schema columns
		appendStringInfo(&buf, 
			"SELECT %s FROM %s LIMIT %d OFFSET %d;", 
			column_str, 
			entry.table_name, 
			MAX_COLUMN_EXPORT_BATCH, 
			offset
		);

		// Execute the chunked query using SPI_exec
		elog(LOG, "Executing SPI_execute query %s", buf.data);
		SetCurrentStatementStartTimestamp();
		select = SPI_execute(buf.data, true, 0);
		elog(LOG, "Executed SPI_execute command with status %d", select);

		if (select != SPI_OK_SELECT) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("SELECT Query execution failed")));
		}
		processed_count = SPI_processed;
		elog(LOG, "Processed %d rows", processed_count);
		elog(LOG, "Number of rows is %lu", SPI_tuptable->numvals);
		if (processed_count > 0) {
			GArrowTable *arrow_table = create_arrow_table(
				arrow_schema,
				column_info,
				&entry,
				total_columns
			);
			// write to disk
			write_arrow_table(entry.table_name, chunk, arrow_schema, arrow_table);
			chunk += 1;
		}
		// Increment offset by number of processed rows
		offset += processed_count;
	} while (processed_count > 0);
	elog(LOG, "Finished processing %lld rows", processed_count);
	pfree(column_str);
	// Move files from temp directly to data directory.
	move_temp_files(entry.table_name);

	g_object_unref(arrow_schema);
	pfree(column_info);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}

/**
 * Update table export status after successfull export.
 */
void update_table_export_metadata(const char *table_name) {
	StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(
		&buf, 
		"UPDATE analytica_exports        \
		 SET last_run_completed = CURRENT_TIMESTAMP, export_status = %d \
		 WHERE table_name = '%s';", 
		ACTIVE,
		table_name
	);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	int connection = SPI_connect();
    if (connection < 0) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Failed to connect to database")));
	}
	elog(LOG, "Created connection for query");
    elog(LOG, "Executing SPI_execute query %s", buf.data);
    int status = SPI_execute(buf.data, /*read_only=*/false, /*count=*/0);
    elog(LOG, "Executed SPI_execute command with status %d", status);	

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
}

void cleanup_inactive_export(const char *table_name) {
	elog(LOG, "Cleaning up data for table %s", table_name);
	// delete data directories
	int status = cleanup_table_data(table_name);
	if (status != 0) {
		elog(LOG, "Failed to cleanup data for table %s", table_name);
		return;
	}
	// delete metadata entry
	delete_export_entry(table_name);
	elog(LOG, "Cleaned up data for table %s", table_name);
}

/**
 * Use SPI to read rows from table.
 * https://www.postgresql.org/docs/current/spi.html
*/
void
ingestor_main()
{
	elog(LOG, "Background worker main called");
	bits32		flags = 0;
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection("postgres", "sushrutshivaswamy", flags);

	int num_of_tables;
	ExportEntry entries[MAX_EXPORT_ENTRIES];
	elog(LOG, "Fetching tables to export");
	get_tables_to_process_in_order(&entries, &num_of_tables);
	elog(LOG, "Beginning export for %d tables", num_of_tables);

	for (int i = 0; i < num_of_tables; i += 1) {
		char *table_name = entries[i].table_name;

		if (entries[i].export_status == INACTIVE) {
			elog(LOG, "Cleaning up inactive entry");
			cleanup_inactive_export(table_name);
			free_export_entry(&entries[i]);
			continue;
		}

		elog(LOG, "Creating data directory for %s with %d columns", table_name, entries[i].num_of_columns);
		setup_data_directories(table_name);

		elog(LOG, "Starting export for %s", table_name);
		export_table_data(entries[i]);
		elog(LOG, "Updating export status");
		update_table_export_metadata(entries[i].table_name);
		elog(LOG, "Freeing export entry");
		free_export_entry(&entries[i]);
		elog(LOG, "Freed export entry");
	}
	exit(0);
}

Datum
ingestor_launch(PG_FUNCTION_ARGS)
{
	pid_t		pid;
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "ingestor");
	sprintf(worker.bgw_function_name, "ingestor_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "ingestor dynamic worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "ingestor dynamic");
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		PG_RETURN_NULL();

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
				 errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));

	Assert(status == BGWH_STARTED);

	elog(LOG, "Background worker started");
	int bg_status = WaitForBackgroundWorkerShutdown(handle);
	elog(LOG, "Background worker closed with status %d\n", bg_status);

	PG_RETURN_INT32(pid);
}
