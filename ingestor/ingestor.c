#include "postgres.h"
#include <string.h>
#include <dirent.h>
// #include "parquet.h"

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

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(ingestor_launch);

PGDLLEXPORT void ingestor_main(void) pg_attribute_noreturn();

#define MAX_COLUMN_NAME_CHARS 100
#define MAX_SUPPORTED_COLUMNS 100

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
    GArrowArray *int_array = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(int_array_builder), &inner_error);
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

    GArrowArray *string_array = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(string_array_builder), &inner_error);
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
static ColumnInfo* get_column_types(int* num_of_columns) {
	StringInfoData buf;
	initStringInfo(&buf);
	// TODO - ensure column schema is deterministic even after new columns are added
	appendStringInfo(&buf, "SELECT attname, atttypid FROM pg_attribute WHERE attrelid = 'distributors'::regclass AND attnum > 0;");
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
static GArrowSchema* create_table_schema(const ColumnInfo *column_info, int num_columns) {
	GError *error = NULL;
	GList *fields = NULL;
	GArrowSchema *temp = NULL;

	// Define data schema
	elog(LOG, "Creating arrow schema");
    temp = garrow_schema_new(fields);
	for (int i = 0; i < num_columns; i += 1) {
		const char *column_name = column_info[i].column_name;
		Oid column_type = column_info[i].column_type;
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
	int num_columns
) {
	Assert(SPI_processed > 0);
	elog(LOG, "Creating arrow table");
	GArrowArray **arrow_arrays = (GArrowArray **)palloc(num_columns * sizeof(GArrowArray));
	Datum **table_data = (Datum**)palloc(num_columns * sizeof(Datum*));
	// Iterate over all rows for each column and create Datum Arays for each column
	for (int i = 0; i < num_columns; i += 1) {
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
	for (int i = 0; i < num_columns; i += 1) {
		const ColumnInfo info = column_info[i];
		Oid column_type = info.column_type;
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
	GArrowTable *table = garrow_table_new_arrays(schema, arrow_arrays, num_columns, &error);
	LOG_ARROW_ERROR(error);

	for (int i = 0; i < num_columns; i += 1) {
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
	GArrowSchema *schema,
	GArrowTable* table) {

	GError *error = NULL;
	GParquetWriterProperties *writer_properties = gparquet_writer_properties_new();
	elog(LOG, "Create arrow writer properties");
    GParquetArrowFileWriter *writer = gparquet_arrow_file_writer_new_path(schema, "./sample.parquet", writer_properties, &error);
	LOG_ARROW_ERROR(error);
	elog(LOG, "Created arrow file writer.");

	gboolean is_write_successfull = 
        gparquet_arrow_file_writer_write_table(writer, table, 10000, &error);
	LOG_ARROW_ERROR(error);
	elog(LOG, "Wrote arrow table  to disk");

    gboolean file_closed = gparquet_arrow_file_writer_close(writer, &error);
	LOG_ARROW_ERROR(error);

	Assert(file_closed);

	elog(LOG, "Sucessfully wrote columnar file to disk.");
}

/**
 * Use SPI to read rows from table.
 * https://www.postgresql.org/docs/current/spi.html
*/
void
ingestor_main()
{
	elog(LOG, "Background worker main called");
	int offset = 0;
	int processed_count;
	int select;

	list_current_directories();

	bits32		flags = 0;
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection("postgres", "sushrutshivaswamy", flags);

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
	ColumnInfo *column_info = get_column_types(&total_columns);
	elog(LOG, "Extracted %d column types", total_columns);

	GArrowSchema *arrow_schema = create_table_schema(column_info, total_columns);
	elog(LOG, "Created arow schema");
	do {

		StringInfoData buf;
		initStringInfo(&buf);
		// TODO - order of columns in result should match schema columns
		appendStringInfo(&buf, "SELECT * FROM distributors LIMIT %d OFFSET %d;", 20, offset);

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
			GArrowTable *arrow_table = create_arrow_table(arrow_schema, column_info, total_columns);
			// write to disk
			write_arrow_table(arrow_schema, arrow_table);

			g_object_unref(arrow_table);
		}
		// Increment offset by number of processed rows
		offset += processed_count;
	} while (processed_count > 0);
	
	g_object_unref(arrow_schema);
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	pfree(column_info);
	elog(LOG, "SPI_finish called");
	exit(0);
}


Datum
ingestor_launch(PG_FUNCTION_ARGS)
{
	// Extract table name
	char*		table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	Assert(strlen(table_name) > 0);
	// Extract columns
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(1);
	Assert(ARR_NDIM(arr) == 1);
	Assert(ARR_ELEMTYPE(arr) == TEXTOID);
	Datum	   *column_datums;
	int num_of_columns;
	deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT, &column_datums, NULL, &num_of_columns);
	Assert(num_of_columns > 0);

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