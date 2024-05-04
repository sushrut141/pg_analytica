#include "postgres.h"
#include <string.h>
#include "parquet.h"

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

/* Stores information about column names and column types. */
typedef struct _ColumnInfo {
	char column_name[MAX_COLUMN_NAME_CHARS];
	char column_type[MAX_COLUMN_NAME_CHARS];
} ColumnInfo;

/** 
 * Returns list of columns and type of each column.
 * num_of_columns is populated with columns in the table. 
 * Caller should free the returned pointer.
 */
static ColumnInfo* get_column_types(int* num_of_columns) {
	StringInfoData buf;
	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT column_name, data_type FROM distributors;");
	int status = SPI_execute(buf.data, true, 0);
	elog(LOG, "Executed SPI_execute query %s with status %d", buf.data, select);
	if (status != SPI_OK_SELECT) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Failed to deduce column types")));
	}
	bool isnull;
	ColumnInfo* columns = palloc_array(ColumnInfo, MAX_SUPPORTED_COLUMNS);
	for (int i = 0; i < SPI_processed; i += 1) {
		ColumnInfo column_info;
		Datum column_name_data = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
		char* column_name_value = TextDatumGetCString(column_name_data);

		Datum column_type_data = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
		char* column_type_value = TextDatumGetCString(column_type_data);

		Assert(strlen(column_name_value < MAX_COLUMN_NAME_CHARS));
		Assert(strlen(column_type_value < MAX_COLUMN_NAME_CHARS));

		strcpy(column_info.column_name, column_name_value);
		strcpy(column_info.column_name, column_type_value);

		columns[i] = column_info;
	}
	*num_of_columns = SPI_processed;
	return columns;
}

/**
 * Return the type of the column based on name.
 */
static const char* get_column_type_for(const char *name, ColumnInfo* columns, int num_columns) {
	for (int i = 0; i < num_columns; i += 1) {
		if (strcmp(columns[i].column_name, name)) {
			return columns[i].column_type;
		}
	}
	return NULL;
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
    temp = garrow_schema_new(fields);
	for (int i = 0; i < num_columns; i += 1) {
		const char *column_name = column_info[i].column_name;
		const char *column_type = get_column_type_for(column_name, column_info, num_columns);
		// TODO - try to use postgres type OIDs here like INTOID32
		// TODO - handle unsigned int
		/* Integer type */
		if (strcmp(column_type, "integer")) {
			GArrowDataType *int_type = garrow_int64_data_type_new();
			GArrowField *int_field = garrow_field_new(column_name, int_type);
			temp = garrow_schema_add_field(temp, i, int_field, &error);
		} else 
		/* Text type */
		if (strcmp(column_type, "character varying")) {
			GArrowDataType *string_type = garrow_string_data_type_new();
    		GArrowField *string_field = garrow_field_new(column_name, string_type);
			temp = garrow_schema_add_field(temp, i, string_field, &error);
		} else 
		/* Float / Decimal type */
		if (
			strcmp(column_type, "double precision") ||
			strcmp(column_type, "numeric")
		) {
			GArrowDataType *double_type = garrow_double_data_type_new();
			GArrowField *double_field = garrow_field_new(column_name, double_type);
			temp = garrow_schema_add_field(temp, i, double_field, &error);
		}  else {
			elog(LOG, "Unknown column type %s encountered", column_type);
			return NULL;
		}
	}
	return temp;
}

static int64* create_int64_data_array(Datum* data, int num_values) {
	int64 *int_data = (int64*)palloc(num_values * sizeof(int64));
	for (int i = 0; i < num_values; i += 1) {
		int_data[i] = DatumGetInt64(data[i]);
	}
	return int_data;
}

static char** create_string_data_array(Datum* data, int num_values) {
	char **string_data = (char**)palloc(num_values * sizeof(char*));
	for (int i = 0; i < num_values; i += 1) {
		char* cstring = TextDatumGetCString(data[i]);
		string_data[i] = (char*)palloc(sizeof(char) * strlen(cstring) + 1);

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
				table_data[i][j] = NULL;
			}
		}
	}
	// TODO - handle error
	GError *error;
	// Populate arrow arrays for each column
	for (int i = 0; i < num_columns; i += 1) {
		const ColumnInfo info = column_info[i];
		const char *column_type = info.column_type;
		if (strcmp(column_type, "integer")) {
			const int64* int_data = create_int64_data_array(table_data[i], SPI_processed);
			arrow_arrays[i] = create_int64_array(int_data, SPI_processed, error);
			pfree(int_data);
		} else 
		/* Text type */
		if (strcmp(column_type, "character varying")) {
			const char** string_data = create_string_data_array(table_data[i], SPI_processed);
			arrow_arrays[i] = create_string_array(string_data, SPI_processed, error);
			pfree(string_data);
		} else 
		/* Float / Decimal type */
		if (
			strcmp(column_type, "double precision") ||
			strcmp(column_type, "numeric")
		) {
			// TODO
		}  else {
			// TODO
		}
	}
	GArrowTable *table = garrow_table_new_arrays(schema, arrow_arrays, num_columns, &error);

	for (int i = 0; i < num_columns; i += 1) {
		pfree(arrow_arrays[i]);
		pfree(table_data[i]);
	}
	pfree(arrow_arrays);
	pfree(table_data);

	return table;
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

	bits32		flags = 0;
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection("postgres", "sushrutshivaswamy", flags);

	do {
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		int connection = SPI_connect();
		if (connection == SPI_ERROR_CONNECT) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("Failed to connect to database")));
		}
		elog(LOG, "Created connection for query");
		PushActiveSnapshot(GetTransactionSnapshot());

		StringInfoData buf;
		initStringInfo(&buf);
		appendStringInfo(&buf, "SELECT * FROM distributors LIMIT %d OFFSET %d;", 5, offset);

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

		int num_of_columns = SPI_tuptable->numvals;
		for (int i = 0 ; i < processed_count; i += 1) {
			elog(LOG, "Printing values for row %d", i + 1);
			for (int j = 0; j < 2; j += 1) {
				switch (SPI_tuptable->tupdesc->attrs[j].atttypid) {
					case INT2OID:
					case INT4OID:
					case INT8OID:
					{
						elog(LOG, "Reading int column");
						bool isnull;
						Datum column_data = SPI_getbinval(SPI_tuptable->vals[i],
									   SPI_tuptable->tupdesc,
									   j + 1, &isnull);
						if (!isnull) {
							int column_value = DatumGetUInt32(column_data);
							elog(LOG, "%d |", column_value);
						} else {
							elog(LOG, "null |");
						}
						break;
					}
					case VARCHAROID:
					{
						elog(LOG, "Reading varchar column");
						bool isnull;
						Datum column_data = SPI_getbinval(SPI_tuptable->vals[i],
									   SPI_tuptable->tupdesc,
									   j + 1, &isnull);
						if (!isnull) {
							char* column_value = TextDatumGetCString(column_data);
							elog(LOG, "%s |", column_value);
						} else {
							elog(LOG, "null |");
						}
						break;
					}
					default:
						elog(LOG, "found some other type with name");
						break;
				}
			}
		}
		// Increment offset by number of processed rows
		offset += processed_count;
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		elog(LOG, "SPI_finish called");
	} while (processed_count > 0);
	exit(0);
}


Datum
ingestor_launch(PG_FUNCTION_ARGS)
{
	setup_logging();
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
