/* -------------------------------------------------------------------------
 *
 * worker_spi.c
 *		Sample background worker code that demonstrates various coding
 *		patterns: establishing a database connection; starting and committing
 *		transactions; using GUC variables, and heeding SIGHUP to reread
 *		the configuration file; reporting to pg_stat_activity; using the
 *		process latch to sleep and exit in case of postmaster death.
 *
 * This code connects to a database, creates a schema and table, and summarizes
 * the numbers contained therein.  To see it working, insert an initial value
 * with "total" type and some initial value; then insert some other rows with
 * "delta" type.  Delta rows will be deleted by this worker and their values
 * aggregated into the total.
 *
 * Copyright (c) 2013-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/worker_spi/worker_spi.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

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

static FILE* fd;
static FILE* worker_fd;

static void setup_logging() {
  fd = fopen("/tmp/pg_analytica.log", "w");
  setvbuf(fd, NULL, _IONBF, 0); 
}


static void setup_worker_logging() {
  worker_fd = fopen("/tmp/pg_analytica_worker.log", "w");
  setvbuf(worker_fd, NULL, _IONBF, 0); 
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
	fprintf(fd, "Input Table: %s\n", table_name);
	// Extract columns
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(1);
	Assert(ARR_NDIM(arr) == 1);
	Assert(ARR_ELEMTYPE(arr) == TEXTOID);
	Datum	   *column_datums;
	int num_of_columns;
	deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT, &column_datums, NULL, &num_of_columns);
	Assert(num_of_columns > 0);
	fprintf(fd, "Input number of columns: %d\n", num_of_columns);
	for (int i = 0; i < num_of_columns; i += 1) {
		fprintf(fd, "Input Ordering Column: %s\n", TextDatumGetCString(column_datums[i]));
	}

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

	fprintf(fd, "Registering background worker\n");

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		PG_RETURN_NULL();

	fprintf(fd, "Waiting for background workert to start up\n");

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

	fprintf(fd, "Background worker started\n");
	int bg_status = WaitForBackgroundWorkerShutdown(handle);
	fprintf(fd, "Background worker closed with status %d\n", bg_status);
	PG_RETURN_INT32(pid);
}
