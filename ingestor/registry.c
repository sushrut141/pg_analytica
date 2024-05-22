#include "postgres.h"
#include "constants.h"
#include "executor/spi.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "catalog/pg_type_d.h"

PG_FUNCTION_INFO_V1(register_table_export);
PG_FUNCTION_INFO_V1(unregister_table_export);

static int execute_query(StringInfoData buf)
{
	int connection = SPI_connect();
    if (connection < 0) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
					errmsg("Failed to connect to database")));
	}
	elog(LOG, "Created connection for query");
    // Execute the chunked query using SPI_exec
    elog(LOG, "Executing SPI_execute query %s", buf.data);
    int status = SPI_execute(buf.data, false, 0);
    elog(LOG, "Executed SPI_execute command with status %d", status);
	
	SPI_finish();
    return status;
}

static char* get_columns_string(Datum *columns, int num_of_columns) {
  int total_size = 0;
  elog(LOG, "Creating stringified columns");
  for (int i = 0; i < num_of_columns; i++) {
    char *column_name = TextDatumGetCString(columns[i]);
    total_size += strlen(column_name) + 1;
    elog(LOG, "Size of column %s is %d", column_name, strlen(column_name));
  }
  char *combined_string = (char*)palloc(total_size * sizeof(char));
  elog(LOG, "Allocated memory");
  combined_string[0] = '\0'; 
  for (int i = 0; i < num_of_columns; i++) {
    strcat(combined_string, TextDatumGetCString(columns[i]));
    elog(LOG, "Creating combined string %s", combined_string);
    if (i < num_of_columns - 1) {
      strcat(combined_string, ",");
    }
  }
  return combined_string;
}

Datum register_table_export(PG_FUNCTION_ARGS) {
    // TODO - add validation to ensure table exists 
    // and column types are supported for export
    int num_of_args = PG_NARGS();
    if (num_of_args != 3) {
        ereport(ERROR, (errcode(ERRCODE_RAISE_EXCEPTION),
						errmsg("Invalid number of arguments. Expected format is register_export(table_name text, columns_to_export text[], export_frequency_hours int)")));
    }
    // Extract table name
	char *table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    // Extract columns to export
    ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(1);
	Assert(ARR_NDIM(arr) == 1);
	Assert(ARR_ELEMTYPE(arr) == TEXTOID);
	Datum *column_datums;
	int num_of_columns;
	deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT, &column_datums, NULL, &num_of_columns);
    char *column_str;
    column_str = get_columns_string(column_datums, num_of_columns);
    elog(LOG, "Created column string  as %s", column_str);
    // Extract export frequency
    int32 export_frequency_hours = PG_GETARG_INT32(2);

    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(
        &buf, 
        "INSERT INTO analytica_exports (table_name, columns_to_export, export_frequency_hours, export_status) VALUES ('%s', '{%s}', %d, %d);",
        table_name,
        column_str,
        export_frequency_hours,
        PENDING
    );

    int status = execute_query(buf);
    if (status < 0) {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("Query execution failed")));
    }
    pfree(column_str);
    elog(LOG, "Scheduled export for table %s with frequency of %d hours", table_name, export_frequency_hours);
    PG_RETURN_INT32(1);
}

Datum unregister_table_export(PG_FUNCTION_ARGS) {
    int num_of_args = PG_NARGS();
    if (num_of_args != 1) {
        ereport(ERROR, (errcode(ERRCODE_RAISE_EXCEPTION),
						errmsg("Invalid number of arguments. Expected format is unregister_export(table_name text)")));
    }
    // Extract table name
	char *table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(
		&buf, 
		"UPDATE analytica_exports        \
		 SET export_status = %d \
		 WHERE table_name = '%s';", 
		INACTIVE,
		table_name
	);

    int status = execute_query(buf);
    if (status < 0) {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("Failed to unregister table.")));
    }

    elog(LOG, "Marked %s table export as inactive. Table data will be deleted at next available window.", table_name);
    PG_RETURN_INT32(1);
}