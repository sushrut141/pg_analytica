#include "postgres.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "catalog/pg_type_d.h"

PG_FUNCTION_INFO_V1(register_export);
PG_FUNCTION_INFO_V1(unregister_export);

Datum register_export(PG_FUNCTION_ARGS) {
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
    // Extract export frequency
    int32 export_frequency_hours = PG_GETARG_INT32(2);

    elog(LOG, "Scheduling export for table %s with frequency of %d hours", table_name, export_frequency_hours);
    PG_RETURN_INT32(1);
}

Datum unregister_export(PG_FUNCTION_ARGS) {
    int num_of_args = PG_NARGS();
    if (num_of_args != 1) {
        ereport(ERROR, (errcode(ERRCODE_RAISE_EXCEPTION),
						errmsg("Invalid number of arguments. Expected format is unregister_export(table_name text)")));
    }
    // Extract table name
	char *table_name = text_to_cstring(PG_GETARG_TEXT_PP(0));

    elog(LOG, "Removed export for table %s. Table data will be deleted at next available window.", table_name);
    PG_RETURN_INT32(1);
}