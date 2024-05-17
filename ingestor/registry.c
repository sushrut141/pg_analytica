#include "postgres.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(register_export);

Datum register_export(PG_FUNCTION_ARGS) {
    int32 value = 74;
    PG_RETURN_INT32(value);
}