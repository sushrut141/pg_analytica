#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(get_sum);

Datum
get_sum(PG_FUNCTION_ARGS)
{
    bool isnull, isnull2;

    isnull = PG_ARGISNULL(0);
    isnull2 = PG_ARGISNULL(1);
    if (isnull || isnull2)
      ereport( ERROR,
               ( errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               errmsg("two and only two integer values are required as input")));

    int32 a = PG_GETARG_INT32(0);
    int32 b = PG_GETARG_INT32(1);
    int32 sum;

    sum = a + b;

    PG_RETURN_INT32(sum);
}
