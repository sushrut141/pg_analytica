
/* Header for arrow parquet */
#include <parquet-glib/parquet-glib.h>
#include <arrow-glib/arrow-glib.h>
#include <gio/gio.h>

 GArrowArray* create_int64_array(const int *data, int num_values, GError *error);

 GArrowArray* create_string_array(const char **data, int num_values, GError *error);