#include "parquet.h"

#define ASSIGN_IF_NOT_NULL(check_ptr, dest_ptr)                                \
  {                                                                            \
    if (check_ptr != NULL) {                                                   \
      *dest_ptr = *check_ptr;                                                  \
    }                                                                          \
  }

GArrowArray* create_int64_array(const int *data, int num_values, GError *error) {
    GError *inner_error = NULL;
    GArrowInt32ArrayBuilder *int_array_builder;
    gboolean success = TRUE;
    int_array_builder = garrow_int32_array_builder_new();
    
    for (int i = 0; i < num_values; i++) {
        int value = data[i];
        if (value == 0) {
          value = 0;
        }
        garrow_int32_array_builder_append_value(int_array_builder, value, &inner_error);
        ASSIGN_IF_NOT_NULL(inner_error, error);
    }
    GArrowArray *int_array = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(int_array_builder), &inner_error);

    ASSIGN_IF_NOT_NULL(inner_error, error);
    g_object_unref(int_array_builder);

    return int_array;
}

GArrowArray* create_string_array(const char **data, int num_values, GError *error) {
    GError *inner_error;
    GArrowStringArrayBuilder *string_array_builder;
    string_array_builder = garrow_string_array_builder_new();

    for (int i = 0; i < num_values; i++) {
        char* value = data[i];
        if (value == 0) {
          value = "";
        }
        garrow_string_array_builder_append_value(string_array_builder, value, &inner_error);
        ASSIGN_IF_NOT_NULL(inner_error, error);
    }

    GArrowArray *string_array = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(string_array_builder), &inner_error);
    ASSIGN_IF_NOT_NULL(inner_error, error);
    g_object_unref(string_array_builder);

    return string_array;
}