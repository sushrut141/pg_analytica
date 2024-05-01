#include <parquet-glib/parquet-glib.h>
#include <arrow-glib/arrow-glib.h>
#include <gio/gio.h>


int main(int argc, char *argv[]) {

    // Error handling variable
    GError *error = NULL;
    GList *fields = NULL;
    GArrowSchema *schema = NULL;

    // Define data schema
    schema = garrow_schema_new(fields);

    // Create fields for the schema (replace data types if needed)
    GArrowDataType *int_type = garrow_int32_data_type_new();
    GArrowDataType *string_type = garrow_string_data_type_new();
    GArrowField *int_field = garrow_field_new("int_column", int_type);
    GArrowField *string_field = garrow_field_new("string_column", string_type);

    GArrowSchema *schema_with_int = garrow_schema_add_field(schema, 0, int_field, &error);
    GArrowSchema *final_schema = garrow_schema_add_field(schema_with_int, 1, string_field, &error);

    // Release references after schema creation
    g_object_unref(string_field);
    g_object_unref(int_field);
    g_object_unref(string_type);
    g_object_unref(int_type);

    // Define number of rows
    int num_rows = 100000;

    // Create data arrays
    GArrowInt32ArrayBuilder *int_array_builder;
    GArrowStringArrayBuilder *string_array_builder;
    gboolean success = TRUE;
    int_array_builder = garrow_int32_array_builder_new();
    string_array_builder = garrow_string_array_builder_new();


    // Set values in the arrays (replace with your data)
    for (int i = 0; i < num_rows; i++) {
        garrow_int32_array_builder_append_value(int_array_builder, i, &error);
        garrow_string_array_builder_append_value(string_array_builder, "yolo", &error);
    }
    GArrowArray *int_array = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(int_array_builder), &error);
    GArrowArray *string_array = garrow_array_builder_finish(GARROW_ARRAY_BUILDER(string_array_builder), &error);
    g_object_unref(int_array_builder);
    g_object_unref(string_array_builder);

    // Create Arrow table
    GArrowArray *arrays[] = {int_array, string_array};
    GArrowTable *table = garrow_table_new_arrays(final_schema, &arrays, 2, &error);

    // Create Parquet writer and write data
    GParquetWriterProperties *writer_properties = gparquet_writer_properties_new();
    GParquetArrowFileWriter *writer = gparquet_arrow_file_writer_new_path(final_schema, "./sample.parquet", writer_properties, &error);
    
    // Release references after table creation
    g_object_unref(int_array);
    g_object_unref(string_array);

    gboolean is_write_successfull = 
        gparquet_arrow_file_writer_write_table(writer, table, 10000, &error);
    gboolean file_closed = gparquet_arrow_file_writer_close(writer, &error);

    if (error != NULL) {
        g_printerr("Error creating Parquet writer: %s\n", error->message);
        g_error_free(error);
        return 1;
    }

    // Clean up resources
    g_object_unref(writer);
    g_object_unref(writer_properties);
    g_object_unref(table);

    return 0;
}