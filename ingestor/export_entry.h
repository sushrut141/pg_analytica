#ifndef _EXPORT_ENTRY_H
#define _EXPORT_ENTRY_H

#include <string.h>

#include "utils/elog.h"
#include "utils/palloc.h"

/**
 * Detail about table to be processed for export.
 * Callers are responsible for freeing memory of individual fields.
 */
typedef struct _ExportEntry {
	char *table_name;
	char **columns_to_export;
	int num_of_columns;
} ExportEntry;

void initialize_export_entry(const char *table_name, int num_of_columns, ExportEntry *entry) {
    // Initialize memory for column names.
    entry->num_of_columns = num_of_columns;
    entry->columns_to_export = (char**)palloc(num_of_columns * sizeof(char*));
    // Initialize memory and set table name.
    entry->table_name = (char*)palloc(strlen(table_name) * sizeof(char));
    strcpy(entry->table_name, table_name);
}

void export_entry_add_column(ExportEntry *entry, char *column_name, int column_num) {
    size_t column_name_size = strlen(column_name);
    entry->columns_to_export[column_num] = (char*)palloc(column_name_size*sizeof(char));
    strcpy(entry->columns_to_export[column_num], column_name);
}

void free_export_entry(ExportEntry *entry) {
    pfree(entry->table_name);
    elog(LOG, "Freed entry table name");
    for (int i = 0; i < entry->num_of_columns; i += 1) {
        elog(LOG, "Freeing column name %s", entry->columns_to_export[i]);
        pfree(entry->columns_to_export[i]);
    }
    elog(LOG, "Freed column names");
    pfree(entry->columns_to_export);
    elog(LOG, "Freed column name container");
}

#endif