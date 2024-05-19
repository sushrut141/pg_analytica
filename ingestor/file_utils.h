#ifndef _FILE_UTILS_H
#define _FILE_UTILS_H
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>

/*
 * Populates the root path for the extension data.
 * Assumes that buffer has PATH_MAX space available.
 */
void populate_root_path(char *out, bool relative) {
    if (!relative) {
        if (getcwd(out, PATH_MAX) == NULL) {
            perror("getcwd");
        }
    }
    strcat(out, "/pg_analytica/");
}

/*
 * Populates the data path for columnar files for the supplied table in out.
 * Assumes that buffer has PATH_MAX space available.
 */
void populate_data_path_for_table(const char *table, char *out, bool relative) {
    populate_root_path(out, relative);
    strcat(out, table);
}

/*
 * Populates the temp path for columnar files for the supplied table in out.
 * Assumes that buffer has PATH_MAX space available.
 */
void populate_temp_path_for_table(const char *table, char *out, bool relative) {
    // TODO - getcwd does not work here.
    populate_data_path_for_table(table, out, relative);
    strcat(out, "/temp");
}

#endif