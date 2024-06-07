#ifndef _FILE_UTILS_H
#define _FILE_UTILS_H
#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "postgres.h"

/*
 * Populates the root path for the extension data.
 * Assumes that buffer has PATH_MAX space available.
 */
void populate_root_path(char *out, bool relative) {
  if (!relative) {
    if (getcwd(out, PATH_MAX) == NULL) {
      perror("getcwd");
    }
  } else {
    strcat(out, ".");
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

static void delete_files_in_directory(const char *path) {
  DIR *dir = opendir(path);
  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    char filepath[PATH_MAX];
    snprintf(filepath, sizeof(filepath), "%s/%s", path, entry->d_name);

    // Skip special entries (".", "..")
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    if (unlink(filepath) == -1) {
      elog(LOG, "Failed to delete file %s", filepath);
      perror("unlink");
    }
  }
  closedir(dir);
}

int cleanup_table_data(const char *table_name) {
  char temp_path[PATH_MAX];
  char data_path[PATH_MAX];
  populate_temp_path_for_table(table_name, temp_path, /*relative=*/false);
  populate_data_path_for_table(table_name, data_path, /*relative=*/false);

  // delete temp files if they exist
  delete_files_in_directory(temp_path);
  // delete temp directory
  if (rmdir(temp_path) != 0) {
    elog(LOG, "Failed to delete temp data directory %s", temp_path);
    perror("rmdir");
    return 1;
  }
  // delete main data files
  delete_files_in_directory(data_path);
  // delete data directory
  if (rmdir(data_path) != 0) {
    elog(LOG, "Failed to delete temp data file %s", data_path);
    perror("rmdir");
    return 1;
  }
  return 0;
}

#endif