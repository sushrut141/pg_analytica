/*-------------------------------------------------------------------------
 *
 * heapam_handler.c
 *	  heap table access method code
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_handler.c
 *
 *
 * NOTES
 *	  This files wires up the lower level heapam.c et al routines with the
 *	  tableam abstraction.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"


#include "access/tableam.h"

static const TableAmRoutine customam_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = NULL,

	.scan_begin = NULL,
	.scan_end = NULL,
	.scan_rescan = NULL,
	.scan_getnextslot = NULL,

	.scan_set_tidrange = NULL,
	.scan_getnextslot_tidrange = NULL,

	.parallelscan_estimate = NULL,
	.parallelscan_initialize = NULL,
	.parallelscan_reinitialize = NULL,

	.index_fetch_begin = NULL,
	.index_fetch_reset = NULL,
	.index_fetch_end = NULL,
	.index_fetch_tuple = NULL,

	.tuple_insert = NULL,
	.tuple_insert_speculative = NULL,
	.tuple_complete_speculative = NULL,
	.multi_insert = NULL,
	.tuple_delete = NULL,
	.tuple_update = NULL,
	.tuple_lock = NULL,

	.tuple_fetch_row_version = NULL,
	.tuple_get_latest_tid = NULL,
	.tuple_tid_valid = NULL,
	.tuple_satisfies_snapshot = NULL,
	.index_delete_tuples = NULL,

	.relation_set_new_filenode = NULL,
	.relation_nontransactional_truncate = NULL,
	.relation_copy_data = NULL,
	.relation_copy_for_cluster = NULL,
	.relation_vacuum = NULL,
	.scan_analyze_next_block = NULL,
	.scan_analyze_next_tuple = NULL,
	.index_build_range_scan = NULL,
	.index_validate_scan = NULL,

	.relation_size = NULL,
	.relation_needs_toast_table = NULL,
	.relation_toast_am = NULL,
	.relation_fetch_toast_slice = NULL,

	.relation_estimate_size = NULL,

	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
	.scan_sample_next_block = NULL,
	.scan_sample_next_tuple = NULL
};

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(custom_tableam_handler);

Datum custom_tableam_handler(PG_FUNCTION_ARGS) {
  PG_RETURN_POINTER(&customam_methods);
}