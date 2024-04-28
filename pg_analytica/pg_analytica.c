/**
 * Table Access Method definition for storing data in columnar format.
*/
#include "postgres.h"


#include "access/tableam.h"
#include "utils/builtins.h"

FILE* fd;
#define DEBUG_FUNC() fprintf(fd, "in %s\n", __func__);


static const TupleTableSlotOps* memam_slot_callbacks(
  Relation relation
) {
	DEBUG_FUNC();
  return &TTSOpsVirtual;
}

static TableScanDesc memam_beginscan(
  Relation relation,
  Snapshot snapshot,
  int nkeys,
  struct ScanKeyData *key,
  ParallelTableScanDesc parallel_scan,
  uint32 flags
) {
	DEBUG_FUNC();
  return NULL;
}

static void memam_rescan(
  TableScanDesc sscan,
  struct ScanKeyData *key,
  bool set_params,
  bool allow_strat,
  bool allow_sync,
  bool allow_pagemode
) {
	DEBUG_FUNC();
}

static void memam_endscan(TableScanDesc sscan) {
	DEBUG_FUNC();
}

static bool memam_getnextslot(
  TableScanDesc sscan,
  ScanDirection direction,
  TupleTableSlot *slot
) {
	DEBUG_FUNC();
  return false;
}

static IndexFetchTableData* memam_index_fetch_begin(Relation rel) {
	DEBUG_FUNC();
  return NULL;
}

static void memam_index_fetch_reset(IndexFetchTableData *scan) {
	DEBUG_FUNC();
}

static void memam_index_fetch_end(IndexFetchTableData *scan) {
	DEBUG_FUNC();
}

static bool memam_index_fetch_tuple(
  struct IndexFetchTableData *scan,
  ItemPointer tid,
  Snapshot snapshot,
  TupleTableSlot *slot,
  bool *call_again,
  bool *all_dead
) {
	DEBUG_FUNC();
  return false;
}

static void memam_tuple_insert_speculative(
  Relation relation,
  TupleTableSlot *slot,
  CommandId cid,
  int options,
  struct BulkInsertStateData* bistate,
  uint32 specToken) {
	DEBUG_FUNC();
}

static void memam_tuple_complete_speculative(
  Relation relation,
  TupleTableSlot *slot,
  uint32 specToken,
  bool succeeded) {
	DEBUG_FUNC();
}

static void memam_multi_insert(
  Relation relation,
  TupleTableSlot **slots,
  int nslots,
  CommandId cid,
  int options,
  struct BulkInsertStateData* bistate
) {
	DEBUG_FUNC();
}

static TM_Result memam_tuple_delete(
  Relation relation,
  ItemPointer tid,
  CommandId cid,
  Snapshot snapshot,
  Snapshot crosscheck,
  bool wait,
  TM_FailureData *tmfd,
  bool changingPart
) {
	DEBUG_FUNC();
  TM_Result result = TM_Ok;
  return result;
}

static TM_Result memam_tuple_update(
  Relation relation,
  ItemPointer otid,
  TupleTableSlot *slot,
  CommandId cid,
  Snapshot snapshot,
  Snapshot crosscheck,
  bool wait,
  TM_FailureData *tmfd,
  LockTupleMode *lockmode,
  bool *update_indexes
) {
	DEBUG_FUNC();
  TM_Result result = TM_Ok;
  return result;
}

static TM_Result memam_tuple_lock(
  Relation relation,
  ItemPointer tid,
  Snapshot snapshot,
  TupleTableSlot *slot,
  CommandId cid,
  LockTupleMode mode,
  LockWaitPolicy wait_policy,
  uint8 flags,
  TM_FailureData *tmfd)
{
	DEBUG_FUNC();
  TM_Result result = TM_Ok;
  return result;
}

static bool memam_fetch_row_version(
  Relation relation,
  ItemPointer tid,
  Snapshot snapshot,
  TupleTableSlot *slot
) {
	DEBUG_FUNC();
  return false;
}

static void memam_get_latest_tid(
  TableScanDesc sscan,
  ItemPointer tid
) {
	DEBUG_FUNC();
}

static bool memam_tuple_tid_valid(TableScanDesc scan, ItemPointer tid) {
	DEBUG_FUNC();
  return false;
}

static bool memam_tuple_satisfies_snapshot(
  Relation rel,
  TupleTableSlot *slot,
  Snapshot snapshot
) {
	DEBUG_FUNC();
  return false;
}

static TransactionId memam_index_delete_tuples(
  Relation rel,
  TM_IndexDeleteOp *delstate
) {
	DEBUG_FUNC();
  TransactionId id = 0;
  return id;
}

static void memam_relation_nontransactional_truncate(
  Relation rel
) {
	DEBUG_FUNC();
}

static void memam_relation_copy_data(
  Relation rel,
  const RelFileNode *newrnode
) {
	DEBUG_FUNC();
}

static void memam_relation_copy_for_cluster(Relation NewTable,
	Relation OldTable,
	Relation OldIndex,
	bool use_sort,
	TransactionId OldestXmin,
	TransactionId *xid_cutoff,
	MultiXactId *multi_cutoff,
	double *num_tuples,
	double *tups_vacuumed,
	double *tups_recently_dead) {
	DEBUG_FUNC();
}

static void memam_vacuum_rel(
  Relation rel,
  struct VacuumParams *params,
  BufferAccessStrategy bstrategy
) {
	DEBUG_FUNC();
}

static bool memam_scan_analyze_next_block(
  TableScanDesc scan,
  BlockNumber blockno,
  BufferAccessStrategy bstrategy
) {
	DEBUG_FUNC();
  return false;
}

static bool memam_scan_analyze_next_tuple(
  TableScanDesc scan,
  TransactionId OldestXmin,
  double *liverows,
  double *deadrows,
  TupleTableSlot *slot
) {
	DEBUG_FUNC();
  return false;
}

static double memam_index_build_range_scan(
	Relation table_rel,
	Relation index_rel,
	struct IndexInfo *index_info,
	bool allow_sync,
	bool anyvisible,
	bool progress,
	BlockNumber start_blockno,
	BlockNumber numblocks,
	IndexBuildCallback callback,
	void *callback_state,
	TableScanDesc scan) {
	DEBUG_FUNC();
  return 0;
}

static void memam_index_validate_scan(
	Relation table_rel,
	Relation index_rel,
	struct IndexInfo *index_info,
	Snapshot snapshot,
	struct ValidateIndexState *state) {
	DEBUG_FUNC();
}

static bool memam_relation_needs_toast_table(Relation rel) {
	DEBUG_FUNC();
  return false;
}

static Oid memam_relation_toast_am(Relation rel) {
	DEBUG_FUNC();
  Oid oid = 0;
  return oid;
}

static void memam_fetch_toast_slice(
  Relation toastrel,
  Oid valueid,
  int32 attrsize,
  int32 sliceoffset,
  int32 slicelength,
  struct varlena *result
) {
	DEBUG_FUNC();
}

static void memam_estimate_rel_size(
  Relation rel,
  int32 *attr_widths,
  BlockNumber *pages,
  double *tuples,
  double *allvisfrac
) {
	DEBUG_FUNC();
}

static bool memam_scan_sample_next_block(
  TableScanDesc scan, struct SampleScanState *scanstate
) {
	DEBUG_FUNC();
  return false;
}

static bool memam_scan_sample_next_tuple(
  TableScanDesc scan,
  struct SampleScanState *scanstate,
  TupleTableSlot *slot
) {
	DEBUG_FUNC();
  return false;
}

static Size parallelscan_estimate (Relation rel) {
	DEBUG_FUNC();
	return 10000;
}

static Size parallelscan_initialize(Relation rel,
									ParallelTableScanDesc pscan) {
	DEBUG_FUNC();
	return 10000;									
}

static void parallelscan_reinitialize(Relation rel,
									ParallelTableScanDesc pscan) {
	DEBUG_FUNC();
}

static uint64 relation_size (Relation rel, ForkNumber forkNumber) {
	DEBUG_FUNC();
	return 10000;
}

static void memam_tuple_insert(
  Relation relation,
  TupleTableSlot *slot,
  CommandId cid,
  int options,
  struct BulkInsertStateData* bistate
) {
	fprintf(fd, "in %s\n", __func__);
	fprintf(fd, "reading from table %s \n", relation->rd_rel->relname.data);
	fprintf(fd, "found %d attributes in tuple descriptor \n", slot->tts_tupleDescriptor->natts);

	bool shouldFree = true;
	HeapTuple	tuple = ExecFetchSlotHeapTuple(slot, true, &shouldFree);
	int num_of_attributes = slot->tts_tupleDescriptor->natts;
	fprintf(fd, "Iterating over %d columns \n", num_of_attributes);

	fprintf(fd, "Row ");
	for (int i = 0; i < num_of_attributes; i += 1) {
		switch (slot->tts_tupleDescriptor->attrs[i].atttypid) {
			case INT2OID:
			case INT4OID:
			case INT8OID:
			{
				bool isnull;
				Datum column_data = heap_getattr(tuple, i + 1, slot->tts_tupleDescriptor, &isnull);
				if (!isnull) {
					int column_value = DatumGetUInt32(column_data);
					fprintf(fd, "%d |", column_value);
				} else {
					fprintf(fd, "null |");
				}
				break;
			}
			case VARCHAROID:
			{
				bool isnull;
				Datum column_data = heap_getattr(tuple, i + 1, slot->tts_tupleDescriptor, &isnull);
				if (!isnull) {
					char* column_value = TextDatumGetCString(column_data);
					fprintf(fd, "%s |", column_value);
				} else {
					fprintf(fd, "null |");
				}
				break;
			}
			default:
				fprintf(fd, "found some other type with name %s\n", slot->tts_tupleDescriptor->attrs[i].attname.data);
				break;
		}	
	}
	if (shouldFree)
		pfree(tuple);
	fprintf(fd, "\n");
	fprintf(fd, "end of function %s\n", __func__);
}

static void memam_relation_set_new_filelocator(
  Relation rel,
  const RelFileNode *newrlocator,
  char persistence,
  TransactionId *freezeXid,
  MultiXactId *minmulti
) {
	Assert(persistence == RELPERSISTENCE_PERMANENT);
	DEBUG_FUNC();
}


const TableAmRoutine customam_methods = {
  .type = T_TableAmRoutine,

  .slot_callbacks = memam_slot_callbacks,

  .scan_begin = memam_beginscan,
  .scan_end = memam_endscan,
  .scan_rescan = memam_rescan,
  .scan_getnextslot = memam_getnextslot,

  .parallelscan_estimate = parallelscan_estimate,
  .parallelscan_initialize = parallelscan_initialize,
  .parallelscan_reinitialize = parallelscan_reinitialize,

  .index_fetch_begin = memam_index_fetch_begin,
  .index_fetch_reset = memam_index_fetch_reset,
  .index_fetch_end = memam_index_fetch_end,
  .index_fetch_tuple = memam_index_fetch_tuple,

  .tuple_insert = memam_tuple_insert,
  .tuple_insert_speculative = memam_tuple_insert_speculative,
  .tuple_complete_speculative = memam_tuple_complete_speculative,
  .multi_insert = memam_multi_insert,
  .tuple_delete = memam_tuple_delete,
  .tuple_update = memam_tuple_update,
  .tuple_lock = memam_tuple_lock,

  .tuple_fetch_row_version = memam_fetch_row_version,
  .tuple_get_latest_tid = memam_get_latest_tid,
  .tuple_tid_valid = memam_tuple_tid_valid,
  .tuple_satisfies_snapshot = memam_tuple_satisfies_snapshot,
  .index_delete_tuples = memam_index_delete_tuples,

  .relation_set_new_filenode = memam_relation_set_new_filelocator,
  .relation_nontransactional_truncate = memam_relation_nontransactional_truncate,
  .relation_copy_data = memam_relation_copy_data,
  .relation_copy_for_cluster = memam_relation_copy_for_cluster,
  .relation_vacuum = memam_vacuum_rel,
  .scan_analyze_next_block = memam_scan_analyze_next_block,
  .scan_analyze_next_tuple = memam_scan_analyze_next_tuple,
  .index_build_range_scan = memam_index_build_range_scan,
  .index_validate_scan = memam_index_validate_scan,

  .relation_size = relation_size,
  .relation_needs_toast_table = memam_relation_needs_toast_table,
  .relation_toast_am = memam_relation_toast_am,
  .relation_fetch_toast_slice = memam_fetch_toast_slice,

  .relation_estimate_size = memam_estimate_rel_size,

  .scan_sample_next_block = memam_scan_sample_next_block,
  .scan_sample_next_tuple = memam_scan_sample_next_tuple
};

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_analytica_tableam_handler);

Datum pg_analytica_tableam_handler(PG_FUNCTION_ARGS) {
  // Init logging.
  fd = fopen("/tmp/pg_analytica.log", "a");
  setvbuf(fd, NULL, _IONBF, 0); 
  fprintf(fd, "\n\npg_analytica_tableam_handler loaded\n");

  PG_RETURN_POINTER(&customam_methods);
}