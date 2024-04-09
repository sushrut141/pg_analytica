#include "postgres.h"
#include "fmgr.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "nodes/execnodes.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "utils/builtins.h"
#include "executor/tuptable.h"

PG_MODULE_MAGIC;

const TableAmRoutine memam_methods;

static const TupleTableSlotOps* memam_slot_callbacks(
  Relation relation
) {
  return NULL;
}

static TableScanDesc memam_beginscan(
  Relation relation,
  Snapshot snapshot,
  int nkeys,
  struct ScanKeyData *key,
  ParallelTableScanDesc parallel_scan,
  uint32 flags
) {
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
}

static void memam_endscan(TableScanDesc sscan) {
}

static bool memam_getnextslot(
  TableScanDesc sscan,
  ScanDirection direction,
  TupleTableSlot *slot
) {
  return false;
}

static IndexFetchTableData* memam_index_fetch_begin(Relation rel) {
  return NULL;
}

static void memam_index_fetch_reset(IndexFetchTableData *scan) {
}

static void memam_index_fetch_end(IndexFetchTableData *scan) {
}

static bool memam_index_fetch_tuple(
  struct IndexFetchTableData *scan,
  ItemPointer tid,
  Snapshot snapshot,
  TupleTableSlot *slot,
  bool *call_again,
  bool *all_dead
) {
  return false;
}

static void memam_tuple_insert(
  Relation relation,
  TupleTableSlot *slot,
  CommandId cid,
  int options,
  BulkInsertState bistate
) {
}

static void memam_tuple_insert_speculative(
  Relation relation,
  TupleTableSlot *slot,
  CommandId cid,
  int options,
  BulkInsertState bistate,
  uint32 specToken) {
}

static void memam_tuple_complete_speculative(
  Relation relation,
  TupleTableSlot *slot,
  uint32 specToken,
  bool succeeded) {
}

static void memam_multi_insert(
  Relation relation,
  TupleTableSlot **slots,
  int ntuples,
  CommandId cid,
  int options,
  BulkInsertState bistate
) {
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
  TM_Result result = {};
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
  TU_UpdateIndexes *update_indexes
) {
  TM_Result result = {};
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
  TM_Result result = {};
  return result;
}

static bool memam_fetch_row_version(
  Relation relation,
  ItemPointer tid,
  Snapshot snapshot,
  TupleTableSlot *slot
) {
  return false;
}

static void memam_get_latest_tid(
  TableScanDesc sscan,
  ItemPointer tid
) {
}

static bool memam_tuple_tid_valid(TableScanDesc scan, ItemPointer tid) {
  return false;
}

static bool memam_tuple_satisfies_snapshot(
  Relation rel,
  TupleTableSlot *slot,
  Snapshot snapshot
) {
  return false;
}

static TransactionId memam_index_delete_tuples(
  Relation rel,
  TM_IndexDeleteOp *delstate
) {
  TransactionId id = {};
  return id;
}

static void memam_relation_set_new_filelocator(
  Relation rel,
  const RelFileLocator *newrlocator,
  char persistence,
  TransactionId *freezeXid,
  MultiXactId *minmulti
) {
}

static void memam_relation_nontransactional_truncate(
  Relation rel
) {
}

static void memam_relation_copy_data(
  Relation rel,
  const RelFileLocator *newrlocator
) {
}

static void memam_relation_copy_for_cluster(
  Relation OldHeap,
  Relation NewHeap,
  Relation OldIndex,
  bool use_sort,
  TransactionId OldestXmin,
  TransactionId *xid_cutoff,
  MultiXactId *multi_cutoff,
  double *num_tuples,
  double *tups_vacuumed,
  double *tups_recently_dead
) {
}

static void memam_vacuum_rel(
  Relation rel,
  VacuumParams *params,
  BufferAccessStrategy bstrategy
) {
}

static bool memam_scan_analyze_next_block(
  TableScanDesc scan,
  BlockNumber blockno,
  BufferAccessStrategy bstrategy
) {
  return false;
}

static bool memam_scan_analyze_next_tuple(
  TableScanDesc scan,
  TransactionId OldestXmin,
  double *liverows,
  double *deadrows,
  TupleTableSlot *slot
) {
  return false;
}

static double memam_index_build_range_scan(
  Relation heapRelation,
  Relation indexRelation,
  IndexInfo *indexInfo,
  bool allow_sync,
  bool anyvisible,
  bool progress,
  BlockNumber start_blockno,
  BlockNumber numblocks,
  IndexBuildCallback callback,
  void *callback_state,
  TableScanDesc scan
) {
  return 0;
}

static void memam_index_validate_scan(
  Relation heapRelation,
  Relation indexRelation,
  IndexInfo *indexInfo,
  Snapshot snapshot,
  ValidateIndexState *state
) {
}

static bool memam_relation_needs_toast_table(Relation rel) {
  return false;
}

static Oid memam_relation_toast_am(Relation rel) {
  Oid oid = {};
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
}

static void memam_estimate_rel_size(
  Relation rel,
  int32 *attr_widths,
  BlockNumber *pages,
  double *tuples,
  double *allvisfrac
) {
}

static bool memam_scan_sample_next_block(
  TableScanDesc scan, SampleScanState *scanstate
) {
  return false;
}

static bool memam_scan_sample_next_tuple(
  TableScanDesc scan,
  SampleScanState *scanstate,
  TupleTableSlot *slot
) {
  return false;
}

const TableAmRoutine memam_methods = {
  .type = T_TableAmRoutine,

  .slot_callbacks = memam_slot_callbacks,

  .scan_begin = memam_beginscan,
  .scan_end = memam_endscan,
  .scan_rescan = memam_rescan,
  .scan_getnextslot = memam_getnextslot,

  .parallelscan_estimate = table_block_parallelscan_estimate,
  .parallelscan_initialize = table_block_parallelscan_initialize,
  .parallelscan_reinitialize = table_block_parallelscan_reinitialize,

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

  .relation_set_new_filelocator = memam_relation_set_new_filelocator,
  .relation_nontransactional_truncate = memam_relation_nontransactional_truncate,
  .relation_copy_data = memam_relation_copy_data,
  .relation_copy_for_cluster = memam_relation_copy_for_cluster,
  .relation_vacuum = memam_vacuum_rel,
  .scan_analyze_next_block = memam_scan_analyze_next_block,
  .scan_analyze_next_tuple = memam_scan_analyze_next_tuple,
  .index_build_range_scan = memam_index_build_range_scan,
  .index_validate_scan = memam_index_validate_scan,

  .relation_size = table_block_relation_size,
  .relation_needs_toast_table = memam_relation_needs_toast_table,
  .relation_toast_am = memam_relation_toast_am,
  .relation_fetch_toast_slice = memam_fetch_toast_slice,

  .relation_estimate_size = memam_estimate_rel_size,

  .scan_sample_next_block = memam_scan_sample_next_block,
  .scan_sample_next_tuple = memam_scan_sample_next_tuple
};

PG_FUNCTION_INFO_V1(mem_tableam_handler);

Datum mem_tableam_handler(PG_FUNCTION_ARGS) {
  PG_RETURN_POINTER(&memam_methods);
}