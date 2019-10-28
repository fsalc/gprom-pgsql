#include <stdio.h>
#include "postgres.h"
#include "access/tableam.h"
#include "access/relation.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "utils/snapmgr.h"

#define AUDIT_LOG_TABLE "audit_log"

#define INSERT_AL_QUERY "INSERT INTO audit_log VALUES ($1, $2, $3)"
#define AL_QUERY_NARGS 3
#define AL_QUERY_OIDS (Oid[]){INT4OID, INT4OID, VARCHAROID}
#define AL_QUERY_DATA(snapshot, query) (Datum[]){Int32GetDatum(snapshot->xmin), Int32GetDatum(snapshot->xmax), CStringGetTextDatum(query)}

PG_MODULE_MAGIC;

/* Function Pointers for Hooks */

static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static SPIPlanPtr al_plan = NULL; // Keeps SPI plan around to optimize audit log insertion

/* Audit Logging Functionality */
static void
gprom_ExecutorEnd(QueryDesc *queryDesc)
{
    char *tbl = ((RangeTblEntry*)linitial(queryDesc->plannedstmt->rtable))->eref->aliasname;
    if (strcmp(tbl, AUDIT_LOG_TABLE))
    {
        SPI_connect(); // Reuse connection?
        if(!al_plan)
        {
            al_plan = SPI_prepare(INSERT_AL_QUERY, AL_QUERY_NARGS, AL_QUERY_OIDS);
            SPI_keepplan(al_plan);
        }
        SPI_execute_plan(al_plan, AL_QUERY_DATA(queryDesc->snapshot, queryDesc->sourceText), NULL, false, 0);
        SPI_finish();
    }

    // Hand control to prev/standard ExecutorEnd
    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}

/* Snapshot-Specific Table Scan */

typedef struct snapshot_ctx_t {
    TableScanDesc scandesc;
} snapshot_ctx_t;

// SRF to run table scan
Datum
snapshot(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;

    if(SRF_IS_FIRSTCALL())
    {
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        funcctx->user_fctx = palloc(sizeof(snapshot_ctx_t));
        snapshot_ctx_t *snapshot_ctx = (snapshot_ctx_t *)funcctx->user_fctx;

        List *qnl = stringToQualifiedNameList(PG_GETARG_CSTRING(0));
        RangeVar *rv = makeRangeVarFromNameList(qnl);
        Relation rel = relation_openrv(rv, AccessShareLock);
		//funcctx->tuple_desc = BlessTupleDesc(CreateTupleDescCopy(RelationGetDescr(rel)));

        Snapshot newsnap = (Snapshot) MemoryContextAlloc(TopTransactionContext, sizeof(SnapshotData));
        memcpy(newsnap, GetActiveSnapshot(), sizeof(SnapshotData)); // shallow copy, very prone to breakage
        newsnap->regd_count = 0;
        newsnap->active_count = 0;
        newsnap->xmax = PG_GETARG_INT32(1);
        newsnap->xmin = PG_GETARG_INT32(1);

        snapshot_ctx->scandesc = table_beginscan(rel, newsnap, 0, NULL);

        MemoryContextSwitchTo(oldctx);
    }

    funcctx = SRF_PERCALL_SETUP();
    snapshot_ctx_t *snapshot_ctx = (snapshot_ctx_t *)funcctx->user_fctx;
    TupleTableSlot *slot = MakeTupleTableSlot(snapshot_ctx->scandesc->rs_rd->rd_att, &TTSOpsBufferHeapTuple);

    bool ip = table_scan_getnextslot(snapshot_ctx->scandesc, ForwardScanDirection, slot);
    if(!ip)
    {
        SRF_RETURN_DONE(funcctx);
    }
    else
    {
        HeapTuple tuple = slot->tts_ops->get_heap_tuple(slot);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
}

PG_FUNCTION_INFO_V1(snapshot);

/* PG Module Loading/Unloading */

void
_PG_init(void)
{
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = gprom_ExecutorEnd;
}

void
_PG_fini(void)
{
    if(al_plan)
        SPI_freeplan(al_plan);
    ExecutorEnd_hook = prev_ExecutorEnd;
}
