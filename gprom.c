#include <stdio.h>
#include "postgres.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"

#define AUDIT_LOG_TABLE "audit_log"

#define INSERT_AL_QUERY "INSERT INTO audit_log VALUES ($1, $2, $3)"
#define AL_QUERY_NARGS 3
#define AL_QUERY_OIDS (Oid[]){INT4OID, INT4OID, VARCHAROID}
#define AL_QUERY_DATA(snapshot, query) (Datum[]){Int32GetDatum(snapshot->xmin), Int32GetDatum(snapshot->xmax), CStringGetTextDatum(query)}

PG_MODULE_MAGIC;

/* Function Pointers for Hooks */

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
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

// Walk plan to look for this function 
Datum
snapshot(PG_FUNCTION_ARGS)
{
    return Int32GetDatum(PG_GETARG_INT32(0));
}

PG_FUNCTION_INFO_V1(snapshot);

typedef struct snapshot_walker_ctx {
    int xact;
} snapshot_walker_ctx;

// Walker to find this function
bool
snapshot_walker(Node *node, void *ctx)
{
    if (node == NULL)
        return false;
    
    // TODO TODO TODO Not hardcode funcid
    if (is_funcclause(node) && ((FuncExpr *)node)->funcid == 41024) {
        Const *c = (Const *)linitial(((FuncExpr *)node)->args);
        ((snapshot_walker_ctx *)ctx)->xact = DatumGetInt32(c->constvalue);
        return true;
    }
        
    return expression_tree_walker(node, snapshot_walker, ctx);
}

// Walker to find SeqScanStates
bool
seqscanstate_walker(PlanState *node, void *ctx) 
{
    if (node == NULL)
        return false;
        
    if (IsA(node, SeqScanState))
        return true;
        
    return planstate_tree_walker(node, seqscanstate_walker, ctx);
}

static void
gprom_ExecutorStart(QueryDesc *queryDesc, int eflags)
{   
    // run standard executorstart to initialize planstate tree
    standard_ExecutorStart(queryDesc, eflags);
    
    // find snapshot function and get argument
    snapshot_walker_ctx *ssctx = palloc(sizeof(snapshot_walker_ctx));
    bool ss = expression_tree_walker((Node *)queryDesc->plannedstmt->planTree->targetlist, snapshot_walker, ssctx);
    if (ss) {
        SeqScanState *node = NULL;
        // find seqscanstate and modify its estate's snapshot
        // root planstate is not visited by the walker, check top level before descending
        if (IsA(queryDesc->planstate, SeqScanState))
        {
            SeqScanState *node = (SeqScanState *)queryDesc->planstate;
            node->ss.ps.state->es_snapshot->xmin = ssctx->xact;
            node->ss.ps.state->es_snapshot->xmax = ssctx->xact;
        } else {
            bool t = planstate_tree_walker(queryDesc->planstate, seqscanstate_walker, NULL);
        }
    }
    
    // Hand control to prev/standard ExecutorStart
    pfree(ssctx);
    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
}

/* PG Module Loading/Unloading */

void
_PG_init(void)
{
    prev_ExecutorStart = ExecutorStart_hook;
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorStart_hook = gprom_ExecutorStart;
    ExecutorEnd_hook = gprom_ExecutorEnd;
}

void
_PG_fini(void)
{
    if(al_plan)
        SPI_freeplan(al_plan);
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorEnd_hook = prev_ExecutorEnd;
}
