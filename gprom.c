#include <stdio.h>
#include "postgres.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"

#define AUDIT_LOG_TABLE "audit_log"

#define INSERT_AL_QUERY "INSERT INTO audit_log VALUES ($1, $2, $3)"
#define AL_QUERY_NARGS 3
#define AL_QUERY_OIDS (Oid[]){INT4OID, INT4OID, VARCHAROID}
#define AL_QUERY_DATA(snapshot, query) (Datum[]){Int32GetDatum(snapshot->xmin), Int32GetDatum(snapshot->xmax), CStringGetTextDatum(query)}

PG_MODULE_MAGIC;

/* Function Pointers for Hooks */

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static SPIPlanPtr plan = NULL;

/* Audit Logging Functionality */

static void
gprom_ExecutorEnd(QueryDesc *queryDesc)
{
    char *tbl = ((RangeTblEntry*)linitial(queryDesc->plannedstmt->rtable))->eref->aliasname;
    if (strcmp(tbl, AUDIT_LOG_TABLE))
    {
        SPI_connect(); // Reuse connection?
        if(!plan)
        {
            plan = SPI_prepare(INSERT_AL_QUERY, AL_QUERY_NARGS, AL_QUERY_OIDS);
            SPI_keepplan(plan);
        }
        SPI_execute_plan(plan, AL_QUERY_DATA(queryDesc->snapshot, queryDesc->sourceText), NULL, false, 0);
        SPI_finish();
    }
    
    // Hand control to prev/standard ExecutorEnd
    if (prev_ExecutorEnd)
        prev_ExecutorEnd(queryDesc);
    else
        standard_ExecutorEnd(queryDesc);
}

/* Snapshot-Specific Table Scan */

// This is a dummy function that the hook will check the query for, and if it is in the query,
// set the table scan snapshot to the argument in this function.
Datum 
snapshot(PG_FUNCTION_ARGS)
{
    int s = PG_GETARG_INT32(0);
    return Int32GetDatum(s);
}

PG_FUNCTION_INFO_V1(snapshot);

static void
gprom_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    List *tlist = queryDesc->plannedstmt->planTree->targetlist;
    ListCell *cell;
    
    foreach(cell, tlist) 
    {
        TargetEntry *tentr = (TargetEntry*)lfirst(cell);
        FuncExpr *fexpr = (FuncExpr*)tentr->expr;
        if(!strcmp(tentr->resname, "snapshot"))
        {
            elog(INFO, "%d", fexpr->args->elements)
            // Set estate snapshot to snapshot desired argument
        }
    }
    
    // Hand control to prev/standard ExecutorStart
    if (prev_ExecutorStart)
        prev_ExecutorStart(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
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
    if(plan)
        SPI_freeplan(plan);
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorEnd_hook = prev_ExecutorEnd;
}
