"""
Helper functions that write to the audit schema in SQL Server.

Every job calls these functions to record what it did:
  start_run()  -> one row in audit.pipeline_runs (status RUNNING)
  log_step()   -> one row in audit.step_logs (rows read, accepted, quarantined...)
  finish_run() -> closes the run (status SUCCESS or FAILED)

Connection details come from environment variables (.env), never from the code.
"""

import os
import uuid

import pymssql


def get_connection():
    """Open a new connection to SQL Server using the variables from .env."""
    return pymssql.connect(
        server=os.environ["SQLSERVER_HOST"],
        port=int(os.environ["SQLSERVER_PORT"]),
        user=os.environ["SQLSERVER_USER"],
        password=os.environ["SQLSERVER_PASSWORD"],
        database=os.environ["SQLSERVER_DB"],
    )


def _execute(sql, params):
    """Run one INSERT or UPDATE and save it (commit). Opens and closes its own connection."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        # %s placeholders let the driver pass values safely (no string formatting in SQL)
        cursor.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def start_run(pipeline_name, triggered_by="manual"):
    """Create a new run and return its run_id."""
    run_id = str(uuid.uuid4())
    _execute(
        "Insert Into audit.pipeline_runs "
        "(run_id, pipeline_name, started_at, status, triggered_by) "
        "Values (%s, %s, sysutcdatetime(), 'RUNNING', %s)",
        (run_id, pipeline_name, triggered_by),
    )
    return run_id


def log_step(run_id, layer_name, source_name, rows_read, rows_accepted,
             rows_quarantined, execution_time_sec, status, error_message=None):
    """Record the result of one step (for example: Bronze load of one file)."""
    _execute(
        "Insert Into audit.step_logs "
        "(run_id, layer_name, source_name, rows_read, rows_accepted, "
        " rows_quarantined, execution_time_sec, status, error_message) "
        "Values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (run_id, layer_name, source_name, rows_read, rows_accepted,
         rows_quarantined, execution_time_sec, status, error_message),
    )


def finish_run(run_id, status):
    """Close the run with its final status: SUCCESS or FAILED."""
    _execute(
        "Update audit.pipeline_runs "
        "Set finished_at = sysutcdatetime(), status = %s Where run_id = %s",
        (status, run_id),
    )
    
    
def get_watermark(source_table):
    """Return the last processed ingestion time (microseconds) or None if never processed."""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("Select last_ingestion_us From audit.watermarks "
                       "Where source_table = %s", (source_table,))
        row = cursor.fetchone()
        return int(row[0]) if row else None
    finally:
        conn.close()


def set_watermark(source_table, last_ingestion_us):
    """Create or update the watermark of one source table."""
    _execute(
        "Merge audit.watermarks As t "
        "Using (Select %s As source_table, %s As us) As s "
        "On t.source_table = s.source_table "
        "When Matched Then Update Set last_ingestion_us = s.us, updated_at = sysutcdatetime() "
        "When Not Matched Then Insert (source_table, last_ingestion_us) "
        "Values (s.source_table, s.us);",
        (source_table, int(last_ingestion_us)),
    )