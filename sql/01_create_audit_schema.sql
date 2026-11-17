-- Phase 2, Step 2.2: audit schema for run and step logging.
-- Run in SSMS against the database nyc_taxi_dw.

Use nyc_taxi_dw;
Go

-- A schema is a folder inside the database. All our logging tables live in "audit".
Create Schema audit;
Go

-- One row per pipeline run.
Create Table audit.pipeline_runs (
    run_id         Uniqueidentifier Not Null Primary Key,  -- unique id created by the job
    pipeline_name  Varchar(100)     Not Null,
    started_at     Datetime2        Not Null,
    finished_at    Datetime2        Null,                  -- stays empty while RUNNING
    status         Varchar(20)      Not Null,
    triggered_by   Varchar(50)      Null,                  -- manual or airflow
    Constraint ck_pipeline_runs_status
        Check (status In ('RUNNING', 'SUCCESS', 'FAILED'))
);
Go

-- One row per step inside a run (for example: Bronze load of one file).
Create Table audit.step_logs (
    step_log_id         Bigint Identity(1, 1) Primary Key,
    run_id              Uniqueidentifier Not Null
        References audit.pipeline_runs (run_id),           -- links the step to its run
    layer_name          Varchar(20)  Not Null,             -- bronze, silver or gold
    source_name         Varchar(200) Not Null,             -- for example yellow_tripdata_2025-01
    rows_read           Bigint       Null,
    rows_accepted       Bigint       Null,
    rows_quarantined    Bigint       Null,
    execution_time_sec  Decimal(10, 2) Null,
    status              Varchar(20)  Not Null,
    error_message       Nvarchar(2000) Null,
    logged_at           Datetime2    Not Null Default sysutcdatetime(),
    Constraint ck_step_logs_status
        Check (status In ('SUCCESS', 'FAILED', 'SKIPPED'))
);
Go

-- Check: both tables should be listed.
Select table_schema, table_name
From information_schema.tables
Where table_schema = 'audit';
Go