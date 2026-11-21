# Design decisions (ADRs)

Each decision has the same parts: **Context** (the problem), **Decision**, **Alternatives** we compared, and the
**Consequences** (what we gain and what we accept).

---

## ADR-01: Delta Lake lakehouse with Bronze, Silver and Gold

**Context.** We need a raw storage layer, a layered design, safe writes and a way to add columns later.
**Decision.** Delta tables on the local disk (ADLS Gen2 ready), organized as Bronze, Silver and Gold, with a quarantine table for each gate.
**Alternatives.** Plain Parquet (no transactions, no schema evolution), Apache Iceberg (more setup with Spark in Docker), loading straight into SQL Server (no raw layer, no replay).
**Consequences.** ACID writes (a failed run never leaves half a table), `mergeSchema` handled the new `request_source` column, idempotent writes for the stream. We accept that Delta needs Spark to read it.

## ADR-02: PySpark as the processing engine; DuckDB and PyArrow only for profiling

**Context.** 83 million rows, two schemas to unify, streaming, and Delta tables.
**Decision.** PySpark runs every pipeline job. PyArrow (footers) and DuckDB (SQL over Parquet) were used only to explore the data and design the rules.
**Alternatives.** pandas (does not fit 83 million rows comfortably), dbt alone (cannot do streaming or Delta writes), DuckDB for the pipeline (no Structured Streaming).
**Consequences.** One engine for batch and streaming. Exploration was fast because no cluster was needed.

## ADR-03: Spark in one container, local mode

**Context.** One laptop (6 cores, 32 GB RAM).
**Decision.** One Spark container with `local[*]` (6 threads in the heavy jobs), no master and workers.
**Alternatives.** A standalone cluster (three or more containers, no speed gain on one machine).
**Consequences.** Simple setup and easy demo. The same code runs on a real cluster without change.

## ADR-04: REST API, Kafka and Structured Streaming for the last month

**Context.** The Requirements ask for a second source type and recommend streaming.
**Decision.** 2026-09 is served by a FastAPI "dispatch" API, a producer sends it to Kafka (one broker, KRaft mode, 3 partitions for Yellow), and Spark Structured Streaming writes it to Bronze through Gate 1.
**Alternatives.** Reading the file in batch (no second source), a streaming source with no REST step.
**Consequences.** Real streaming ingestion. Silver and Gold stay incremental batch (micro-batch design): the data does not need second-level latency for our questions. Single broker and replication factor 1 because there is one machine.

## ADR-05: Safe, idempotent streaming

**Context.** An early producer resumed from the number of messages in a topic and sent 1.77 million trips twice.
**Decision.** The producer now stops if a topic is partly filled and skips it if complete. Stream writes use Delta `txnAppId` and `txnVersion`, so a repeated micro-batch is ignored.
**Alternatives.** Exactly-once with transactions (heavier), removing duplicates later in Silver only.
**Consequences.** No duplicates at the source and the sink. The cost is that a partly filled topic must be reset.

## ADR-06: Quarantine hard rules, flag soft rules

**Context.** Profiling showed rare impossible values (a 398,000 mile trip, a 5-million mph speed) and many unusual but real ones (negative refunds, zero distance).
**Decision.** A row that breaks a **hard** rule goes to quarantine with the reason. A row that breaks a **soft** rule stays and gets a flag in `data_quality_flags`. Limits come from physics and business logic, not from percentiles.
**Alternatives.** Percentile limits (they would reject a fixed share of valid rows), deleting negative amounts (would hide refunds and distort revenue).
**Consequences.** Only 0.036% of rows are quarantined and every reason count matches the profiling. Analysts can exclude flagged rows.

## ADR-07: Incremental loading with watermarks and slices

**Context.** Re-running the full load every time is slow and risky.
**Decision.** Bronze skips files already loaded. Silver and Gold process one ingestion slice at a time and store a watermark in `audit.watermarks` after each slice. The fact load into SQL Server works month by month.
**Alternatives.** Full reload (slow), change data capture (the source has no change log).
**Consequences.** A re-run with no new data takes seconds, and a crash resumes from the last finished slice.

## ADR-08: Serving layer design (SQL Server)

**Context.** 83 million fact rows, a laptop disk, and a dashboard that must feel fast.
**Decision.** SQL Server (the required warehouse) holds the four dimensions, two aggregate tables for the dashboard, and the full fact as a clustered columnstore with a 128-bit `trip_key`. The fact has no primary key (uniqueness is proven by a dbt test). Foreign keys link the fact and the aggregates to the dimensions. The transaction log is set to simple recovery.
**Alternatives.** Aggregates only (no trip-level detail in the warehouse), a row-store fact with a 64-character key (several times larger).
**Consequences.** 3.65 GB for 83 million rows and fast dashboards. We accept a hash key with a tiny collision chance, which the dbt `unique` test checks.

## ADR-09: dbt for marts and data tests

**Context.** We need business-ready views and automated tests on the warehouse.
**Decision.** dbt builds four views and runs unique, not-null, relationships, accepted-values and range tests.
**Alternatives.** Great Expectations (more setup, same coverage), hand-written SQL checks (no standard way to run and report them).
**Consequences.** The tests run inside the Airflow DAG. The 7 tests on the 83-million-row fact are tagged heavy and run on demand.

## ADR-10: Airflow standalone with its own virtual environments

**Context.** Orchestration with retries and alerts on one machine.
**Decision.** One Airflow container (`standalone`, SQLite, sequential executor). Airflow and dbt live in separate virtual environments inside the image so their packages cannot clash with Spark. The tasks call the same Python jobs.
**Alternatives.** Full Airflow with Postgres and workers (heavy), Prefect or Dagster (less common in the market).
**Consequences.** Light and easy to start. Not meant for production, and we say so in the README.

## ADR-11: Local first, Azure by configuration

**Context.** Cloud credits are limited and local iteration is faster.
**Decision.** All paths and credentials come from environment variables, so switching storage to Azure Data Lake Storage Gen2 is a configuration change.
**Alternatives.** Building in the cloud from day one.
**Consequences.** Lower cost during development. The Azure move and Terraform are designed but not executed (future work).
