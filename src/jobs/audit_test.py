"""
Connection test: writes one fake run and one fake step to the audit tables,
reads them back, then deletes them so the audit tables stay clean.
Run with:  docker compose run --rm spark python -u src/jobs/audit_test.py
"""

from common import audit

# 1) Write a fake run, a fake step, and close the run
run_id = audit.start_run("audit_connection_test")
print("run_id:", run_id)
audit.log_step(run_id, "bronze", "fake_source", 100, 90, 10, 1.5, "SUCCESS")
audit.finish_run(run_id, "SUCCESS")

# 2) Read them back to prove the data really reached SQL Server
conn = audit.get_connection()
cursor = conn.cursor()
cursor.execute("Select status From audit.pipeline_runs Where run_id = %s", (run_id,))
print("run status in SQL Server:", cursor.fetchone()[0], "(expected SUCCESS)")
cursor.execute("Select count(*) From audit.step_logs Where run_id = %s", (run_id,))
print("step rows in SQL Server:", cursor.fetchone()[0], "(expected 1)")

# 3) Clean up: delete the child rows first, then the parent row
cursor.execute("Delete From audit.step_logs Where run_id = %s", (run_id,))
cursor.execute("Delete From audit.pipeline_runs Where run_id = %s", (run_id,))
conn.commit()
conn.close()
print("cleanup done")