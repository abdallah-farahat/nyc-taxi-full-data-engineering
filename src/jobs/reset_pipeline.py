"""
Wipes the lakehouse (Bronze, Silver, Gold, quarantine, checkpoints) and the Silver/Gold
watermarks, so the whole pipeline can be tested from zero.
audit.pipeline_runs and audit.step_logs are KEPT (they are our run history).
Needs CONFIRM_RESET=yes, so it can never run by accident.
"""
import os
import shutil
import sys

from common import audit

if os.getenv("CONFIRM_RESET") != "yes":
    sys.exit("Refusing to reset: set CONFIRM_RESET=yes")

lakehouse = os.environ["LAKEHOUSE_PATH"]
for name in os.listdir(lakehouse):
    path = os.path.join(lakehouse, name)
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
    else:
        os.remove(path)

audit._execute("Delete From audit.watermarks", ())
print("reset done: lakehouse emptied and watermarks deleted")