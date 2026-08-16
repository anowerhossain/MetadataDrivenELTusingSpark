import os, glob
from src.helpers.luigi_runner import LuigiRunner

CONFIG_DIR = "config/tasks"
JOBS_DIR = os.path.join("config", "jobs")
job_files = sorted(glob.glob(os.path.join(JOBS_DIR, "*.toml")))

sel_job_opt = os.path.basename(job_files[0])
sel_job_path = os.path.join(JOBS_DIR, sel_job_opt)

runner = LuigiRunner(config_dir=CONFIG_DIR, jobs_dir=JOBS_DIR, job_file=sel_job_path)
if hasattr(runner, "load_job_pipeline") and not runner.active_job:
    runner.load_job_pipeline(sel_job_path)

if runner.active_job and runner.active_job.tasks:
    valid_ids = {t.task_id for t in runner.active_job.tasks}
    runner.config_map = {tid: cfg for tid, cfg in runner.config_map.items() if tid in valid_ids}

print("Task IDs passed to visual canvas renderer:", list(runner.config_map.keys()))
