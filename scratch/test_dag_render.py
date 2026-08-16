import os
from src.helpers.luigi_runner import LuigiRunner

runner = LuigiRunner(config_dir="config/tasks", jobs_dir="config/jobs")
job_file = os.path.join("config", "jobs", "customer_sales_pipeline.toml")
runner.load_job_pipeline(job_file)

print("Active Task IDs in config_map:", list(runner.config_map.keys()))
for t_id, cfg in runner.config_map.items():
    print(f"  - {t_id}: name='{cfg.job.task_name}', depends_on={cfg.job.depends_on}")
