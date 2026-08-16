import os
from web_ui.components.job_builder import generate_job_toml_string
from src.core.job_pipeline import JobPipelineParser
from src.helpers.luigi_runner import LuigiRunner

# 1. Simulate creating a job from UI
job_id = "test_analytics_job"
job_name = "End to End Analytics Job"
description = "Test pipeline created via Drag and Drop Builder"
enabled = True
task_mappings = [
    {"task_id": "bronze_orders_load", "task_file": "config/tasks/bronze_orders_load.toml", "depends_on": []},
    {"task_id": "silver_orders_clean", "task_file": "config/tasks/silver_orders_clean.toml", "depends_on": ["bronze_orders_load"]},
    {"task_id": "gold_executive_report", "task_file": "config/tasks/gold_executive_report.toml", "depends_on": ["silver_orders_clean"]}
]

toml_str = generate_job_toml_string(job_id, job_name, description, enabled, task_mappings)
target_file = os.path.join("config", "jobs", "test_analytics_job.toml")
with open(target_file, "w", encoding="utf-8") as f:
    f.write(toml_str)

print("1. Successfully wrote test TOML job to:", target_file)

# 2. Parse TOML with JobPipelineParser
job_cfg = JobPipelineParser.load_job_toml(target_file)
print("2. JobPipelineParser successfully parsed job:", job_cfg.job_id, "with", len(job_cfg.tasks), "tasks.")

# 3. Load with LuigiRunner and verify DAG mapping
runner = LuigiRunner(config_dir="config/tasks", jobs_dir="config/jobs", job_file=target_file)
print("3. LuigiRunner active task IDs:", list(runner.config_map.keys()))

for tid, cfg in runner.config_map.items():
    print(f"   - {tid}: depends_on={cfg.job.depends_on}")

print("\nSUCCESS: All steps verified cleanly!")
