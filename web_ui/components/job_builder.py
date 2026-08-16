"""
Interactive Visual Job Builder & Configurator Component for Streamlit Web UI.
Allows users to dynamically compose multi-task enterprise job workflows, map dependencies visually,
and save production TOML configuration files directly to config/jobs/<job_id>.toml.
"""

import os
import glob
import re
from typing import Dict, Any, List, Optional
import streamlit as st
from src.core.config import ConfigParser


def generate_job_toml_string(
    job_id: str,
    job_name: str,
    description: str,
    enabled: bool,
    task_mappings: List[Dict[str, Any]]
) -> str:
    """Generates clean production TOML configuration string for a composite Job pipeline."""
    lines = [
        "# =============================================================================",
        "# Production Enterprise Job Pipeline TOML Configuration",
        "# Metadata-Driven PySpark & Iceberg Framework with Spotify Luigi Orchestration",
        "# =============================================================================",
        "[job]",
        f'job_id = "{job_id}"',
        f'job_name = "{job_name}"',
        f'description = "{description}"',
        f'enabled = {"true" if enabled else "false"}',
        ""
    ]

    for tmap in task_mappings:
        lines.append("[[job.tasks]]")
        lines.append(f'task_id = "{tmap["task_id"]}"')
        lines.append(f'task_file = "{tmap["task_file"]}"')
        
        deps = tmap.get("depends_on", [])
        if deps:
            formatted_deps = ", ".join([f'"{d}"' for d in deps])
            lines.append(f'depends_on = [{formatted_deps}]')
        else:
            lines.append('depends_on = []')
        lines.append("")

    return "\n".join(lines)


def render_job_builder_form(defaults: Optional[Dict[str, Any]] = None, is_editing: bool = False, current_filepath: Optional[str] = None):
    """
    Renders interactive visual form for creating and editing composite Job pipelines (config/jobs/*.toml).
    """
    st.subheader("💼 Enterprise Job Pipeline Form Builder")
    st.markdown("Compose enterprise business pipelines by combining reusable tasks from `config/tasks/` and mapping workflow dependencies.")

    tasks_dir = os.path.join("config", "tasks")
    jobs_dir = os.path.join("config", "jobs")
    os.makedirs(jobs_dir, exist_ok=True)

    # Discover available task files
    available_tasks: Dict[str, str] = {}  # task_id -> task_file
    if os.path.exists(tasks_dir):
        for tf in sorted(glob.glob(os.path.join(tasks_dir, "*.toml"))):
            try:
                cfg = ConfigParser.load_toml(tf)
                available_tasks[cfg.job.task_id] = os.path.relpath(tf, ".").replace("\\", "/")
            except Exception:
                pass

    if not available_tasks:
        st.warning("⚠️ No active tasks found in `config/tasks/`. Please create tasks in `config/tasks/` first before building a Job pipeline.")
        return

    # Extract defaults if editing
    job_section = defaults.get("job", {}) if defaults else {}
    def_job_id = job_section.get("job_id", "")
    def_job_name = job_section.get("job_name", "")
    def_desc = job_section.get("description", "")
    def_enabled = job_section.get("enabled", True)

    existing_task_entries = job_section.get("tasks", []) if defaults else []
    def_selected_task_ids = [t.get("task_id") for t in existing_task_entries if isinstance(t, dict) and t.get("task_id") in available_tasks]
    if not def_selected_task_ids and available_tasks:
        def_selected_task_ids = list(available_tasks.keys())

    with st.form(key="job_builder_form"):
        col1, col2 = st.columns(2)
        with col1:
            job_id = st.text_input(
                "Job ID (Unique Identifier slug):",
                value=def_job_id,
                help="Unique identifier for the job pipeline, e.g. 'daily_sales_pipeline'.",
                disabled=is_editing
            )
            job_name = st.text_input(
                "Job Name (Display Label):",
                value=def_job_name,
                help="Human-readable title, e.g. 'Daily Customer & Sales Ingestion & Analytics Job'."
            )
        with col2:
            enabled = st.checkbox("Job Enabled", value=def_enabled, help="Uncheck to temporarily disable this job pipeline.")
            description = st.text_area(
                "Job Description:",
                value=def_desc,
                help="Summary of business purpose and target data domains.",
                height=85
            )

        st.divider()
        st.subheader("🧩 Task Composition & Dependency Mapping")
        selected_task_ids = st.multiselect(
            "Select Tasks to include in this Job Pipeline:",
            options=list(available_tasks.keys()),
            default=def_selected_task_ids,
            help="Choose tasks from config/tasks/ that form this pipeline."
        )

        task_mappings: List[Dict[str, Any]] = []
        if selected_task_ids:
            for tid in selected_task_ids:
                tfile = available_tasks[tid]
                task_mappings.append({
                    "task_id": tid,
                    "task_file": tfile,
                    "depends_on": []
                })

        submit_btn = st.form_submit_button("💾 Save Enterprise Job Pipeline", type="primary", use_container_width=True)

    if submit_btn:
        clean_job_id = re.sub(r"[^a-zA-Z0-9_]", "_", job_id.strip().lower())
        if not clean_job_id:
            st.error("❌ Job ID is required.")
            return

        if not job_name.strip():
            st.error("❌ Job Name is required.")
            return

        if not task_mappings:
            st.error("❌ At least one task must be selected for the job pipeline.")
            return

        target_file = os.path.join(jobs_dir, f"{clean_job_id}.toml")
        toml_content = generate_job_toml_string(
            job_id=clean_job_id,
            job_name=job_name.strip(),
            description=description.strip(),
            enabled=enabled,
            task_mappings=task_mappings
        )

        try:
            with open(target_file, "w", encoding="utf-8") as f:
                f.write(toml_content)
            st.success(f"✅ Enterprise Job Pipeline configuration saved successfully to `{target_file}`!")
            with st.expander("📄 View Generated TOML File Preview"):
                st.code(toml_content, language="toml")
        except Exception as err:
            st.error(f"❌ Failed to save Job configuration file: {err}")
