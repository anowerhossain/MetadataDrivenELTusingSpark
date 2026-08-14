import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

"""
Production Streamlit Web UI Control Panel for PySpark Metadata-Driven ETL Framework.
Enables interactive filterable task catalog with schema search, bulk status operations,
visual column transformations ([transform.rename], [transform.cast], [transform.derived]),
pre-filled form editing, raw TOML editing, credential validation, execution, and failure recovery.
"""

import os
import sys
import glob
import subprocess
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, List

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None

try:
    import streamlit as st
except ImportError:
    st = None

CONFIG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config", "tasks"))
FAILED_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "failed_jobs"))
SUCCESS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "success_jobs"))


def run_command_stream(cmd_list):
    """Executes a command and yields stdout/stderr output lines in real time."""
    proc = subprocess.Popen(
        cmd_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    for line in iter(proc.stdout.readline, ""):
        yield line
    proc.stdout.close()
    proc.wait()
    yield f"\n[Process completed with exit code: {proc.returncode}]\n"


def parse_toml_to_dict(filepath: str) -> Dict[str, Any]:
    """Parses TOML file into python dict safely."""
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, "rb") as f:
            if tomllib:
                return tomllib.load(f)
            else:
                import toml
                return toml.load(f)
    except Exception:
        return {}


def parse_kv_string_to_dict(kv_str: str) -> Dict[str, str]:
    """Parses key-value string (e.g. 'OLD=NEW, A=B' or 'OLD=NEW\\nA=B') into dict."""
    result = {}
    if not kv_str:
        return result

    lines = kv_str.replace("\n", ",").split(",")
    for line in lines:
        if "=" in line:
            parts = line.split("=", 1)
            k = parts[0].strip()
            v = parts[1].strip()
            if k and v:
                result[k] = v
    return result


def dict_to_kv_string(d: Dict[str, Any], multiline: bool = True) -> str:
    """Converts a dict into a key=value string for UI text area inputs."""
    if not isinstance(d, dict) or not d:
        return ""
    sep = "\n" if multiline else ", "
    return sep.join(f"{k}={v}" for k, v in d.items())


def build_job_catalog(toml_files: List[str]) -> List[Dict[str, Any]]:
    """Reads all TOML files and builds structured metadata catalog list."""
    catalog = []
    for filepath in toml_files:
        d = parse_toml_to_dict(filepath)
        task_sec = d.get("task", {}) or d.get("job", {})
        task_type = task_sec.get("type", "table_load")
        src_sec = d.get("source", {})
        load_sec = d.get("load", {})
        tgt_sec = d.get("target", {})
        email_sec = d.get("email_notification", {}) or d.get("email", {})

        tgt_str = f"{tgt_sec.get('database', '')}.{tgt_sec.get('table', '')}" if tgt_sec.get('database') else tgt_sec.get('table', '')
        email_str = "📧 Enabled" if email_sec.get("enabled", False) else "⚪ Off"

        catalog.append({
            "File Name": os.path.basename(filepath),
            "Task ID": task_sec.get("task_id", task_sec.get("job_id", "N/A")),
            "Task Name": task_sec.get("task_name", task_sec.get("job_name", "N/A")),
            "Task Type": task_type.upper(),
            "Status": "🟢 Active" if task_sec.get("enabled", True) else "🔴 Disabled",
            "Enabled": task_sec.get("enabled", True),
            "Email Alerts": email_str,
            "Source Engine": src_sec.get("type", task_type).upper(),
            "Schema / Db": src_sec.get("schema", "N/A"),
            "Source Table": src_sec.get("table", "N/A"),
            "Load Type": load_sec.get("type", "N/A").upper(),
            "Target Table": tgt_str,
            "File Path": filepath
        })
    return catalog


def toggle_job_enabled_state(filepath: str, new_state: bool):
    """Toggles [task] enabled field directly in the TOML file."""
    if not os.path.exists(filepath):
        return
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    updated = []
    in_job_sec = False
    replaced = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_job_sec = (stripped == "[task]")

        if in_job_sec and line.strip().startswith("enabled"):
            updated.append(f"enabled = {str(new_state).lower()}\n")
            replaced = True
        else:
            updated.append(line)

    if not replaced:
        updated.insert(0, f"enabled = {str(new_state).lower()}\n")

    with open(filepath, "w", encoding="utf-8") as f:
        f.writelines(updated)


def generate_toml_string(
    task_id: str,
    task_name: str,
    enabled: bool = True,
    description: str = "",
    task_type: str = "table_load",
    qlik_server_url: str = "",
    qlik_task_name: str = "",
    qlik_action: str = "RELOAD_TARGET",
    qlik_sense_url: str = "",
    qlik_sense_app_id: str = "",
    qlik_np_url: str = "",
    qlik_np_report_id: str = "",
    qlik_np_output_format: str = "PDF",
    source_type: str = "oracle",
    connection_name: str = "oracle_prod",
    schema_name: str = "BANK",
    table_name: str = "CUSTOMER",
    columns_str: str = "*",
    exclude_cols_str: str = "",
    jdbc_part_col: str = "",
    jdbc_num_parts: int = 4,
    jdbc_fetch_size: int = 10000,
    load_type: str = "FULL",
    watermark_col: str = "",
    watermark_type: str = "timestamp",
    primary_keys_str: str = "",
    merge_keys_str: str = "",
    target_catalog: str = "hive",
    target_database: str = "edw_bronze",
    target_table: str = "",
    partition_col: str = "",
    transform_rename_str: str = "",
    transform_cast_str: str = "",
    transform_derived_str: str = "",
    audit_enabled: bool = True,
    audit_insert_ts: str = "dwh_insert_ts",
    audit_updated_ts: str = "dwh_updated_ts",
    audit_task_id: str = "dwh_etl_run_id",
    audit_source_sys: str = "dwh_job_user",
    audit_timezone: str = "Asia/Dhaka",
    preload_operations: List[str] = None,
    postload_operations: List[str] = None,
    null_checks_str: str = "",
    unique_checks_str: str = "",
    minimum_rows: int = 1,
    retries: int = 3,
    retry_delay: float = 30.0,
    backoff_multiplier: float = 2.0,
    exponential_backoff: bool = True,
    resource_profile: str = "auto",
    executor_memory: str = "",
    shuffle_partitions: int = 0,
    sftp_path: str = "/remote/path/",
    sftp_file_pattern: str = "*.csv",
    sftp_file_format: str = "csv",
    sftp_delimiter: str = ",",
    sftp_header: bool = True,
    sftp_encoding: str = "utf-8",
    sftp_sheet_name: str = "0",
    sftp_header_row: int = 0,
    email_enabled: bool = False,
    email_from: str = "noreply@company.com",
    email_to_str: str = "",
    email_cc_str: str = "",
    email_bcc_str: str = "",
    email_subject_prefix: str = "[ETL TASK FAILURE]",
    email_template: str = "job_failed",
    email_subject: str = "",
    email_succ_enabled: bool = False,
    email_succ_to_str: str = "",
    email_succ_template: str = "job_success",
    email_dq_enabled: bool = False,
    email_dq_to_str: str = "",
    email_dq_template: str = "data_quality_failed"
) -> str:
    """Constructs clean, standardized TOML string from form field inputs."""
    if task_type == "qlik_replicate":
        return f"""[task]
task_id = "{task_id}"
task_name = "{task_name}"
type = "qlik_replicate"
enabled = {str(enabled).lower()}
description = "{description}"

[qlik_replicate]
server_url = "{qlik_server_url}"
task_name = "{qlik_task_name}"
action = "{qlik_action}"
timeout_seconds = 300
poll_interval_seconds = 5
""".strip() + "\n"

    elif task_type == "qlik_sense":
        return f"""[task]
task_id = "{task_id}"
task_name = "{task_name}"
type = "qlik_sense"
enabled = {str(enabled).lower()}
description = "{description}"

[qlik_sense]
server_url = "{qlik_sense_url}"
app_id = "{qlik_sense_app_id}"
timeout_seconds = 600
poll_interval_seconds = 10
""".strip() + "\n"

    elif task_type == "qlik_nprinting":
        return f"""[task]
task_id = "{task_id}"
task_name = "{task_name}"
type = "qlik_nprinting"
enabled = {str(enabled).lower()}
description = "{description}"

[qlik_nprinting]
server_url = "{qlik_np_url}"
report_id = "{qlik_np_report_id}"
output_format = "{qlik_np_output_format}"
timeout_seconds = 600
poll_interval_seconds = 10
""".strip() + "\n"

    cols_list = [c.strip() for c in columns_str.split(",") if c.strip()]
    excl_list = [c.strip() for c in exclude_cols_str.split(",") if c.strip()]
    pkeys_list = [c.strip() for c in primary_keys_str.split(",") if c.strip()]
    mkeys_list = [c.strip() for c in merge_keys_str.split(",") if c.strip()]
    nulls_list = [c.strip() for c in null_checks_str.split(",") if c.strip()]
    uniques_list = [c.strip() for c in unique_checks_str.split(",") if c.strip()]

    email_to_list = [t.strip() for t in email_to_str.split(",") if t.strip()]
    email_cc_list = [c.strip() for c in email_cc_str.split(",") if c.strip()]
    email_bcc_list = [b.strip() for b in email_bcc_str.split(",") if b.strip()]

    rename_dict = parse_kv_string_to_dict(transform_rename_str)
    cast_dict = parse_kv_string_to_dict(transform_cast_str)
    derived_dict = parse_kv_string_to_dict(transform_derived_str)

    toml_content = f"""[task]
task_id = "{task_id}"
task_name = "{task_name}"
type = "table_load"
enabled = {str(enabled).lower()}
description = "{description}"

[source]
type = "{source_type.lower()}"
connection = "{connection_name}"
schema = "{schema_name}"
table = "{table_name}"
"""
    if source_type.lower() == "sftp":
        toml_content += f'path = "{sftp_path}"\n'
        toml_content += f'file_pattern = "{sftp_file_pattern}"\n'
        toml_content += f'file_format = "{sftp_file_format}"\n'
        if sftp_file_format.lower() == "csv":
            toml_content += f'delimiter = "{sftp_delimiter}"\n'
            toml_content += f'header = {str(sftp_header).lower()}\n'
            toml_content += f'encoding = "{sftp_encoding}"\n'
        else:
            toml_content += f'sheet_name = "{sftp_sheet_name}"\n'
            toml_content += f'header_row = {sftp_header_row}\n'

    if cols_list or excl_list:
        toml_content += "\n[source.extraction]\n"
        if cols_list:
            toml_content += f"columns = {cols_list}\n"
        if excl_list:
            toml_content += f"exclude_columns = {excl_list}\n"

    if source_type.lower() != "sftp" and (jdbc_part_col or jdbc_fetch_size):
        toml_content += "\n[source.jdbc]\n"
        if jdbc_fetch_size:
            toml_content += f"fetch_size = {jdbc_fetch_size}\n"
        if jdbc_part_col:
            toml_content += f'partition_column = "{jdbc_part_col}"\nnum_partitions = {jdbc_num_parts}\n'

    toml_content += f"""
[load]
type = "{load_type.upper()}"
"""
    if watermark_col:
        toml_content += f"""
[load.incremental]
column = "{watermark_col}"
watermark_type = "{watermark_type}"
"""
    if pkeys_list or mkeys_list:
        toml_content += "\n[keys]\n"
        if pkeys_list:
            toml_content += f"primary_key = {pkeys_list}\n"
        if mkeys_list:
            toml_content += f"merge_keys = {mkeys_list}\n"

    toml_content += f"""
[target]
catalog = "{target_catalog}"
database = "{target_database}"
table = "{target_table}"
"""
    if partition_col:
        toml_content += f"""
[target.partition]
type = "days"
column = "{partition_col}"
"""
    toml_content += """
[target.maintenance]
enabled = true
compact_small_files = true
target_file_size_mb = 128
rewrite_manifests = true
remove_orphan_files = true
orphan_file_retention_days = 3
"""

    if rename_dict:
        toml_content += "\n[transform.rename]\n"
        for k, v in rename_dict.items():
            toml_content += f'{k} = "{v}"\n'

    if cast_dict:
        toml_content += "\n[transform.cast]\n"
        for k, v in cast_dict.items():
            toml_content += f'{k} = "{v}"\n'

    if derived_dict:
        toml_content += "\n[transform.derived]\n"
        for k, v in derived_dict.items():
            val_str = v if (v.startswith("'") or v.startswith('"') or v.lower() in ("true", "false") or v.isdigit()) else f'"{v}"'
            toml_content += f'{k} = {val_str}\n'

    if audit_enabled or audit_insert_ts != "dwh_insert_ts" or audit_updated_ts != "dwh_updated_ts" or audit_task_id != "dwh_etl_run_id" or audit_source_sys != "dwh_job_user" or audit_timezone != "Asia/Dhaka":
        toml_content += f"""
[audit_columns]
enabled = {str(audit_enabled).lower()}
insert_ts_column = "{audit_insert_ts}"
updated_ts_column = "{audit_updated_ts}"
run_id_column = "{audit_task_id}"
job_user_column = "{audit_source_sys}"
timezone = "{audit_timezone}"
"""

    if preload_operations:
        toml_content += f"\n[preload]\noperations = {preload_operations}\n"

    if postload_operations:
        toml_content += f"\n[postload]\noperations = {postload_operations}\n"

    if nulls_list or uniques_list or minimum_rows != 1:
        toml_content += "\n[quality]\n"
        if nulls_list:
            toml_content += f"null_check = {nulls_list}\n"
        if uniques_list:
            toml_content += f"unique_check = {uniques_list}\n"
        if minimum_rows != 1:
            toml_content += f"minimum_rows = {minimum_rows}\n"

    if retries != 3 or retry_delay != 30.0 or backoff_multiplier != 2.0 or not exponential_backoff:
        toml_content += f"""
[execution]
retries = {retries}
retry_delay_seconds = {retry_delay}
backoff_multiplier = {backoff_multiplier}
exponential_backoff = {str(exponential_backoff).lower()}
"""
    if resource_profile != "auto" or executor_memory or shuffle_partitions > 0:
        toml_content += f"""
[resources]
profile = "{resource_profile}"
"""
        if executor_memory:
            toml_content += f'executor_memory = "{executor_memory}"\n'
        if shuffle_partitions > 0:
            toml_content += f"shuffle_partitions = {shuffle_partitions}\n"

    if email_enabled or email_to_list:
        toml_content += f"""
[email_notification]
enabled = {str(email_enabled).lower()}
template = "{email_template}"
from = "{email_from}"
to = {email_to_list}
cc = {email_cc_list}
bcc = {email_bcc_list}
subject_prefix = "{email_subject_prefix}"
"""
        if email_subject:
            toml_content += f'subject = "{email_subject}"\n'

        if email_succ_enabled and email_succ_to_str.strip():
            succ_to_list = [t.strip() for t in email_succ_to_str.split(",") if t.strip()]
            toml_content += f"""
[[email_notification.events]]
event = "on_success"
enabled = true
to = {succ_to_list}
template = "{email_succ_template}"
subject_prefix = "[SUCCESS NOTICE]"
"""

        if email_dq_enabled and email_dq_to_str.strip():
            dq_to_list = [t.strip() for t in email_dq_to_str.split(",") if t.strip()]
            toml_content += f"""
[[email_notification.events]]
event = "on_quality_failure"
enabled = true
to = {dq_to_list}
template = "{email_dq_template}"
subject_prefix = "[DATA QUALITY ALERT]"
"""

    return toml_content.strip() + "\n"


def render_visual_form(defaults: Dict[str, Any], is_editing: bool = False, current_filepath: str = ""):
    """Renders guided visual form builder supporting Table Load, Qlik Replicate, Qlik Sense, and Qlik NPrinting tasks."""
    task_sec = defaults.get("task", {}) or defaults.get("job", {})
    src_sec = defaults.get("source", {})
    
    st.markdown("### 🧩 Select Task Execution Module")
    task_type_keys = ["table_load", "qlik_replicate", "qlik_sense", "qlik_nprinting"]
    task_type_labels = [
        "📊 Table Load (Database / SFTP -> Iceberg)",
        "🔄 Qlik Replicate Task Refresh",
        "📈 Qlik Sense Report Refresh",
        "🖨️ Qlik NPrinting Report"
    ]
    cur_type = task_sec.get("type", "table_load").lower()
    t_idx = task_type_keys.index(cur_type) if cur_type in task_type_keys else 0

    sel_label = st.selectbox(
        "Task Type*",
        task_type_labels,
        index=t_idx,
        key="reactive_task_type_select_box",
        help="Select the framework Task execution module: Table Load, Qlik Replicate, Qlik Sense, or Qlik NPrinting."
    )
    selected_task_type = task_type_keys[task_type_labels.index(sel_label)]

    if selected_task_type == "qlik_replicate":
        with st.form("visual_qlik_replicate_form"):
            st.subheader("1. Task Header & Metadata")
            c1, c2 = st.columns(2)
            with c1:
                t_id = st.text_input("Task ID*", value=task_sec.get("task_id", "qlik_orders_refresh"))
                t_name = st.text_input("Task Name*", value=task_sec.get("task_name", "Qlik Replicate Orders Refresh"))
            with c2:
                t_enabled = st.checkbox("Task Active (Enabled)", value=task_sec.get("enabled", True))
                t_desc = st.text_input("Task Description", value=task_sec.get("description", "Refresh Qlik Replicate Task"))

            st.subheader("2. Qlik Replicate API Parameters")
            qr_sec = defaults.get("qlik_replicate", {})
            qr1, qr2 = st.columns(2)
            with qr1:
                qr_url = st.text_input("Qlik Replicate Server URL*", value=qr_sec.get("server_url", "https://qlik-em.company.com"))
                qr_task = st.text_input("Replication Task Name*", value=qr_sec.get("task_name", "OracleToIcebergOrders"))
            with qr2:
                qr_action = st.selectbox("Action*", ["RELOAD_TARGET", "RESUME", "RUN"], index=0)

            st.divider()
            default_fname = os.path.basename(current_filepath) if is_editing else f"{t_id}.toml"
            save_filename = st.text_input("Save TOML File Name*", value=default_fname)
            form_saved = st.form_submit_button("💾 Save Task Configuration", type="primary")

        if form_saved:
            toml_str = generate_toml_string(
                job_id=t_id, job_name=t_name, enabled=t_enabled, description=t_desc,
                task_type="qlik_replicate", qlik_server_url=qr_url, qlik_task_name=qr_task, qlik_action=qr_action
            )
            save_path = os.path.join(CONFIG_DIR, save_filename if save_filename.endswith(".toml") else f"{save_filename}.toml")
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(toml_str)
            st.success(f"Successfully saved Qlik Replicate Task to `{save_path}`!")
            st.rerun()
        return

    elif selected_task_type == "qlik_sense":
        with st.form("visual_qlik_sense_form"):
            st.subheader("1. Task Header & Metadata")
            c1, c2 = st.columns(2)
            with c1:
                t_id = st.text_input("Task ID*", value=task_sec.get("task_id", "qlik_sense_sales"))
                t_name = st.text_input("Task Name*", value=task_sec.get("task_name", "Qlik Sense Sales App Reload"))
            with c2:
                t_enabled = st.checkbox("Task Active (Enabled)", value=task_sec.get("enabled", True))
                t_desc = st.text_input("Task Description", value=task_sec.get("description", "Reload Qlik Sense Sales App"))

            st.subheader("2. Qlik Sense QRS API Parameters")
            qs_sec = defaults.get("qlik_sense", {})
            qs1, qs2 = st.columns(2)
            with qs1:
                qs_url = st.text_input("Qlik Sense Server URL*", value=qs_sec.get("server_url", "https://qlik-sense.company.com"))
            with qs2:
                qs_app = st.text_input("App ID / Task GUID*", value=qs_sec.get("app_id", "app-guid-12345"))

            st.divider()
            default_fname = os.path.basename(current_filepath) if is_editing else f"{t_id}.toml"
            save_filename = st.text_input("Save TOML File Name*", value=default_fname)
            form_saved = st.form_submit_button("💾 Save Task Configuration", type="primary")

        if form_saved:
            toml_str = generate_toml_string(
                job_id=t_id, job_name=t_name, enabled=t_enabled, description=t_desc,
                task_type="qlik_sense", qlik_sense_url=qs_url, qlik_sense_app_id=qs_app
            )
            save_path = os.path.join(CONFIG_DIR, save_filename if save_filename.endswith(".toml") else f"{save_filename}.toml")
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(toml_str)
            st.success(f"Successfully saved Qlik Sense Task to `{save_path}`!")
            st.rerun()
        return

    elif selected_task_type == "qlik_nprinting":
        with st.form("visual_qlik_nprinting_form"):
            st.subheader("1. Task Header & Metadata")
            c1, c2 = st.columns(2)
            with c1:
                t_id = st.text_input("Task ID*", value=task_sec.get("task_id", "qlik_np_fin_report"))
                t_name = st.text_input("Task Name*", value=task_sec.get("task_name", "Financial NPrinting Report"))
            with c2:
                t_enabled = st.checkbox("Task Active (Enabled)", value=task_sec.get("enabled", True))
                t_desc = st.text_input("Task Description", value=task_sec.get("description", "Generate Financial Report"))

            st.subheader("2. Qlik NPrinting API Parameters")
            np_sec = defaults.get("qlik_nprinting", {})
            np1, np2 = st.columns(2)
            with np1:
                np_url = st.text_input("Qlik NPrinting Server URL*", value=np_sec.get("server_url", "https://nprinting.company.com:4993"))
                np_rpt = st.text_input("Report ID / Task GUID*", value=np_sec.get("report_id", "rpt-guid-8888"))
            with np2:
                np_fmt = st.selectbox("Output Format*", ["PDF", "EXCEL", "CSV", "HTML"], index=0)

            st.divider()
            default_fname = os.path.basename(current_filepath) if is_editing else f"{t_id}.toml"
            save_filename = st.text_input("Save TOML File Name*", value=default_fname)
            form_saved = st.form_submit_button("💾 Save Task Configuration", type="primary")

        if form_saved:
            toml_str = generate_toml_string(
                job_id=t_id, job_name=t_name, enabled=t_enabled, description=t_desc,
                task_type="qlik_nprinting", qlik_np_url=np_url, qlik_np_report_id=np_rpt, qlik_np_output_format=np_fmt
            )
            save_path = os.path.join(CONFIG_DIR, save_filename if save_filename.endswith(".toml") else f"{save_filename}.toml")
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(toml_str)
            st.success(f"Successfully saved Qlik NPrinting Task to `{save_path}`!")
            st.rerun()
        return

    job_sec = task_sec
    src_ext = src_sec.get("extraction", {})
    src_jdbc = src_sec.get("jdbc", {})
    load_sec = defaults.get("load", {})
    inc_sec = load_sec.get("incremental", {})
    keys_sec = defaults.get("keys", {})
    target_sec = defaults.get("target", {})
    tgt_part = target_sec.get("partition", {})
    transform_sec = defaults.get("transform", {})
    rename_sec = transform_sec.get("rename", {})
    cast_sec = transform_sec.get("cast", {})
    derived_sec = transform_sec.get("derived", {})
    preload_sec = defaults.get("preload", {})
    postload_sec = defaults.get("postload", {})
    dq_sec = defaults.get("data_quality", {})
    exec_sec = defaults.get("execution", {})
    res_sec = defaults.get("resources", {})

    st.markdown("### 🎛️ Select Data Ingestion Source Engine")
    src_type_val = src_sec.get("type", "oracle").lower()
    src_options = ["oracle", "mysql", "postgresql", "sqlserver", "sftp"]
    src_idx = src_options.index(src_type_val) if src_type_val in src_options else 0

    col_src1, col_src2 = st.columns([2, 3])
    with col_src1:
        source_type = st.selectbox(
            "Source Engine / Type*",
            src_options,
            index=src_idx,
            key="reactive_source_type_select",
            help="Switch between RDBMS database sources (Oracle, MySQL, Postgres, SQL Server) and SFTP File Ingestion."
        )
    with col_src2:
        sftp_sec = src_sec.get("sftp", {})
        if source_type == "sftp":
            fmt_val = (src_sec.get("file_format") or sftp_sec.get("file_format", "csv")).lower()
            fmt_idx = 0 if fmt_val in ("csv",) else 1
            sftp_file_format = st.radio(
                "SFTP File Format*",
                ["csv", "excel"],
                index=fmt_idx,
                horizontal=True,
                key="reactive_sftp_format_select",
                help="Select format of raw files on SFTP server."
            )
        else:
            sftp_file_format = "csv"
            st.caption("⚡ **RDBMS Direct Database Ingestion Mode**: Extracts relational tables using parallel JDBC partitioning.")

    with st.form("visual_task_form"):
        st.subheader("1. Task Header & Metadata")
        c1, c2 = st.columns(2)
        with c1:
            task_id = st.text_input(
                "Task ID*",
                value=job_sec.get("task_id", "oracle_customer_load" if source_type != "sftp" else "sftp_invoices_load"),
                help="Unique system identifier for the ETL job (e.g. mysql_orders_load). Used in logs and watermarks."
            )
            task_name = st.text_input(
                "Task Name*",
                value=job_sec.get("task_name", "Oracle Customer Ingestion" if source_type != "sftp" else "SFTP File Ingestion"),
                help="Human-readable title describing what this pipeline job ingests."
            )
        with c2:
            enabled = st.checkbox(
                "Task Active (Enabled)",
                value=job_sec.get("enabled", True),
                help="If unchecked, batch runner will skip this job file without executing."
            )
            description = st.text_input(
                "Task Description",
                value=job_sec.get("description", "Ingest dataset into Apache Iceberg Bronze layer"),
                help="Brief business description of the pipeline job."
            )

        st.divider()

        sftp_path = "/remote/path/"
        sftp_file_pattern = "*.csv"
        sftp_delimiter = ","
        sftp_header = True
        sftp_encoding = "utf-8"
        sftp_sheet_name = "0"
        sftp_header_row = 0

        jdbc_part_col = ""
        jdbc_num_parts = 4
        jdbc_fetch_size = 10000

        if source_type == "sftp":
            st.subheader("2. 📁 SFTP Source Parameters & File Discovery")
            st.info("📁 **SFTP File Ingestion Active**: Configured for dynamic file matching, parsing, deduplication, and Iceberg audit table logging.")
            
            sc1, sc2 = st.columns(2)
            with sc1:
                connection_name = st.text_input(
                    "SFTP Connection Prefix Name*",
                    value=src_sec.get("connection", "sftp_prod"),
                    help="Environment credential prefix (e.g. sftp_prod -> resolves $SFTP_PROD_HOST, $SFTP_PROD_USERNAME, $SFTP_PROD_PASSWORD)."
                )
                sftp_path = st.text_input(
                    "SFTP Remote Directory / Local Path*",
                    value=src_sec.get("path") or sftp_sec.get("path", "/remote/incoming/"),
                    help="SFTP directory path or local staging path (e.g. /remote/data/ or config/tasks/sample_data/)."
                )
                sftp_file_pattern = st.text_input(
                    "File Pattern (Glob)*",
                    value=src_sec.get("file_pattern") or sftp_sec.get("file_pattern", "*.csv" if sftp_file_format == "csv" else "*.xlsx"),
                    help="Glob file pattern (e.g. *.csv, *.xlsx, invoices_*.csv, settlements_*.xlsx)."
                )
            with sc2:
                schema_name = st.text_input(
                    "Target Iceberg Schema Identifier",
                    value=src_sec.get("schema", "sftp_invoices"),
                    help="Logical schema identifier used in target Iceberg naming."
                )
                table_name = st.text_input(
                    "Target Iceberg Dataset Name*",
                    value=src_sec.get("table", "raw_invoices"),
                    help="Logical dataset name for watermarking and failure tracking."
                )

            st.markdown(f"##### 📄 **{sftp_file_format.upper()}** File Parsing Options")
            fp1, fp2, fp3 = st.columns(3)
            if sftp_file_format == "csv":
                with fp1:
                    sftp_delimiter = st.text_input(
                        "CSV Delimiter",
                        value=src_sec.get("delimiter") or sftp_sec.get("delimiter", ","),
                        help="CSV column separator (e.g. comma ',', pipe '|', tab '\\t')."
                    )
                with fp2:
                    sftp_header = st.checkbox(
                        "CSV Contains Header Row",
                        value=bool(src_sec.get("header", True)),
                        help="First row contains column headers."
                    )
                with fp3:
                    sftp_encoding = st.text_input(
                        "CSV File Encoding",
                        value=src_sec.get("encoding") or sftp_sec.get("encoding", "utf-8"),
                        help="File text encoding (e.g. utf-8, latin1)."
                    )
            else:  # Excel
                with fp1:
                    sftp_sheet_name = st.text_input(
                        "Excel Sheet Name or Index*",
                        value=str(src_sec.get("sheet_name") or sftp_sec.get("sheet_name", "0")),
                        help="Sheet name string (e.g. 'Settlements') or 0-based index (0 for first sheet)."
                    )
                with fp2:
                    sftp_header_row = st.number_input(
                        "Excel Header Row Index",
                        min_value=0,
                        value=int(src_sec.get("header_row") or sftp_sec.get("header_row", 0)),
                        help="0-based row index containing column names (0 = first row)."
                    )
                with fp3:
                    st.text_input(
                        "Target Audit Table",
                        value=src_sec.get("audit_table") or sftp_sec.get("audit_table", "hive.etl_audit.sftp_file_audit"),
                        help="Target Apache Iceberg audit table for recording SFTP metadata."
                    )

            st.markdown("##### 🔍 Column Projections & Filtering")
            cp1, cp2 = st.columns(2)
            with cp1:
                existing_cols = src_ext.get("columns", [])
                cols_val = ", ".join(existing_cols) if isinstance(existing_cols, list) else str(existing_cols)
                columns_str = st.text_input(
                    "Extraction Columns (comma-separated)",
                    value=cols_val,
                    help="Column names to extract. Leave empty to extract all."
                )
            with cp2:
                existing_excl = src_ext.get("exclude_columns") or transform_sec.get("exclude", [])
                excl_val = ", ".join(existing_excl) if isinstance(existing_excl, list) else str(existing_excl)
                exclude_cols_str = st.text_input(
                    "Excluded Columns (comma-separated)",
                    value=excl_val,
                    help="Column names to exclude/drop before target write."
                )

        else:  # RDBMS Sources (Oracle, MySQL, Postgres, SQL Server)
            st.subheader("2. 🗄️ Source RDBMS Database Connection & Table")
            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                connection_name = st.text_input(
                    "Connection Prefix Name*",
                    value=src_sec.get("connection", "oracle_prod"),
                    help="Environment credential prefix (e.g. oracle_prod -> ORACLE_PROD_JDBC_URL)."
                )
            with sc2:
                schema_name = st.text_input(
                    "Source Database / Schema*",
                    value=src_sec.get("schema", "BANK"),
                    help="Source database schema or database name (e.g. BANK in Oracle, dbo in SQL Server)."
                )
            with sc3:
                table_name = st.text_input(
                    "Source Table Name*",
                    value=src_sec.get("table", "CUSTOMER"),
                    help="Source table name in the source database."
                )

            st.markdown("##### 🔍 Column Projections & JDBC Fetch Size")
            cp1, cp2, cp3 = st.columns(3)
            with cp1:
                existing_cols = src_ext.get("columns", [])
                cols_val = ", ".join(existing_cols) if isinstance(existing_cols, list) else str(existing_cols)
                columns_str = st.text_input(
                    "Extraction Columns (comma-separated)",
                    value=cols_val,
                    help="Column names to extract. Leave empty to extract all."
                )
            with cp2:
                existing_excl = src_ext.get("exclude_columns") or transform_sec.get("exclude", [])
                excl_val = ", ".join(existing_excl) if isinstance(existing_excl, list) else str(existing_excl)
                exclude_cols_str = st.text_input(
                    "Excluded Columns (comma-separated)",
                    value=excl_val,
                    help="Column names to drop/exclude."
                )
            with cp3:
                jdbc_fetch_size = st.number_input(
                    "JDBC Fetch Size",
                    min_value=1000,
                    max_value=100000,
                    value=int(src_jdbc.get("fetch_size", 10000)),
                    step=5000,
                    help="Number of rows PySpark fetches per network roundtrip from the source database."
                )

            st.divider()
            st.subheader("3. Intra-Table Parallel JDBC Partitioning (For Large RDBMS Tables)")
            jc1, jc2 = st.columns(2)
            with jc1:
                jdbc_part_col = st.text_input(
                    "JDBC Split Column (Numeric/Date)",
                    value=src_jdbc.get("partition_column", ""),
                    help="Column used to split query across parallel Spark workers (e.g. invoice_id). Optional."
                )
            with jc2:
                jdbc_num_parts = st.number_input(
                    "Number of JDBC Split Partitions",
                    min_value=1,
                    max_value=64,
                    value=int(src_jdbc.get("num_partitions", 4)),
                    help="Number of parallel JDBC sub-queries Spark will execute against the source database."
                )

        st.divider()
        st.subheader("4. Load Strategy & Key Configuration")
        lc1, lc2, lc3 = st.columns(3)
        with lc1:
            load_type_val = load_sec.get("type", "FULL").upper()
            load_opts = ["FULL", "INCREMENTAL", "UPSERT"]
            load_idx = load_opts.index(load_type_val) if load_type_val in load_opts else 0
            load_type = st.selectbox(
                "Ingestion Load Type*",
                load_opts,
                index=load_idx,
                help="FULL = Overwrite table; INCREMENTAL = Append rows > watermark; UPSERT = MERGE INTO update/insert."
            )
        with lc2:
            watermark_col = st.text_input(
                "Watermark Column (For INCREMENTAL/UPSERT)",
                value=inc_sec.get("column", "UPDATED_AT"),
                help="Source column used to filter new/updated records (e.g. UPDATED_AT or MODIFIED_DATE)."
            )
            watermark_type = st.selectbox(
                "Watermark Column Data Type",
                ["timestamp", "date", "numeric"],
                help="Data format of the watermark column."
            )
        with lc3:
            existing_pkeys = keys_sec.get("primary_key", [])
            pkeys_val = ", ".join(existing_pkeys) if isinstance(existing_pkeys, list) else str(existing_pkeys)
            primary_keys_str = st.text_input(
                "Primary Key Columns",
                value=pkeys_val,
                help="Comma-separated column list (e.g. customer_id or store_id, transaction_id). Identifies primary key(s) in source table."
            )

            existing_mkeys = keys_sec.get("merge_keys", [])
            mkeys_val = ", ".join(existing_mkeys) if isinstance(existing_mkeys, list) else str(existing_mkeys)
            merge_keys_str = st.text_input(
                "Merge Keys (Required for UPSERT)",
                value=mkeys_val,
                help="Comma-separated column list (e.g. order_id or tenant_id, order_id). Join key(s) used in MERGE INTO statements for UPSERT loads."
            )

        st.divider()
        st.subheader("5. Target Apache Iceberg Catalog & Table")
        tc1, tc2, tc3 = st.columns(3)
        with tc1:
            target_catalog = st.text_input(
                "Iceberg Catalog*",
                value=target_sec.get("catalog", "hive"),
                help="Spark catalog name (e.g. hive or iceberg_catalog)."
            )
        with tc2:
            target_database = st.text_input(
                "Target Database*",
                value=target_sec.get("database", "edw_bronze"),
                help="Hive/Iceberg target schema or database name."
            )
        with tc3:
            target_table = st.text_input(
                "Target Table Name*",
                value=target_sec.get("table", "customer"),
                help="Name of target Apache Iceberg table in Hive metastore."
            )
            partition_col = st.text_input(
                "Target Iceberg Partition Column",
                value=tgt_part.get("column", ""),
                help="Target partition column for Iceberg hidden partitioning (e.g. days(created_at))."
            )

        st.divider()
        st.subheader("6. Column Data Transformations ([transform])")
        tr1, tr2, tr3 = st.columns(3)
        with tr1:
            transform_rename_str = st.text_area(
                "Column Renames ([transform.rename])",
                value=dict_to_kv_string(rename_sec, multiline=True),
                height=120,
                help="Enter 1 rule per line or comma-separated: OLD_NAME=new_name\nExample:\nCUSTOMER_ID=customer_id\nCUSTOMER_NAME=customer_name"
            )
        with tr2:
            transform_cast_str = st.text_area(
                "Data Type Casts ([transform.cast])",
                value=dict_to_kv_string(cast_sec, multiline=True),
                height=120,
                help="Enter 1 rule per line or comma-separated: COL_NAME=TARGET_TYPE\nExample:\nCUSTOMER_ID=BIGINT\nBALANCE=DECIMAL(18,2)"
            )
        with tr3:
            transform_derived_str = st.text_area(
                "Derived Columns ([transform.derived])",
                value=dict_to_kv_string(derived_sec, multiline=True),
                height=120,
                help="Enter 1 rule per line or comma-separated: NEW_COL=EXPRESSION\nExample:\nsource_system='ORACLE'\ningested_at=current_timestamp()\nis_active=true"
            )

        st.divider()
        st.subheader("6b. 🛡️ DWH Standard Audit Columns ([audit_columns])")
        st.info(
            "Standard Enterprise Data Warehouse (DWH) audit columns automatically added to every target Iceberg table "
            "to track row-level lineage, ingestion timestamp, update execution timestamp, and job run metadata."
        )

        audit_sec = defaults.get("audit_columns", defaults.get("audit", {}))
        audit_enabled = st.checkbox(
            "Enable DWH Audit Columns Generation",
            value=audit_sec.get("enabled", True),
            help="When enabled, automatically appends standardized audit columns to all records written into target Iceberg tables."
        )

        ac1, ac2, ac3, ac4, ac5 = st.columns(5)
        with ac1:
            audit_insert_ts = st.text_input(
                "Insert Timestamp Column",
                value=audit_sec.get("insert_ts_column", audit_sec.get("insert_ts", "dwh_insert_ts")),
                help="🕒 dwh_insert_ts: Bangladesh Standard Time (BST, UTC+6) timestamp recording when the row was inserted."
            )
        with ac2:
            audit_updated_ts = st.text_input(
                "Update Timestamp Column",
                value=audit_sec.get("updated_ts_column", audit_sec.get("updated_ts", "dwh_updated_ts")),
                help="🔄 dwh_updated_ts: Bangladesh Standard Time (BST, UTC+6) timestamp recording the last update execution time."
            )
        with ac3:
            audit_task_id = st.text_input(
                "ETL Run ID Column",
                value=audit_sec.get("run_id_column", audit_sec.get("task_id_column", "dwh_etl_run_id")),
                help="🆔 dwh_etl_run_id: Unique pipeline execution run identifier."
            )
        with ac4:
            audit_source_sys = st.text_input(
                "Job User / Owner Column",
                value=audit_sec.get("job_user_column", audit_sec.get("source_system_column", "dwh_job_user")),
                help="👤 dwh_job_user: System user or execution account that executed the ETL job."
            )
        with ac5:
            audit_timezone = st.text_input(
                "Audit Timezone",
                value=audit_sec.get("timezone", "Asia/Dhaka"),
                help="🇧🇩 Timezone for dwh_insert_ts & dwh_updated_ts (default: Asia/Dhaka for Bangladesh Standard Time BST)."
            )

        st.divider()
        st.subheader("7. Preload & Postload Hook Operations")
        hc1, hc2 = st.columns(2)
        with hc1:
            default_preload = preload_sec.get("operations", ["validate_source", "validate_target", "check_watermark"])
            all_preload_opts = ["validate_source", "validate_target", "check_watermark"]
            valid_preload_defaults = [op for op in default_preload if op in all_preload_opts]
            preload_operations = st.multiselect(
                "Preload Operations ([preload])",
                options=all_preload_opts,
                default=valid_preload_defaults,
                help="Operations executed BEFORE extraction. validate_source: DB check; validate_target: Catalog check; check_watermark: Read last watermark."
            )
        with hc2:
            default_postload = postload_sec.get("operations", ["update_watermark", "compact_table", "refresh_metadata", "remove_orphan_files"])
            all_postload_opts = ["update_watermark", "compact_table", "refresh_metadata", "remove_orphan_files"]
            valid_postload_defaults = [op for op in default_postload if op in all_postload_opts]
            postload_operations = st.multiselect(
                "Postload Operations ([postload])",
                options=all_postload_opts,
                default=valid_postload_defaults,
                help="Operations executed AFTER successful write. update_watermark: Save state; compact_table: Iceberg compaction; refresh_metadata: Catalog refresh; remove_orphan_files: Iceberg orphan file removal."
            )

        st.divider()
        st.subheader("8. Data Quality Checks & Retry Execution Rules")
        dq1, dq2, dq3 = st.columns(3)
        with dq1:
            existing_nulls = dq_sec.get("null_check", [])
            nulls_val = ", ".join(existing_nulls) if isinstance(existing_nulls, list) else str(existing_nulls)
            null_checks_str = st.text_input(
                "NULL Check Columns",
                value=nulls_val,
                help="Comma-separated list (e.g. order_id, created_at, customer_id). Fails fast if ANY listed column contains a NULL value in incoming batch."
            )
            existing_uniques = dq_sec.get("unique_check", [])
            uniques_val = ", ".join(existing_uniques) if isinstance(existing_uniques, list) else str(existing_uniques)
            unique_checks_str = st.text_input(
                "Unique Check Columns",
                value=uniques_val,
                help="Comma-separated list (e.g. store_id, transaction_id). Checks COMPOSITE uniqueness across the combined values of all listed columns."
            )
            minimum_rows = st.number_input(
                "Minimum Expected Rows",
                min_value=0,
                value=int(dq_sec.get("minimum_rows", 1)),
                help="Fail-safe assertion. Pipeline fails fast if extracted batch row count is below this threshold (e.g. 1)."
            )
        with dq2:
            retries = st.number_input(
                "Max Retry Attempts",
                min_value=1,
                max_value=10,
                value=int(exec_sec.get("retries", 3)),
                help="Number of retries for transient operational or network errors."
            )
            retry_delay = st.number_input(
                "Initial Retry Delay (sec)",
                min_value=1.0,
                value=float(exec_sec.get("retry_delay_seconds", 30.0)),
                help="Base delay seconds before 1st retry."
            )
            backoff_multiplier = st.number_input(
                "Backoff Multiplier",
                min_value=1.0,
                value=float(exec_sec.get("backoff_multiplier", 2.0)),
                help="Exponential backoff doubling factor (e.g. 2.0 -> 30s, 60s, 120s)."
            )
            exponential_backoff = st.checkbox(
                "Enable Exponential Doubling",
                value=exec_sec.get("exponential_backoff", True),
                help="If enabled, retry delays double on each successive attempt."
            )
        with dq3:
            prof_val = res_sec.get("profile", "auto").lower()
            prof_opts = ["auto", "light", "medium", "heavy"]
            prof_idx = prof_opts.index(prof_val) if prof_val in prof_opts else 0
            resource_profile = st.selectbox(
                "Spark Cluster Resource Profile",
                prof_opts,
                index=prof_idx,
                help="auto = Auto-tune memory/cores; light = 2GB; medium = 4GB; heavy = 8GB."
            )
            executor_memory = st.text_input(
                "Executor Memory Override",
                value=res_sec.get("executor_memory", ""),
                help="Explicit executor memory (e.g. 8g). Leave empty to use auto-tuner profile."
            )
            shuffle_partitions = st.number_input(
                "Shuffle Partitions Override",
                min_value=0,
                value=int(res_sec.get("shuffle_partitions", 0)),
                help="Explicit shuffle partition count (e.g. 200). 0 = Auto."
            )

        st.divider()
        st.subheader("9. 📧 Automated Email Alerts & Multi-Event Routing ([email_notification])")
        email_sec = defaults.get("email_notification", defaults.get("email", {}))

        email_enabled = st.checkbox(
            "Enable Automated Email Notifications",
            value=email_sec.get("enabled", False),
            help="When enabled, sends automated alert emails according to event routing rules below."
        )

        # Parse existing events list if present
        events_list = email_sec.get("events", [])
        succ_ev = next((e for e in events_list if str(e.get("event")).lower() in ("on_success", "success")), {})
        dq_ev = next((e for e in events_list if str(e.get("event")).lower() in ("on_quality_failure", "data_quality_failed")), {})

        tab_fail, tab_succ, tab_dq = st.tabs([
            "🔴 Failure Alerts (on_failure)",
            "🟢 Success Notices (on_success)",
            "⚠️ Quality Failure Alerts (on_quality_failure)"
        ])

        with tab_fail:
            em1, em2, em3 = st.columns(3)
            with em1:
                email_from = st.text_input(
                    "Sender Email Address (From)",
                    value=email_sec.get("from") or email_sec.get("sender") or "noreply@company.com",
                    help="Sender address formatted in TOML as `from = 'noreply@company.com'`."
                )
                email_subject_prefix = st.text_input(
                    "Subject Prefix",
                    value=email_sec.get("subject_prefix", "[ETL JOB FAILURE]"),
                    help="Prefix prepended to failure email subjects."
                )
            with em2:
                tmpl_val = str(email_sec.get("template", "job_failed")).lower()
                if source_type.lower() == "sftp":
                    tmpl_opts = ["job_failed", "job_success", "missing_file", "data_quality_failed", "sla_breached", "data_anomaly"]
                else:
                    tmpl_opts = ["job_failed", "job_success", "data_quality_failed", "sla_breached", "data_anomaly"]

                tmpl_idx = tmpl_opts.index(tmpl_val) if tmpl_val in tmpl_opts else 0
                email_template = st.selectbox(
                    "HTML Failure Alert Preset Template",
                    tmpl_opts,
                    index=tmpl_idx,
                    help="Select responsive HTML failure alert template preset."
                )
                to_val = email_sec.get("to", [])
                to_str_init = ", ".join(to_val) if isinstance(to_val, list) else str(to_val)
                email_to_str = st.text_input(
                    "Failure Recipients (To)",
                    value=to_str_init,
                    help="Comma-separated failure email addresses (e.g. devops@company.com, oncall@company.com)."
                )
            with em3:
                email_subject = st.text_input(
                    "Custom Subject Pattern (Optional)",
                    value=email_sec.get("subject", ""),
                    help="Optional pattern (e.g. `{subject_prefix} Job '{task_name}' Status: {status}`). Leave empty for default."
                )
                cc_val = email_sec.get("cc", [])
                cc_str_init = ", ".join(cc_val) if isinstance(cc_val, list) else str(cc_val)
                email_cc_str = st.text_input(
                    "CC Recipients",
                    value=cc_str_init,
                    help="Comma-separated CC email addresses."
                )
                bcc_val = email_sec.get("bcc", [])
                bcc_str_init = ", ".join(bcc_val) if isinstance(bcc_val, list) else str(bcc_val)
                email_bcc_str = st.text_input(
                    "BCC Recipients",
                    value=bcc_str_init,
                    help="Comma-separated BCC email addresses."
                )

        with tab_succ:
            st.caption("Route execution completion summaries to business users and analysts when pipeline finishes successfully.")
            email_succ_enabled = st.checkbox(
                "Enable Job Success Notifications",
                value=bool(succ_ev.get("enabled", False)),
                help="Sends a green summary report with row counts and duration upon successful load."
            )
            sc1, sc2 = st.columns(2)
            with sc1:
                succ_to_val = succ_ev.get("to", [])
                succ_to_str_init = ", ".join(succ_to_val) if isinstance(succ_to_val, list) else str(succ_to_val)
                email_succ_to_str = st.text_input(
                    "Success Recipients (To)",
                    value=succ_to_str_init,
                    help="Comma-separated success notification recipients (e.g. business-analysts@company.com)."
                )
            with sc2:
                email_succ_template = st.selectbox(
                    "Success Template",
                    ["job_success", "data_anomaly"],
                    index=0,
                    help="Select template for success notification."
                )

        with tab_dq:
            st.caption("Route data quality violation reports to Data Governance and QA teams.")
            email_dq_enabled = st.checkbox(
                "Enable Data Quality Failure Alerts",
                value=bool(dq_ev.get("enabled", False)),
                help="Sends an orange warning alert with sample violating records when validation checks fail."
            )
            dq1, dq2 = st.columns(2)
            with dq1:
                dq_to_val = dq_ev.get("to", [])
                dq_to_str_init = ", ".join(dq_to_val) if isinstance(dq_to_val, list) else str(dq_to_val)
                email_dq_to_str = st.text_input(
                    "Data Quality Recipients (To)",
                    value=dq_to_str_init,
                    help="Comma-separated QA / Governance recipients (e.g. data-qa@company.com)."
                )
            with dq2:
                email_dq_template = st.selectbox(
                    "Quality Failure Template",
                    ["data_quality_failed", "job_failed"],
                    index=0,
                    help="Select template for data quality alert."
                )

        st.divider()
        if is_editing:
            default_filename = os.path.basename(current_filepath)
        else:
            default_filename = f"{task_id}.toml"

        save_filename = st.text_input(
            "Save TOML File Name*",
            value=default_filename,
            help="Filename inside config/tasks/ directory (e.g. customer_load.toml)."
        )

        sub1, sub2 = st.columns([1, 4])
        with sub1:
            form_saved = st.form_submit_button("💾 Save TOML Configuration", type="primary")

    if form_saved:
        generated_toml = generate_toml_string(
            task_id=task_id,
            task_name=task_name,
            enabled=enabled,
            description=description,
            source_type=source_type,
            connection_name=connection_name,
            schema_name=schema_name,
            table_name=table_name,
            columns_str=columns_str,
            exclude_cols_str=exclude_cols_str,
            jdbc_part_col=jdbc_part_col,
            jdbc_num_parts=jdbc_num_parts,
            jdbc_fetch_size=jdbc_fetch_size,
            load_type=load_type,
            watermark_col=watermark_col,
            watermark_type=watermark_type,
            primary_keys_str=primary_keys_str,
            merge_keys_str=merge_keys_str,
            target_catalog=target_catalog,
            target_database=target_database,
            target_table=target_table,
            partition_col=partition_col,
            transform_rename_str=transform_rename_str,
            transform_cast_str=transform_cast_str,
            transform_derived_str=transform_derived_str,
            audit_enabled=audit_enabled,
            audit_insert_ts=audit_insert_ts,
            audit_updated_ts=audit_updated_ts,
            audit_task_id=audit_task_id,
            audit_source_sys=audit_source_sys,
            audit_timezone=audit_timezone,
            preload_operations=preload_operations,
            postload_operations=postload_operations,
            null_checks_str=null_checks_str,
            unique_checks_str=unique_checks_str,
            minimum_rows=minimum_rows,
            retries=retries,
            retry_delay=retry_delay,
            backoff_multiplier=backoff_multiplier,
            exponential_backoff=exponential_backoff,
            resource_profile=resource_profile,
            executor_memory=executor_memory,
            shuffle_partitions=shuffle_partitions,
            sftp_path=sftp_path,
            sftp_file_pattern=sftp_file_pattern,
            sftp_file_format=sftp_file_format,
            sftp_delimiter=sftp_delimiter,
            sftp_header=sftp_header,
            sftp_encoding=sftp_encoding,
            sftp_sheet_name=sftp_sheet_name,
            sftp_header_row=sftp_header_row,
            email_enabled=email_enabled,
            email_from=email_from,
            email_to_str=email_to_str,
            email_cc_str=email_cc_str,
            email_bcc_str=email_bcc_str,
            email_subject_prefix=email_subject_prefix,
            email_template=email_template,
            email_subject=email_subject,
            email_succ_enabled=email_succ_enabled,
            email_succ_to_str=email_succ_to_str,
            email_succ_template=email_succ_template,
            email_dq_enabled=email_dq_enabled,
            email_dq_to_str=email_dq_to_str,
            email_dq_template=email_dq_template
        )

        final_path = os.path.join(CONFIG_DIR, save_filename if save_filename.endswith(".toml") else f"{save_filename}.toml")
        with open(final_path, "w", encoding="utf-8") as f:
            f.write(generated_toml)

        st.success(f"Successfully saved TOML task configuration to `{final_path}`!")
        st.subheader("Generated TOML Preview:")
        st.code(generated_toml, language="toml")
        st.rerun()



# -----------------------------------------------------------------------------
# User Authentication & Role-Based Access Control (RBAC) System
# Hardcoded credentials for primary testing (mirrors etl_audit.etl_users table)
# -----------------------------------------------------------------------------
USERS_DB = {
    "admin": {
        "password": "admin123",
        "role": "ADMIN",
        "name": "System Administrator",
        "email": "admin@company.com"
    },
    "developer": {
        "password": "dev123",
        "role": "DEVELOPER",
        "name": "ETL Data Engineer",
        "email": "dev@company.com"
    },
    "viewer": {
        "password": "view123",
        "role": "VIEWER",
        "name": "Business Viewer / Auditor",
        "email": "viewer@company.com"
    }
}

ROLE_PERMISSIONS = {
    "ADMIN": ["VIEW_CATALOG", "CREATE_TASK", "EDIT_TASK", "EXECUTE_TASK", "DELETE_TASK", "MANAGE_USERS"],
    "DEVELOPER": ["VIEW_CATALOG", "CREATE_TASK", "EDIT_TASK", "EXECUTE_TASK"],
    "VIEWER": ["VIEW_CATALOG"]
}


def has_permission(permission: str) -> bool:
    """Checks if the currently logged-in user role has the required permission."""
    role = st.session_state.get("user_role", "VIEWER")
    return permission in ROLE_PERMISSIONS.get(role, [])


def render_login_page():
    """Renders professional Streamlit User Authentication Login Page."""
    st.markdown("""
        <style>
            .login-card {
                background-color: #161b22;
                border: 1px solid #30363d;
                border-radius: 12px;
                padding: 32px;
                max-width: 480px;
                margin: 40px auto;
            }
            .role-pill {
                display: inline-block;
                padding: 4px 10px;
                border-radius: 12px;
                font-size: 12px;
                font-weight: 700;
                margin-right: 6px;
            }
            .role-admin { background-color: #8957e5; color: #ffffff; }
            .role-dev { background-color: #1f6feb; color: #ffffff; }
            .role-viewer { background-color: #238636; color: #ffffff; }
        </style>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.title("🔐 ETL Platform Login")
        st.caption("Metadata-Driven PySpark & Iceberg Framework • Secure RBAC Access")
        st.markdown("---")

        with st.form("login_form"):
            username_input = st.text_input("Username*", value="admin")
            password_input = st.text_input("Password*", type="password", value="admin123")
            submitted = st.form_submit_button("🔓 Log In", type="primary", use_container_width=True)

            if submitted:
                user_info = USERS_DB.get(username_input.strip().lower())
                if user_info and user_info["password"] == password_input:
                    st.session_state.authenticated = True
                    st.session_state.username = username_input.strip().lower()
                    st.session_state.user_role = user_info["role"]
                    st.session_state.user_name = user_info["name"]
                    st.toast(f"Welcome back, {user_info['name']} ({user_info['role']})!")
                    st.rerun()
                else:
                    st.error("Invalid username or password. Please try again.")

        st.markdown("---")
        st.markdown("#### 🔑 Demo Credentials for Testing:")
        st.markdown("""
        - 🛡️ **`admin` / `admin123`**: **ADMIN Role** (Full access: Create, Edit, Run, Delete, User Management)
        - 💻 **`developer` / `dev123`**: **DEVELOPER Role** (Create, Edit, Run tasks)
        - 👁️ **`viewer` / `view123`**: **VIEWER Role** (Read-only access to catalog & logs)
        """)


def main():
    if st is None:
        print("ERROR: Streamlit is not installed. Please run 'pip install streamlit' to launch the web dashboard.")
        sys.exit(1)

    st.set_page_config(
        page_title="CDP Iceberg ETL Task Control Center",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.get("authenticated", False):
        render_login_page()
        return

    if "current_page" not in st.session_state:
        st.session_state.current_page = "Dashboard & Task Executor"

    st.markdown("""
        <style>
            [data-testid="stSidebar"] {
                background-color: #0e1117;
                padding-top: 1rem;
            }
            .sidebar-nav-header {
                font-size: 11px;
                font-weight: 700;
                color: #8b949e;
                text-transform: uppercase;
                letter-spacing: 1.2px;
                margin-top: 16px;
                margin-bottom: 8px;
                padding-left: 4px;
            }
            .stButton > button {
                width: 100%;
                text-align: left !important;
                justify-content: flex-start !important;
                border-radius: 8px;
                font-weight: 600;
                padding: 10px 16px;
            }
        </style>
    """, unsafe_allow_html=True)

    st.title("Metadata-Driven ETL Control Center")
    st.caption("Cloudera Data Platform (CDP) • Apache Iceberg • Multi-Database Source Management UI")

    # User Account & Role Profile Badge
    user_role = st.session_state.get("user_role", "VIEWER")
    user_name = st.session_state.get("user_name", "User")
    badge_color = "#8957e5" if user_role == "ADMIN" else ("#1f6feb" if user_role == "DEVELOPER" else "#238636")
    
    st.sidebar.markdown(f"### 👤 {user_name}")
    st.sidebar.markdown(f"<span style='background-color:{badge_color}; color:white; padding:3px 10px; border-radius:12px; font-weight:700; font-size:12px;'>ROLE: {user_role}</span>", unsafe_allow_html=True)
    if st.sidebar.button("🚪 Log Out", key="btn_logout", use_container_width=True):
        st.session_state.authenticated = False
        st.session_state.username = None
        st.session_state.user_role = None
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.markdown("<div class='sidebar-nav-header'>NAVIGATION BAR</div>", unsafe_allow_html=True)

    all_nav_options = [
        ("Dashboard & Task Executor", "dash", "VIEW_CATALOG"),
        ("Task Builder", "builder", "CREATE_TASK"),
        ("TOML Task Editor & Configurator", "editor", "EDIT_TASK"),
        ("Failure Recovery Center", "failure", "EXECUTE_TASK"),
        ("User & Access Control", "users", "MANAGE_USERS")
    ]
    nav_options = [(lbl, key) for lbl, key, perm in all_nav_options if has_permission(perm)]

    for label, key_suffix in nav_options:
        is_active = (st.session_state.current_page == label)
        btn_type = "primary" if is_active else "secondary"
        if st.sidebar.button(label, key=f"nav_bar_{key_suffix}", use_container_width=True, type=btn_type):
            st.session_state.current_page = label
            st.rerun()

    page = st.session_state.current_page

    os.makedirs(CONFIG_DIR, exist_ok=True)
    toml_files = sorted(glob.glob(os.path.join(CONFIG_DIR, "*.toml")))

    # -------------------------------------------------------------------------
    # PAGE 1: Dashboard & Task Executor
    # -------------------------------------------------------------------------
    if page == "Dashboard & Task Executor":
        st.header("📊 ETL Task Execution & Catalog Dashboard")

        catalog_data = build_job_catalog(toml_files)

        # KPI Metrics Cards
        total_jobs = len(catalog_data)
        active_jobs = sum(1 for item in catalog_data if item["Enabled"])
        unique_schemas = len(set(item["Schema / Db"] for item in catalog_data if item["Schema / Db"] != "N/A"))

        fail_count = 0
        if os.path.exists(FAILED_DIR):
            for d in os.listdir(FAILED_DIR):
                dp = os.path.join(FAILED_DIR, d)
                if os.path.isdir(dp):
                    fail_count += len(glob.glob(os.path.join(dp, "*.txt")))

        success_count = 0
        if os.path.exists(SUCCESS_DIR):
            for d in os.listdir(SUCCESS_DIR):
                dp = os.path.join(SUCCESS_DIR, d)
                if os.path.isdir(dp):
                    success_count += len(glob.glob(os.path.join(dp, "*.txt")))

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Configured Tasks", f"{total_jobs} Tasks")
        m2.metric("Active Enabled Tasks", f"{active_jobs} Active")
        m3.metric("Successful Task Runs", f"{success_count} Successes")
        m4.metric("Pending Failures", f"{fail_count} Failures", delta_color="inverse")

        st.divider()
        st.subheader("🗂️ Interactive Filterable Task Catalog")
        st.markdown("Search and filter your ETL jobs by **Source Schema**, Engine Type, Load Mode, or Free Text search.")

        # Multi-Criteria Search & Filter Toolbar
        f1, f2, f3, f4 = st.columns(4)

        with f1:
            all_schemas = sorted(list(set(item["Schema / Db"] for item in catalog_data if item["Schema / Db"] != "N/A")))
            selected_schema = st.selectbox("🏛️ Filter by Schema / Database:", ["All Schemas"] + all_schemas)

        with f2:
            all_engines = sorted(list(set(item["Source Engine"] for item in catalog_data if item["Source Engine"] != "N/A")))
            selected_engine = st.selectbox("🗄️ Filter by Source Engine:", ["All Engines"] + all_engines)

        with f3:
            all_modes = sorted(list(set(item["Load Type"] for item in catalog_data if item["Load Type"] != "N/A")))
            selected_mode = st.selectbox("⚡ Filter by Load Type:", ["All Modes"] + all_modes)

        with f4:
            selected_status = st.selectbox("🔘 Filter by Status:", ["All Statuses", "Active (Enabled)", "Disabled"])

        search_query = st.text_input("🔍 Search by Task ID, Task Name, or Table Name:", value="", help="Type any keyword to instantly filter the catalog grid.")

        # Filter Catalog List
        filtered_catalog = catalog_data
        if selected_schema != "All Schemas":
            filtered_catalog = [i for i in filtered_catalog if i["Schema / Db"] == selected_schema]
        if selected_engine != "All Engines":
            filtered_catalog = [i for i in filtered_catalog if i["Source Engine"] == selected_engine]
        if selected_mode != "All Modes":
            filtered_catalog = [i for i in filtered_catalog if i["Load Type"] == selected_mode]
        if selected_status == "Active (Enabled)":
            filtered_catalog = [i for i in filtered_catalog if i["Enabled"]]
        elif selected_status == "Disabled":
            filtered_catalog = [i for i in filtered_catalog if not i["Enabled"]]

        if search_query.strip():
            q = search_query.strip().lower()
            filtered_catalog = [
                i for i in filtered_catalog
                if q in i["Task ID"].lower() or q in i["Task Name"].lower() or q in i["Source Table"].lower() or q in i["Schema / Db"].lower()
            ]

        # Display Catalog Table Grid
        if pd is not None:
            df_catalog = pd.DataFrame(filtered_catalog)
            if not df_catalog.empty:
                display_df = df_catalog[["File Name", "Task ID", "Task Name", "Source Engine", "Schema / Db", "Source Table", "Load Type", "Target Table", "Status"]]
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("No task configurations match the selected filter criteria.")
        else:
            st.table(filtered_catalog)

        st.divider()
        st.subheader("🛠️ Bulk Operations & Status Management")
        st.markdown("Select multiple jobs to perform bulk activation (`enabled = true`), bulk deactivation (`enabled = false`), or bulk validation.")

        filtered_file_paths = [i["File Path"] for i in filtered_catalog]

        bcol1, bcol2 = st.columns([3, 1])
        with bcol1:
            bulk_selected_jobs = st.multiselect(
                "Select Target Jobs for Bulk Action:",
                options=filtered_file_paths,
                default=filtered_file_paths,
                format_func=lambda x: os.path.basename(x),
                help="Select one or more TOML job files to apply bulk actions."
            )
        with bcol2:
            st.write("")
            st.caption(f"Selected: **{len(bulk_selected_jobs)} / {len(filtered_file_paths)}** jobs")

        ba1, ba2, ba3 = st.columns(3)
        with ba1:
            if st.button("🟢 Bulk Activate Selected (Set Enabled=True)", key="bulk_activate"):
                if bulk_selected_jobs:
                    count = 0
                    for path in bulk_selected_jobs:
                        toggle_job_enabled_state(path, True)
                        count += 1
                    st.toast(f"Successfully activated {count} jobs!")
                    st.rerun()
                else:
                    st.warning("No jobs selected for bulk activation.")
        with ba2:
            if st.button("🔴 Bulk Deactivate Selected (Set Enabled=False)", key="bulk_deactivate"):
                if bulk_selected_jobs:
                    count = 0
                    for path in bulk_selected_jobs:
                        toggle_job_enabled_state(path, False)
                        count += 1
                    st.toast(f"Successfully deactivated {count} jobs!")
                    st.rerun()
                else:
                    st.warning("No jobs selected for bulk deactivation.")
        with ba3:
            btn_bulk_validate = st.button("🔍 Bulk Validate Selected", key="bulk_validate")

        st.divider()
        st.subheader("⚡ Single Job Action & Execution Engine")
        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("🎯 Single Task Execution")
            if not filtered_catalog:
                st.warning("No jobs available in the current filtered selection.")
                selected_job = None
            else:
                selected_job = st.selectbox(
                    "Select Target Job File:",
                    filtered_file_paths,
                    format_func=lambda x: os.path.basename(x)
                )

            if selected_job:
                st.caption(f"Config Path: `{selected_job}`")

                sel_obj = next((i for i in catalog_data if i["File Path"] == selected_job), None)
                cur_enabled = sel_obj["Enabled"] if sel_obj else True

                qa1, qa2, qa3 = st.columns(3)
                with qa1:
                    btn_val_single = st.button("🔍 Pre-flight Validate", key="val_single")
                with qa2:
                    btn_run_single = st.button("🚀 Run Task", key="run_single", type="primary")
                with qa3:
                    toggle_btn_label = "🔴 Disable Job" if cur_enabled else "🟢 Enable Job"
                    if st.button(toggle_btn_label, key="toggle_job"):
                        toggle_job_enabled_state(selected_job, not cur_enabled)
                        st.toast(f"Toggled job state to {'Enabled' if not cur_enabled else 'Disabled'}!")
                        st.rerun()

        with col2:
            st.subheader("⚡ Multi-Table Batch Execution")
            st.markdown("Executes all active `.toml` files in `config/tasks/` in parallel worker threads.")
            parallel_workers = st.slider("Parallel Worker Threads (`--parallel`):", min_value=1, max_value=16, value=4)

            btn_val_batch = st.button("🔍 Pre-flight Validate All Jobs", key="val_batch")
            btn_run_batch = st.button("🔥 Run Parallel Multi-Table Batch", key="run_batch", type="primary")

        st.divider()
        st.subheader("💻 Live Output Terminal Log")
        log_container = st.empty()

        if selected_job and btn_val_single:
            cmd = [sys.executable, "main.py", "--config", selected_job, "--validate"]
            log_text = f"Running command: {' '.join(cmd)}\n\n"
            for line in run_command_stream(cmd):
                log_text += line
                log_container.code(log_text, language="text")

        elif selected_job and btn_run_single:
            cmd = [sys.executable, "main.py", "--config", selected_job]
            log_text = f"Running command: {' '.join(cmd)}\n\n"
            for line in run_command_stream(cmd):
                log_text += line
                log_container.code(log_text, language="text")

        elif btn_val_batch:
            cmd = [sys.executable, "main.py", "--config-dir", CONFIG_DIR, "--parallel", str(parallel_workers), "--validate"]
            log_text = f"Running command: {' '.join(cmd)}\n\n"
            for line in run_command_stream(cmd):
                log_text += line
                log_container.code(log_text, language="text")

        elif btn_run_batch:
            cmd = [sys.executable, "main.py", "--config-dir", CONFIG_DIR, "--parallel", str(parallel_workers)]
            log_text = f"Running command: {' '.join(cmd)}\n\n"
            for line in run_command_stream(cmd):
                log_text += line
                log_container.code(log_text, language="text")

    # -------------------------------------------------------------------------
    # PAGE 2: Task Builder
    # -------------------------------------------------------------------------
    elif page == "Task Builder":
        st.header("Create New Task")
        st.markdown("Fill out the interactive form below with guided tooltips to generate a production TOML file.")
        render_visual_form(defaults={}, is_editing=False)

    # -------------------------------------------------------------------------
    # PAGE 3: TOML Task Editor & Configurator (Visual Form + Code Editor)
    # -------------------------------------------------------------------------
    elif page == "TOML Task Editor & Configurator":
        st.header("✏️ Edit Existing TOML Task Configuration")
        st.markdown("Select an existing job file to edit via **Visual Form Builder (Guided)** or **Raw TOML Code Editor**.")

        if not toml_files:
            st.warning("No TOML configuration files found in `config/tasks/`.")
        else:
            selected_file = st.selectbox(
                "Select TOML File to Modify:",
                toml_files,
                format_func=lambda x: os.path.basename(x),
                key="editor_file_select"
            )

            edit_mode = st.radio(
                "Choose Editing Mode:",
                ["🎛️ Visual Form Editor (Guided Form)", "📝 Raw TOML Code Editor"],
                horizontal=True
            )

            if selected_file:
                parsed_defaults = parse_toml_to_dict(selected_file)

                if edit_mode == "🎛️ Visual Form Editor (Guided Form)":
                    st.info(f"Loaded existing configuration from `{os.path.basename(selected_file)}` into visual form.")
                    render_visual_form(defaults=parsed_defaults, is_editing=True, current_filepath=selected_file)

                else:
                    with open(selected_file, "r", encoding="utf-8") as f:
                        current_content = f.read()

                    edited_content = st.text_area(
                        f"Editing `{os.path.basename(selected_file)}` Code:",
                        value=current_content,
                        height=500
                    )

                    ec1, ec2 = st.columns([1, 4])
                    with ec1:
                        save_edited = st.button("💾 Save Code Changes", type="primary", key="save_edited_code")
                    with ec2:
                        val_edited = st.button("🔍 Pre-flight Validate Credentials", key="val_edited_code")

                    if save_edited:
                        if tomllib is not None:
                            try:
                                tomllib.loads(edited_content)
                            except Exception as parse_err:
                                st.error(f"TOML Syntax Error: Cannot save invalid TOML structure: {parse_err}")
                                st.stop()

                        with open(selected_file, "w", encoding="utf-8") as f:
                            f.write(edited_content)
                        st.success(f"Saved changes to `{selected_file}` successfully!")
                        st.rerun()

                    if val_edited:
                        cmd = [sys.executable, "main.py", "--config", selected_file, "--validate"]
                        log_text = f"Running command: {' '.join(cmd)}\n\n"
                        log_box = st.empty()
                        for line in run_command_stream(cmd):
                            log_text += line
                            log_box.code(log_text, language="text")

    # -------------------------------------------------------------------------
    # PAGE 4: Execution Audit & Failure Recovery Center
    # -------------------------------------------------------------------------
    elif page == "Failure Recovery Center":
        st.header("🚨 Execution Audit & Failure Recovery Center")
        st.markdown("Inspect job success audit markers (`success_jobs/YYYYMMDD/`), review failed job logs (`failed_jobs/YYYYMMDD/`), and trigger one-click reruns.")

        tab_fail, tab_success = st.tabs(["🚨 Failed Jobs Markers", "✅ Success Jobs Markers"])

        with tab_fail:
            if not os.path.exists(FAILED_DIR):
                st.info("No failures recorded yet. The `failed_jobs/` directory does not exist.")
            else:
                date_dirs = sorted([d for d in os.listdir(FAILED_DIR) if os.path.isdir(os.path.join(FAILED_DIR, d))], reverse=True)
                if not date_dirs:
                    st.info("All failure marker directories have been cleared/recovered! No failed jobs present.")
                else:
                    selected_date = st.selectbox("Select Failure Date Folder (YYYYMMDD):", date_dirs, key="fail_date_sel")
                    date_path = os.path.join(FAILED_DIR, selected_date)
                    marker_files = sorted(glob.glob(os.path.join(date_path, "*.txt")))

                    st.subheader(f"Failure Marker Files for Date: `{selected_date}` ({len(marker_files)} files)")

                    fc1, fc2 = st.columns([1, 1])
                    with fc1:
                        rerun_workers = st.slider("Rerun Parallel Workers:", min_value=1, max_value=16, value=4, key="rerun_workers")
                        btn_rerun = st.button(f"🔥 One-Click Rerun Failed Jobs for {selected_date}", type="primary")

                    if marker_files:
                        for mf in marker_files:
                            with st.expander(f"📄 {os.path.basename(mf)}"):
                                with open(mf, "r", encoding="utf-8") as f:
                                    st.code(f.read(), language="text")

                    if btn_rerun:
                        cmd = [sys.executable, "main.py", "--rerun-failed", selected_date, "--parallel", str(rerun_workers)]
                        log_text = f"Running command: {' '.join(cmd)}\n\n"
                        log_box = st.empty()
                        for line in run_command_stream(cmd):
                            log_text += line
                            log_box.code(log_text, language="text")

        with tab_success:
            if not os.path.exists(SUCCESS_DIR):
                st.info("No successful job runs recorded yet. The `success_jobs/` directory does not exist.")
            else:
                s_date_dirs = sorted([d for d in os.listdir(SUCCESS_DIR) if os.path.isdir(os.path.join(SUCCESS_DIR, d))], reverse=True)
                if not s_date_dirs:
                    st.info("No success markers recorded yet.")
                else:
                    selected_s_date = st.selectbox("Select Success Date Folder (YYYYMMDD):", s_date_dirs, key="succ_date_sel")
                    s_date_path = os.path.join(SUCCESS_DIR, selected_s_date)
                    s_marker_files = sorted(glob.glob(os.path.join(s_date_path, "*.txt")))

                    st.subheader(f"Success Marker Files for Date: `{selected_s_date}` ({len(s_marker_files)} files)")

                    if s_marker_files:
                        for smf in s_marker_files:
                            with st.expander(f"✅ {os.path.basename(smf)}"):
                                with open(smf, "r", encoding="utf-8") as f:
                                    st.code(f.read(), language="text")


if __name__ == "__main__":
    main()
