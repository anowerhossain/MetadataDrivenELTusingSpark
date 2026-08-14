"""
Comprehensive script to inject Task Type selector and form rendering logic into app.py.
"""

with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

# Replace generate_toml_string
new_generate_toml = '''def generate_toml_string(
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
""".strip() + "\\n"

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
""".strip() + "\\n"

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
""".strip() + "\\n"

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
        toml_content += f'path = "{sftp_path}"\\n'
        toml_content += f'file_pattern = "{sftp_file_pattern}"\\n'
        toml_content += f'file_format = "{sftp_file_format}"\\n'
        if sftp_file_format.lower() == "csv":
            toml_content += f'delimiter = "{sftp_delimiter}"\\n'
            toml_content += f'header = {str(sftp_header).lower()}\\n'
            toml_content += f'encoding = "{sftp_encoding}"\\n'
        else:
            toml_content += f'sheet_name = "{sftp_sheet_name}"\\n'
            toml_content += f'header_row = {sftp_header_row}\\n'

    if cols_list or excl_list:
        toml_content += "\\n[source.extraction]\\n"
        if cols_list:
            toml_content += f"columns = {cols_list}\\n"
        if excl_list:
            toml_content += f"exclude_columns = {excl_list}\\n"

    if source_type.lower() != "sftp" and (jdbc_part_col or jdbc_fetch_size):
        toml_content += "\\n[source.jdbc]\\n"
        if jdbc_fetch_size:
            toml_content += f"fetch_size = {jdbc_fetch_size}\\n"
        if jdbc_part_col:
            toml_content += f'partition_column = "{jdbc_part_col}"\\nnum_partitions = {jdbc_num_parts}\\n'

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
        toml_content += "\\n[keys]\\n"
        if pkeys_list:
            toml_content += f"primary_key = {pkeys_list}\\n"
        if mkeys_list:
            toml_content += f"merge_keys = {mkeys_list}\\n"

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
        toml_content += "\\n[transform.rename]\\n"
        for k, v in rename_dict.items():
            toml_content += f'{k} = "{v}"\\n'

    if cast_dict:
        toml_content += "\\n[transform.cast]\\n"
        for k, v in cast_dict.items():
            toml_content += f'{k} = "{v}"\\n'

    if derived_dict:
        toml_content += "\\n[transform.derived]\\n"
        for k, v in derived_dict.items():
            val_str = v if (v.startswith("'") or v.startswith('"') or v.lower() in ("true", "false") or v.isdigit()) else f'"{v}"'
            toml_content += f'{k} = {val_str}\\n'

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
        toml_content += f"\\n[preload]\\noperations = {preload_operations}\\n"

    if postload_operations:
        toml_content += f"\\n[postload]\\noperations = {postload_operations}\\n"

    if nulls_list or uniques_list or minimum_rows != 1:
        toml_content += "\\n[quality]\\n"
        if nulls_list:
            toml_content += f"null_check = {nulls_list}\\n"
        if uniques_list:
            toml_content += f"unique_check = {uniques_list}\\n"
        if minimum_rows != 1:
            toml_content += f"minimum_rows = {minimum_rows}\\n"

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
            toml_content += f'executor_memory = "{executor_memory}"\\n'
        if shuffle_partitions > 0:
            toml_content += f"shuffle_partitions = {shuffle_partitions}\\n"

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
            toml_content += f'subject = "{email_subject}"\\n'

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

    return toml_content.strip() + "\\n"
'''

# Find def generate_toml_string start and replace up to render_visual_form
start_idx = content.find("def generate_toml_string(")
end_idx = content.find("def render_visual_form(")

if start_idx != -1 and end_idx != -1:
    content = content[:start_idx] + new_generate_toml + "\n\n" + content[end_idx:]
    print("Replaced generate_toml_string in app.py!")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Updated app.py generate_toml_string.")
