"""
Update app.py render_visual_form and generate_toml_string with Task Type selector and fields for Table Load, Qlik Replicate, Qlik Sense, and Qlik NPrinting.
"""

with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

# Replace generate_toml_string header & logic
old_gen_toml = """def generate_toml_string(
    job_id: str,
    job_name: str,
    enabled: bool = True,
    description: str = "",
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
    audit_job_id: str = "dwh_etl_run_id",
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
    email_subject_prefix: str = "[ETL JOB FAILURE]",
    email_template: str = "job_failed",
    email_subject: str = "",
    email_succ_enabled: bool = False,
    email_succ_to_str: str = "",
    email_succ_template: str = "job_success",
    email_dq_enabled: bool = False,
    email_dq_to_str: str = "",
    email_dq_template: str = "data_quality_failed"
) -> str:
    \"\"\"Constructs clean, standardized TOML string from form field inputs.\"\"\"
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

    toml_content = f\"\"\"[task]
task_id = "{job_id}"
task_name = "{job_name}"
enabled = {str(enabled).lower()}
description = "{description}" """

new_gen_toml = """def generate_toml_string(
    job_id: str,
    job_name: str,
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
    audit_job_id: str = "dwh_etl_run_id",
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
    email_subject_prefix: str = "[ETL JOB FAILURE]",
    email_template: str = "job_failed",
    email_subject: str = "",
    email_succ_enabled: bool = False,
    email_succ_to_str: str = "",
    email_succ_template: str = "job_success",
    email_dq_enabled: bool = False,
    email_dq_to_str: str = "",
    email_dq_template: str = "data_quality_failed"
) -> str:
    \"\"\"Constructs clean, standardized TOML string from form field inputs.\"\"\"
    if task_type == "qlik_replicate":
        return f\"\"\"[task]
task_id = "{job_id}"
task_name = "{job_name}"
type = "qlik_replicate"
enabled = {str(enabled).lower()}
description = "{description}"

[qlik_replicate]
server_url = "{qlik_server_url}"
task_name = "{qlik_task_name}"
action = "{qlik_action}"
timeout_seconds = 300
poll_interval_seconds = 5
\"\"\".strip() + "\\n"

    elif task_type == "qlik_sense":
        return f\"\"\"[task]
task_id = "{job_id}"
task_name = "{job_name}"
type = "qlik_sense"
enabled = {str(enabled).lower()}
description = "{description}"

[qlik_sense]
server_url = "{qlik_sense_url}"
app_id = "{qlik_sense_app_id}"
timeout_seconds = 600
poll_interval_seconds = 10
\"\"\".strip() + "\\n"

    elif task_type == "qlik_nprinting":
        return f\"\"\"[task]
task_id = "{job_id}"
task_name = "{job_name}"
type = "qlik_nprinting"
enabled = {str(enabled).lower()}
description = "{description}"

[qlik_nprinting]
server_url = "{qlik_np_url}"
report_id = "{qlik_np_report_id}"
output_format = "{qlik_np_output_format}"
timeout_seconds = 600
poll_interval_seconds = 10
\"\"\".strip() + "\\n"

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

    toml_content = f\"\"\"[task]
task_id = "{job_id}"
task_name = "{job_name}"
type = "table_load"
enabled = {str(enabled).lower()}
description = "{description}" """

if old_gen_toml in content:
    content = content.replace(old_gen_toml, new_gen_toml)
    print("Updated generate_toml_string logic!")
else:
    print("Could not find old_gen_toml string pattern exactly.")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)
