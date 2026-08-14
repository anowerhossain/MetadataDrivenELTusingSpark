"""
Python script to inject Task Type selector into render_visual_form and generate_toml_string in app.py.
"""

with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

# Replace build_job_catalog with build_task_catalog and update keys
old_catalog = """def build_job_catalog(toml_files: List[str]) -> List[Dict[str, Any]]:
    \"\"\"Reads all TOML files and builds structured metadata catalog list.\"\"\"
    catalog = []
    for filepath in toml_files:
        d = parse_toml_to_dict(filepath)
        job_sec = d.get("job", {})
        src_sec = d.get("source", {})
        load_sec = d.get("load", {})
        tgt_sec = d.get("target", {})
        email_sec = d.get("email_notification", {}) or d.get("email", {})

        tgt_str = f"{tgt_sec.get('database', '')}.{tgt_sec.get('table', '')}" if tgt_sec.get('database') else tgt_sec.get('table', '')
        email_str = "📧 Enabled" if email_sec.get("enabled", False) else "⚪ Off"

        catalog.append({
            "File Name": os.path.basename(filepath),
            "Task ID": job_sec.get("task_id", "N/A"),
            "Task Name": job_sec.get("task_name", "N/A"),
            "Status": "🟢 Active" if job_sec.get("enabled", True) else "🔴 Disabled",
            "Enabled": job_sec.get("enabled", True),
            "Email Alerts": email_str,
            "Source Engine": src_sec.get("type", "N/A").upper(),
            "Schema / Db": src_sec.get("schema", "N/A"),
            "Source Table": src_sec.get("table", "N/A"),
            "Load Type": load_sec.get("type", "N/A").upper(),
            "Target Table": tgt_str,
            "File Path": filepath
        })
    return catalog"""

new_catalog = """def build_job_catalog(toml_files: List[str]) -> List[Dict[str, Any]]:
    \"\"\"Reads all TOML files and builds structured metadata catalog list.\"\"\"
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
    return catalog"""

if old_catalog in content:
    content = content.replace(old_catalog, new_catalog)
    print("Replaced build_job_catalog!")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Catalog builder updated.")
