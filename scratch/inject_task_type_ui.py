"""
Inject Task Type selector & dynamic form UI into render_visual_form in app.py.
"""

with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

old_render_start = """def render_visual_form(defaults: Dict[str, Any], is_editing: bool = False, current_filepath: str = ""):
    \"\"\"Renders guided visual form builder with field tooltips including transform rules & preload/postload hooks.\"\"\"
    job_sec = defaults.get("job", {})
    src_sec = defaults.get("source", {})"""

new_render_start = """def render_visual_form(defaults: Dict[str, Any], is_editing: bool = False, current_filepath: str = ""):
    \"\"\"Renders guided visual form builder supporting Table Load, Qlik Replicate, Qlik Sense, and Qlik NPrinting tasks.\"\"\"
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

    job_sec = task_sec"""

if old_render_start in content:
    content = content.replace(old_render_start, new_render_start)
    print("Injected Task Type selector and dynamic forms into render_visual_form!")
else:
    print("Could not find old_render_start pattern.")

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Finished task UI injection script.")
