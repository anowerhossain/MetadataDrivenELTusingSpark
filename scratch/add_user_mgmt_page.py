"""
Script to inject User & Access Control management page into web_ui/app.py.
"""

with open("web_ui/app.py", "r", encoding="utf-8") as f:
    content = f.read()

user_mgmt_code = '''
    # -------------------------------------------------------------------------
    # PAGE 5: User & Access Control Management (ADMIN Only)
    # -------------------------------------------------------------------------
    elif page == "User & Access Control":
        st.header("👥 User Authentication & Role-Based Access Control (RBAC)")
        st.markdown("Manage system user accounts, assigned roles, and inspect Apache Iceberg authentication DDL schemas (`etl_audit.etl_users`).")

        tab_users, tab_matrix, tab_ddl = st.tabs(["👤 Active Users List", "🛡️ Role Permission Matrix", "📜 Iceberg DDL Schema"])

        with tab_users:
            st.subheader("Configured System Accounts")
            users_list = []
            for u_id, u_data in USERS_DB.items():
                users_list.append({
                    "Username": u_id,
                    "Display Name": u_data["name"],
                    "Role": u_data["role"],
                    "Email": u_data["email"],
                    "Status": "🟢 Active"
                })
            if pd is not None:
                st.dataframe(pd.DataFrame(users_list), use_container_width=True, hide_index=True)
            else:
                st.table(users_list)

        with tab_matrix:
            st.subheader("Role Permission Mapping Matrix")
            matrix_data = [
                {"Permission": "VIEW_CATALOG (Read Catalog & Logs)", "ADMIN": "✅ Allowed", "DEVELOPER": "✅ Allowed", "VIEWER": "✅ Allowed"},
                {"Permission": "CREATE_TASK (Create New Pipeline)", "ADMIN": "✅ Allowed", "DEVELOPER": "✅ Allowed", "VIEWER": "🔒 Denied"},
                {"Permission": "EDIT_TASK (Modify Pipeline TOML)", "ADMIN": "✅ Allowed", "DEVELOPER": "✅ Allowed", "VIEWER": "🔒 Denied"},
                {"Permission": "EXECUTE_TASK (Run Pipelines & Recovery)", "ADMIN": "✅ Allowed", "DEVELOPER": "✅ Allowed", "VIEWER": "🔒 Denied"},
                {"Permission": "DELETE_TASK (Delete Pipeline Files)", "ADMIN": "✅ Allowed", "DEVELOPER": "🔒 Denied", "VIEWER": "🔒 Denied"},
                {"Permission": "MANAGE_USERS (User Accounts Management)", "ADMIN": "✅ Allowed", "DEVELOPER": "🔒 Denied", "VIEWER": "🔒 Denied"}
            ]
            if pd is not None:
                st.dataframe(pd.DataFrame(matrix_data), use_container_width=True, hide_index=True)
            else:
                st.table(matrix_data)

        with tab_ddl:
            st.subheader("Apache Iceberg RBAC DDL (`sql/04_create_rbac_tables_ddl.sql`)")
            ddl_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "sql", "04_create_rbac_tables_ddl.sql"))
            if os.path.exists(ddl_file):
                with open(ddl_file, "r", encoding="utf-8") as f:
                    st.code(f.read(), language="sql")
            else:
                st.info("DDL file sql/04_create_rbac_tables_ddl.sql not found.")
'''

if "User & Access Control" not in content:
    target_idx = content.find("if __name__ == \"__main__\":")
    if target_idx != -1:
        content = content[:target_idx] + user_mgmt_code + "\n\n" + content[target_idx:]
        print("Injected User & Access Control page into web_ui/app.py!")

with open("web_ui/app.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Finished inject script.")
