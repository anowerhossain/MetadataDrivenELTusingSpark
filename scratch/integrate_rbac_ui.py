"""
Python script to update web_ui/app.py with complete User Authentication & Role-Based Access Control (RBAC).
"""

with open("web_ui/app.py", "r", encoding="utf-8") as f:
    content = f.read()

# Add sys.path insertion at top
sys_path_code = """import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

"""

if "sys.path.insert(0" not in content:
    content = sys_path_code + content

# Replace CONFIG_DIR with relative path from web_ui
content = content.replace('CONFIG_DIR = os.path.join("config", "tasks")', 'CONFIG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "config", "tasks"))')
content = content.replace('FAILED_DIR = "failed_jobs"', 'FAILED_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "failed_jobs"))')
content = content.replace('SUCCESS_DIR = "success_jobs"', 'SUCCESS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "success_jobs"))')

# Define RBAC user database and permission helper
rbac_code = '''
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
'''

# Find def main() and inject RBAC check
if "def render_login_page" not in content:
    main_idx = content.find("def main():")
    if main_idx != -1:
        content = content[:main_idx] + rbac_code + "\n\n" + content[main_idx:]
        print("Injected RBAC functions into app.py!")

with open("web_ui/app.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Finished integrating RBAC definitions.")
