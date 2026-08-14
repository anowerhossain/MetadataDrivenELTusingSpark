"""
Script to update main() in web_ui/app.py with login enforcement, sidebar user profile badge, logout, and role permission checks.
"""

with open("web_ui/app.py", "r", encoding="utf-8") as f:
    content = f.read()

# Replace main() implementation
old_main = """def main():
    if st is None:
        print("ERROR: Streamlit is not installed. Please run 'pip install streamlit' to launch the web dashboard.")
        sys.exit(1)

    st.set_page_config(
        page_title="CDP Iceberg ETL Task Control Center",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    if "current_page" not in st.session_state:
        st.session_state.current_page = "Task Builder" """

new_main = """def main():
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

    if not st.session_state.authenticated:
        render_login_page()
        return

    if "current_page" not in st.session_state:
        st.session_state.current_page = "Dashboard & Task Executor" """

if old_main in content:
    content = content.replace(old_main, new_main)
    print("Replaced main entry point!")

# Add Sidebar User Profile & Logout Widget
old_sidebar_header = """    st.sidebar.markdown("## ETL CONTROL")
    st.sidebar.caption("Metadata Engine • CDP Iceberg")
    st.sidebar.markdown("<div class='sidebar-nav-header'>NAVIGATION BAR</div>", unsafe_allow_html=True)"""

new_sidebar_header = """    # User Account & Role Profile Badge
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
    st.sidebar.markdown("<div class='sidebar-nav-header'>NAVIGATION BAR</div>", unsafe_allow_html=True)"""

if old_sidebar_header in content:
    content = content.replace(old_sidebar_header, new_sidebar_header)
    print("Replaced sidebar profile badge!")

# Update Navigation Options according to role
old_nav_options = """    nav_options = [
        ("Task Builder", "builder"),
        ("Dashboard & Task Executor", "dash"),
        ("TOML Task Editor & Configurator", "editor"),
        ("Failure Recovery Center", "failure")
    ]"""

new_nav_options = """    all_nav_options = [
        ("Dashboard & Task Executor", "dash", "VIEW_CATALOG"),
        ("Task Builder", "builder", "CREATE_TASK"),
        ("TOML Task Editor & Configurator", "editor", "EDIT_TASK"),
        ("Failure Recovery Center", "failure", "EXECUTE_TASK"),
        ("User & Access Control", "users", "MANAGE_USERS")
    ]
    nav_options = [(lbl, key) for lbl, key, perm in all_nav_options if has_permission(perm)]"""

if old_nav_options in content:
    content = content.replace(old_nav_options, new_nav_options)
    print("Replaced navigation options with role filtering!")

with open("web_ui/app.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Main RBAC integration complete.")
