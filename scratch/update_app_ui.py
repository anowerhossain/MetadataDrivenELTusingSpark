"""
Update app.py to incorporate Task Type selector (Table Load, Qlik Replicate, Qlik Sense, Qlik NPrinting)
and replace all remaining 'Job' strings in UI headers and form sections.
"""

import re

with open("app.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1. Update page config and titles
content = content.replace('page_title="CDP Iceberg ETL Job Control Center"', 'page_title="CDP Iceberg ETL Task Control Center"')
content = content.replace('st.header("📊 ETL Job Execution & Catalog Dashboard")', 'st.header("📊 ETL Task Execution & Catalog Dashboard")')
content = content.replace('st.subheader("1. Job Configuration")', 'st.subheader("1. Task Header & Metadata")')
content = content.replace('Job Active (Enabled)', 'Task Active (Enabled)')
content = content.replace('visual_job_form', 'visual_task_form')
content = content.replace('Create new job', 'Create New Task')
content = content.replace('Job Configuration', 'Task Configuration')
content = content.replace('Total Configured Tasks", f"{total_jobs} Jobs"', 'Total Configured Tasks", f"{total_jobs} Tasks"')
content = content.replace('Job Catalog', 'Task Catalog')

with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)

print("Successfully updated app.py UI labels!")
