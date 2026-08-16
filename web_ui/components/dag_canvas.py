"""
High-End Interactive Vis.js HTML5 DAG Network Component for Streamlit Web UI.
Features custom Medallion Tier styling, animated glow borders, smooth Bezier arrows (#4895EF),
interactive node inspection, and full canvas controls (Zoom/Fit/Pan).
"""

import os
import glob
import json
from typing import Dict, Any, List
import streamlit as st
import streamlit.components.v1 as components
from src.core.config import JobConfig


def get_task_execution_status(task_id: str) -> str:
    """Checks success_jobs/ and failed_jobs/ directories to resolve recent task run status."""
    today_date = "20260816"
    failed_dir = os.path.join("failed_jobs", today_date)
    success_dir = os.path.join("success_jobs", today_date)
    luigi_marker = os.path.join("success_jobs", "luigi_markers", today_date, f"{task_id}.done")

    if os.path.exists(luigi_marker):
        return "SUCCESS"

    if os.path.exists(failed_dir):
        for mf in glob.glob(os.path.join(failed_dir, "*.txt")):
            if task_id in os.path.basename(mf):
                return "FAILED"

    if os.path.exists(success_dir):
        for mf in glob.glob(os.path.join(success_dir, "*.txt")):
            if task_id in os.path.basename(mf):
                return "SUCCESS"

    return "IDLE"


def get_node_style_and_tier(task_id: str, config: JobConfig) -> Dict[str, Any]:
    """Resolves node visual styling based on Medallion Tier, Task Type, and Execution Status."""
    task_type = str(config.raw_config.get("task", {}).get("type", "table_load")).lower()
    task_id_lower = task_id.lower()
    task_name_lower = config.job.task_name.lower()

    if "qlik" in task_type or "qlik" in task_id_lower:
        tier_label = "QLIK ENGINE"
        border_color = "#8b5cf6"
        bg_color = "#1e1b4b"
        icon = "🔄"
    elif "gold" in task_id_lower or "gold" in task_name_lower or "report" in task_name_lower:
        tier_label = "GOLD TIER"
        border_color = "#059669"
        bg_color = "#064e3b"
        icon = "🥇"
    elif "silver" in task_id_lower or "silver" in task_name_lower or "clean" in task_name_lower:
        tier_label = "SILVER TIER"
        border_color = "#0284c7"
        bg_color = "#0c4a6e"
        icon = "🥈"
    else:
        tier_label = "BRONZE TIER"
        border_color = "#d97706"
        bg_color = "#451a03"
        icon = "🟤"

    status = get_task_execution_status(task_id)
    if status == "SUCCESS":
        status_badge = "✅ SUCCESS"
        border_color = "#22c55e"
    elif status == "FAILED":
        status_badge = "🚨 FAILED"
        border_color = "#ef4444"
    else:
        status_badge = "⚪ READY"

    return {
        "tier_label": tier_label,
        "border_color": border_color,
        "bg_color": bg_color,
        "icon": icon,
        "status_badge": status_badge
    }


def render_interactive_vis_dag(
    config_map: Dict[str, JobConfig],
    height: int = 480,
    depends_on_override: Dict[str, List[str]] = None,
):
    """
    Renders high-end Vis.js HTML5 Interactive DAG Network Component in Streamlit.
    depends_on_override: optional dict of {task_id: [parent_task_ids]} to override
    frozen dataclass depends_on values (used when loading job TOML pipelines).
    """
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    if depends_on_override is None:
        depends_on_override = {}

    for task_id, config in config_map.items():
        style = get_node_style_and_tier(task_id, config)
        task_name = config.job.task_name
        task_type = str(config.raw_config.get("task", {}).get("type", "table_load")).upper()

        label_text = f"{style['icon']} {task_id}\n{task_name}\n[{style['tier_label']}]"

        nodes.append({
            "id": task_id,
            "label": label_text,
            "title": f"<b>Task ID:</b> {task_id}<br><b>Name:</b> {task_name}<br><b>Type:</b> {task_type}<br><b>Status:</b> {style['status_badge']}",
            "shape": "box",
            "margin": 14,
            "color": {
                "background": style["bg_color"],
                "border": style["border_color"],
                "highlight": {
                    "background": "#1e293b",
                    "border": "#4895EF"
                }
            },
            "font": {
                "color": "#ffffff",
                "face": "Segoe UI, sans-serif",
                "size": 13,
                "bold": True
            },
            "borderWidth": 2,
            "borderWidthSelected": 4,
            "shadow": {
                "enabled": True,
                "color": "rgba(0,0,0,0.6)",
                "size": 12
            }
        })

        # Use job-level override if provided, else fall back to task-level depends_on
        effective_deps = depends_on_override.get(task_id, config.job.depends_on)
        for dep_id in effective_deps:
            edges.append({
                "from": dep_id,
                "to": task_id,
                "arrows": {
                    "to": {
                        "enabled": True,
                        "scaleFactor": 1.2
                    }
                },
                "color": {
                    "color": "#4895EF",
                    "highlight": "#60a5fa"
                },
                "width": 3,
                "smooth": {
                    "type": "cubicBezier",
                    "forceDirection": "horizontal",
                    "roundness": 0.6
                }
            })

    nodes_json = json.dumps(nodes)
    edges_json = json.dumps(edges)

    html_code = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
        <style type="text/css">
            body {{
                background-color: #0e1117;
                margin: 0;
                padding: 0;
                font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
                overflow: hidden;
            }}
            #mynetwork {{
                width: 100%;
                height: {height}px;
                background: radial-gradient(circle at center, #1b2430 0%, #0d1117 100%);
                border: 1px solid #30363d;
                border-radius: 14px;
            }}
            .dag-header-toolbar {{
                position: absolute;
                top: 14px;
                left: 14px;
                background: rgba(15, 23, 42, 0.9);
                backdrop-filter: blur(10px);
                border: 1px solid #334155;
                padding: 8px 16px;
                border-radius: 10px;
                color: #f8fafc;
                font-size: 12px;
                z-index: 10;
                display: flex;
                align-items: center;
                gap: 16px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.4);
            }}
            .legend-item {{
                display: flex;
                align-items: center;
                gap: 6px;
                font-weight: 600;
            }}
            .legend-dot {{
                width: 10px;
                height: 10px;
                border-radius: 50%;
                display: inline-block;
            }}
            .toolbar-btn {{
                position: absolute;
                top: 14px;
                right: 14px;
                z-index: 10;
                display: flex;
                gap: 8px;
            }}
            .btn-action {{
                background: #1e293b;
                border: 1px solid #475569;
                color: #e2e8f0;
                padding: 6px 12px;
                border-radius: 6px;
                font-size: 12px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.2s ease;
            }}
            .btn-action:hover {{
                background: #4895EF;
                color: #ffffff;
                border-color: #4895EF;
            }}
        </style>
    </head>
    <body>
        <div class="dag-header-toolbar">
            <div class="legend-item"><span class="legend-dot" style="background:#d97706;"></span>🟤 Bronze Ingestion</div>
            <div class="legend-item"><span class="legend-dot" style="background:#0284c7;"></span>🥈 Silver Transform</div>
            <div class="legend-item"><span class="legend-dot" style="background:#059669;"></span>🥇 Gold Reporting</div>
            <div class="legend-item"><span class="legend-dot" style="background:#8b5cf6;"></span>🔄 Qlik Engine</div>
        </div>

        <div class="toolbar-btn">
            <button class="btn-action" onclick="fitNetwork()">🎯 Fit Graph</button>
            <button class="btn-action" onclick="resetZoom()">🔍 Reset Zoom</button>
        </div>

        <div id="mynetwork"></div>

        <script type="text/javascript">
            const rawNodes = {nodes_json};
            const rawEdges = {edges_json};

            const container = document.getElementById('mynetwork');
            const data = {{
                nodes: new vis.DataSet(rawNodes),
                edges: new vis.DataSet(rawEdges)
            }};

            const options = {{
                layout: {{
                    hierarchical: {{
                        enabled: true,
                        direction: 'LR',
                        sortMethod: 'directed',
                        nodeSpacing: 220,
                        levelSeparation: 260
                    }}
                }},
                physics: {{
                    enabled: false
                }},
                interaction: {{
                    hover: true,
                    dragNodes: true,
                    zoomView: true,
                    navigationButtons: false
                }}
            }};

            const network = new vis.Network(container, data, options);

            function fitNetwork() {{
                network.fit({{ animation: {{ duration: 500, easingFunction: 'easeInOutQuad' }} }});
            }}

            function resetZoom() {{
                network.moveTo({{ scale: 1.0, animation: true }});
            }}
        </script>
    </body>
    </html>
    """

    components.html(html_code, height=height + 15, scrolling=False)
