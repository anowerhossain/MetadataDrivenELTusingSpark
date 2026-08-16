"""
Interactive Vis.js HTML5 DAG Canvas Component for Streamlit Web UI.
Renders color-coded Medallion Architecture cards (Bronze/Silver/Gold/Qlik), smooth curved Bezier arrows (#4895EF),
live execution status badges, zoom/pan controls, and interactive node inspectors.
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

    # Determine Tier / Module
    if "qlik" in task_type or "qlik" in task_id_lower:
        tier_label = "QLIK REFRESH"
        border_color = "#8b5cf6"
        bg_color = "#1e1b4b"
        text_color = "#c4b5fd"
        icon = "🔄"
    elif "gold" in task_id_lower or "gold" in task_name_lower or "report" in task_name_lower:
        tier_label = "GOLD TIER"
        border_color = "#059669"
        bg_color = "#064e3b"
        text_color = "#a7f3d0"
        icon = "🥇"
    elif "silver" in task_id_lower or "silver" in task_name_lower or "clean" in task_name_lower:
        tier_label = "SILVER TIER"
        border_color = "#0284c7"
        bg_color = "#0c4a6e"
        text_color = "#bae6fd"
        icon = "🥈"
    else:
        tier_label = "BRONZE TIER"
        border_color = "#d97706"
        bg_color = "#451a03"
        text_color = "#fde68a"
        icon = "🟤"

    status = get_task_execution_status(task_id)
    if status == "SUCCESS":
        status_badge = "✅ SUCCESS"
        status_color = "#22c55e"
    elif status == "FAILED":
        status_badge = "🚨 FAILED"
        status_color = "#ef4444"
    else:
        status_badge = "⚪ READY"
        status_color = "#9ca3af"

    return {
        "tier_label": tier_label,
        "border_color": border_color,
        "bg_color": bg_color,
        "text_color": text_color,
        "icon": icon,
        "status_badge": status_badge,
        "status_color": status_color
    }


def render_interactive_vis_dag(config_map: Dict[str, JobConfig], height: int = 500):
    """
    Renders high-end Vis.js HTML5 Interactive DAG Network Component in Streamlit.
    """
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []

    for task_id, config in config_map.items():
        style = get_node_style_and_tier(task_id, config)
        task_name = config.job.task_name
        task_type = str(config.raw_config.get("task", {}).get("type", "table_load")).upper()

        label_html = f"{style['icon']} <b>{task_id}</b>\n{task_name}\n[{style['tier_label']}]"

        nodes.append({
            "id": task_id,
            "label": label_html,
            "title": f"Task ID: {task_id}<br>Name: {task_name}<br>Type: {task_type}<br>Status: {style['status_badge']}",
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
                "face": "Segoe UI, Arial",
                "size": 13,
                "multi": "html"
            },
            "borderWidth": 2,
            "borderWidthSelected": 4,
            "shadow": {
                "enabled": True,
                "color": "rgba(0,0,0,0.5)",
                "size": 10
            }
        })

        for dep_id in config.job.depends_on:
            edges.append({
                "from": dep_id,
                "to": task_id,
                "arrows": "to",
                "color": {
                    "color": "#4895EF",
                    "highlight": "#60a5fa"
                },
                "width": 2.5,
                "smooth": {
                    "type": "cubicBezier",
                    "forceDirection": "horizontal",
                    "roundness": 0.5
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
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                overflow: hidden;
            }}
            #mynetwork {{
                width: 100%;
                height: {height}px;
                background: radial-gradient(circle, #1a202c 0%, #0e1117 100%);
                border: 1px solid #30363d;
                border-radius: 12px;
            }}
            .dag-legend {{
                position: absolute;
                top: 12px;
                left: 12px;
                background: rgba(15, 23, 42, 0.85);
                backdrop-filter: blur(8px);
                border: 1px solid #334155;
                padding: 10px 14px;
                border-radius: 8px;
                color: #e2e8f0;
                font-size: 12px;
                z-index: 10;
                display: flex;
                gap: 12px;
                box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            }}
            .legend-item {{
                display: flex;
                align-items: center;
                gap: 6px;
            }}
            .legend-dot {{
                width: 10px;
                height: 10px;
                border-radius: 50%;
            }}
        </style>
    </head>
    <body>
        <div class="dag-legend">
            <div class="legend-item"><span class="legend-dot" style="background:#d97706;"></span>🟤 Bronze Ingestion</div>
            <div class="legend-item"><span class="legend-dot" style="background:#0284c7;"></span>🥈 Silver Transform</div>
            <div class="legend-item"><span class="legend-dot" style="background:#059669;"></span>🥇 Gold Reporting</div>
            <div class="legend-item"><span class="legend-dot" style="background:#8b5cf6;"></span>🔄 Qlik Refresh</div>
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
                        nodeSpacing: 180,
                        levelSeparation: 240
                    }}
                }},
                physics: {{
                    enabled: false
                }},
                interaction: {{
                    hover: true,
                    dragNodes: true,
                    zoomView: true,
                    navigationButtons: true
                }}
            }};

            const network = new vis.Network(container, data, options);
        </script>
    </body>
    </html>
    """

    components.html(html_code, height=height + 10, scrolling=False)
