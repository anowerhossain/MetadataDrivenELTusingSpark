"""
Visual Pipeline Canvas Builder — dropdown-driven dependency selection.
Dropdowns select upstream parents → canvas auto-updates with connection arrows.
Cards are freely draggable in all directions on the SVG canvas.
"""

import os
import glob
import re
import json
from typing import Dict, Any, List, Optional
import streamlit as st
import streamlit.components.v1 as components
from src.core.config import ConfigParser
from web_ui.components.job_builder import generate_job_toml_string


# ── Tier helper ───────────────────────────────────────────────────────────────

def _get_tier(tid: str) -> dict:
    t = tid.lower()
    if "qlik" in t:
        return {"label": "QLIK ENGINE", "bg": "#1e1b4b", "border": "#8b5cf6", "icon": "🔄"}
    if "gold" in t or "report" in t:
        return {"label": "GOLD TIER",   "bg": "#064e3b", "border": "#059669", "icon": "🥇"}
    if "silver" in t or "clean" in t:
        return {"label": "SILVER TIER", "bg": "#0c4a6e", "border": "#0284c7", "icon": "🥈"}
    return {"label": "BRONZE TIER",     "bg": "#451a03", "border": "#d97706", "icon": "🟤"}


# ── Canvas renderer ───────────────────────────────────────────────────────────

def render_canvas(
    selected_task_ids: List[str],
    edges: List[Dict[str, str]],   # [{"from": "taskA", "to": "taskB"}, ...]
    height: int = 380,
):
    """
    Pure SVG canvas.
    - Cards placed in a staggered grid, freely draggable in all directions.
    - Arrows pre-drawn from the edges list (driven by dropdowns).
    - Arrows animate in smoothly when dependencies are selected.
    """
    CARD_W, CARD_H = 184, 66
    COLS = 3
    GAP_X, GAP_Y = 230, 110
    PAD_X, PAD_Y = 40, 40

    nodes = []
    for i, tid in enumerate(selected_task_ids):
        col = i % COLS
        row = i // COLS
        tier = _get_tier(tid)
        nodes.append({
            "id":     tid,
            "x":      PAD_X + col * GAP_X,
            "y":      PAD_Y + row * GAP_Y,
            "bg":     tier["bg"],
            "border": tier["border"],
            "icon":   tier["icon"],
            "label":  tier["label"],
        })

    svg_h = max(
        PAD_Y * 2 + ((len(selected_task_ids) - 1) // COLS + 1) * GAP_Y + CARD_H,
        height - 10
    )

    nodes_json = json.dumps(nodes)
    edges_json = json.dumps(edges)

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html, body {{
    width: 100%; height: 100%;
    background: #0d1117;
    font-family: 'Segoe UI', sans-serif;
    overflow: hidden;
  }}
  #info-bar {{
    padding: 7px 14px;
    background: #161b22;
    border-bottom: 1px solid #30363d;
    font-size: 12px; color: #58a6ff; font-weight: 600;
  }}
  #canvas-wrap {{
    position: relative;
    width: 100%;
    height: calc(100% - 34px);
    overflow: auto;
    background: radial-gradient(ellipse at center, #1b2430 0%, #0d1117 100%);
  }}
  svg {{ position: absolute; top: 0; left: 0; user-select: none; }}
  .task-group {{ cursor: grab; }}
  .task-group:active {{ cursor: grabbing; }}
  .arrow-path {{
    fill: none;
    stroke: #4895ef;
    stroke-width: 2.5;
    marker-end: url(#arr);
    stroke-dasharray: 300;
    stroke-dashoffset: 300;
    animation: draw-arrow .45s ease forwards;
  }}
  @keyframes draw-arrow {{
    to {{ stroke-dashoffset: 0; }}
  }}
</style>
</head>
<body>

<div id="info-bar">
  🖐 <b>Drag Mode:</b> Drag any task card freely (Left / Right / Up / Down). &nbsp;|&nbsp;
  Arrows update automatically when you change the dropdowns above.
</div>

<div id="canvas-wrap">
<svg id="svg" xmlns="http://www.w3.org/2000/svg" width="1200" height="{svg_h}">
  <defs>
    <marker id="arr" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
      <polygon points="0 0,10 3.5,0 7" fill="#4895ef"/>
    </marker>
  </defs>
  <g id="edges-layer"></g>
  <g id="nodes-layer"></g>
</svg>
</div>

<script>
const CARD_W = {CARD_W}, CARD_H = {CARD_H};
const rawNodes = {nodes_json};
const rawEdges = {edges_json};

// ── State ─────────────────────────────────────────────────────────────────────
const pos = {{}};  // tid → {{x, y}}
let dragging = null;

const svg       = document.getElementById('svg');
const edgesL    = document.getElementById('edges-layer');
const nodesL    = document.getElementById('nodes-layer');
const wrap      = document.getElementById('canvas-wrap');

function svgPt(e) {{
  const r = svg.getBoundingClientRect();
  return {{ x: e.clientX - r.left + wrap.scrollLeft,
            y: e.clientY - r.top  + wrap.scrollTop }};
}}

// ── Arrow path: right-edge → left-edge with cubic bezier ─────────────────────
function makePath(fid, tid) {{
  const a = pos[fid], b = pos[tid];
  if (!a || !b) return '';
  const ax = a.x + CARD_W, ay = a.y + CARD_H / 2;
  const bx = b.x,          by = b.y + CARD_H / 2;
  const mx = (ax + bx) / 2;
  return `M${{ax}},${{ay}} C${{mx}},${{ay}} ${{mx}},${{by}} ${{bx}},${{by}}`;
}}

// ── Draw all arrows from rawEdges ─────────────────────────────────────────────
function drawEdges() {{
  edgesL.innerHTML = '';
  rawEdges.forEach(e => {{
    const d = makePath(e.from, e.to);
    if (!d) return;

    const path = document.createElementNS('http://www.w3.org/2000/svg','path');
    path.setAttribute('class','arrow-path');
    path.setAttribute('d', d);

    // Label on the midpoint of the arrow
    const a = pos[e.from], b = pos[e.to];
    const lx = (a.x + CARD_W + b.x) / 2;
    const ly = (a.y + CARD_H/2 + b.y + CARD_H/2) / 2 - 8;

    const label = document.createElementNS('http://www.w3.org/2000/svg','text');
    label.setAttribute('x', lx); label.setAttribute('y', ly);
    label.setAttribute('font-size','10'); label.setAttribute('fill','#60a5fa');
    label.setAttribute('text-anchor','middle');
    label.textContent = `${{e.from}} → ${{e.to}}`;

    edgesL.appendChild(path);
    edgesL.appendChild(label);
  }});
}}

// ── Build node cards ──────────────────────────────────────────────────────────
rawNodes.forEach(n => {{
  pos[n.id] = {{ x: n.x, y: n.y }};

  const g = document.createElementNS('http://www.w3.org/2000/svg','g');
  g.setAttribute('class','task-group');
  g.setAttribute('data-id', n.id);
  g.setAttribute('transform',`translate(${{n.x}},${{n.y}})`);

  // Card bg
  const rect = document.createElementNS('http://www.w3.org/2000/svg','rect');
  rect.setAttribute('width', CARD_W); rect.setAttribute('height', CARD_H);
  rect.setAttribute('rx',10);
  rect.setAttribute('fill', n.bg); rect.setAttribute('stroke', n.border);
  rect.setAttribute('stroke-width', 2);
  rect.setAttribute('filter','drop-shadow(0 4px 10px rgba(0,0,0,.65))');
  g.appendChild(rect);

  // Icon
  const icon = document.createElementNS('http://www.w3.org/2000/svg','text');
  icon.setAttribute('x', 26); icon.setAttribute('y', CARD_H / 2);
  icon.setAttribute('font-size','18');
  icon.setAttribute('dominant-baseline','middle'); icon.setAttribute('text-anchor','middle');
  icon.textContent = n.icon;
  g.appendChild(icon);

  // Task name
  const nm = document.createElementNS('http://www.w3.org/2000/svg','text');
  nm.setAttribute('x', CARD_W/2 + 10); nm.setAttribute('y', CARD_H/2 - 9);
  nm.setAttribute('font-size','12'); nm.setAttribute('font-weight','700');
  nm.setAttribute('fill','#f1f5f9');
  nm.setAttribute('text-anchor','middle'); nm.setAttribute('dominant-baseline','middle');
  nm.textContent = n.id.length > 18 ? n.id.slice(0,17)+'…' : n.id;
  g.appendChild(nm);

  // Tier label
  const tl = document.createElementNS('http://www.w3.org/2000/svg','text');
  tl.setAttribute('x', CARD_W/2 + 10); tl.setAttribute('y', CARD_H/2 + 10);
  tl.setAttribute('font-size','10'); tl.setAttribute('fill','#94a3b8');
  tl.setAttribute('text-anchor','middle'); tl.setAttribute('dominant-baseline','middle');
  tl.textContent = n.label;
  g.appendChild(tl);

  // Bottom colour strip
  const strip = document.createElementNS('http://www.w3.org/2000/svg','rect');
  strip.setAttribute('x',0); strip.setAttribute('y', CARD_H - 4);
  strip.setAttribute('width', CARD_W); strip.setAttribute('height', 4);
  strip.setAttribute('rx', 3); strip.setAttribute('fill', n.border);
  g.appendChild(strip);

  nodesL.appendChild(g);

  // Drag: mousedown only
  g.addEventListener('mousedown', e => {{
    const pt = svgPt(e);
    dragging = {{ tid: n.id, ox: pt.x - pos[n.id].x, oy: pt.y - pos[n.id].y, el: g }};
    e.preventDefault(); e.stopPropagation();
  }});
}});

// ── Drag handlers ─────────────────────────────────────────────────────────────
wrap.addEventListener('mousemove', e => {{
  if (!dragging) return;
  const pt = svgPt(e);
  const nx = pt.x - dragging.ox, ny = pt.y - dragging.oy;
  pos[dragging.tid].x = nx; pos[dragging.tid].y = ny;
  dragging.el.setAttribute('transform',`translate(${{nx}},${{ny}})`);
  drawEdges();
  e.preventDefault();
}});
wrap.addEventListener('mouseup',    () => {{ dragging = null; }});
wrap.addEventListener('mouseleave', () => {{ dragging = null; }});

// Initial draw
drawEdges();
</script>
</body>
</html>"""

    components.html(html, height=height, scrolling=False)


# ── Streamlit page ────────────────────────────────────────────────────────────

def render_drag_job_builder():
    st.subheader("🎨 Visual Pipeline Flow Builder")
    st.markdown(
        "Select tasks and set their upstream dependencies using the dropdowns below. "
        "The **visual flow canvas updates automatically** with connection arrows."
    )

    tasks_dir = os.path.join("config", "tasks")
    jobs_dir  = os.path.join("config", "jobs")
    os.makedirs(jobs_dir, exist_ok=True)

    # ── Discover tasks ─────────────────────────────────────────────────────────
    available_tasks: Dict[str, str] = {}
    if os.path.exists(tasks_dir):
        for tf in sorted(glob.glob(os.path.join(tasks_dir, "*.toml"))):
            try:
                cfg = ConfigParser.load_toml(tf)
                available_tasks[cfg.job.task_id] = os.path.relpath(tf, ".").replace("\\", "/")
            except Exception:
                pass

    if not available_tasks:
        st.warning("⚠️ No tasks found in `config/tasks/`. Create tasks first.")
        return

    # ── Job metadata ───────────────────────────────────────────────────────────
    c1, c2 = st.columns(2)
    with c1:
        job_id   = st.text_input("Job ID:",   value="custom_sales_pipeline", key="drag_job_id_input")
        job_name = st.text_input("Job Name:", value="Custom Enterprise Sales Pipeline", key="drag_job_name_input")
    with c2:
        enabled     = st.checkbox("Job Enabled", value=True, key="drag_enabled_chk")
        description = st.text_area("Job Description:", value="Pipeline built via Visual Flow Builder.", key="drag_desc_input", height=85)

    st.divider()

    # ── Task selection ─────────────────────────────────────────────────────────
    st.subheader("🧩 Step 1 — Select Tasks")
    selected_task_ids = st.multiselect(
        "Choose tasks to include in this pipeline:",
        options=list(available_tasks.keys()),
        default=list(available_tasks.keys())[:3] if len(available_tasks) >= 3 else list(available_tasks.keys()),
        key="drag_tasks_multiselect"
    )

    if not selected_task_ids:
        st.info("Select at least one task to continue.")
        return

    st.divider()

    # ── Dependency dropdowns ───────────────────────────────────────────────────
    st.subheader("🔗 Step 2 — Set Dependencies")
    st.caption("Select upstream parent tasks for each task. The canvas below updates automatically.")

    task_mappings: List[Dict[str, Any]] = []
    edges: List[Dict[str, str]] = []

    cols_per_row = 2
    task_chunks = [selected_task_ids[i:i+cols_per_row] for i in range(0, len(selected_task_ids), cols_per_row)]

    for chunk in task_chunks:
        cols = st.columns(cols_per_row)
        for col_idx, tid in enumerate(chunk):
            possible_parents = [t for t in selected_task_ids if t != tid]

            # Smart defaults based on medallion tier naming
            defaults: List[str] = []
            if "silver" in tid.lower():
                defaults = [p for p in possible_parents if "bronze" in p.lower()]
            elif "gold" in tid.lower():
                defaults = [p for p in possible_parents if "silver" in p.lower()]
            elif "qlik" in tid.lower():
                defaults = [p for p in possible_parents if "gold" in p.lower()]

            with cols[col_idx]:
                tier = _get_tier(tid)
                st.markdown(f"**{tier['icon']} `{tid}`** &nbsp; <span style='color:#64748b;font-size:11px'>[{tier['label']}]</span>", unsafe_allow_html=True)
                selected_parents = st.multiselect(
                    "Upstream depends on:",
                    options=possible_parents,
                    default=defaults,
                    key=f"dep_{tid}",
                    label_visibility="collapsed"
                )

            task_mappings.append({
                "task_id":   tid,
                "task_file": available_tasks[tid],
                "depends_on": selected_parents
            })

            # Build edge list for canvas
            for parent in selected_parents:
                edges.append({"from": parent, "to": tid})

    # ── Visual canvas — auto-updated from edges above ─────────────────────────
    st.divider()
    st.subheader("📊 Step 3 — Visual Flow Canvas (Auto-Updated)")
    st.caption("Drag cards freely in any direction. Arrows reflect your dependency selections above.")

    render_canvas(selected_task_ids, edges, height=400)

    # ── Save ───────────────────────────────────────────────────────────────────
    st.divider()
    if st.button("Save Job Pipeline", type="primary", use_container_width=True, key="btn_save_drag_job"):
        clean_job_id = re.sub(r"[^a-zA-Z0-9_]", "_", job_id.strip().lower())
        if not clean_job_id:
            st.error("❌ Job ID is required."); return
        if not job_name.strip():
            st.error("❌ Job Name is required."); return

        target_file  = os.path.join(jobs_dir, f"{clean_job_id}.toml")
        toml_content = generate_job_toml_string(
            job_id=clean_job_id, job_name=job_name.strip(),
            description=description.strip(), enabled=enabled,
            task_mappings=task_mappings
        )
        try:
            with open(target_file, "w", encoding="utf-8") as f:
                f.write(toml_content)
            st.success(f"🎉 Pipeline saved to `{target_file}`!")
            st.info("👉 Go to **'Luigi Workflow Orchestrator (DAG)'** in the sidebar to run this pipeline.")
            with st.expander("📄 View Generated TOML"):
                st.code(toml_content, language="toml")
        except Exception as err:
            st.error(f"❌ Save failed: {err}")
