"""
Visual Pipeline Canvas Builder — Pure HTML5 SVG drag-and-drop.
No external libraries (no Vis.js). Uses native mousedown/mousemove/mouseup
events on SVG elements so dragging works reliably inside Streamlit iframes.
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


# ── Tier helpers ──────────────────────────────────────────────────────────────

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

def render_drag_and_drop_canvas(
    selected_task_ids: List[str],
    available_tasks: Dict[str, str],
    height: int = 460,
):
    """
    Pure SVG + HTML5 canvas.
    • Mouse drag works in all 4 directions inside Streamlit iframes.
    • Click [🔗 Connect] button → click two cards to draw an arrow.
    • Click [🗑 Clear] to remove all arrows.
    """
    CARD_W, CARD_H = 180, 68
    COLS = 3
    GAP_X, GAP_Y = 220, 110
    PAD_X, PAD_Y = 40, 40

    # Initial positions
    positions = {}
    for i, tid in enumerate(selected_task_ids):
        col = i % COLS
        row = i // COLS
        positions[tid] = {"x": PAD_X + col * GAP_X, "y": PAD_Y + row * GAP_Y}

    nodes_json = json.dumps(
        [{"id": tid, "x": positions[tid]["x"], "y": positions[tid]["y"],
          **_get_tier(tid)} for tid in selected_task_ids]
    )

    canvas_height = PAD_Y * 2 + ((len(selected_task_ids) - 1) // COLS + 1) * GAP_Y + CARD_H
    svg_height = max(canvas_height, height - 60)

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html, body {{ width: 100%; height: 100%; background: #0d1117; font-family: 'Segoe UI', sans-serif; overflow: hidden; }}

  #toolbar {{
    display: flex; align-items: center; gap: 8px;
    padding: 8px 12px;
    background: #161b22;
    border-bottom: 1px solid #30363d;
  }}
  #status {{
    flex: 1; font-size: 12px; color: #58a6ff; font-weight: 600;
    padding: 4px 10px;
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 6px;
    min-height: 28px; line-height: 20px;
  }}
  .tb-btn {{
    padding: 5px 14px; border-radius: 6px; font-size: 12px; font-weight: 700;
    cursor: pointer; border: none; transition: opacity .15s;
  }}
  .tb-btn:hover {{ opacity: .85; }}
  #btn-connect  {{ background: #0284c7; color: #fff; }}
  #btn-cancel   {{ background: #7c3aed; color: #fff; display: none; }}
  #btn-clear    {{ background: #1e293b; color: #e2e8f0; border: 1px solid #475569; }}

  #canvas-wrap {{
    position: relative;
    width: 100%;
    height: calc(100vh - 46px);
    overflow: auto;
    background: radial-gradient(ellipse at center, #1b2430 0%, #0d1117 100%);
  }}

  svg {{
    position: absolute;
    top: 0; left: 0;
    cursor: default;
    user-select: none;
  }}

  .task-group {{ cursor: grab; }}
  .task-group:active {{ cursor: grabbing; }}
  .task-rect {{
    rx: 10; ry: 10;
    filter: drop-shadow(0 4px 12px rgba(0,0,0,.6));
    transition: filter .15s;
  }}
  .task-group:hover .task-rect {{
    filter: drop-shadow(0 0 14px rgba(72,149,239,.55));
  }}
  .task-icon  {{ font-size: 18px; dominant-baseline: middle; text-anchor: middle; }}
  .task-name  {{ font-size: 12px; font-weight: 700; fill: #f1f5f9; text-anchor: middle; dominant-baseline: middle; }}
  .task-tier  {{ font-size: 10px; fill: #94a3b8; text-anchor: middle; dominant-baseline: middle; }}

  .arrow-line {{
    fill: none; stroke: #4895ef; stroke-width: 2.5;
    marker-end: url(#arrow-head);
  }}
  .arrow-line.preview {{ stroke-dasharray: 6 4; stroke: #60a5fa; }}
  .arrow-hit {{
    fill: none; stroke: transparent; stroke-width: 12; cursor: pointer;
  }}
</style>
</head>
<body>

<div id="toolbar">
  <button class="tb-btn" id="btn-connect" onclick="startConnect()">🔗 Connect Tasks</button>
  <button class="tb-btn" id="btn-cancel"  onclick="cancelConnect()">✖ Cancel</button>
  <button class="tb-btn" id="btn-clear"   onclick="clearArrows()">🗑 Clear Arrows</button>
  <div id="status">🖐 Drag Mode — grab any card and move it freely in all directions.</div>
</div>

<div id="canvas-wrap">
<svg id="svg" xmlns="http://www.w3.org/2000/svg"
     width="1200" height="{svg_height}">

  <defs>
    <marker id="arrow-head" markerWidth="10" markerHeight="7"
            refX="9" refY="3.5" orient="auto">
      <polygon points="0 0, 10 3.5, 0 7" fill="#4895ef"/>
    </marker>
  </defs>

  <g id="edges-layer"></g>
  <g id="preview-layer"></g>
  <g id="nodes-layer"></g>
</svg>
</div>

<script>
const CARD_W = {CARD_W}, CARD_H = {CARD_H};
const rawNodes = {nodes_json};

// ── State ────────────────────────────────────────────────────────────────────
const positions = {{}};
const edges     = [];
let connectMode   = false;
let connectSource = null;
let dragging      = null;   // {{tid, ox, oy, el}}
let didDrag       = false;  // distinguish click vs drag

// ── SVG refs ─────────────────────────────────────────────────────────────────
const svg         = document.getElementById('svg');
const edgesLayer  = document.getElementById('edges-layer');
const nodesLayer  = document.getElementById('nodes-layer');
const statusEl    = document.getElementById('status');
const wrap        = document.getElementById('canvas-wrap');

function svgPt(e) {{
  const r = svg.getBoundingClientRect();
  return {{ x: e.clientX - r.left + wrap.scrollLeft,
            y: e.clientY - r.top  + wrap.scrollTop }};
}}

// Arrow endpoints: right-edge of source → left-edge of target
function edgePts(fromId, toId) {{
  const a = positions[fromId], b = positions[toId];
  // midpoint x for bezier control
  const ax = a.x + CARD_W, ay = a.y + CARD_H / 2;
  const bx = b.x,          by = b.y + CARD_H / 2;
  const mx = (ax + bx) / 2;
  return {{ ax, ay, bx, by, mx }};
}}

function makePath(fromId, toId) {{
  const {{ ax, ay, bx, by, mx }} = edgePts(fromId, toId);
  return `M${{ax}},${{ay}} C${{mx}},${{ay}} ${{mx}},${{by}} ${{bx}},${{by}}`;
}}

// ── Draw arrows ───────────────────────────────────────────────────────────────
function redrawEdges() {{
  edgesLayer.innerHTML = '';
  edges.forEach((edge, i) => {{
    // visible line
    const path = document.createElementNS('http://www.w3.org/2000/svg','path');
    path.setAttribute('fill','none');
    path.setAttribute('stroke','#4895ef');
    path.setAttribute('stroke-width','2.5');
    path.setAttribute('marker-end','url(#arrow-head)');
    path.setAttribute('d', makePath(edge.from, edge.to));
    edgesLayer.appendChild(path);

    // fat invisible hit-area to click-delete the arrow
    const hit = document.createElementNS('http://www.w3.org/2000/svg','path');
    hit.setAttribute('fill','none');
    hit.setAttribute('stroke','transparent');
    hit.setAttribute('stroke-width','14');
    hit.setAttribute('style','cursor:pointer');
    hit.setAttribute('d', makePath(edge.from, edge.to));
    hit.title = `${{edge.from}} → ${{edge.to}} (click to delete)`;
    hit.addEventListener('click', () => {{ edges.splice(i,1); redrawEdges(); }});
    edgesLayer.appendChild(hit);
  }});
}}

// ── Build node cards ──────────────────────────────────────────────────────────
rawNodes.forEach(n => {{
  positions[n.id] = {{ x: n.x, y: n.y }};

  const g = document.createElementNS('http://www.w3.org/2000/svg','g');
  g.setAttribute('class','task-group');
  g.setAttribute('data-id', n.id);
  g.setAttribute('transform',`translate(${{n.x}},${{n.y}})`);

  const rect = document.createElementNS('http://www.w3.org/2000/svg','rect');
  rect.setAttribute('width', CARD_W); rect.setAttribute('height', CARD_H);
  rect.setAttribute('rx',10);
  rect.setAttribute('fill', n.bg); rect.setAttribute('stroke', n.border);
  rect.setAttribute('stroke-width', 2);
  rect.setAttribute('filter','drop-shadow(0 4px 10px rgba(0,0,0,.6))');
  g.appendChild(rect);

  const icon = document.createElementNS('http://www.w3.org/2000/svg','text');
  icon.setAttribute('x',24); icon.setAttribute('y', CARD_H/2 - 4);
  icon.setAttribute('font-size','18'); icon.setAttribute('dominant-baseline','middle');
  icon.setAttribute('text-anchor','middle');
  icon.textContent = n.icon;
  g.appendChild(icon);

  const nameEl = document.createElementNS('http://www.w3.org/2000/svg','text');
  nameEl.setAttribute('x', CARD_W/2+8); nameEl.setAttribute('y', CARD_H/2 - 8);
  nameEl.setAttribute('font-size','12'); nameEl.setAttribute('font-weight','700');
  nameEl.setAttribute('fill','#f1f5f9');
  nameEl.setAttribute('text-anchor','middle'); nameEl.setAttribute('dominant-baseline','middle');
  nameEl.textContent = n.id.length > 18 ? n.id.slice(0,17)+'…' : n.id;
  g.appendChild(nameEl);

  const tierEl = document.createElementNS('http://www.w3.org/2000/svg','text');
  tierEl.setAttribute('x', CARD_W/2+8); tierEl.setAttribute('y', CARD_H/2+10);
  tierEl.setAttribute('font-size','10'); tierEl.setAttribute('fill','#94a3b8');
  tierEl.setAttribute('text-anchor','middle'); tierEl.setAttribute('dominant-baseline','middle');
  tierEl.textContent = n.label;
  g.appendChild(tierEl);

  const strip = document.createElementNS('http://www.w3.org/2000/svg','rect');
  strip.setAttribute('x',0); strip.setAttribute('y', CARD_H-4);
  strip.setAttribute('width', CARD_W); strip.setAttribute('height',4);
  strip.setAttribute('rx',4); strip.setAttribute('fill', n.border);
  strip.setAttribute('opacity',0.7);
  g.appendChild(strip);

  nodesLayer.appendChild(g);

  // ── mousedown = start drag (only when NOT in connect mode) ──
  g.addEventListener('mousedown', e => {{
    if (connectMode) return;  // click handler handles connect mode
    const pt = svgPt(e);
    didDrag = false;
    dragging = {{ tid: n.id, ox: pt.x - n.x, oy: pt.y - n.y, el: g }};
    e.preventDefault();
    e.stopPropagation();
  }});

  // ── click = connect cards (only when in connect mode) ──
  g.addEventListener('click', e => {{
    if (!connectMode) return;
    e.stopPropagation();
    if (!connectSource) {{
      connectSource = n.id;
      // highlight selected card
      rect.setAttribute('stroke','#60a5fa');
      rect.setAttribute('stroke-width','3');
      statusEl.innerHTML = `🎯 <b>Parent:</b> <code>${{n.id}}</code> → now <b>click the child task</b> card to draw the arrow.`;
    }} else if (connectSource !== n.id) {{
      edges.push({{ from: connectSource, to: n.id }});
      redrawEdges();
      // reset source card stroke
      document.querySelectorAll('.task-group').forEach(grp => {{
        const r2 = grp.querySelector('rect');
        const rawN = rawNodes.find(x => x.id === grp.getAttribute('data-id'));
        if (rawN) {{ r2.setAttribute('stroke', rawN.border); r2.setAttribute('stroke-width','2'); }}
      }});
      statusEl.innerHTML = `✅ Arrow: <b>${{connectSource}}</b> → <b>${{n.id}}</b>. Pick next parent or click <b>✖ Cancel</b>.`;
      connectSource = null;
    }}
  }});
}});

// ── Drag move / up ────────────────────────────────────────────────────────────
wrap.addEventListener('mousemove', e => {{
  if (!dragging) return;
  const pt = svgPt(e);
  const nx = pt.x - dragging.ox;
  const ny = pt.y - dragging.oy;
  positions[dragging.tid].x = nx;
  positions[dragging.tid].y = ny;
  dragging.el.setAttribute('transform',`translate(${{nx}},${{ny}})`);
  didDrag = true;
  redrawEdges();
  e.preventDefault();
}});

wrap.addEventListener('mouseup', () => {{ dragging = null; }});
wrap.addEventListener('mouseleave', () => {{ dragging = null;
}});

function startConnect() {{
  connectMode = true; connectSource = null;
  document.getElementById('btn-connect').style.display = 'none';
  document.getElementById('btn-cancel').style.display  = 'inline-block';
  statusEl.innerHTML = '🔗 <b>Connect Mode:</b> Click a <b>Parent Task</b> card, then a <b>Child Task</b> card to draw a blue arrow.';
}}

function cancelConnect() {{
  connectMode = false; connectSource = null;
  // reset all card strokes
  document.querySelectorAll('.task-group').forEach(grp => {{
    const r2 = grp.querySelector('rect');
    const rawN = rawNodes.find(x => x.id === grp.getAttribute('data-id'));
    if (rawN) {{ r2.setAttribute('stroke', rawN.border); r2.setAttribute('stroke-width','2'); }}
  }});
  document.getElementById('btn-connect').style.display = 'inline-block';
  document.getElementById('btn-cancel').style.display  = 'none';
  statusEl.innerHTML = '🖐 <b>Drag Mode</b> — grab any card and move it freely in all directions.';
}}

function clearArrows() {{
  edges.length = 0;
  redrawEdges();
  statusEl.innerHTML = '🗑 All arrows cleared. Drag cards or click 🔗 Connect Tasks.';
}}

// fix typo-safe alias
function redrawEdgesAndClear() {{ edges.length = 0; redrawEdges(); status.innerHTML = '🗑 All arrows cleared.'; }}
document.getElementById('btn-clear').onclick = redrawEdgesAndClear;

</script>
</body>
</html>"""

    components.html(html, height=height, scrolling=False)


# ── Streamlit page ────────────────────────────────────────────────────────────

def render_drag_job_builder():
    st.subheader("🎨 Visual Pipeline Canvas Builder")
    st.markdown(
        "Select tasks → drag cards **freely** in any direction → click **🔗 Connect Tasks** "
        "to draw dependency arrows → **💾 Save** to generate the job TOML."
    )

    tasks_dir = os.path.join("config", "tasks")
    jobs_dir  = os.path.join("config", "jobs")
    os.makedirs(jobs_dir, exist_ok=True)

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

    c1, c2 = st.columns(2)
    with c1:
        job_id = st.text_input("Job ID:", value="custom_sales_pipeline", key="drag_job_id_input")
        job_name = st.text_input("Job Name:", value="Custom Enterprise Sales Pipeline", key="drag_job_name_input")
    with c2:
        enabled = st.checkbox("Job Enabled", value=True, key="drag_enabled_chk")
        description = st.text_area("Job Description:", value="Pipeline built via Visual Canvas.", key="drag_desc_input", height=85)

    st.divider()
    st.subheader("🧩 Step 1 — Select Tasks")
    selected_task_ids = st.multiselect(
        "Choose tasks to place on the canvas:",
        options=list(available_tasks.keys()),
        default=list(available_tasks.keys())[:3] if len(available_tasks) >= 3 else list(available_tasks.keys()),
        key="drag_tasks_multiselect"
    )
    if not selected_task_ids:
        st.info("Select at least one task to display on the canvas.")
        return

    st.divider()
    st.subheader("🎨 Step 2 — Visual Canvas")
    st.caption("Drag cards freely (all directions) · Click **🔗 Connect Tasks** to draw arrows · Click an arrow to delete it")

    render_drag_and_drop_canvas(selected_task_ids, available_tasks, height=480)

    st.divider()
    if st.button("💾 Save Enterprise Job Pipeline", type="primary", use_container_width=True, key="btn_save_drag_job"):
        clean_job_id = re.sub(r"[^a-zA-Z0-9_]", "_", job_id.strip().lower())
        if not clean_job_id:
            st.error("❌ Job ID is required."); return
        if not job_name.strip():
            st.error("❌ Job Name is required."); return

        task_mappings: List[Dict[str, Any]] = []
        for tid in selected_task_ids:
            possible = [o for o in selected_task_ids if o != tid]
            defaults: List[str] = []
            if "silver" in tid.lower():
                defaults = [p for p in possible if "bronze" in p.lower()]
            elif "gold" in tid.lower():
                defaults = [p for p in possible if "silver" in p.lower()]
            task_mappings.append({"task_id": tid, "task_file": available_tasks[tid], "depends_on": defaults})

        target_file = os.path.join(jobs_dir, f"{clean_job_id}.toml")
        toml_content = generate_job_toml_string(
            job_id=clean_job_id, job_name=job_name.strip(),
            description=description.strip(), enabled=enabled, task_mappings=task_mappings
        )
        try:
            with open(target_file, "w", encoding="utf-8") as f:
                f.write(toml_content)
            st.success(f"🎉 Saved to `{target_file}`!")
            st.info("👉 Go to **'Luigi Workflow Orchestrator (DAG)'** in the sidebar to run this pipeline.")
            with st.expander("📄 View Generated TOML"):
                st.code(toml_content, language="toml")
        except Exception as err:
            st.error(f"❌ Save failed: {err}")
