"""
visualize.py  ·  dbt Neural Lineage Visualizer
================================================
Reads the SQLite lineage cache (.lineage_*.db) and opens an interactive,
neural-network-style visualization in your default browser.

Rendering is done entirely on an HTML5 Canvas (not SVG), which lets the
browser draw all nodes and edges in a single GPU-accelerated pass, giving
smooth 60 fps even on large graphs.

Usage (from repo root):
    python demo/visualize.py
    python demo/visualize.py demo/.lineage_a6de393b768e6de4.db
"""
from __future__ import annotations

import json
import os
import sys
import sqlite3
import webbrowser
import tempfile
from pathlib import Path
from collections import defaultdict


# ── Colour palette ─────────────────────────────────────────────────────────────
NODE_COLORS = {
    "base":   {"fill": "#3b0764", "glow": "#a855f7"},
    "stg":    {"fill": "#1e3a8a", "glow": "#60a5fa"},
    "int":    {"fill": "#134e4a", "glow": "#2dd4bf"},
    "fct":    {"fill": "#78350f", "glow": "#fbbf24"},
    "dim":    {"fill": "#14532d", "glow": "#4ade80"},
    "rep":    {"fill": "#881337", "glow": "#fb7185"},
    "mart":   {"fill": "#7c2d12", "glow": "#fb923c"},
    "source": {"fill": "#0f172a", "glow": "#94a3b8"},
    "other":  {"fill": "#1e1b4b", "glow": "#818cf8"},
}

TRANSFORM_COLORS = {
    "passthrough": "#475569",
    "rename":      "#6366f1",
    "cast":        "#0ea5e9",
    "arithmetic":  "#f59e0b",
    "aggregation": "#ef4444",
    "conditional": "#a855f7",
    "function":    "#22c55e",
    "window":      "#f97316",
    "complex":     "#ec4899",
    "unknown":     "#334155",
}

CATEGORY_LABELS = {
    "base":   "Base",
    "stg":    "Staging",
    "int":    "Intermediate",
    "fct":    "Fact",
    "dim":    "Dimension",
    "rep":    "Report",
    "mart":   "Mart",
    "source": "Source",
    "other":  "Other",
}


def categorize(name: str, resource_type: str) -> str:
    if resource_type == "source":
        return "source"
    n = name.lower()
    for p in ("base_", "stg_", "int_", "fct_", "dim_", "rep_", "mart_"):
        if n.startswith(p):
            return p.rstrip("_")
    return "other"


def load_data(db_path: Path):
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    rtype: dict[str, str] = {}
    for r in con.execute("SELECT name, resource_type FROM models"):
        rtype[r["name"]] = r["resource_type"] or "model"

    pairs: dict[tuple, dict] = {}
    for r in con.execute("SELECT source_model, target_model, transform_type FROM edges"):
        s, t, tt = r["source_model"], r["target_model"], r["transform_type"] or "unknown"
        if s == t:
            continue
        key = (s, t)
        if key not in pairs:
            pairs[key] = {"count": 0, "transforms": defaultdict(int)}
        pairs[key]["count"] += 1
        pairs[key]["transforms"][tt] += 1

    con.close()

    in_deg: dict[str, int] = defaultdict(int)
    out_deg: dict[str, int] = defaultdict(int)
    all_names: set[str] = set()
    for (s, t) in pairs:
        all_names.add(s)
        all_names.add(t)
        out_deg[s] += 1
        in_deg[t] += 1

    nodes = []
    for name in sorted(all_names):
        cat = categorize(name, rtype.get(name, "model"))
        col = NODE_COLORS.get(cat, NODE_COLORS["other"])
        nodes.append({
            "id":       name,
            "category": cat,
            "fill":     col["fill"],
            "glow":     col["glow"],
            "in":       in_deg[name],
            "out":      out_deg[name],
            "degree":   in_deg[name] + out_deg[name],
        })

    links = []
    for (s, t), m in pairs.items():
        dom = max(m["transforms"], key=m["transforms"].get)
        links.append({
            "source":    s,
            "target":    t,
            "count":     m["count"],
            "transform": dom,
            "color":     TRANSFORM_COLORS.get(dom, "#334155"),
        })

    stats = {
        "models":       len(nodes),
        "connections":  len(links),
        "column_edges": sum(m["count"] for m in pairs.values()),
    }
    return nodes, links, stats


# ── HTML / Canvas template ─────────────────────────────────────────────────────
_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>dbt Lineage · Neural View</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: #030310;
    overflow: hidden;
    height: 100vh; width: 100vw;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Inter', sans-serif;
    color: #e2e8f0;
  }

  #topbar {
    position: fixed; top: 0; left: 0; right: 0; height: 52px;
    background: rgba(3, 3, 18, 0.9);
    backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px);
    border-bottom: 1px solid rgba(255,255,255,0.05);
    display: flex; align-items: center; padding: 0 20px; gap: 18px;
    z-index: 100;
  }
  #logo {
    display: flex; align-items: center; gap: 9px;
    font-size: 13px; font-weight: 600; letter-spacing: 0.01em;
    white-space: nowrap; color: #e2e8f0;
  }
  .logo-orb {
    width: 9px; height: 9px; border-radius: 50%;
    background: #a855f7;
    box-shadow: 0 0 10px #a855f7, 0 0 22px #a855f750;
    animation: orb-pulse 2.4s ease-in-out infinite;
  }
  @keyframes orb-pulse {
    0%,100% { transform: scale(1);   box-shadow: 0 0 8px  #a855f7, 0 0 16px #a855f740; }
    50%      { transform: scale(1.3); box-shadow: 0 0 14px #a855f7, 0 0 30px #a855f770; }
  }
  #stats-row { display: flex; gap: 10px; flex: 1; }
  .stat-pill {
    font-size: 11px; color: #64748b;
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 6px; padding: 3px 10px; white-space: nowrap;
  }
  .stat-pill b { color: #cbd5e1; }

  #search-wrap { position: relative; }
  #search {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.09);
    border-radius: 8px; color: #e2e8f0;
    font-size: 12px; padding: 6px 12px 6px 30px;
    width: 210px; outline: none;
    transition: border-color 0.2s, box-shadow 0.2s;
  }
  #search::placeholder { color: #334155; }
  #search:focus {
    border-color: rgba(168,85,247,0.45);
    box-shadow: 0 0 0 3px rgba(168,85,247,0.08);
  }
  .search-icon {
    position: absolute; left: 9px; top: 50%;
    transform: translateY(-50%);
    color: #475569; font-size: 13px; pointer-events: none;
  }

  #filters {
    position: fixed; top: 60px; left: 18px;
    display: flex; gap: 5px; flex-wrap: wrap;
    max-width: 680px; z-index: 90;
  }
  .fpill {
    font-size: 10.5px; padding: 4px 11px;
    border-radius: 20px;
    border: 1px solid rgba(255,255,255,0.08);
    background: rgba(255,255,255,0.03);
    color: #64748b; cursor: pointer;
    transition: all 0.18s; user-select: none;
  }
  .fpill:hover { background: rgba(255,255,255,0.07); color: #cbd5e1; }
  .fpill.on { background: rgba(255,255,255,0.08); border-color: currentColor; }

  #graph {
    position: fixed; top: 0; left: 0;
    width: 100%; height: 100%;
    display: block;
  }

  #tip {
    position: fixed;
    background: rgba(6, 6, 22, 0.94);
    backdrop-filter: blur(14px); -webkit-backdrop-filter: blur(14px);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 10px; padding: 11px 15px;
    font-size: 12px; color: #e2e8f0;
    pointer-events: none; opacity: 0;
    transition: opacity 0.12s; z-index: 300;
    max-width: 270px; min-width: 180px;
  }
  #tip.show { opacity: 1; }
  #tip-name  { font-size: 13px; font-weight: 700; margin-bottom: 5px; word-break: break-all; }
  #tip-badge {
    display: inline-block; font-size: 10px; padding: 2px 8px;
    border-radius: 4px; margin-bottom: 9px;
    text-transform: uppercase; letter-spacing: 0.06em;
    background: rgba(255,255,255,0.06);
  }
  .tip-row {
    display: flex; justify-content: space-between;
    gap: 16px; font-size: 11px; color: #64748b; margin-top: 3px;
  }
  .tip-row b { color: #cbd5e1; }

  #panel {
    position: fixed; right: -350px; top: 52px; bottom: 0;
    width: 330px; overflow-y: auto;
    background: rgba(5, 5, 20, 0.97);
    backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px);
    border-left: 1px solid rgba(255,255,255,0.06);
    transition: right 0.28s cubic-bezier(0.4,0,0.2,1);
    z-index: 200; padding: 20px 18px;
  }
  #panel.open { right: 0; }
  #panel-x {
    position: absolute; top: 12px; right: 12px;
    background: none; border: none; color: #475569;
    font-size: 20px; cursor: pointer; line-height: 1; padding: 4px 6px;
  }
  #panel-x:hover { color: #e2e8f0; }
  #p-name { font-size: 13px; font-weight: 700; word-break: break-all; margin-bottom: 6px; }
  #p-badge {
    display: inline-block; font-size: 10px; padding: 3px 10px;
    border-radius: 6px; background: rgba(255,255,255,0.06);
    border: 1px solid rgba(255,255,255,0.08);
    text-transform: uppercase; letter-spacing: 0.07em; margin-bottom: 18px;
  }
  .psec-title {
    font-size: 10px; text-transform: uppercase; letter-spacing: 0.1em;
    color: #475569; margin-bottom: 8px; display: flex; align-items: center; gap: 6px;
  }
  .psec-title::after { content: ''; flex: 1; height: 1px; background: rgba(255,255,255,0.05); }
  .psec { margin-bottom: 18px; }
  .pitem {
    font-size: 11px; color: #94a3b8; padding: 5px 8px; border-radius: 6px;
    background: rgba(255,255,255,0.025);
    display: flex; justify-content: space-between; align-items: center;
    cursor: pointer; transition: all 0.13s; margin-bottom: 3px;
    word-break: break-all;
  }
  .pitem:hover { background: rgba(255,255,255,0.065); color: #e2e8f0; }
  .pitem .cnt {
    font-size: 10px; background: rgba(255,255,255,0.07);
    border-radius: 4px; padding: 1px 7px;
    flex-shrink: 0; margin-left: 6px; white-space: nowrap;
  }
  .pempty { font-size: 11px; color: #334155; padding: 4px 8px; }

  #legend {
    position: fixed; bottom: 18px; left: 18px;
    background: rgba(3, 3, 16, 0.88);
    backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
    border: 1px solid rgba(255,255,255,0.05);
    border-radius: 10px; padding: 12px 16px;
    display: flex; gap: 22px; z-index: 90;
  }
  .lcol-title {
    font-size: 9.5px; text-transform: uppercase;
    letter-spacing: 0.1em; color: #334155; margin-bottom: 7px;
  }
  .litem { display: flex; align-items: center; gap: 6px; font-size: 10.5px; color: #64748b; margin-bottom: 3.5px; }
  .ldot  { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
  .lline { width: 14px; height: 2px; border-radius: 1px; flex-shrink: 0; }

  #zbtns {
    position: fixed; bottom: 18px; right: 18px;
    display: flex; flex-direction: column; gap: 4px; z-index: 90;
  }
  .zbtn {
    width: 30px; height: 30px;
    background: rgba(3, 3, 16, 0.88);
    backdrop-filter: blur(12px);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 7px; color: #64748b;
    font-size: 15px; cursor: pointer;
    display: flex; align-items: center; justify-content: center;
    transition: all 0.15s; user-select: none;
  }
  .zbtn:hover { background: rgba(255,255,255,0.07); color: #e2e8f0; }

  #hint {
    position: fixed; bottom: 18px; left: 50%; transform: translateX(-50%);
    font-size: 10.5px; color: #1e293b; pointer-events: none; z-index: 80;
    white-space: nowrap;
  }

  #panel::-webkit-scrollbar { width: 3px; }
  #panel::-webkit-scrollbar-track { background: transparent; }
  #panel::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.08); border-radius: 2px; }
</style>
</head>
<body>

<div id="topbar">
  <div id="logo"><div class="logo-orb"></div>Neural Lineage</div>
  <div id="stats-row">
    <div class="stat-pill">Models <b id="s-models">-</b></div>
    <div class="stat-pill">Connections <b id="s-conn">-</b></div>
    <div class="stat-pill">Column Edges <b id="s-col">-</b></div>
  </div>
  <div id="search-wrap">
    <span class="search-icon">o</span>
    <input id="search" type="text" placeholder="Search models..." autocomplete="off" spellcheck="false">
  </div>
</div>

<div id="filters"></div>
<canvas id="graph"></canvas>

<div id="tip">
  <div id="tip-name"></div>
  <div id="tip-badge"></div>
  <div class="tip-row"><span>Upstream</span><b id="t-in"></b></div>
  <div class="tip-row"><span>Downstream</span><b id="t-out"></b></div>
  <div class="tip-row"><span>Connections</span><b id="t-deg"></b></div>
</div>

<div id="panel">
  <button id="panel-x">x</button>
  <div id="p-name"></div>
  <div id="p-badge"></div>
  <div class="psec">
    <div class="psec-title">Upstream <span id="p-in-n" style="color:#475569;font-size:11px;text-transform:none;letter-spacing:0;margin-left:4px"></span></div>
    <div id="p-in-list"></div>
  </div>
  <div class="psec">
    <div class="psec-title">Downstream <span id="p-out-n" style="color:#475569;font-size:11px;text-transform:none;letter-spacing:0;margin-left:4px"></span></div>
    <div id="p-out-list"></div>
  </div>
</div>

<div id="legend">
  <div><div class="lcol-title">Model Layer</div><div id="leg-nodes"></div></div>
  <div><div class="lcol-title">Transform Type</div><div id="leg-tt"></div></div>
</div>

<div id="zbtns">
  <button class="zbtn" id="z-in">+</button>
  <button class="zbtn" id="z-fit" title="Fit all">o</button>
  <button class="zbtn" id="z-out">-</button>
</div>

<div id="hint">Scroll to zoom · Drag canvas to pan · Drag nodes · Click to explore</div>

<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
// ── Injected data ─────────────────────────────────────────────────────────────
const DATA             = __DATA__;
const NODE_COLORS      = __NODE_COLORS__;
const TRANSFORM_COLORS = __TRANSFORM_COLORS__;
const CAT_LABELS       = __CATEGORY_LABELS__;

// ── Stats bar ─────────────────────────────────────────────────────────────────
document.getElementById("s-models").textContent = DATA.stats.models.toLocaleString();
document.getElementById("s-conn").textContent   = DATA.stats.connections.toLocaleString();
document.getElementById("s-col").textContent    = DATA.stats.column_edges.toLocaleString();

// ── Pre-process BEFORE D3 mutates link source/target ─────────────────────────
const nodeById = Object.fromEntries(DATA.nodes.map(n => [n.id, n]));

// Neighbor sets for O(1) hover dimming
const neighborOf = {};
DATA.nodes.forEach(n => { neighborOf[n.id] = new Set([n.id]); });
DATA.links.forEach(l => {
  neighborOf[l.source] = neighborOf[l.source] || new Set([l.source]);
  neighborOf[l.target] = neighborOf[l.target] || new Set([l.target]);
  neighborOf[l.source].add(l.target);
  neighborOf[l.target].add(l.source);
});

// Upstream / downstream for info panel
const upOf = {}, downOf = {};
DATA.links.forEach(l => {
  (upOf[l.target]   = upOf[l.target]   || []).push({ model: l.source, count: l.count });
  (downOf[l.source] = downOf[l.source] || []).push({ model: l.target, count: l.count });
});
Object.values(upOf).forEach(  a => a.sort((x,y) => y.count - x.count));
Object.values(downOf).forEach(a => a.sort((x,y) => y.count - x.count));

// Random phase for per-node halo pulse (organic feel)
DATA.nodes.forEach(n => { n._phase = Math.random() * Math.PI * 2; });

// Pre-group links by transform type for batched canvas rendering
const linkBatches = {};
DATA.links.forEach(l => {
  (linkBatches[l.transform] = linkBatches[l.transform] || []).push(l);
});

// ── Canvas setup ──────────────────────────────────────────────────────────────
const canvas = document.getElementById("graph");
const ctx    = canvas.getContext("2d");
let W = canvas.width  = window.innerWidth;
let H = canvas.height = window.innerHeight;

// Node radius: larger hubs, smaller leaves
const nodeR = n => Math.max(4, Math.log1p(n.degree) * 5.2);

// ── State ─────────────────────────────────────────────────────────────────────
let transform    = d3.zoomIdentity;
let hoveredNode  = null;
let selectedNode = null;
let searchQuery  = "";
let activeFilters = new Set();
let wasDragging  = false;

function nodeVisible(n) {
  return activeFilters.size === 0 || activeFilters.has(n.category);
}
function nodeOpacity(n) {
  if (hoveredNode) return neighborOf[hoveredNode.id].has(n.id) ? 1 : 0.06;
  if (searchQuery)  return n.id.toLowerCase().includes(searchQuery) ? 1 : 0.04;
  if (activeFilters.size > 0) return nodeVisible(n) ? 1 : 0.03;
  return 1;
}
function edgeOpacity(l) {
  const sid = l.source.id || l.source;
  const tid = l.target.id || l.target;
  if (hoveredNode) {
    return (sid === hoveredNode.id || tid === hoveredNode.id) ? 0.8 : 0.015;
  }
  if (searchQuery) {
    return (sid.toLowerCase().includes(searchQuery) || tid.toLowerCase().includes(searchQuery)) ? 0.6 : 0.01;
  }
  if (activeFilters.size > 0) {
    const sn = nodeById[sid], tn = nodeById[tid];
    return (sn && tn && nodeVisible(sn) && nodeVisible(tn)) ? 0.14 : 0.01;
  }
  return 0.14;
}

// Screen to world coordinate conversion
function screenToWorld(sx, sy) {
  return [(sx - transform.x) / transform.k, (sy - transform.y) / transform.k];
}

// Hit-test: find node closest to screen position
function getNodeAt(sx, sy) {
  const [wx, wy] = screenToWorld(sx, sy);
  const hitPad = Math.max(5, 10 / transform.k);
  let best = null, bestD2 = Infinity;
  DATA.nodes.forEach(n => {
    if (n.x == null) return;
    const dx = n.x - wx, dy = n.y - wy;
    const d2 = dx*dx + dy*dy;
    const hr = nodeR(n) + hitPad;
    if (d2 < hr*hr && d2 < bestD2) { best = n; bestD2 = d2; }
  });
  return best;
}

// ── D3 force simulation ───────────────────────────────────────────────────────
const RX = { source:0.08, base:0.2, stg:0.35, int:0.5, fct:0.64, dim:0.64, mart:0.79, rep:0.88, other:0.5 };
const RY = { source:0.5,  base:0.5, stg:0.5,  int:0.5, fct:0.28, dim:0.72, mart:0.5,  rep:0.5,  other:0.5 };

const sim = d3.forceSimulation(DATA.nodes)
  .force("link",   d3.forceLink(DATA.links).id(d => d.id)
                     .distance(d => 65 + d.count * 1.5).strength(0.32))
  .force("charge", d3.forceManyBody().strength(n => -90 - n.degree * 18).distanceMax(400))
  .force("center", d3.forceCenter(W/2, H/2).strength(0.03))
  .force("collide",d3.forceCollide().radius(n => nodeR(n) + 9).strength(0.75))
  .force("x",      d3.forceX(n => W * (RX[n.category] ?? 0.5)).strength(0.04))
  .force("y",      d3.forceY(n => H * (RY[n.category] ?? 0.5)).strength(0.04))
  .alphaDecay(0.015)
  .velocityDecay(0.38)
  .on("tick", () => { /* canvas loop handles rendering */ });

// ── Canvas draw functions ─────────────────────────────────────────────────────

function drawBackground() {
  const g = ctx.createRadialGradient(W*0.48, H*0.4, 0, W*0.5, H*0.5, Math.max(W,H)*0.72);
  g.addColorStop(0, "#0c0c2e");
  g.addColorStop(1, "#020209");
  ctx.fillStyle = g;
  ctx.fillRect(0, 0, W, H);
}

function drawEdges() {
  // Batch edges by transform type to minimise ctx state changes.
  // One beginPath..stroke per transform type = ~10 draw calls for 482 edges.
  const lw     = 0.6 / transform.k;
  const lwHi   = 1.5 / transform.k;

  Object.entries(linkBatches).forEach(([tt, links]) => {
    const color = TRANSFORM_COLORS[tt] || "#334155";

    // Normal (dim) pass
    ctx.beginPath();
    ctx.strokeStyle = color;
    links.forEach(l => {
      if (!l.source.x) return;
      const op = edgeOpacity(l);
      if (op < 0.05) return; // skip nearly invisible edges
      const sx = l.source.x, sy = l.source.y;
      const tx = l.target.x, ty = l.target.y;
      // Subtle quadratic curve for organic look
      const mx = (sx+tx)*0.5 + (ty-sy)*0.12;
      const my = (sy+ty)*0.5 - (tx-sx)*0.12;
      ctx.moveTo(sx, sy);
      ctx.quadraticCurveTo(mx, my, tx, ty);
    });
    // Single stroke call covers all edges of this color
    // We vary opacity per-edge via globalAlpha grouping:
    // For simplicity, do two passes (dim + bright)
    ctx.lineWidth = lw;
    ctx.globalAlpha = hoveredNode || searchQuery || activeFilters.size ? 0.025 : 0.14;
    ctx.stroke();

    // Highlighted pass (edges connected to hovered node or matching search)
    if (hoveredNode || searchQuery) {
      ctx.beginPath();
      links.forEach(l => {
        if (!l.source.x) return;
        const op = edgeOpacity(l);
        if (op < 0.3) return;
        const sx = l.source.x, sy = l.source.y;
        const tx = l.target.x, ty = l.target.y;
        const mx = (sx+tx)*0.5 + (ty-sy)*0.12;
        const my = (sy+ty)*0.5 - (tx-sx)*0.12;
        ctx.moveTo(sx, sy);
        ctx.quadraticCurveTo(mx, my, tx, ty);
      });
      ctx.lineWidth = lwHi;
      ctx.globalAlpha = 0.78;
      ctx.stroke();
    }
  });

  ctx.globalAlpha = 1;
}

function drawHalos() {
  // Pulsing outer ring per node — the "neuron firing" effect.
  // Drawn before nodes so they appear behind.
  const t = Date.now() / 2200;
  DATA.nodes.forEach(n => {
    if (n.x == null) return;
    const op = nodeOpacity(n);
    if (op < 0.1) return;
    const nr = nodeR(n);
    const pulse = (Math.sin(t + n._phase) + 1) * 0.5; // 0..1
    ctx.globalAlpha = op * (0.04 + pulse * 0.1);
    ctx.fillStyle = n.glow;
    ctx.beginPath();
    ctx.arc(n.x, n.y, nr * (2.2 + pulse * 1.0), 0, Math.PI * 2);
    ctx.fill();
  });
  ctx.globalAlpha = 1;
}

function drawNodes() {
  DATA.nodes.forEach(n => {
    if (n.x == null) return;
    const nr  = nodeR(n);
    const op  = nodeOpacity(n);
    const hi  = (n === hoveredNode);
    const sel = (n === selectedNode);

    // Static glow halo (always on, fixed size)
    ctx.globalAlpha = op * 0.22;
    ctx.fillStyle = n.glow;
    ctx.beginPath();
    ctx.arc(n.x, n.y, nr * 1.9, 0, Math.PI * 2);
    ctx.fill();

    // Main fill
    ctx.globalAlpha = op;
    ctx.fillStyle = hi ? n.glow : n.fill;
    ctx.beginPath();
    ctx.arc(n.x, n.y, nr, 0, Math.PI * 2);
    ctx.fill();

    // Stroke ring
    ctx.strokeStyle = n.glow;
    ctx.lineWidth = (hi || sel ? 2 : 1.2) / transform.k;
    ctx.stroke();

    // Selected indicator ring
    if (sel) {
      ctx.globalAlpha = op * 0.55;
      ctx.strokeStyle = "#ffffff";
      ctx.lineWidth = 1.5 / transform.k;
      ctx.beginPath();
      ctx.arc(n.x, n.y, nr + 5 / transform.k, 0, Math.PI * 2);
      ctx.stroke();
    }
  });
  ctx.globalAlpha = 1;
}

function drawLabels() {
  // Labels rendered in screen-space so they stay the same size regardless of zoom.
  const fadeIn = Math.min(1, (transform.k - 1.8) / 2.5);
  if (fadeIn <= 0) return;
  ctx.font = "9px -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
  ctx.textAlign = "center";
  DATA.nodes.forEach(n => {
    if (n.x == null) return;
    const op = nodeOpacity(n);
    if (op < 0.2) return;
    const sx = transform.applyX(n.x);
    const sy = transform.applyY(n.y);
    const sr = nodeR(n) * transform.k;
    ctx.globalAlpha = op * fadeIn * 0.75;
    ctx.fillStyle = "#94a3b8";
    ctx.fillText(n.id, sx, sy + sr + 11);
  });
  ctx.globalAlpha = 1;
}

// ── Main render loop ──────────────────────────────────────────────────────────
// Canvas is fast enough to run at 60 fps unconditionally.
// The pulsing halos require continuous animation anyway.
function draw() {
  ctx.clearRect(0, 0, W, H);
  drawBackground();
  ctx.save();
  ctx.translate(transform.x, transform.y);
  ctx.scale(transform.k, transform.k);
  drawEdges();
  drawHalos();
  drawNodes();
  ctx.restore();
  drawLabels(); // screen-space, no transform
}

(function loop() {
  draw();
  requestAnimationFrame(loop);
})();

// ── D3 zoom ───────────────────────────────────────────────────────────────────
const zoomBehavior = d3.zoom()
  .scaleExtent([0.04, 22])
  .filter(ev => {
    // Prevent zoom pan starting from a node — let drag handle that
    if (ev.type === "mousedown" || ev.type === "pointerdown") {
      return !getNodeAt(ev.offsetX ?? ev.clientX, ev.offsetY ?? ev.clientY);
    }
    return !ev.button;
  })
  .on("zoom", ev => { transform = ev.transform; });

d3.select(canvas).call(zoomBehavior);

// ── D3 drag (individual nodes) ────────────────────────────────────────────────
d3.select(canvas).call(
  d3.drag()
    .subject(ev => {
      // ev.x / ev.y are canvas-relative screen coords in D3 drag
      return getNodeAt(ev.x, ev.y);
    })
    .on("start", (ev) => {
      wasDragging = false;
      if (!ev.subject) return;
      if (!ev.active) sim.alphaTarget(0.3).restart();
      ev.subject.fx = ev.subject.x;
      ev.subject.fy = ev.subject.y;
    })
    .on("drag", (ev) => {
      if (!ev.subject) return;
      wasDragging = true;
      ev.subject.fx = (ev.x - transform.x) / transform.k;
      ev.subject.fy = (ev.y - transform.y) / transform.k;
    })
    .on("end", (ev) => {
      if (!ev.subject) return;
      if (!ev.active) sim.alphaTarget(0);
      ev.subject.fx = null;
      ev.subject.fy = null;
    })
);

// ── Mouse events (hover + click) ──────────────────────────────────────────────
canvas.addEventListener("mousemove", ev => {
  if (wasDragging) return;
  const node = getNodeAt(ev.offsetX, ev.offsetY);
  hoveredNode = node;
  canvas.style.cursor = node ? "pointer" : "default";
  if (node) { positionTip(ev); showTip(node); }
  else hideTip();
});

canvas.addEventListener("mouseleave", () => {
  hoveredNode = null;
  canvas.style.cursor = "default";
  hideTip();
});

canvas.addEventListener("click", ev => {
  if (wasDragging) { wasDragging = false; return; }
  const node = getNodeAt(ev.offsetX, ev.offsetY);
  if (node) {
    selectedNode = node;
    openPanel(node);
  } else {
    selectedNode = null;
    document.getElementById("panel").classList.remove("open");
  }
});

// ── Tooltip ───────────────────────────────────────────────────────────────────
const tip = document.getElementById("tip");
function showTip(n) {
  document.getElementById("tip-name").textContent = n.id;
  const badge = document.getElementById("tip-badge");
  badge.textContent = CAT_LABELS[n.category] || n.category;
  const gc = NODE_COLORS[n.category]?.glow || "#94a3b8";
  badge.style.color = gc;
  badge.style.background = gc + "18";
  document.getElementById("t-in").textContent  = n.in;
  document.getElementById("t-out").textContent = n.out;
  document.getElementById("t-deg").textContent = n.degree;
  tip.classList.add("show");
}
function positionTip(ev) {
  const x = ev.clientX + 14, y = ev.clientY - 10;
  const w = tip.offsetWidth || 200, h = tip.offsetHeight || 100;
  tip.style.left = (x + w > innerWidth  ? x - w - 26 : x) + "px";
  tip.style.top  = (y + h > innerHeight ? y - h       : y) + "px";
}
function hideTip() { tip.classList.remove("show"); }

// ── Info panel ────────────────────────────────────────────────────────────────
function openPanel(n) {
  document.getElementById("p-name").textContent = n.id;
  const badge = document.getElementById("p-badge");
  badge.textContent = CAT_LABELS[n.category] || n.category;
  badge.style.color = NODE_COLORS[n.category]?.glow || "#94a3b8";

  const ups  = upOf[n.id]   || [];
  const doms = downOf[n.id] || [];
  document.getElementById("p-in-n").textContent  = ups.length;
  document.getElementById("p-out-n").textContent = doms.length;

  function fill(el, items) {
    el.innerHTML = "";
    if (!items.length) { el.innerHTML = `<div class="pempty">None</div>`; return; }
    items.forEach(it => {
      const div = document.createElement("div");
      div.className = "pitem";
      div.innerHTML = `<span>${it.model}</span><span class="cnt">${it.count} col${it.count!==1?"s":""}</span>`;
      div.addEventListener("click", () => {
        const t = nodeById[it.model];
        if (t) { centerOn(t); selectedNode = t; openPanel(t); }
      });
      el.appendChild(div);
    });
  }
  fill(document.getElementById("p-in-list"),  ups);
  fill(document.getElementById("p-out-list"), doms);
  document.getElementById("panel").classList.add("open");
}

document.getElementById("panel-x").addEventListener("click", () => {
  document.getElementById("panel").classList.remove("open");
  selectedNode = null;
});

// ── Search ────────────────────────────────────────────────────────────────────
document.getElementById("search").addEventListener("input", ev => {
  searchQuery = ev.target.value.trim().toLowerCase();
  if (searchQuery) {
    const m = DATA.nodes.find(n => n.id.toLowerCase().includes(searchQuery));
    if (m && m.x) centerOn(m);
  }
});

// ── Category filter pills ─────────────────────────────────────────────────────
const catCounts = {};
DATA.nodes.forEach(n => { catCounts[n.category] = (catCounts[n.category] || 0) + 1; });

const filtersEl  = document.getElementById("filters");
const allCats = [...new Set(DATA.nodes.map(n => n.category))].sort();

function makePill(cat, label, color) {
  const p = document.createElement("div");
  p.className = "fpill" + (cat === "__all__" ? " on" : "");
  p.style.color = color;
  p.dataset.cat = cat;
  p.textContent  = label;
  filtersEl.appendChild(p);
}
makePill("__all__", "All", "#64748b");
allCats.forEach(c => makePill(c, `${CAT_LABELS[c]||c} ${catCounts[c]||0}`, NODE_COLORS[c]?.glow || "#64748b"));

filtersEl.addEventListener("click", ev => {
  const pill = ev.target.closest(".fpill");
  if (!pill) return;
  const cat = pill.dataset.cat;
  if (cat === "__all__") {
    activeFilters.clear();
    filtersEl.querySelectorAll(".fpill").forEach(p => p.classList.remove("on"));
    pill.classList.add("on");
  } else {
    filtersEl.querySelector("[data-cat='__all__']").classList.remove("on");
    activeFilters[activeFilters.has(cat) ? "delete" : "add"](cat);
    pill.classList.toggle("on");
    if (!activeFilters.size) filtersEl.querySelector("[data-cat='__all__']").classList.add("on");
  }
});

// ── Legend ────────────────────────────────────────────────────────────────────
const legNodes = document.getElementById("leg-nodes");
Object.entries(NODE_COLORS).forEach(([cat, col]) => {
  if (!catCounts[cat]) return;
  const d = document.createElement("div"); d.className = "litem";
  d.innerHTML = `<div class="ldot" style="background:${col.glow};box-shadow:0 0 5px ${col.glow}80"></div>${CAT_LABELS[cat]||cat} <span style="color:#334155;margin-left:3px">${catCounts[cat]}</span>`;
  legNodes.appendChild(d);
});
const ttCounts = {};
DATA.links.forEach(l => { ttCounts[l.transform] = (ttCounts[l.transform]||0)+1; });
const legTT = document.getElementById("leg-tt");
Object.entries(TRANSFORM_COLORS).forEach(([tt, col]) => {
  if (!ttCounts[tt]) return;
  const d = document.createElement("div"); d.className = "litem";
  d.innerHTML = `<div class="lline" style="background:${col}"></div>${tt}`;
  legTT.appendChild(d);
});

// ── Zoom controls ─────────────────────────────────────────────────────────────
function fitAll() {
  const xs = DATA.nodes.map(n => n.x).filter(v => v != null);
  const ys = DATA.nodes.map(n => n.y).filter(v => v != null);
  if (!xs.length) return;
  const x0=Math.min(...xs), x1=Math.max(...xs);
  const y0=Math.min(...ys), y1=Math.max(...ys);
  const bw=x1-x0||1, bh=y1-y0||1, pad=90;
  const k = Math.min(0.95, (W-pad*2)/bw, (H-pad*2)/bh);
  d3.select(canvas).transition().duration(750)
    .call(zoomBehavior.transform,
      d3.zoomIdentity.translate(W/2 - k*(x0+bw/2), H/2 - k*(y0+bh/2)).scale(k));
}
function centerOn(n) {
  if (!n.x) return;
  const k = 2.4;
  d3.select(canvas).transition().duration(600)
    .call(zoomBehavior.transform,
      d3.zoomIdentity.translate(W/2 - k*n.x, H/2 - k*n.y).scale(k));
}

setTimeout(fitAll, 1500); // fit after physics settle

document.getElementById("z-in").onclick  = () => d3.select(canvas).transition().duration(250).call(zoomBehavior.scaleBy, 1.45);
document.getElementById("z-out").onclick = () => d3.select(canvas).transition().duration(250).call(zoomBehavior.scaleBy, 0.68);
document.getElementById("z-fit").onclick = fitAll;

// ── Window resize ─────────────────────────────────────────────────────────────
window.addEventListener("resize", () => {
  W = canvas.width  = innerWidth;
  H = canvas.height = innerHeight;
  sim.force("center", d3.forceCenter(W/2, H/2).strength(0.03));
  sim.alpha(0.1).restart();
});
</script>
</body>
</html>
"""


def build_html(nodes: list, links: list, stats: dict) -> str:
    return (
        _HTML
        .replace("__DATA__",             json.dumps({"nodes": nodes, "links": links, "stats": stats}))
        .replace("__NODE_COLORS__",      json.dumps(NODE_COLORS))
        .replace("__TRANSFORM_COLORS__", json.dumps(TRANSFORM_COLORS))
        .replace("__CATEGORY_LABELS__",  json.dumps(CATEGORY_LABELS))
    )


def find_db(start: Path) -> Path | None:
    for directory in (start, start.parent / "demo"):
        if directory.is_dir():
            for p in sorted(directory.glob(".lineage_*.db")):
                return p
    return None


def main() -> None:
    if len(sys.argv) >= 2:
        db_path = Path(sys.argv[1])
    else:
        db_path = find_db(Path(__file__).parent)

    if db_path is None or not db_path.exists():
        print("ERROR: could not find a .lineage_*.db file.")
        print("Run 'python demo/run_demo.py' first to build the cache, then retry.")
        sys.exit(1)

    print(f"Reading graph from: {db_path}")
    nodes, links, stats = load_data(db_path)
    print(f"  {stats['models']} models  "
          f"  {stats['connections']} model connections  "
          f"  {stats['column_edges']} column edges")

    html = build_html(nodes, links, stats)

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".html", delete=False,
        prefix="dbt_neural_lineage_", encoding="utf-8"
    )
    tmp.write(html)
    tmp.close()

    print(f"\nOpening visualization in browser -> {tmp.name}")
    print("Tip: scroll to zoom, drag to pan, click a node for details.")
    webbrowser.open(f"file://{tmp.name}")


if __name__ == "__main__":
    main()
