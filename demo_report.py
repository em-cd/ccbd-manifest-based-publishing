"""
demo_report.py — HTML report generator
=======================================

Takes the result dict from demo.py and produces a self-contained,
beautifully-styled HTML report at results/demo_report.html.

The report has two tabs:
  - Demo Results: what each reader saw, comparison of safe vs naive
  - Benchmarks: charts derived from results.csv (publishing overhead,
    S3 vs Azure throughput, scan selectivity)

Regency aesthetic: parchment cream, ink, sepia, gold, sage (safe),
rose (naive). Pride & Prejudice themed.
"""

import csv
import json
import os
from typing import Optional


# ── CSV reading + processing for the Benchmarks tab ───────────────────
def _parse_float(s):
    if s is None or s == "":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _avg(values):
    vs = [v for v in values if v is not None]
    return sum(vs) / len(vs) if vs else None


def _read_bench_csv(path: str) -> list:
    """Parse results.csv into a list of dicts, with numeric columns converted."""
    rows = []
    numeric_cols = ['elapsed_ms', 'total_mb', 'throughput_mb_s',
                    'scan_ms', 'agg_ms', 'rows_scanned', 'rows_matched',
                    'num_groups', 'validation_ms', 'metadata_ms',
                    'object_count', 'list_requests', 'object_size_mb',
                    'total_bytes', 'row_count']
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for k in numeric_cols:
                if k in row:
                    row[k] = _parse_float(row[k])
            rows.append(row)
    return rows


def _process_benchmarks(rows: list) -> dict:
    """Aggregate the CSV rows into chart-ready data structures."""
    sizes = ["S", "M", "L"]
    backends = ["s3", "azure"]

    # 1) Publishing overhead — validation_ms + metadata_ms per backend × size
    publish = {}
    for backend in backends:
        publish[backend] = {"validation": [], "metadata": [], "labels": [],
                            "total_seconds": []}
        for size in sizes:
            relevant = [r for r in rows
                        if r.get("test_type") == "publish"
                        and r.get("size_label") == size
                        and r.get("backend") == backend]
            if relevant:
                val = _avg([r.get("validation_ms") for r in relevant]) or 0
                meta = _avg([r.get("metadata_ms") for r in relevant]) or 0
                publish[backend]["labels"].append(size)
                publish[backend]["validation"].append(round(val, 1))
                publish[backend]["metadata"].append(round(meta, 1))
                publish[backend]["total_seconds"].append(round((val + meta) / 1000.0, 2))

    # 2) Upload throughput + total upload time (seconds) per backend × size
    upload = {"sizes": [], "s3": [], "azure": [],
              "s3_seconds": [], "azure_seconds": []}
    for size in sizes:
        s3_runs = [r for r in rows if r.get("test_type") == "upload"
                   and r.get("size_label") == size and r.get("backend") == "s3"]
        az_runs = [r for r in rows if r.get("test_type") == "upload"
                   and r.get("size_label") == size and r.get("backend") == "azure"]
        s3_tp = _avg([r.get("throughput_mb_s") for r in s3_runs])
        az_tp = _avg([r.get("throughput_mb_s") for r in az_runs])
        s3_ms = _avg([r.get("elapsed_ms") for r in s3_runs])
        az_ms = _avg([r.get("elapsed_ms") for r in az_runs])
        if s3_tp is not None or az_tp is not None:
            upload["sizes"].append(size)
            upload["s3"].append(round(s3_tp, 1) if s3_tp is not None else None)
            upload["azure"].append(round(az_tp, 1) if az_tp is not None else None)
            upload["s3_seconds"].append(round(s3_ms / 1000.0, 2) if s3_ms is not None else None)
            upload["azure_seconds"].append(round(az_ms / 1000.0, 2) if az_ms is not None else None)

    # 3) Scan — scan_ms + agg_ms + rows_matched per preset (S3, Large)
    scan = {"presets": [], "scan_ms": [], "agg_ms": [],
            "rows_matched": [], "rows_scanned": []}
    seen = set()
    for r in rows:
        if not (r.get("test_type", "").startswith("scan/") and r.get("size_label") == "L" and r.get("backend") == "s3"):
            continue
        preset = r.get("preset_name") or r.get("test_type", "").split("/", 1)[-1]
        if preset in seen:
            continue
        runs = [x for x in rows
                if x.get("preset_name") == preset and x.get("size_label") == "L" and x.get("backend") == "s3"]
        if not runs:
            continue
        scan["presets"].append(preset)
        scan["scan_ms"].append(round(_avg([x.get("scan_ms") for x in runs]) or 0, 1))
        scan["agg_ms"].append(round(_avg([x.get("agg_ms") for x in runs]) or 0, 1))
        scan["rows_matched"].append(int(_avg([x.get("rows_matched") for x in runs]) or 0))
        scan["rows_scanned"].append(int(_avg([x.get("rows_scanned") for x in runs]) or 0))
        seen.add(preset)

    # 4) Scan scaling — scan_ms per preset across S → M → L (S3)
    # Used by the "Scan Time Scaling" chart to show linear growth with size.
    scan_scaling = {"sizes": ["S", "M", "L"], "presets": [], "data": {}}
    presets_all = sorted(set(r.get("preset_name") for r in rows
                             if r.get("preset_name") and r.get("backend") == "s3"))
    for preset in presets_all:
        per_size = []
        for size in ["S", "M", "L"]:
            runs = [x for x in rows
                    if x.get("preset_name") == preset
                    and x.get("size_label") == size
                    and x.get("backend") == "s3"]
            ms = _avg([x.get("scan_ms") for x in runs])
            per_size.append(round(ms / 1000.0, 1) if ms is not None else None)
        # Only include presets that have data for all three sizes
        if all(v is not None for v in per_size):
            scan_scaling["presets"].append(preset)
            scan_scaling["data"][preset] = per_size

    return {"publish": publish, "upload": upload, "scan": scan,
            "scan_scaling": scan_scaling,
            "row_count": len(rows)}


def _bar_row_html(item: dict, max_count: int, side: str) -> str:
    """One row inside an aggregates list — value + proportional bar + count."""
    w = round(item["count"] / max_count * 100) if max_count else 0
    return f"""
        <div class="agg-row">
          <span class="val">{item['value']}</span>
          <span class="bar"><span class="bar-fill" style="width:{w}%"></span></span>
          <span class="cnt">{item['count']:,}</span>
        </div>"""


def _agg_block_html(label: str, items: list, side: str) -> str:
    """A labeled top-3 list (moods / events / characters)."""
    if not items:
        return f"""
      <div class="agg-block">
        <div class="agg-block-label">{label}</div>
        <div class="agg-empty">no data</div>
      </div>"""
    max_count = items[0]["count"]
    rows = "".join(_bar_row_html(it, max_count, side) for it in items)
    return f"""
      <div class="agg-block">
        <div class="agg-block-label">{label}</div>
        {rows}
      </div>"""


def _reader_card_html(record: dict, side: str, baseline_v1: Optional[int], baseline_v2: Optional[int]) -> str:
    """One reader's enquiry card."""
    initials = record.get("initials", "??")
    name = record.get("reader", "A Stranger")
    t = record.get("time", 0.0)

    # Determine outcome verdict
    if not record.get("ok"):
        outcome_class = "outcome-calamity"
        outcome_label = "✕  CALAMITY"
        outcome_sub = "the dataset has vanished entirely"
        rows_html = f"<div class='reader-error'>{record.get('error', 'read failed')[:140]}</div>"
        aggs_html = ""
    else:
        rows = record["rows"]
        if baseline_v1 and rows == baseline_v1:
            outcome_class = "outcome-v1"; outcome_label = "✓  Volume the First, intact"
            outcome_sub = f"{rows:,} rows"
        elif baseline_v2 and rows == baseline_v2:
            outcome_class = "outcome-v2"; outcome_label = "✓  Volume the Second, atomically arrived"
            outcome_sub = f"{rows:,} rows"
        else:
            outcome_class = "outcome-partial"; outcome_label = "⚠  Partial reading"
            ref = baseline_v2 or baseline_v1 or 1
            outcome_sub = f"{rows:,} rows (expected ~{ref:,})"
        rows_html = f"<div class='reader-rows'>{rows:,}<span class='reader-rows-sub'>rows</span></div>"
        aggs_html = f"""
      <div class="reader-aggs">
        {_agg_block_html("Top Moods",      record.get("top_moods", []),      side)}
        {_agg_block_html("Top Events",     record.get("top_events", []),     side)}
        {_agg_block_html("Top Characters", record.get("top_characters", []), side)}
      </div>"""

    return f"""
    <article class="reader-card {outcome_class}">
      <header class="reader-card-head">
        <div class="reader-avatar">{initials}</div>
        <div class="reader-meta">
          <div class="reader-name">{name}</div>
          <div class="reader-time">enquired at t = {t:.1f}s</div>
        </div>
        {rows_html}
      </header>
      <div class="reader-outcome">
        <span class="outcome-label">{outcome_label}</span>
        <span class="outcome-sub">{outcome_sub}</span>
      </div>{aggs_html}
    </article>"""


# ── Tab + benchmarks HTML fragments ──────────────────────────────────
def _tabs_nav_html(has_bench: bool) -> str:
    if not has_bench:
        return ""
    return """
  <nav class="tabs">
    <button class="tab active" data-tab="demo">Demo Results</button>
    <button class="tab" data-tab="benchmarks">Benchmarks</button>
  </nav>"""


def _benchmarks_tab_html(bench: dict) -> str:
    return """
  <section class="tab-content" data-tab-content="benchmarks">
    <div class="bench-intro">
      <h2 class="bench-h2">Empirical Findings</h2>
      <p class="bench-p">
        Three measurements taken across {row_count} benchmark runs on S3 and Azure.
        Datasets sized Small (~3.6M rows), Medium (~17.9M rows), Large (~35.8M rows).
      </p>
    </div>

    <div class="bench-card">
      <h3 class="bench-h3">Publishing Overhead</h3>
      <p class="bench-finding">
        Validation and manifest writing add only milliseconds to a publish operation —
        a negligible cost compared to the upload itself, which takes seconds to minutes.
        The manifest is essentially free.
      </p>
      <div class="chart-box"><canvas id="chart-overhead"></canvas></div>
    </div>

    <div class="bench-card">
      <h3 class="bench-h3">Transfer Throughput: S3 vs Azure</h3>
      <p class="bench-finding">
        Both backends achieve comparable throughput. Differences become more visible
        on larger datasets due to network and concurrency factors.
      </p>
      <div class="chart-box"><canvas id="chart-throughput"></canvas></div>
    </div>

    <div class="bench-card">
      <h3 class="bench-h3">Scan Selectivity (Predicate Pushdown)</h3>
      <p class="bench-finding">
        Scan time does not always shrink with row selectivity. Queries that match few rows
        can still pay the full file-listing and metadata cost, while queries matching many
        rows benefit from streaming reads. Predicate pushdown matters most when filter
        columns align with parquet's row-group statistics.
      </p>
      <div class="chart-box tall"><canvas id="chart-selectivity"></canvas></div>
    </div>

    <div class="bench-footer">
      Charts rendered from <code>results.csv</code> at report generation time.
    </div>
  </section>""".format(row_count=bench["row_count"])


# ── The main entrypoint ───────────────────────────────────────────────
def generate_html_report(result: dict,
                         output_path: str = "results/demo_report.html",
                         benchmarks_csv: Optional[str] = None) -> str:
    """Generate the standalone HTML report. Returns the output path.

    If `benchmarks_csv` points to a readable CSV, a second 'Benchmarks'
    tab will be included with charts derived from that data.
    """
    safe_reads = result.get("safe_reads", [])
    naive_reads = result.get("naive_reads", [])
    v1 = result.get("baseline_v1")
    v2 = result.get("baseline_v2")
    bucket = result.get("bucket", "(unknown bucket)")
    backend_name = result.get("backend", "s3").upper()
    dataset_id = result.get("dataset_id", "demo")

    # ── Try to load benchmark data ──────────────────────────────────
    bench_data = None
    if benchmarks_csv and os.path.exists(benchmarks_csv):
        try:
            rows = _read_bench_csv(benchmarks_csv)
            bench_data = _process_benchmarks(rows)
            if not (bench_data["publish"]["s3"]["labels"] or bench_data["upload"]["sizes"] or bench_data["scan"]["presets"]):
                bench_data = None  # CSV exists but is empty / no rows
        except Exception as e:
            print(f"⚠  Could not process {benchmarks_csv}: {e}")
            bench_data = None

    # Build the per-side cards
    safe_cards_html  = "\n".join(_reader_card_html(r, "safe",  v1, v2) for r in safe_reads)
    naive_cards_html = "\n".join(_reader_card_html(r, "naive", v1, v2) for r in naive_reads)

    # Tally for the ledger
    safe_ok      = sum(1 for r in safe_reads  if r.get("ok"))
    safe_fail    = sum(1 for r in safe_reads  if not r.get("ok"))
    naive_ok     = sum(1 for r in naive_reads if r.get("ok"))
    naive_fail   = sum(1 for r in naive_reads if not r.get("ok"))
    naive_partial = sum(
        1 for r in naive_reads
        if r.get("ok") and v2 and r.get("rows") != v2 and r.get("rows") != v1
    )
    naive_damages = (naive_fail + naive_partial) * 300

    # Tab + benchmark HTML fragments (only injected if bench data exists)
    tabs_nav_html = _tabs_nav_html(bench_data is not None)
    benchmarks_tab_html = _benchmarks_tab_html(bench_data) if bench_data else ""
    chartjs_script = (
        '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>'
        if bench_data else ""
    )
    bench_data_json = json.dumps(bench_data) if bench_data else "null"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>A Tale of Two Publishing Methods · Demo Report</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IM+Fell+English+SC&family=Cormorant+Garamond:ital,wght@0,400;0,500;0,600;0,700;1,400;1,500&family=EB+Garamond:ital,wght@0,400;0,500;1,400&display=swap" rel="stylesheet">
<style>
  :root {{
    --parchment:  #F5ECD7;
    --parchment-2:#EFE4C8;
    --cream:      #FAF4E8;
    --ink:        #2C1810;
    --sepia:      #8B6914;
    --sepia-soft: #B89968;
    --gold:       #C9A84C;
    --gold-deep:  #8B6914;
    --sage:       #5C7A4E;
    --sage-bg:    #DDE5D5;
    --sage-soft:  #A8BFA0;
    --rose:       #C4485A;
    --rose-bg:    #ECD5D9;
    --rose-soft:  #D89AA4;
    --dusty-blue: #4A6FA5;
  }}

  * {{ box-sizing: border-box; }}
  html, body {{ margin: 0; padding: 0; }}
  body {{
    font-family: 'EB Garamond', 'Georgia', serif;
    background-color: var(--parchment);
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='200' height='200'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='2' stitchTiles='stitch'/%3E%3CfeColorMatrix values='0 0 0 0 0.55 0 0 0 0 0.4 0 0 0 0 0.1 0 0 0 0.07 0'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");
    color: var(--ink);
    padding: 32px 20px 80px;
    min-height: 100vh;
  }}

  /* ── Title block ───────────────────────────────────────── */
  .title-block {{
    max-width: 1100px;
    margin: 0 auto 28px;
    text-align: center;
    padding-bottom: 24px;
    border-bottom: 2px solid var(--gold);
    position: relative;
  }}
  .title-block::after {{
    content: '❦';
    position: absolute;
    bottom: -12px; left: 50%;
    transform: translateX(-50%);
    background: var(--parchment);
    padding: 0 16px;
    color: var(--gold-deep);
    font-size: 22px;
  }}
  h1.title {{
    font-family: 'IM Fell English SC', serif;
    font-size: 44px;
    color: var(--ink);
    letter-spacing: 0.04em;
    margin: 0 0 8px;
    line-height: 1.15;
  }}
  .subtitle {{
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    font-size: 22px;
    color: var(--sepia);
    margin: 0;
    line-height: 1.4;
  }}
  .meta-row {{
    margin-top: 14px;
    display: flex; justify-content: center; gap: 18px;
    flex-wrap: wrap;
    font-family: 'IM Fell English SC', serif;
    font-size: 13px;
    letter-spacing: 0.18em;
    color: var(--sepia);
  }}
  .meta-row .pill {{
    background: var(--cream);
    border: 1px solid var(--sepia-soft);
    padding: 4px 10px;
    border-radius: 2px;
    font-family: ui-monospace, 'SF Mono', 'Menlo', monospace;
    font-size: 13px;
    letter-spacing: 0.04em;
    text-transform: none;
    color: var(--ink);
  }}

  /* ── Stage with two libraries ──────────────────────────── */
  .stage {{
    max-width: 1280px;
    margin: 0 auto;
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    gap: 0;
    align-items: start;
  }}
  .library {{
    background: var(--cream);
    border: 1.5px solid var(--sepia-soft);
    border-radius: 2px;
    padding: 24px;
    box-shadow:
      inset 0 0 0 1px var(--cream),
      inset 0 0 0 2px var(--sepia-soft),
      0 4px 12px rgba(44,24,16,0.08);
  }}
  .library.safe  {{ border-top: 5px solid var(--sage); }}
  .library.naive {{ border-top: 5px solid var(--rose); }}

  .library-head {{
    text-align: center;
    margin-bottom: 18px;
    padding-bottom: 14px;
    border-bottom: 1px dotted var(--sepia-soft);
  }}
  .library-chapter {{
    font-family: 'IM Fell English SC', serif;
    font-size: 15px;
    color: var(--gold-deep);
    letter-spacing: 0.22em;
    margin-bottom: 6px;
  }}
  .library-title {{
    font-family: 'IM Fell English SC', serif;
    font-size: 29px;
    color: var(--ink);
    margin: 0 0 4px;
    letter-spacing: 0.02em;
  }}
  .library-sub {{
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 19px;
  }}
  /* CCBD function-name banner */
  .library-method {{
    display: inline-block;
    margin: 10px 0 6px;
    padding: 6px 14px;
    font-family: 'IM Fell English SC', serif;
    font-size: 14px;
    letter-spacing: 0.16em;
    background: var(--sage-bg);
    color: var(--sage);
    border: 1.5px solid var(--sage);
    border-radius: 2px;
  }}
  .library-method.naive {{
    background: var(--rose-bg);
    color: var(--rose);
    border-color: var(--rose);
  }}
  .library-method code {{
    font-family: ui-monospace, 'SF Mono', 'Menlo', monospace;
    font-size: 13px;
    letter-spacing: 0;
    background: rgba(0,0,0,0.06);
    padding: 1px 6px;
    border-radius: 2px;
  }}

  /* ── View toggle: show both / Darcy only / Wickham only ── */
  .view-toggle {{
    max-width: 1280px;
    margin: 0 auto 20px;
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
  }}
  .view-toggle-label {{
    font-family: 'IM Fell English SC', serif;
    font-size: 12px;
    color: var(--sepia);
    letter-spacing: 0.18em;
    margin-right: 6px;
  }}
  .view-btn {{
    font-family: 'Cormorant Garamond', serif;
    font-size: 16px;
    padding: 6px 14px;
    background: var(--cream);
    color: var(--sepia);
    border: 1.5px solid var(--sepia-soft);
    border-radius: 2px;
    cursor: pointer;
    transition: all 180ms ease;
  }}
  .view-btn:hover {{
    background: var(--parchment-2);
    color: var(--ink);
  }}
  .view-btn.active {{
    background: var(--ink);
    color: var(--gold);
    border-color: var(--ink);
    font-weight: 600;
  }}

  /* Stage solo modes: hide one side */
  .stage {{
    transition: all 500ms cubic-bezier(0.16,1,0.3,1);
  }}
  .stage.show-safe-only {{
    grid-template-columns: minmax(0, 1fr) 0 0;
  }}
  .stage.show-naive-only {{
    grid-template-columns: 0 0 minmax(0, 1fr);
  }}
  .stage.show-safe-only .library.naive,
  .stage.show-naive-only .library.safe {{
    opacity: 0;
    visibility: hidden;
    transform: scale(0.85);
    padding: 0 !important;
    border: none !important;
    box-shadow: none !important;
    overflow: hidden;
  }}
  .stage.show-safe-only .divider,
  .stage.show-naive-only .divider {{
    opacity: 0;
    width: 0;
    margin: 0;
  }}

  .library .prefix-pill {{
    display: inline-block;
    margin-top: 10px;
    font-family: ui-monospace, 'SF Mono', 'Menlo', monospace;
    font-size: 13px;
    background: var(--ink);
    color: var(--gold);
    padding: 3px 10px;
    border-radius: 2px;
    border: 1px solid var(--gold);
  }}
  .library.naive .prefix-pill {{
    color: var(--rose-soft);
    border-color: var(--rose-soft);
  }}

  /* ── Vertical divider with ornament ────────────────────── */
  .divider {{
    width: 40px;
    margin: 30px 0;
    position: relative;
    background: linear-gradient(to bottom, transparent, var(--gold) 8%, var(--gold) 92%, transparent);
    background-size: 1.5px 100%;
    background-repeat: no-repeat;
    background-position: center;
  }}
  .divider::before {{
    content: '❦';
    position: absolute;
    top: 50%; left: 50%;
    transform: translate(-50%, -50%);
    background: var(--parchment);
    color: var(--gold-deep);
    font-size: 22px;
    padding: 8px 0;
  }}

  /* ── Reader cards ──────────────────────────────────────── */
  .reader-card {{
    background: rgba(255,255,255,0.6);
    border: 1px solid var(--sepia-soft);
    border-left: 4px solid var(--sepia-soft);
    border-radius: 0 3px 3px 0;
    padding: 16px 18px;
    margin-bottom: 14px;
    box-shadow: 0 1px 3px rgba(44,24,16,0.06);
  }}
  .reader-card.outcome-v1      {{ border-left-color: var(--sage); }}
  .reader-card.outcome-v2      {{ border-left-color: var(--sage); background: linear-gradient(to right, var(--sage-bg) 0, rgba(255,255,255,0.6) 60%); }}
  .reader-card.outcome-partial {{ border-left-color: #C9A84C; background: linear-gradient(to right, #F5E9C8 0, rgba(255,255,255,0.6) 60%); }}
  .reader-card.outcome-calamity{{ border-left-color: var(--rose); background: linear-gradient(to right, var(--rose-bg) 0, rgba(255,255,255,0.6) 60%); }}

  .reader-card-head {{
    display: grid;
    grid-template-columns: auto 1fr auto;
    gap: 14px;
    align-items: center;
    margin-bottom: 10px;
  }}
  .reader-avatar {{
    width: 38px; height: 38px;
    border-radius: 50%;
    background: var(--ink);
    color: var(--gold);
    display: flex; align-items: center; justify-content: center;
    font-family: 'IM Fell English SC', serif;
    font-size: 16px;
    letter-spacing: 0.04em;
    border: 1.5px solid var(--gold);
    flex-shrink: 0;
  }}
  .reader-name {{
    font-family: 'Cormorant Garamond', serif;
    font-weight: 600;
    font-size: 22px;
    color: var(--ink);
    line-height: 1.1;
  }}
  .reader-time {{
    font-family: 'EB Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 16px;
    margin-top: 2px;
  }}
  .reader-rows {{
    font-family: 'Cormorant Garamond', serif;
    font-weight: 600;
    font-size: 32px;
    color: var(--ink);
    line-height: 1;
    text-align: right;
  }}
  .reader-rows-sub {{
    font-family: 'EB Garamond', serif;
    font-style: italic;
    font-weight: 400;
    font-size: 15px;
    color: var(--sepia);
    margin-left: 5px;
  }}
  .reader-error {{
    font-family: ui-monospace, 'SF Mono', 'Menlo', monospace;
    font-size: 15px;
    color: var(--rose);
    background: rgba(196,72,90,0.06);
    padding: 4px 8px;
    border-radius: 2px;
  }}
  .reader-outcome {{
    font-family: 'Cormorant Garamond', serif;
    font-size: 19px;
    padding: 8px 0;
    border-top: 1px dotted var(--sepia-soft);
    margin-bottom: 4px;
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 8px;
    flex-wrap: wrap;
  }}
  .outcome-label {{ font-weight: 600; }}
  .outcome-v1      .outcome-label {{ color: var(--sage); }}
  .outcome-v2      .outcome-label {{ color: var(--sage); }}
  .outcome-partial .outcome-label {{ color: var(--gold-deep); }}
  .outcome-calamity .outcome-label {{ color: var(--rose); }}
  .outcome-sub {{
    font-style: italic; color: var(--sepia); font-size: 16px;
  }}

  /* ── Aggregates ────────────────────────────────────────── */
  .reader-aggs {{
    margin-top: 8px;
    display: grid;
    grid-template-columns: 1fr;
    gap: 8px;
  }}
  .agg-block {{
    background: rgba(255,255,255,0.4);
    border: 1px solid var(--sepia-soft);
    border-radius: 2px;
    padding: 8px 10px;
  }}
  .agg-block-label {{
    font-family: 'IM Fell English SC', serif;
    font-size: 12px;
    letter-spacing: 0.2em;
    color: var(--gold-deep);
    margin-bottom: 6px;
  }}
  .agg-row {{
    display: grid;
    grid-template-columns: 1fr 1fr auto;
    gap: 8px;
    align-items: center;
    font-size: 16px;
    padding: 2px 0;
  }}
  .agg-row .val {{
    font-family: 'Cormorant Garamond', serif;
    color: var(--ink);
    font-weight: 500;
  }}
  .agg-row .bar {{
    height: 6px;
    background: rgba(139,105,20,0.12);
    border-radius: 100px;
    overflow: hidden;
  }}
  .agg-row .bar-fill {{
    display: block;
    height: 100%;
    background: var(--sage);
    border-radius: 100px;
  }}
  .library.naive .agg-row .bar-fill {{ background: var(--rose); }}
  .agg-row .cnt {{
    font-family: ui-monospace, 'SF Mono', 'Menlo', monospace;
    font-size: 13px;
    color: var(--sepia);
  }}
  .agg-empty {{
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 16px;
    text-align: center;
    padding: 6px;
  }}

  /* ── Ledger at the bottom ──────────────────────────────── */
  .ledger {{
    max-width: 1100px;
    margin: 40px auto 0;
    background: var(--cream);
    border: 1.5px solid var(--gold);
    border-radius: 2px;
    padding: 28px;
    box-shadow: 0 6px 20px rgba(139,105,20,0.15);
  }}
  .ledger-title {{
    font-family: 'IM Fell English SC', serif;
    font-size: 22px;
    color: var(--gold-deep);
    letter-spacing: 0.2em;
    text-align: center;
    margin-bottom: 18px;
  }}
  .ledger-quote {{
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    font-size: 21px;
    color: var(--ink);
    text-align: center;
    padding: 0 24px;
    margin-bottom: 22px;
    line-height: 1.5;
  }}
  .ledger-cols {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
  }}
  .ledger-col {{
    text-align: center;
  }}
  .ledger-col h3 {{
    font-family: 'IM Fell English SC', serif;
    font-size: 17px;
    letter-spacing: 0.16em;
    margin: 0 0 8px;
  }}
  .ledger-col.safe  h3 {{ color: var(--sage); }}
  .ledger-col.naive h3 {{ color: var(--rose); }}
  .ledger-stat {{
    font-family: 'Cormorant Garamond', serif;
    font-size: 42px;
    font-weight: 600;
    margin: 8px 0 4px;
  }}
  .ledger-col.safe  .ledger-stat {{ color: var(--sage); }}
  .ledger-col.naive .ledger-stat {{ color: var(--rose); }}
  .ledger-detail {{
    font-family: 'EB Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 17px;
  }}

  footer {{
    max-width: 1100px;
    margin: 30px auto 0;
    text-align: center;
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 16px;
  }}

  /* ── Tabs (Demo Results / Benchmarks) ────────────────────── */
  .tabs {{
    max-width: 1280px;
    margin: 0 auto 24px;
    display: flex;
    gap: 4px;
    border-bottom: 2px solid var(--gold);
    padding-bottom: 0;
  }}
  .tab {{
    font-family: 'IM Fell English SC', serif;
    font-size: 16px;
    letter-spacing: 0.16em;
    padding: 12px 22px;
    background: transparent;
    border: 1.5px solid var(--sepia-soft);
    border-bottom: none;
    border-radius: 3px 3px 0 0;
    color: var(--sepia);
    cursor: pointer;
    transition: all 180ms ease;
    margin-bottom: -2px;  /* sit on top of the gold border */
  }}
  .tab:hover {{
    background: var(--parchment-2);
    color: var(--ink);
  }}
  .tab.active {{
    background: var(--ink);
    color: var(--gold);
    border-color: var(--ink);
    border-bottom: 2px solid var(--ink);
  }}
  .tab-content {{ display: none; }}
  .tab-content.active {{ display: block; }}

  /* ── Benchmarks tab cards ────────────────────────────────── */
  .bench-intro {{
    max-width: 1100px;
    margin: 0 auto 24px;
    text-align: center;
  }}
  .bench-h2 {{
    font-family: 'IM Fell English SC', serif;
    font-size: 27px;
    color: var(--ink);
    margin: 0 0 10px;
    letter-spacing: 0.04em;
  }}
  .bench-p {{
    font-family: 'EB Garamond', serif;
    font-style: italic;
    font-size: 19px;
    color: var(--sepia);
    margin: 0;
  }}
  .bench-card {{
    max-width: 1100px;
    margin: 0 auto 28px;
    background: var(--cream);
    border: 1.5px solid var(--sepia-soft);
    border-top: 5px solid var(--gold);
    border-radius: 2px;
    padding: 22px 26px 26px;
    box-shadow: 0 4px 12px rgba(44,24,16,0.08);
  }}
  .bench-h3 {{
    font-family: 'IM Fell English SC', serif;
    font-size: 22px;
    color: var(--gold-deep);
    margin: 0 0 6px;
    letter-spacing: 0.06em;
  }}
  .bench-finding {{
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    font-size: 20px;
    color: var(--ink);
    margin: 0 0 18px;
    line-height: 1.5;
    opacity: 0.85;
  }}
  .chart-box {{
    position: relative;
    height: 280px;
    background: rgba(255,255,255,0.4);
    border: 1px dotted var(--sepia-soft);
    border-radius: 2px;
    padding: 14px;
  }}
  .chart-box.tall {{ height: 360px; }}
  .bench-footer {{
    max-width: 1100px;
    margin: 30px auto 0;
    text-align: center;
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 16px;
  }}
  .bench-footer code {{
    font-family: ui-monospace, 'SF Mono', 'Menlo', monospace;
    font-size: 15px;
    background: var(--cream);
    border: 1px solid var(--sepia-soft);
    padding: 1px 6px;
    border-radius: 2px;
    font-style: normal;
  }}

  @media (max-width: 880px) {{
    .stage {{ grid-template-columns: 1fr; }}
    .divider {{
      width: 100%; height: 30px; margin: 16px 0;
      background: linear-gradient(to right, transparent, var(--gold) 8%, var(--gold) 92%, transparent);
      background-size: 100% 1.5px;
    }}
    .ledger-cols {{ grid-template-columns: 1fr; }}
    h1.title {{ font-size: 34px; }}
  }}
</style>
</head>
<body>

  <div class="title-block">
    <h1 class="title">A Tale of Two Publishing Methods</h1>
    <p class="subtitle">In which manifests, like good manners, prevent considerable embarrassment.</p>
    <div class="meta-row">
      <span class="pill">{backend_name.lower()}://{bucket}/{dataset_id}/</span>
      <span>v1: {v1:,} rows</span>
      <span>v2: {v2:,} rows</span>
    </div>
  </div>
{tabs_nav_html}

  <section class="tab-content active" data-tab-content="demo">

    <div class="view-toggle">
      <span class="view-toggle-label">Show:</span>
      <button class="view-btn active" data-view="both">Both libraries</button>
      <button class="view-btn" data-view="safe">Mr. Darcy only (safe)</button>
      <button class="view-btn" data-view="naive">Mr. Wickham only (naive)</button>
    </div>

    <div class="stage" id="stage">

      <section class="library safe">
        <header class="library-head">
          <div class="library-chapter">CHAPTER THE FIRST</div>
          <h2 class="library-title">The Manifested Gentleman</h2>
          <div class="library-method">SAFE PUBLISHING · <code>publish()</code></div>
          <div class="library-sub">Mr. Darcy stages new data, validates it, then atomically flips the manifest pointer.</div>
          <div class="prefix-pill">published/{dataset_id}/latest.json</div>
        </header>
        {safe_cards_html if safe_cards_html else '<p class="agg-empty">No reads recorded.</p>'}
      </section>

      <div class="divider"></div>

      <section class="library naive">
        <header class="library-head">
          <div class="library-chapter">CHAPTER THE SECOND</div>
          <h2 class="library-title">The Impetuous Overwriter</h2>
          <div class="library-method naive">NAIVE PUBLISHING · <code>naive_publish()</code></div>
          <div class="library-sub">Mr. Wickham deletes the curated dataset, then re-uploads in place. No manifest.</div>
          <div class="prefix-pill">curated/{dataset_id}/</div>
        </header>
        {naive_cards_html if naive_cards_html else '<p class="agg-empty">No reads recorded.</p>'}
      </section>

    </div>

    <div class="ledger">
      <div class="ledger-title">❦  THE LEDGER OF CONSEQUENCES  ❦</div>
      <p class="ledger-quote">
        &ldquo;It is a truth universally acknowledged, that a dataset in possession
        of a good manifest must be in want of no readers in distress.&rdquo;
      </p>
      <div class="ledger-cols">
        <div class="ledger-col safe">
          <h3>Mr. Darcy's Library</h3>
          <div class="ledger-stat">{safe_ok}/{len(safe_reads)}</div>
          <div class="ledger-detail">enquiries answered without incident</div>
        </div>
        <div class="ledger-col naive">
          <h3>Mr. Wickham's Library</h3>
          <div class="ledger-stat">£{naive_damages}</div>
          <div class="ledger-detail">in damages from {naive_fail} calamities &amp; {naive_partial} partial readings</div>
        </div>
      </div>
    </div>

  </section>
{benchmarks_tab_html}

  <footer>
    Generated by demo.py · Pride &amp; Prejudice themed dataset versioning demo
  </footer>

{chartjs_script}
<script>
  // Tab switching
  const tabs = document.querySelectorAll('.tab');
  const tabContents = document.querySelectorAll('.tab-content');
  const BENCH = {bench_data_json};
  let chartsRendered = false;

  tabs.forEach(tab => {{
    tab.addEventListener('click', () => {{
      const target = tab.dataset.tab;
      tabs.forEach(t => t.classList.toggle('active', t.dataset.tab === target));
      tabContents.forEach(c => c.classList.toggle('active', c.dataset.tabContent === target));
      if (target === 'benchmarks' && !chartsRendered && BENCH) {{
        renderCharts();
        chartsRendered = true;
      }}
    }});
  }});

  // View toggle (show both / Darcy only / Wickham only)
  const viewBtns = document.querySelectorAll('.view-btn');
  const stageEl = document.getElementById('stage');
  viewBtns.forEach(btn => {{
    btn.addEventListener('click', () => {{
      const view = btn.dataset.view;
      viewBtns.forEach(b => b.classList.toggle('active', b === btn));
      stageEl.classList.remove('show-safe-only', 'show-naive-only');
      if (view === 'safe')  stageEl.classList.add('show-safe-only');
      if (view === 'naive') stageEl.classList.add('show-naive-only');
    }});
  }});

  function renderCharts() {{
    if (typeof Chart === 'undefined' || !BENCH) return;

    // Regency-themed defaults
    Chart.defaults.font.family = "'EB Garamond', 'Georgia', serif";
    Chart.defaults.color = '#2C1810';
    Chart.defaults.font.size = 13;

    const COLOURS = {{
      sage:  '#5C7A4E', sageBg: '#A8BFA0',
      rose:  '#C4485A', roseBg: '#D89AA4',
      gold:  '#C9A84C', sepia: '#8B6914',
      ink:   '#2C1810', cream: '#FAF4E8',
    }};

    // Chart 1: publishing overhead — stacked bars per size, S3 only
    const pub = BENCH.publish.s3;
    if (pub && pub.labels && pub.labels.length) {{
      new Chart(document.getElementById('chart-overhead'), {{
        type: 'bar',
        data: {{
          labels: pub.labels.map(s => `Size ${{s}}`),
          datasets: [
            {{ label: 'Validation (ms)',      data: pub.validation, backgroundColor: COLOURS.sage }},
            {{ label: 'Manifest write (ms)',  data: pub.metadata,   backgroundColor: COLOURS.gold }},
          ]
        }},
        options: {{
          responsive: true, maintainAspectRatio: false,
          plugins: {{
            legend: {{ position: 'bottom' }},
            title: {{ display: true, text: 'Manifest-writing overhead (S3, milliseconds)', font: {{ size: 15, weight: 'normal' }} }},
          }},
          scales: {{
            x: {{ stacked: true, grid: {{ display: false }} }},
            y: {{ stacked: true, beginAtZero: true, title: {{ display: true, text: 'ms (lower is better)' }} }}
          }}
        }}
      }});
    }}

    // Chart 2: throughput S3 vs Azure
    const up = BENCH.upload;
    if (up && up.sizes && up.sizes.length) {{
      new Chart(document.getElementById('chart-throughput'), {{
        type: 'bar',
        data: {{
          labels: up.sizes.map(s => `Size ${{s}}`),
          datasets: [
            {{ label: 'S3',    data: up.s3,    backgroundColor: COLOURS.sage }},
            {{ label: 'Azure', data: up.azure, backgroundColor: COLOURS.rose }},
          ]
        }},
        options: {{
          responsive: true, maintainAspectRatio: false,
          plugins: {{
            legend: {{ position: 'bottom' }},
            title: {{ display: true, text: 'Upload throughput (MB/s, higher is better)', font: {{ size: 15, weight: 'normal' }} }},
          }},
          scales: {{
            x: {{ grid: {{ display: false }} }},
            y: {{ beginAtZero: true, title: {{ display: true, text: 'MB/s' }} }}
          }}
        }}
      }});
    }}

    // Chart 3: scan selectivity — scan_ms bars + rows_matched line
    const sc = BENCH.scan;
    if (sc && sc.presets && sc.presets.length) {{
      new Chart(document.getElementById('chart-selectivity'), {{
        data: {{
          labels: sc.presets,
          datasets: [
            {{
              type: 'bar', label: 'Scan time (ms)',
              data: sc.scan_ms, backgroundColor: COLOURS.sage, yAxisID: 'y',
              order: 2,
            }},
            {{
              type: 'line', label: 'Rows matched',
              data: sc.rows_matched, borderColor: COLOURS.rose,
              backgroundColor: COLOURS.rose, yAxisID: 'y1',
              pointRadius: 5, pointStyle: 'circle', borderWidth: 2,
              tension: 0.2, order: 1,
            }},
          ]
        }},
        options: {{
          responsive: true, maintainAspectRatio: false,
          plugins: {{
            legend: {{ position: 'bottom' }},
            title: {{ display: true, text: 'Scan time vs rows matched, by query preset (S3, Large)', font: {{ size: 15, weight: 'normal' }} }},
          }},
          scales: {{
            x: {{ grid: {{ display: false }} }},
            y:  {{ position: 'left',  beginAtZero: true, title: {{ display: true, text: 'Scan time (ms)' }} }},
            y1: {{ position: 'right', beginAtZero: true, title: {{ display: true, text: 'Rows matched' }}, grid: {{ display: false }} }},
          }}
        }}
      }});
    }}
  }}
</script>

</body>
</html>
"""

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    return output_path