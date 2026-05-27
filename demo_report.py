"""
demo_report.py — HTML report generator
=======================================

Takes the result dict from demo.py and produces a self-contained,
beautifully-styled HTML report at results/demo_report.html.

The report shows:
  - Title + setup (which bucket, baseline row counts)
  - Two columns side-by-side: Safe (Manifest) vs Naive (Overwrite)
  - For each side: each reader's enquiry card with time, row count,
    outcome verdict, and top-3 aggregates (moods / events / characters)
  - Final ledger comparing total damages

Regency aesthetic: parchment cream, ink, sepia, gold, sage (safe),
rose (naive). Pride & Prejudice themed.
"""

import os
from typing import Optional


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


# ── The main entrypoint ───────────────────────────────────────────────
def generate_html_report(result: dict, output_path: str = "results/demo_report.html") -> str:
    """Generate the standalone HTML report. Returns the output path."""
    safe_reads = result.get("safe_reads", [])
    naive_reads = result.get("naive_reads", [])
    v1 = result.get("baseline_v1")
    v2 = result.get("baseline_v2")
    bucket = result.get("bucket", "(unknown bucket)")
    backend_name = result.get("backend", "s3").upper()
    dataset_id = result.get("dataset_id", "demo")

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
    font-size: 18px;
  }}
  h1.title {{
    font-family: 'IM Fell English SC', serif;
    font-size: 38px;
    color: var(--ink);
    letter-spacing: 0.04em;
    margin: 0 0 8px;
    line-height: 1.15;
  }}
  .subtitle {{
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    font-size: 18px;
    color: var(--sepia);
    margin: 0;
    line-height: 1.4;
  }}
  .meta-row {{
    margin-top: 14px;
    display: flex; justify-content: center; gap: 18px;
    flex-wrap: wrap;
    font-family: 'IM Fell English SC', serif;
    font-size: 11px;
    letter-spacing: 0.18em;
    color: var(--sepia);
  }}
  .meta-row .pill {{
    background: var(--cream);
    border: 1px solid var(--sepia-soft);
    padding: 4px 10px;
    border-radius: 2px;
    font-family: ui-monospace, 'SF Mono', 'Menlo', monospace;
    font-size: 11px;
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
    font-size: 12px;
    color: var(--gold-deep);
    letter-spacing: 0.22em;
    margin-bottom: 6px;
  }}
  .library-title {{
    font-family: 'IM Fell English SC', serif;
    font-size: 24px;
    color: var(--ink);
    margin: 0 0 4px;
    letter-spacing: 0.02em;
  }}
  .library-sub {{
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 15px;
  }}
  .library .prefix-pill {{
    display: inline-block;
    margin-top: 10px;
    font-family: ui-monospace, 'SF Mono', 'Menlo', monospace;
    font-size: 11px;
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
    font-size: 18px;
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
    font-size: 13px;
    letter-spacing: 0.04em;
    border: 1.5px solid var(--gold);
    flex-shrink: 0;
  }}
  .reader-name {{
    font-family: 'Cormorant Garamond', serif;
    font-weight: 600;
    font-size: 18px;
    color: var(--ink);
    line-height: 1.1;
  }}
  .reader-time {{
    font-family: 'EB Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 13px;
    margin-top: 2px;
  }}
  .reader-rows {{
    font-family: 'Cormorant Garamond', serif;
    font-weight: 600;
    font-size: 26px;
    color: var(--ink);
    line-height: 1;
    text-align: right;
  }}
  .reader-rows-sub {{
    font-family: 'EB Garamond', serif;
    font-style: italic;
    font-weight: 400;
    font-size: 12px;
    color: var(--sepia);
    margin-left: 5px;
  }}
  .reader-error {{
    font-family: ui-monospace, 'SF Mono', 'Menlo', monospace;
    font-size: 12px;
    color: var(--rose);
    background: rgba(196,72,90,0.06);
    padding: 4px 8px;
    border-radius: 2px;
  }}
  .reader-outcome {{
    font-family: 'Cormorant Garamond', serif;
    font-size: 15px;
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
    font-style: italic; color: var(--sepia); font-size: 13px;
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
    font-size: 10px;
    letter-spacing: 0.2em;
    color: var(--gold-deep);
    margin-bottom: 6px;
  }}
  .agg-row {{
    display: grid;
    grid-template-columns: 1fr 1fr auto;
    gap: 8px;
    align-items: center;
    font-size: 13px;
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
    font-size: 11px;
    color: var(--sepia);
  }}
  .agg-empty {{
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 13px;
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
    font-size: 18px;
    color: var(--gold-deep);
    letter-spacing: 0.2em;
    text-align: center;
    margin-bottom: 18px;
  }}
  .ledger-quote {{
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    font-size: 17px;
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
    font-size: 14px;
    letter-spacing: 0.16em;
    margin: 0 0 8px;
  }}
  .ledger-col.safe  h3 {{ color: var(--sage); }}
  .ledger-col.naive h3 {{ color: var(--rose); }}
  .ledger-stat {{
    font-family: 'Cormorant Garamond', serif;
    font-size: 36px;
    font-weight: 600;
    margin: 8px 0 4px;
  }}
  .ledger-col.safe  .ledger-stat {{ color: var(--sage); }}
  .ledger-col.naive .ledger-stat {{ color: var(--rose); }}
  .ledger-detail {{
    font-family: 'EB Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 14px;
  }}

  footer {{
    max-width: 1100px;
    margin: 30px auto 0;
    text-align: center;
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    color: var(--sepia);
    font-size: 13px;
  }}

  @media (max-width: 880px) {{
    .stage {{ grid-template-columns: 1fr; }}
    .divider {{
      width: 100%; height: 30px; margin: 16px 0;
      background: linear-gradient(to right, transparent, var(--gold) 8%, var(--gold) 92%, transparent);
      background-size: 100% 1.5px;
    }}
    .ledger-cols {{ grid-template-columns: 1fr; }}
    h1.title {{ font-size: 28px; }}
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

  <div class="stage">

    <section class="library safe">
      <header class="library-head">
        <div class="library-chapter">CHAPTER THE FIRST</div>
        <h2 class="library-title">The Manifested Gentleman</h2>
        <div class="library-sub">Mr. Darcy publishes via the manifest, atomically and in private.</div>
        <div class="prefix-pill">published/{dataset_id}/latest.json</div>
      </header>
      {safe_cards_html if safe_cards_html else '<p class="agg-empty">No reads recorded.</p>'}
    </section>

    <div class="divider"></div>

    <section class="library naive">
      <header class="library-head">
        <div class="library-chapter">CHAPTER THE SECOND</div>
        <h2 class="library-title">The Impetuous Overwriter</h2>
        <div class="library-sub">Mr. Wickham deletes curated/ and writes in place. No safety net.</div>
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

  <footer>
    Generated by demo.py · Pride &amp; Prejudice themed dataset versioning demo
  </footer>

</body>
</html>
"""

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    return output_path