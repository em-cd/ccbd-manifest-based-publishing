"""
demo.py — Live Manifest-Based Publishing Demo
==============================================

One command:
    python3 demo.py --backend s3
    python3 demo.py --backend azure

What happens:
  1. Starts a small Flask server in a background thread (port 5050)
  2. Opens your browser to http://localhost:5050/
  3. Runs the real threaded writer/reader race-condition demo:
     - Safe demo: writer publishes v2 via manifest while 3 readers
       (Elizabeth, Jane, Mary) enquire mid-publish
     - Naive demo: writer naive-overwrites curated/ while the same
       readers enquire
  4. The browser page polls the server every 250ms and shows everything
     happening live — files appearing in staging, the manifest flipping,
     files getting deleted, reader enquiries arriving with results
  5. At the end, saves results/demo_report.html (the persistent report)
     and keeps the Flask server running so the browser tab stays alive

Press Ctrl+C in the terminal when you want to stop the server.
"""

import argparse
import os
import threading
import time
import webbrowser

import pyarrow.compute as pc

from backend.azure_backend import AzureBackend
from backend.s3_backend import S3Backend
from dataset_gen import generate_dataset
from publish import publish, naive_publish
from read import read_current_dataset, naive_read_dataset
from data_transfer import upload

import demo_server
from demo_report import generate_html_report


# ── Config ────────────────────────────────────────────────────────────
DATA_DIR = "./data"
DATASET_ID = "demo"
PORT = int(os.environ.get("DEMO_PORT", "5050"))

BACKEND_MAP = {
    "s3": S3Backend,
    "azure": AzureBackend,
}

# Readers keyed by their sleep time (when they choose to enquire)
READERS = [
    {"time": 0.2, "name": "Miss Elizabeth", "initials": "EB"},
    {"time": 1.2, "name": "Miss Jane",      "initials": "JB"},
    {"time": 2.0, "name": "Miss Mary",      "initials": "MB"},
]
FINAL_READER = {"time": 3.8, "name": "Lady Catherine", "initials": "LC"}


# ── Aggregate helpers ─────────────────────────────────────────────────
def top_n(table, column: str, n: int = 3) -> list[dict]:
    if column not in table.column_names:
        return []
    try:
        counts = pc.value_counts(table.column(column))
        pairs = [(row["values"], row["counts"]) for row in counts.to_pylist()]
        pairs.sort(key=lambda x: -x[1])
        return [
            {"value": str(v) if v is not None else "(null)", "count": int(c)}
            for v, c in pairs[:n]
        ]
    except Exception:
        return []


def _compute_top_events_from_local(local_path: str, n: int = 5) -> list[dict]:
    """Read a local parquet dataset and return its top-N event_type values.
    Used to pre-compute what the manifest will (correctly) describe."""
    try:
        import pyarrow.dataset as pads
        table = pads.dataset(local_path, format="parquet").to_table()
        return top_n(table, "event_type", n)
    except Exception as e:
        print(f"  (could not compute top events from {local_path}: {e})")
        return []


def _enrich_manifest(backend, dataset_id: str, top_events: list[dict]) -> None:
    """Read the just-published manifest, inject the top events from the
    validation step, and write it back. The audience can then honestly see
    that the manifest itself carries these aggregates."""
    if not top_events:
        return
    key = f"published/{dataset_id}/latest.json"
    try:
        manifest = backend.read_json(key)
        if manifest is None:
            return
        manifest["top_events"] = top_events
        backend.write_json(key, manifest)
    except Exception as e:
        print(f"  (could not enrich manifest at {key}: {e})")


def read_with_aggregates(backend, dataset_id, safe: bool) -> dict:
    try:
        table = read_current_dataset(backend, dataset_id) if safe else naive_read_dataset(backend, dataset_id)
        return {
            "ok": True,
            "rows": table.num_rows,
            "top_moods":      top_n(table, "mood", 3),
            "top_events":     top_n(table, "event_type", 3),
            "top_characters": top_n(table, "character", 3),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── The demo ──────────────────────────────────────────────────────────
def run_publish_demo(backend, backend_name: str):
    # Pass the backend CLASS as factory so the monitor gets its own instance.
    # This avoids segfaults from concurrent boto3 client usage across threads.
    demo_server.configure(BACKEND_MAP[backend_name], backend_name, DATASET_ID)

    # Ensure local datasets exist
    for version in ("v1", "v2"):
        local_path = f"{DATA_DIR}/{DATASET_ID}/{version}"
        if not (os.path.exists(local_path) and os.listdir(local_path)):
            print(f"→ Generating local {version} dataset…")
            generate_dataset(f"{DATASET_ID}/{version}")

    v1_path = f"{DATA_DIR}/{DATASET_ID}/v1"
    v2_path = f"{DATA_DIR}/{DATASET_ID}/v2"

    # Compute top events from the local datasets BEFORE setup — these are what
    # the manifest will (correctly) describe. Push them to the server so the
    # sidebar can display them the moment a version becomes active.
    print("→ Computing manifest aggregates from local datasets…")
    v1_top_events = _compute_top_events_from_local(v1_path, n=5)
    v2_top_events = _compute_top_events_from_local(v2_path, n=5)
    demo_server.set_manifest_top_events("v1", v1_top_events)
    demo_server.set_manifest_top_events("v2", v2_top_events)
    if v1_top_events:
        print(f"  v1 top event: {v1_top_events[0]['value']} ({v1_top_events[0]['count']:,})")
    if v2_top_events:
        print(f"  v2 top event: {v2_top_events[0]['value']} ({v2_top_events[0]['count']:,})")

    # ── SETUP (synchronous, NO monitor thread yet — avoids race on macOS) ──
    demo_server.set_act("setup")
    print("\n══════════════════════════════════════════════════════════════")
    print("  SETUP: publishing v1 baseline to both libraries")
    print("══════════════════════════════════════════════════════════════")

    demo_server.set_writer("safe", "publishing", "Publishing v1 baseline via manifest…")
    publish(backend, DATASET_ID, v1_path, "v1", sleep=0)
    _enrich_manifest(backend, DATASET_ID, v1_top_events)
    demo_server.set_writer("safe", None, "")

    demo_server.set_writer("naive", "uploading", "Loading v1 baseline into curated…")
    demo_server.set_version_hint("naive", "v1")
    upload(backend, DATASET_ID, v1_path, version=None, zone="curated", sleep=0)
    demo_server.set_writer("naive", None, "")

    # Capture baseline row counts
    try:
        v1_rows = read_current_dataset(backend, DATASET_ID).num_rows
    except Exception:
        v1_rows = None
    try:
        import pyarrow.dataset as pads
        v2_rows = pads.dataset(v2_path, format="parquet").count_rows()
    except Exception:
        v2_rows = None
    demo_server.set_baselines(v1_rows, v2_rows)
    print(f"  v1 rows: {v1_rows:,}   v2 rows: {v2_rows:,}")

    # NOW it's safe to start the state monitor — setup is done, no race
    # between concurrent boto3 / pyarrow access on the same client.
    print("→ Starting state monitor for Act I…")
    demo_server.start_monitor(DATASET_ID)

    # Let the page settle before launching Act I
    time.sleep(2.5)

    # ── ACT I — SAFE ──────────────────────────────────────────────
    demo_server.set_act("safe")
    print("\n══════════════════════════════════════════════════════════════")
    print("  ACT I  ·  SAFE MANIFEST-BASED PUBLISHING")
    print("══════════════════════════════════════════════════════════════")

    def safe_writer():
        demo_server.set_writer("safe", "staging", "Mr. Darcy uploads v2 to staging/demo/v2/…")
        publish(backend, DATASET_ID, v2_path, "v2", sleep=0.8)
        _enrich_manifest(backend, DATASET_ID, v2_top_events)
        demo_server.set_writer("safe", None, "v2 published. The manifest pointer has flipped.")

    def safe_reader(r):
        time.sleep(r["time"])
        rec = read_with_aggregates(backend, DATASET_ID, safe=True)
        rec["time"] = r["time"]
        rec["reader"] = r["name"]
        rec["initials"] = r["initials"]
        demo_server.add_read("safe", rec)
        _print_read("SAFE ", r, rec)

    threads = [threading.Thread(target=safe_writer)]
    for r in READERS:
        threads.append(threading.Thread(target=safe_reader, args=(r,)))
    for t in threads: t.start()
    for t in threads: t.join()

    # Lady Catherine reads after settled
    safe_reader(FINAL_READER)

    # ── INTERLUDE — give the audience time to digest before Act II ──
    demo_server.set_act("interlude")
    print("\n──── Intermission — Act I complete. Wickham approaches… ────")
    time.sleep(8.0)

    # ── ACT II — NAIVE ────────────────────────────────────────────
    demo_server.set_act("naive")
    print("\n══════════════════════════════════════════════════════════════")
    print("  ACT II  ·  NAIVE OVERWRITE")
    print("══════════════════════════════════════════════════════════════")

    def naive_writer():
        demo_server.set_writer("naive", "deleting", "Mr. Wickham deletes curated/…")
        demo_server.set_version_hint("naive", "v2")  # the new files will be v2
        naive_publish(backend, DATASET_ID, v2_path, sleep=0.8)
        demo_server.set_writer("naive", None, "v2 in curated/. The damage is done.")

    def naive_reader(r):
        time.sleep(r["time"])
        rec = read_with_aggregates(backend, DATASET_ID, safe=False)
        rec["time"] = r["time"]
        rec["reader"] = r["name"]
        rec["initials"] = r["initials"]
        demo_server.add_read("naive", rec)
        _print_read("NAIVE", r, rec)

    threads = [threading.Thread(target=naive_writer)]
    for r in READERS:
        threads.append(threading.Thread(target=naive_reader, args=(r,)))
    for t in threads: t.start()
    for t in threads: t.join()

    naive_reader(FINAL_READER)
    time.sleep(1.5)

    # ── DONE ──────────────────────────────────────────────────────
    demo_server.set_act("done")
    print("\n══════════════════════════════════════════════════════════════")
    print("  GENERATING PERSISTENT REPORT")
    print("══════════════════════════════════════════════════════════════")

    state = demo_server.snapshot()
    result = {
        "backend": backend_name,
        "bucket": backend.get_root(),
        "dataset_id": DATASET_ID,
        "baseline_v1": v1_rows,
        "baseline_v2": v2_rows,
        "safe_reads": sorted(state["reads"]["safe"], key=lambda r: r["time"]),
        "naive_reads": sorted(state["reads"]["naive"], key=lambda r: r["time"]),
    }

    # Look for a benchmarks CSV in the usual places
    bench_csv = None
    for candidate in ("results.csv", "./results.csv",
                      "results/results.csv", "./results/results.csv",
                      "bench/results.csv"):
        if os.path.exists(candidate):
            bench_csv = candidate
            print(f"  benchmarks CSV: {candidate}")
            break
    if not bench_csv:
        print("  (no results.csv found — Benchmarks tab will be skipped)")

    output_path = generate_html_report(result, benchmarks_csv=bench_csv)
    print(f"  → {output_path}  (saved as backup; live view stays the main attraction)")

    print("\n══════════════════════════════════════════════════════════════")
    print("  DEMO COMPLETE — Flask server still running")
    print(f"  Live view:          http://localhost:{PORT}/")
    print(f"  Backup report:      {os.path.abspath(output_path)}")
    print("  Press Ctrl+C in this terminal to stop the server.")
    print("══════════════════════════════════════════════════════════════\n")


def _print_read(tag: str, r: dict, rec: dict):
    """Terminal log line for a reader's enquiry."""
    if rec.get("ok"):
        top = rec["top_moods"][0]["value"] if rec.get("top_moods") else "—"
        print(f"  [{tag}] {r['name']:<16} @ t={r['time']:.1f}s  →  "
              f"{rec['rows']:>6,} rows   top mood: {top}")
    else:
        err = rec.get("error", "?")[:60]
        print(f"  [{tag}] {r['name']:<16} @ t={r['time']:.1f}s  →  CALAMITY ({err})")


# ── Entry point ───────────────────────────────────────────────────────
def main(backend_name: str):
    # macOS safety: helps Objective-C runtime tolerate fork-like patterns
    # in some C extensions. Harmless on Linux.
    os.environ.setdefault("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "YES")

    backend = BACKEND_MAP[backend_name]()

    # Pre-warm pyarrow's S3 filesystem global init in the main thread,
    # before any other thread might also touch it. This avoids a known
    # race condition that segfaults on macOS Apple Silicon.
    print("→ Pre-warming pyarrow filesystem…")
    try:
        _ = backend.filesystem()
    except Exception as e:
        print(f"  ({e}) — continuing")

    # Start Flask in a daemon thread, trying ports until one works
    global PORT
    flask_thread = threading.Thread(
        target=demo_server.run_server, args=(PORT,), daemon=True
    )
    flask_thread.start()

    # Self-check: is Flask actually responding? Try for up to 5 seconds.
    import urllib.request
    flask_ready = False
    for attempt in range(20):
        time.sleep(0.25)
        try:
            with urllib.request.urlopen(f"http://localhost:{PORT}/api/state", timeout=1) as r:
                if r.status == 200:
                    flask_ready = True
                    break
        except Exception:
            pass

    if not flask_ready:
        print(f"\n❌ Flask never responded on port {PORT}.")
        print(f"   Try: lsof -i :{PORT}    to see what's holding the port.")
        print(f"   Or:  DEMO_PORT=5060 python3 demo_presentation.py --backend=s3")
        print(f"        (run again with a different port via env var)\n")
        # See if there's a clear error visible above this
        return

    print(f"✓ Flask listening on http://localhost:{PORT}/")

    # Open browser
    url = f"http://localhost:{PORT}/"
    print(f"\n→ Opening browser at {url}")
    try:
        webbrowser.open(url)
    except Exception:
        print("  (couldn't auto-open; visit the URL manually)")
    time.sleep(2.0)  # give the page a moment to load before the action starts

    # Run the demo
    try:
        run_publish_demo(backend, backend_name)
    except Exception as e:
        print(f"\nDemo failed: {e}")
        demo_server.log(f"Demo failed: {e}")
        raise

    # Keep alive so the browser tab stays connected
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n→ Stopping server. Goodbye.")
        demo_server.stop_monitor()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Live manifest-based publishing demo")
    parser.add_argument("--backend", choices=["s3", "azure"], required=True)
    args = parser.parse_args()
    main(args.backend)