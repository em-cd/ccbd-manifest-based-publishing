"""
Flask backend for the live demo webapp.
========================================

Wraps your existing S3Backend / publish / naive_publish / read functions
and exposes them as HTTP endpoints that the webapp can call from the
"PRESENTER" tab.

HOW IT FITS WITH YOUR EXISTING CODE
-----------------------------------
This file imports from your repo:
    backend.s3_backend.S3Backend
    publish.publish, publish.naive_publish
    read.read_current_dataset, read.naive_read_dataset
    dataset_gen.generate_dataset  (only used at startup, optional)

So drop this file at the ROOT of your repo (next to demo.py), or run
`python -m server.app` from the root after putting it in a ./server/
folder.

HOW TO RUN
----------
    pip install flask flask-cors
    export AWS_PROFILE=your-profile   # or AWS credentials env vars
    python server/app.py

Then open demo_webapp.html in your browser. It will auto-detect the
server (polls /api/health on load) and switch to LIVE mode. The red dot
in the top-right corner will turn red and say "LIVE · S3".

If the server is NOT running, the webapp falls back to local simulation —
nothing is broken, you just lose the real S3 round-trip.

SAFETY NOTES FOR THE LIVE DEMO
------------------------------
- Uses dataset_id="demo" by default (override with DEMO_DATASET_ID env var)
- All operations run in background threads so the HTTP request returns
  immediately; frontend polls /api/state for progress.
- The frontend never tells the server what bucket to write to. The
  S3Backend is configured from your existing environment.
- Reads return only row counts, never raw data.
"""

from __future__ import annotations

import os
import sys
import threading
import time
import traceback
from typing import Any, Optional

# ── Make the repo root importable ─────────────────────────────────────
# Allows running this file directly as `python3 server/app.py` from the
# repo root. Without this, `from backend...` fails because Python sets
# the import root to the script's directory (server/), not the repo root.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from flask import Flask, jsonify, request
from flask_cors import CORS

# ── Wire up your existing code ────────────────────────────────────────
# Imports below resolve because of the sys.path insertion above, so this
# file can live in server/ and still find backend/, publish.py, read.py
# at the repo root.
from backend.s3_backend import S3Backend
from publish import publish, naive_publish
from read import read_current_dataset, naive_read_dataset


# ── Config ────────────────────────────────────────────────────────────
DATA_DIR     = os.environ.get("DEMO_DATA_DIR", "./data")
DATASET_ID   = os.environ.get("DEMO_DATASET_ID", "demo")
NUM_FILES    = 5                # what the UI expects per version
V1_ROWS      = 3500             # fallback estimates, replaced after first read
V2_ROWS      = 5000

v1_path = f"{DATA_DIR}/{DATASET_ID}/v1"
v2_path = f"{DATA_DIR}/{DATASET_ID}/v2"


# ── Singletons ────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)   # allow the demo HTML (opened from file://) to call us

backend = S3Backend()
state_lock = threading.Lock()

STATE: dict[str, Any] = {
    "safe":  {"progress": 0.0, "complete": False, "version": "v1", "busy": None},
    "naive": {"filesV1": NUM_FILES, "filesV2": 0, "busy": None},
    "row_counts": {"v1": V1_ROWS, "v2": V2_ROWS},
    "last_error": None,
}


def set_state(**updates):
    with state_lock:
        for k, v in updates.items():
            keys = k.split(".")
            ref = STATE
            for part in keys[:-1]:
                ref = ref[part]
            ref[keys[-1]] = v


# ── Background worker ─────────────────────────────────────────────────
def run_in_thread(fn, *args, **kwargs):
    """Fire-and-forget; errors logged + stored on STATE['last_error']."""
    def wrapped():
        try:
            fn(*args, **kwargs)
        except Exception as e:
            traceback.print_exc()
            set_state(last_error=f"{type(e).__name__}: {e}")
    t = threading.Thread(target=wrapped, daemon=True)
    t.start()
    return t


# ── Operations ────────────────────────────────────────────────────────
def op_safe_stage():
    """Run the real publish() to upload v2 to staging + validate +
    write a NEW manifest pointing at v2. Animate progress for the UI."""
    set_state(**{"safe.busy": "staging", "safe.progress": 0.0, "safe.complete": False})

    # Fake-stream progress to the UI while the real upload runs in a sub-thread.
    done = {"flag": False}
    def real_publish():
        try:
            publish(backend, DATASET_ID, v2_path, "v2", sleep=0.0)
        finally:
            done["flag"] = True

    real_thread = threading.Thread(target=real_publish, daemon=True)
    real_thread.start()

    # Progress bar tied to elapsed time, capped at 95% until real op finishes
    start = time.time()
    expected_secs = 8.0   # rough; just for visual smoothness
    while not done["flag"]:
        elapsed = time.time() - start
        p = min(0.95, elapsed / expected_secs)
        set_state(**{"safe.progress": p})
        time.sleep(0.2)
    set_state(**{"safe.progress": 1.0, "safe.complete": True,
                 "safe.version": "v2", "safe.busy": None})


def op_safe_flip():
    """The manifest has already been flipped by op_safe_stage (which
    calls publish()). This endpoint is for the demo flow where you want
    to stage and flip as two separate audience-visible steps.
    In that case, stage will leave version='v1' until flip is called.
    """
    set_state(**{"safe.version": "v2", "safe.busy": None})


def op_safe_rollback():
    """Roll back by setting the manifest to point at v1 again. In a
    real system this reads previous.json and writes it as latest.json.
    Here we delegate to your existing code if you have a rollback
    function; if not, we re-publish v1.
    """
    set_state(**{"safe.busy": "staging"})
    try:
        publish(backend, DATASET_ID, v1_path, "v1", sleep=0.0)
        set_state(**{"safe.version": "v1", "safe.busy": None,
                     "safe.complete": False, "safe.progress": 0.0})
    except Exception:
        traceback.print_exc()
        set_state(**{"safe.busy": None,
                     "last_error": "rollback failed — see server logs"})


def op_naive_delete_then_upload():
    """Run naive_publish: deletes v1 files then uploads v2 in place,
    no manifest, with race-condition window. Update STATE so the UI
    animates files vanishing then re-appearing."""
    set_state(**{"naive.busy": "deleting"})

    # Phase 1: simulate delete by counting down filesV1 over ~2s.
    # The real naive_publish does this internally, but the UI needs
    # to see the count drop. We approximate with timed updates.
    done = {"flag": False}
    def real_naive():
        try:
            naive_publish(backend, DATASET_ID, v2_path, sleep=0.0)
        finally:
            done["flag"] = True
    threading.Thread(target=real_naive, daemon=True).start()

    # Visual: countdown v1 over the first half of expected time, then
    # count up v2 over the second half. Approximate.
    expected_total = 10.0
    start = time.time()
    while not done["flag"]:
        e = time.time() - start
        if e < expected_total * 0.45:
            # deleting phase
            p = e / (expected_total * 0.45)
            set_state(**{"naive.filesV1": max(0, NUM_FILES - int(p * NUM_FILES)),
                         "naive.busy": "deleting"})
        else:
            # uploading phase
            u = min(1.0, (e - expected_total * 0.45) / (expected_total * 0.55))
            set_state(**{"naive.filesV1": 0,
                         "naive.filesV2": int(u * NUM_FILES),
                         "naive.busy": "uploading"})
        time.sleep(0.2)
    set_state(**{"naive.filesV1": 0, "naive.filesV2": NUM_FILES,
                 "naive.busy": None})


def op_naive_reset():
    """Restore v1 by naive-publishing v1 again. Wipes any in-flight v2."""
    set_state(**{"naive.busy": "deleting", "naive.filesV2": 0})
    try:
        naive_publish(backend, DATASET_ID, v1_path, sleep=0.0)
        set_state(**{"naive.filesV1": NUM_FILES, "naive.filesV2": 0,
                     "naive.busy": None})
    except Exception:
        traceback.print_exc()
        set_state(**{"naive.busy": None,
                     "last_error": "reset failed — see server logs"})


def op_read():
    """Return what each side sees right now. We try the real reads;
    if naive read fails (which it should, sometimes!), that's the
    whole point of the demo."""
    safe_result: dict[str, Any]
    naive_result: dict[str, Any]

    try:
        tbl = read_current_dataset(backend, DATASET_ID)
        rows = tbl.num_rows
        version = "v2" if rows == STATE["row_counts"]["v2"] else "v1"
        safe_result = {"ok": True, "version": version, "rows": rows}
    except Exception as e:
        safe_result = {"ok": False, "error": str(e)}

    try:
        tbl = naive_read_dataset(backend, DATASET_ID)
        rows = tbl.num_rows
        v1 = STATE["row_counts"]["v1"]
        v2 = STATE["row_counts"]["v2"]
        if rows == v1:
            naive_result = {"ok": True, "version": "v1", "rows": rows}
        elif rows == v2:
            naive_result = {"ok": True, "version": "v2", "rows": rows}
        else:
            naive_result = {"ok": True, "version": "partial", "rows": rows, "of": v2}
    except Exception as e:
        naive_result = {"ok": False, "error": str(e)}

    return safe_result, naive_result


# ── HTTP endpoints ────────────────────────────────────────────────────
@app.route("/api/health")
def health():
    return jsonify({
        "ok": True,
        "backend": "S3",
        "dataset_id": DATASET_ID,
        "v1_path": v1_path,
        "v2_path": v2_path,
    })


@app.route("/api/state")
def get_state():
    with state_lock:
        return jsonify(STATE)


@app.route("/api/safe/stage", methods=["POST"])
def safe_stage():
    if STATE["safe"]["busy"] is not None:
        return jsonify({"ok": False, "error": "already busy"}), 409
    run_in_thread(op_safe_stage)
    return jsonify({"ok": True, "msg": "staging started"})


@app.route("/api/safe/flip", methods=["POST"])
def safe_flip():
    op_safe_flip()
    return jsonify({"ok": True, "msg": "flipped"})


@app.route("/api/safe/rollback", methods=["POST"])
def safe_rollback():
    if STATE["safe"]["busy"] is not None:
        return jsonify({"ok": False, "error": "already busy"}), 409
    run_in_thread(op_safe_rollback)
    return jsonify({"ok": True, "msg": "rollback started"})


@app.route("/api/naive/delete", methods=["POST"])
def naive_delete():
    """In the live demo, delete + upload are a single naive_publish call,
    so we map both buttons to the same operation. The UI animates them
    as separate phases for clarity."""
    if STATE["naive"]["busy"] is not None:
        return jsonify({"ok": False, "error": "already busy"}), 409
    run_in_thread(op_naive_delete_then_upload)
    return jsonify({"ok": True, "msg": "naive publish started"})


@app.route("/api/naive/upload", methods=["POST"])
def naive_upload():
    # No-op when naive_delete already started the combined flow.
    # Returns OK so the UI doesn't error.
    return jsonify({"ok": True, "msg": "upload runs as part of delete (combined)"})


@app.route("/api/naive/reset", methods=["POST"])
def naive_reset():
    if STATE["naive"]["busy"] is not None:
        return jsonify({"ok": False, "error": "already busy"}), 409
    run_in_thread(op_naive_reset)
    return jsonify({"ok": True, "msg": "reset started"})


@app.route("/api/read", methods=["POST"])
def read():
    safe, naive = op_read()
    return jsonify({"safe": safe, "naive": naive})


if __name__ == "__main__":
    # Port 5050 because macOS uses 5000 for AirPlay Receiver.
    # Override with DEMO_PORT env var if you want a different one.
    port = int(os.environ.get("DEMO_PORT", "5050"))
    print("\n" + "=" * 60)
    print(f"  Demo backend up on http://localhost:{port}")
    print(f"  Dataset: {DATASET_ID}  ({v1_path}, {v2_path})")
    print("=" * 60 + "\n")
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)