"""
demo_server.py
==============
Tiny Flask app used by demo.py to broadcast live state to a browser tab.

Shared state lives in this module. The demo (writer/reader threads in demo.py)
mutates it. A background "state monitor" thread polls real S3 every 300ms so
the file counts and manifest version reflect actual bucket state.

The HTML page polls `/api/state` every 250ms and re-renders.
"""

import copy
import os
import threading
import time

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS


# ── Flask app ─────────────────────────────────────────────────────────
_app = Flask(__name__)
CORS(_app)


# ── Shared state ──────────────────────────────────────────────────────
_lock = threading.Lock()

_state = {
    "safe": {
        "staging_v2_files": 0,
        "active_version": None,         # 'v1' / 'v2' / None
        "writer_busy": None,            # None, 'staging', 'flipping', 'publishing'
        "writer_msg": "",
    },
    "naive": {
        "curated_files": 0,
        "version_hint": None,           # which version we expect curated/ to hold
        "writer_busy": None,            # None, 'deleting', 'uploading'
        "writer_msg": "",
    },
    "act": "idle",                      # 'idle', 'setup', 'safe', 'naive', 'done'
    "baseline_v1": None,
    "baseline_v2": None,
    "reads": {"safe": [], "naive": []},
    "events": [],                       # log line ticker
    "done": False,
    "backend_name": "",
    "bucket": "",
    "dataset_id": "demo",
}

# Configured by demo.py at startup. The monitor thread creates its OWN
# backend instance via this factory, so it doesn't share a boto3 client
# with the demo's main thread (which would risk segfaults on macOS).
_backend_factory = None
_monitor_running = False


# ── Public API for demo.py ────────────────────────────────────────────
def configure(backend_factory, backend_name: str, dataset_id: str = "demo"):
    """backend_factory: a callable (usually the backend CLASS) that returns
    a fresh backend instance. Called once now to read the bucket name, then
    again in the monitor thread to give it a dedicated instance."""
    global _backend_factory
    _backend_factory = backend_factory
    # Get bucket name from a throwaway instance (or reuse main one — doesn't matter)
    tmp = backend_factory()
    with _lock:
        _state["backend_name"] = backend_name
        _state["bucket"] = tmp.get_root()
        _state["dataset_id"] = dataset_id


def set_act(act: str):
    """Top-level act: 'setup' / 'safe' / 'naive' / 'done'."""
    with _lock:
        _state["act"] = act
        if act == "done":
            _state["done"] = True


def set_baselines(v1: int, v2: int):
    with _lock:
        _state["baseline_v1"] = v1
        _state["baseline_v2"] = v2


def set_writer(side: str, busy: str = None, msg: str = ""):
    """Update writer status badge for one side."""
    with _lock:
        _state[side]["writer_busy"] = busy
        _state[side]["writer_msg"] = msg


def set_version_hint(side: str, hint: str):
    """Tell the naive side which version we just published (so the file
    list can be coloured accordingly)."""
    with _lock:
        _state[side]["version_hint"] = hint


def add_read(side: str, record: dict):
    """Append a reader's enquiry to the running list."""
    with _lock:
        _state["reads"][side].append(record)
        # Also as a log event for the ticker
        _state["events"].append({
            "kind": "read",
            "side": side,
            "reader": record.get("reader", "?"),
            "time": record.get("time", 0),
            "ok": record.get("ok", False),
            "rows": record.get("rows"),
            "error": record.get("error"),
        })


def log(msg: str):
    """Add a status line to the event ticker."""
    with _lock:
        _state["events"].append({"kind": "log", "msg": msg})


def snapshot() -> dict:
    with _lock:
        return copy.deepcopy(_state)


_view_html = None
_view_html_path = None


def _find_view_html():
    """Search for demo_view.html in likely places. Sets module globals."""
    global _view_html, _view_html_path
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "demo_view.html"),
        os.path.join(os.getcwd(), "demo_view.html"),
        os.path.join(here, "..", "demo_view.html"),
        "demo_view.html",
    ]
    for path in candidates:
        path = os.path.abspath(path)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                _view_html = f.read()
            _view_html_path = path
            return path

    print("⚠  Could not find demo_view.html. Looked in:")
    for c in candidates:
        print(f"     {os.path.abspath(c)}")
    _view_html = ("<!doctype html><html><head><title>missing</title></head>"
                  "<body style='font-family:serif;padding:60px;background:#F5ECD7'>"
                  "<h1>demo_view.html not found</h1>"
                  "<p>Put it in the same directory as <code>demo_server.py</code>.</p>"
                  "</body></html>")
    return None


# ── HTTP endpoints ────────────────────────────────────────────────────
@_app.route("/api/state")
def api_state():
    return jsonify(snapshot())


@_app.route("/api/health")
def api_health():
    """Cheap connectivity check."""
    return jsonify({
        "ok": True,
        "view_html_loaded": _view_html is not None,
        "view_html_path": _view_html_path,
    })


@_app.route("/")
def index():
    # Serve the HTML we loaded at startup (in-memory, no path issues)
    return _view_html or "demo_view.html not loaded", 200, {"Content-Type": "text/html; charset=utf-8"}


# ── State monitor: polls real S3 to keep state in sync ────────────────
def _monitor_loop(dataset_id: str, poll_interval: float = 0.3):
    """Background thread: queries the backend every 300ms and updates file
    counts + active manifest version. Uses a DEDICATED backend instance to
    avoid sharing a boto3 client with the demo's main thread (which has
    caused segfaults on macOS Apple Silicon)."""
    global _monitor_running

    if _backend_factory is None:
        return

    # Each monitor thread gets its own backend instance → its own boto3 client
    try:
        mon_backend = _backend_factory()
    except Exception as e:
        print(f"⚠ Monitor failed to create backend: {e}")
        return

    while _monitor_running:
        try:
            staging_v2_keys, _ = mon_backend.list_objects(f"staging/{dataset_id}/v2/")
            curated_keys,    _ = mon_backend.list_objects(f"curated/{dataset_id}/")
            manifest = mon_backend.read_json(f"published/{dataset_id}/latest.json")
            active = manifest.get("version") if manifest else None

            with _lock:
                _state["safe"]["staging_v2_files"] = len(staging_v2_keys)
                _state["safe"]["active_version"]   = active
                _state["naive"]["curated_files"]   = len(curated_keys)
        except Exception:
            # transient — try again next tick
            pass
        time.sleep(poll_interval)


def start_monitor(dataset_id: str = "demo"):
    global _monitor_running
    if _monitor_running:
        return
    _monitor_running = True
    t = threading.Thread(target=_monitor_loop, args=(dataset_id,), daemon=True)
    t.start()


def stop_monitor():
    global _monitor_running
    _monitor_running = False


# ── Flask runner ──────────────────────────────────────────────────────
def run_server(port: int = 5050):
    """Blocking. Call from a daemon thread in demo.py."""
    # Resolve where demo_view.html lives once, at startup
    path = _find_view_html()
    if path:
        print(f"  serving demo_view.html from: {path}")
    # Keep werkzeug logs visible — we want to see binding errors.
    import logging
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    try:
        _app.run(host="127.0.0.1", port=port, debug=False, threaded=True,
                 use_reloader=False)
    except Exception as e:
        # Show the actual binding error so the user knows what's wrong.
        print(f"\n❌ FLASK CRASHED ON STARTUP: {type(e).__name__}: {e}")
        print(f"   (port {port} may already be in use — try: lsof -i :{port})\n")
        raise