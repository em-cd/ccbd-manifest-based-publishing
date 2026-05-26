# How to Run the Demo

A Regency-themed, browser-based demo of manifest-based dataset versioning.
Two modes (Cinematic + Presenter), plus a benchmarks tab. Optionally connects
to a live Flask backend to run real S3 operations.

## What's in here

```
ccbd-manifest-based-publishing/
├── demo_webapp.html      ← the demo (open this in your browser)
├── demo.py               ← the terminal demo (alternative)
├── demo_viz.py
├── publish.py, read.py, dataset_gen.py
├── backend/
└── server/
    ├── app.py            ← optional Flask backend for LIVE mode
    └── README.md         ← more technical details
```

---

## Quick start (simulated mode — easiest)

Just open `demo_webapp.html` in your browser. No setup, no server, no Python.

You'll see a grey **SIMULATED** badge in the top right corner. Everything works,
the data is fake but it tells the story perfectly. This is the safest mode for
a live presentation.

**Easier way (recommended):** serve it via a tiny HTTP server so the browser
doesn't choke on `file://` URLs. From the repo root:

```bash
python3 -m http.server 8000
```

Then visit:

```
http://localhost:8000/demo_webapp.html
```

(or `http://localhost:8000/server/demo_webapp.html` if it lives in `server/`)

---

## Live mode (real S3 calls)

If you want the Presenter mode buttons to actually hit S3 instead of running
a simulation, also start the Flask backend.

### Prerequisites

- Python 3.10+
- AWS credentials configured (`aws configure` or `AWS_PROFILE`)
- A demo dataset already on disk at `./data/demo/v1` and `./data/demo/v2`
  (generate them via `dataset_gen` if needed)
- The Python deps:

```bash
pip install flask flask-cors
```

### Run

You need **two terminals**.

**Terminal 1 — start the backend (from the repo root):**

```bash
python3 server/app.py
```

You should see:

```
============================================================
  Demo backend up on http://localhost:5050
  Dataset: demo  (./data/demo/v1, ./data/demo/v2)
============================================================
```

(Port 5050, not 5000 — macOS uses 5000 for AirPlay Receiver.)

**Terminal 2 — serve the webapp:**

```bash
python3 -m http.server 8000
```

**Browser:**

```
http://localhost:8000/demo_webapp.html
```

The badge in the top-right should turn **red and pulse**: `LIVE · S3`.
That means the webapp found your Flask server and will route Presenter mode
button clicks to real S3 operations.

---

## The three modes

Once it's open, three tabs at the top:

### 1. CINEMATIC

A timed play with three acts. Press **▶ Begin the Drama**:

- **Act I** (Darcy solo): the right library disappears, Darcy stages v2,
  validates, flips the manifest. Bennet sisters enquire — all clean reads.
- **Intermission**: full-screen chapter card announces Act II.
- **Act II** (Wickham solo): the left library disappears, Wickham deletes v1
  and chaotically uploads v2. Same sisters enquire and get CALAMITY / partial
  reads. Damages pile up.
- **Finale**: both libraries return side-by-side, the Ledger of Consequences
  reveals.

Use the **PACE slider** to control speed (0.25× – 2×, default 0.6×). Drag the
main scrubber to jump to any moment.

### 2. PRESENTER

Manual buttons for the live demo. The control panel sticks to the top of
your viewport so you don't have to scroll.

- **Mr. Darcy's column** (sage): Stage v2 (`S`), Flip Manifest (`F`), Rollback (`R`)
- **A Lady Enquires** (middle): keys `1`–`4` for Elizabeth / Jane / Mary / Lady Catherine
- **Mr. Wickham's column** (rose): Delete v1 (`D`), Upload v2 (`U`), Reset (`X`)

Keyboard shortcuts work — you can run the whole demo without looking at the
buttons. The damages counter pulses every time a naive read fails.

In **LIVE mode**, every button click hits the Flask backend → real S3 calls.
In **SIMULATED mode**, animations run locally.

### 3. BENCHMARKS

Five charts built from your 329 benchmark runs:

1. Publishing overhead (2.7% – 4.8% of upload cost)
2. Transfer throughput, S3 vs Azure
3. Warm-up effect across runs
4. Selectivity vs scan time (the predicate-pushdown finding)
5. Scan performance, S3 vs Azure

Plus a three-card recommendations panel at the bottom.

---

## Suggested presentation flow

1. **Open in CINEMATIC** at default speed → hit Play. ~20 seconds of staged drama.
2. **Switch to PRESENTER** → run the race-condition manually. Hit `D` to start
   Wickham's overwrite, then immediately mash `1`, `2`, `3` while files are
   missing to capture CALAMITY and partial reads on stage.
3. **Switch to BENCHMARKS** → walk through the five findings + recommendations.
4. **Switch back to CINEMATIC** → let the final ledger be the closer.

Total: ~5 minutes, fully interactive.

---

## Troubleshooting

### Badge stays SIMULATED, but I started Flask

- Hard-refresh the browser: **Cmd + Shift + R** (Mac) — clears cached HTML.
- Confirm Flask is still running. Visit `http://localhost:5050/api/health`
  directly. You should see JSON. If you get "can't connect", Flask died —
  restart it.
- Check the HTML really points at 5050: `grep API_BASE demo_webapp.html`
  should show `5050`, not `5000`.

### Flask crashes immediately

- Most common: `ModuleNotFoundError: No module named 'backend'`. Run from the
  **repo root**, not from inside `server/`:
  ```bash
  python3 server/app.py
  ```
- Or: missing `flask-cors` / AWS creds. See `server/README.md`.

### Browser can't load Google Fonts / Chart.js

The webapp loads fonts and Chart.js from CDN. If you're offline, the layout
still works but loses the fancy typography and the benchmark charts won't
render. Everything else functions normally.

### Port 5050 already in use

```bash
DEMO_PORT=5060 python3 server/app.py
```

Then also update the webapp: open `demo_webapp.html`, find `const API_BASE`,
change `5050` to `5060`.

---

## Running the terminal demo instead

If you'd rather skip the browser entirely:

```bash
pip install rich
python3 demo.py --backend s3
```

Same story (Darcy vs Wickham, the Bennet sisters, chapter cards) but rendered
as styled terminal output with typewriter narration and a live ledger table.
Saves a final PNG visualization at the end.