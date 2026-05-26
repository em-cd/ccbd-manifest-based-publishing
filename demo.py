"""
A Tale of Two Publishing Methods
---------------------------------

A demo of manifest-based vs naive overwrite publishing, staged as a
Regency-era drawing-room drama. All threading and publishing logic is
preserved from the original demo; only the presentation layer has changed.

Requires: pip install rich
"""

import argparse
import os
import threading
import time

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.live import Live
    from rich.text import Text
    from rich.align import Align
    from rich.box import DOUBLE, ROUNDED
except ImportError:
    raise SystemExit(
        "This demo requires the 'rich' library. "
        "Install it with:  pip install rich"
    )

from backend.azure_backend import AzureBackend
from backend.s3_backend import S3Backend
from dataset_gen import generate_dataset
from publish import publish, naive_publish
from read import read_current_dataset, naive_read_dataset
from demo_viz import generate_demo_viz


# ─── Palette (matched to demo_viz.py) ──────────────────────────────────
GOLD       = "#C9A84C"
SAGE       = "#5C7A4E"
ROSE       = "#C4485A"
SEPIA      = "#8B6914"
DUSTY_BLUE = "#4A6FA5"
INK        = "#2C1810"

DATA_DIR = "./data"

BACKEND_MAP = {
    "s3": S3Backend,
    "azure": AzureBackend,
}

# ─── Dramatis Personae ─────────────────────────────────────────────────
# Each reader's sleep-time maps to a character who enquires at that hour.
READERS = {
    0.0: ("Mrs. Bennet",     "ever vigilant for news of the neighbourhood"),
    0.2: ("Miss Elizabeth",  "sharp-witted, the first to enquire"),
    1.2: ("Miss Jane",       "all gentle curiosity"),
    2.0: ("Miss Mary",       "consulting the accounts most diligently"),
    3.8: ("Lady Catherine",  "demanding her due in the final reckoning"),
}

console = Console(highlight=False)
table_lock = threading.Lock()


# ─── Aesthetic helpers ─────────────────────────────────────────────────

def typewriter(text, style="", delay=0.022, end="\n"):
    """Stream text char-by-char with natural pauses at punctuation."""
    for ch in text:
        console.print(ch, style=style, end="", soft_wrap=True)
        if ch in ".!?":
            time.sleep(delay * 6)
        elif ch in ",;:":
            time.sleep(delay * 3)
        else:
            time.sleep(delay)
    if end:
        console.print(end, end="")


def ornament():
    """Decorative divider between scenes."""
    console.print()
    console.print(Align.center(Text("❦  ·  ❦  ·  ❦", style=GOLD)))
    console.print()
    time.sleep(0.6)


def chapter_card(roman, title, subtitle):
    """Gold-bordered chapter opener."""
    body = Text(justify="center")
    body.append("\n")
    body.append(f"CHAPTER {roman}\n", style=f"bold {GOLD}")
    body.append("\n")
    body.append(f"{title}\n", style=f"bold italic {INK}")
    body.append("\n")
    body.append(subtitle, style=f"italic {SEPIA}")
    body.append("\n")
    panel = Panel(
        body,
        box=DOUBLE,
        border_style=GOLD,
        padding=(1, 6),
        width=72,
    )
    console.print()
    console.print(Align.center(panel))
    console.print()
    time.sleep(1.6)


def stage_direction(text):
    """Italic sepia line, like a play's stage direction."""
    console.print()
    typewriter(f"  ❦  {text}", style=f"italic {SEPIA}", delay=0.018)
    console.print()


def speaker(name, line, color=INK):
    """A character speaks. Name is printed boldly, then the line types out."""
    console.print(Text(f"  {name}: ", style=f"bold {color}"), end="")
    typewriter(f"\u201c{line}\u201d", style=f"italic {INK}", delay=0.020)


def narrator(text):
    """Omniscient narrator voice — sepia italic."""
    typewriter(text, style=f"italic {SEPIA}", delay=0.022)


def wait_for_curtain(prompt="Press Enter to continue"):
    """Interactive pause — lets the presenter pace the demo live."""
    console.print()
    console.print(Align.center(Text(f"  ⟪ {prompt} ⟫  ", style=f"dim {SEPIA}")))
    try:
        input()
    except EOFError:
        time.sleep(2)


def make_ledger(act_title, accent):
    """A rich.Table styled to match the parchment aesthetic."""
    table = Table(
        title=Text(act_title, style=f"bold {accent}"),
        title_justify="center",
        border_style=GOLD,
        box=ROUNDED,
        expand=False,
        padding=(0, 1),
        show_lines=False,
    )
    table.add_column("Hour",         style=SEPIA, justify="center", width=8)
    table.add_column("The Enquirer", style=INK,                     width=18)
    table.add_column("Outcome",      justify="center",              width=22)
    table.add_column("Verdict",      style=f"italic {SEPIA}",       width=44)
    return table


# ─── The demo proper ───────────────────────────────────────────────────

def run_publish_demo(backend):
    dataset_id = "demo"
    result = {"safe_rows": [], "naive_rows": []}
    state  = {"v1_rows": None, "v2_rows": None}

    # ── Silent worker functions (original logic, preserved) ───────────
    def writer(local_path, safe=True, version=None, sleep=0.5):
        if safe:
            publish(backend, dataset_id, local_path, version, sleep=sleep)
        else:
            naive_publish(backend, dataset_id, local_path, sleep=sleep)

    def do_read(safe):
        if safe:
            return read_current_dataset(backend, dataset_id)
        return naive_read_dataset(backend, dataset_id)

    def reader_into_ledger(ledger, safe, sleep_time):
        """The original `reader`, but updates the live Rich ledger
        instead of printing logs."""
        time.sleep(sleep_time)
        name, _ = READERS.get(
            sleep_time,
            (f"A Stranger ({sleep_time}s)", "")
        )

        try:
            tbl = do_read(safe)
            rows = tbl.num_rows
            failed, err = False, None
        except Exception as e:
            rows, failed, err = 0, True, str(e)

        v1, v2 = state["v1_rows"], state["v2_rows"]

        if failed:
            outcome = Text("✗  CALAMITY", style=f"bold {ROSE}")
            verdict = "the dataset has vanished — most distressing"
        elif v1 and rows == v1:
            outcome = Text(f"✓  {rows:,} (v1)", style=f"bold {SAGE}")
            verdict = "the previous edition, intact and proper"
        elif v2 and rows == v2:
            outcome = Text(f"✓  {rows:,} (v2)", style=f"bold {DUSTY_BLUE}")
            verdict = "the new edition, atomically arrived"
        elif v2 and 0 < rows < v2:
            outcome = Text(f"⚠  {rows:,} of {v2:,}", style=f"bold {ROSE}")
            verdict = "an incomplete account — silent corruption"
        else:
            outcome = Text(f"?  {rows:,}", style=f"bold {SEPIA}")
            verdict = "an outcome of uncertain provenance"

        with table_lock:
            ledger.add_row(f"t={sleep_time:.1f}s", name, outcome, verdict)
            if safe:
                result["safe_rows"].append(rows)
            else:
                result["naive_rows"].append(rows)

    # ─── PROLOGUE ─────────────────────────────────────────────────────
    console.clear()
    console.print()
    console.print(Align.center(Text(
        "A TALE OF TWO PUBLISHING METHODS",
        style=f"bold italic {INK}"
    )))
    console.print(Align.center(Text(
        "In which manifests, like good manners, prevent considerable embarrassment",
        style=f"italic {SEPIA}"
    )))
    console.print()
    console.print(Align.center(Text(
        f"— A Demonstration upon the {backend.__class__.__name__} Estate —",
        style=GOLD
    )))
    console.print()
    ornament()

    narrator("It is a truth universally acknowledged, that a pipeline in possession ")
    narrator("of good data must be in want of a manifest. Tonight, we shall observe ")
    narrator("two gentlemen at their work, and three ladies who shall enquire most ")
    narrator("inopportunely…")
    console.print()

    # Ensure datasets exist on disk
    for version in ["v1", "v2"]:
        size_for_gen = f"{dataset_id}/{version}"
        local_path = f"{DATA_DIR}/{size_for_gen}"
        if not (os.path.exists(local_path) and len(os.listdir(local_path)) > 0):
            stage_direction(f"the staff prepares manuscript {version}…")
            generate_dataset(size_for_gen)

    v1_local_path = f"{DATA_DIR}/{dataset_id}/v1"
    v2_local_path = f"{DATA_DIR}/{dataset_id}/v2"

    # Initial publish of v1 to BOTH regimes (silent setup)
    stage_direction("the morning post arrives. Volume the First is set into both libraries…")
    writer(v1_local_path, safe=True, version="v1")
    writer(v1_local_path, safe=False)

    # Establish ground truth row counts
    state["v1_rows"] = do_read(True).num_rows
    try:
        import pyarrow.dataset as pads
        state["v2_rows"] = pads.dataset(v2_local_path, format="parquet").count_rows()
    except Exception:
        state["v2_rows"] = None  # best-effort; verdicts will degrade gracefully

    console.print()
    if state["v2_rows"]:
        narrator(
            f"Volume the First contains {state['v1_rows']:,} entries; "
            f"Volume the Second, awaiting publication, contains {state['v2_rows']:,}."
        )
    else:
        narrator(f"Volume the First contains {state['v1_rows']:,} entries.")
    console.print()

    wait_for_curtain("Press Enter to raise the curtain on Act I")

    # ─── ACT I — THE MANIFESTED GENTLEMAN ────────────────────────────
    chapter_card(
        "THE FIRST",
        "The Manifested Gentleman",
        "“I shall not be precipitate — the manifest ensures consistency.”"
    )

    speaker(
        "Mr. Darcy",
        "I shall validate the manuscript ere any announcement is made.",
        color=SAGE
    )
    stage_direction("he stages Volume the Second, validates its contents, and waits…")
    console.print()
    narrator("Three ladies of the household choose this moment to enquire after the library:")
    console.print()
    time.sleep(0.6)

    safe_ledger = make_ledger("Act I  •  The Drawing-Room Ledger", SAGE)
    safe_threads = [
        threading.Thread(target=writer, args=(v2_local_path, True, "v2", 0.8)),
        threading.Thread(target=reader_into_ledger, args=(safe_ledger, True, 0.2)),
        threading.Thread(target=reader_into_ledger, args=(safe_ledger, True, 1.2)),
        threading.Thread(target=reader_into_ledger, args=(safe_ledger, True, 2.0)),
    ]

    with Live(safe_ledger, console=console, refresh_per_second=10,
              vertical_overflow="visible"):
        # Baseline read at t=0 — Mrs. Bennet checks before anything happens
        reader_into_ledger(safe_ledger, True, 0.0)
        for t in safe_threads:
            t.start()
        for t in safe_threads:
            t.join()
        # Final stable read — Lady Catherine demands the new edition
        reader_into_ledger(safe_ledger, True, 3.8)

    console.print()
    speaker(
        "Mr. Darcy",
        "The manifest is updated. No reader has glimpsed an incomplete page.",
        color=SAGE
    )
    narrator("In every enquiry, a complete volume was returned. Not one error. Not one partial.")
    ornament()

    wait_for_curtain("Press Enter to behold the alternative…")

    # ─── ACT II — THE IMPETUOUS OVERWRITER ───────────────────────────
    chapter_card(
        "THE SECOND",
        "The Impetuous Overwriter",
        "“He deleted first and uploaded later — a most imprudent arrangement.”"
    )

    speaker(
        "Mr. Wickham",
        "I see no need to wait, nor to inform anyone! I shall overwrite at once.",
        color=ROSE
    )
    stage_direction(
        "he hurls Volume the First into the fire, page by page, "
        "whilst feeding in Volume the Second…"
    )
    console.print()
    narrator("The same three ladies, alas, choose this very moment to enquire:")
    console.print()
    time.sleep(0.6)

    naive_ledger = make_ledger("Act II  •  The Scandal Ledger", ROSE)
    naive_threads = [
        threading.Thread(target=writer, args=(v2_local_path, False, "v2", 0.8)),
        threading.Thread(target=reader_into_ledger, args=(naive_ledger, False, 0.2)),
        threading.Thread(target=reader_into_ledger, args=(naive_ledger, False, 1.2)),
        threading.Thread(target=reader_into_ledger, args=(naive_ledger, False, 2.0)),
    ]

    with Live(naive_ledger, console=console, refresh_per_second=10,
              vertical_overflow="visible"):
        reader_into_ledger(naive_ledger, False, 0.0)
        for t in naive_threads:
            t.start()
        for t in naive_threads:
            t.join()
        reader_into_ledger(naive_ledger, False, 3.8)

    console.print()
    speaker(
        "Mr. Wickham",
        "I cannot conceive what all the fuss is about.",
        color=ROSE
    )
    ornament()

    # ─── THE LEDGER OF CONSEQUENCES (incremental reveal) ─────────────
    wait_for_curtain("Press Enter for the reckoning")

    console.print()
    console.print(Align.center(Text(
        "  THE LEDGER OF CONSEQUENCES  ",
        style=f"bold {GOLD} on {INK}"
    )))
    console.print()
    time.sleep(0.5)

    damages = 0
    incidents = []
    # Iterate over the readers that ran during Act II (skipping baseline at index 0)
    for sleep_time, rows in zip([0.2, 1.2, 2.0, 3.8], result["naive_rows"][1:]):
        name, _ = READERS[sleep_time]
        v2 = state["v2_rows"]
        if rows == 0:
            kind = "a failed read"
            cost = 300
        elif v2 and rows < v2:
            kind = f"a partial read ({rows:,} of {v2:,})"
            cost = 300
        else:
            continue
        damages += cost
        incidents.append((sleep_time, name, kind, cost, damages))

    if not incidents:
        narrator(
            "By improbable fortune, no calamity was recorded. "
            "Mr. Wickham’s luck shall not hold forever."
        )
    else:
        for sleep_time, name, kind, cost, running in incidents:
            line = (
                f"  t={sleep_time:.1f}s   "
                f"{name:<16}  {kind:<36}  "
                f"£{cost:>3}   →   running total £{running:,}"
            )
            typewriter(line, style=ROSE, delay=0.010)
            time.sleep(0.25)
        console.print()
        console.print(Align.center(Text("─" * 70, style=f"dim {GOLD}")))
        console.print()
        scale = damages * 100
        typewriter(
            f"  At production scale (100× daily):   "
            f"£{scale:,} per day in wasted engineering time",
            style=f"bold {ROSE}",
            delay=0.018
        )
        console.print()

    ornament()

    # ─── FINALE ──────────────────────────────────────────────────────
    narrator("And now, gentle reader, the curtain falls. The full accounting awaits…")
    console.print()
    stage_direction("rendering the final visualisation…")

    # Strip the baseline row from result before handing to the viz, so the
    # 4-entry default in demo_viz.py still aligns with what we produced.
    viz_result = {
        "safe_rows":  result["safe_rows"][1:]  if len(result["safe_rows"])  > 4 else result["safe_rows"],
        "naive_rows": result["naive_rows"][1:] if len(result["naive_rows"]) > 4 else result["naive_rows"],
    }
    generate_demo_viz(viz_result)

    console.print()
    console.print(Align.center(Panel(
        Text("END.", style=f"bold italic {GOLD}", justify="center"),
        box=DOUBLE,
        border_style=GOLD,
        padding=(1, 8),
        width=20,
    )))
    console.print()
    console.print(Align.center(Text(
        "“It is a truth universally acknowledged, that a pipeline in possession",
        style=f"italic {SEPIA}"
    )))
    console.print(Align.center(Text(
        " of good data must be in want of a manifest.”",
        style=f"italic {SEPIA}"
    )))
    console.print()


def main(backend_name):
    backend_cls = BACKEND_MAP[backend_name]
    backend = backend_cls()
    run_publish_demo(backend)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=["s3", "azure"], required=True)
    args = parser.parse_args()
    main(args.backend)