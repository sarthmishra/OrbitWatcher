# backend/scheduler.py
# Orchestrates the full data pipeline on a 6-hour cycle.
#
# Order every cycle:
#   1. Fetch fresh TLEs from CelesTrak
#   2. Propagate + screen conjunctions using k-d tree
#   3. Generate maneuver suggestions
#
# The API never triggers this — it only reads results.

import os
import sys
import time
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import SCHEDULER_INTERVAL_HOURS
from logger import get_logger
from backend.services.tle_fetcher import fetch_and_store
from backend.services.conjunction import run_conjunction_screening
from backend.services.optimizer import generate_maneuvers

log = get_logger(__name__)


def run_pipeline():
    """
    Full data pipeline — runs every SCHEDULER_INTERVAL_HOURS.
    Each step is wrapped in try/except so one failure
    does not stop the rest of the pipeline.
    """
    log.info("=" * 50)
    log.info("Pipeline started.")

    # Step 1 — Fetch TLEs
    try:
        sat_count = fetch_and_store()
        log.info(f"Step 1 complete: {sat_count} satellites stored.")
    except Exception as e:
        log.error(f"Step 1 failed (TLE fetch): {e}")
        log.warning("Continuing with existing TLE data.")

    # Step 2+3 — Propagate + screen conjunctions
    try:
        conj_count = run_conjunction_screening()
        log.info(f"Step 2+3 complete: {conj_count} conjunctions stored.")
    except Exception as e:
        log.error(f"Step 2+3 failed (conjunction screening): {e}")

    # Step 4 — Generate maneuvers
    try:
        man_count = generate_maneuvers()
        log.info(f"Step 4 complete: {man_count} maneuvers generated.")
    except Exception as e:
        log.error(f"Step 4 failed (optimizer): {e}")

    log.info("Pipeline complete.")
    log.info("=" * 50)


def start_scheduler():
    """
    Starts the background scheduler.
    Repeats every SCHEDULER_INTERVAL_HOURS automatically.
    """
    scheduler = BackgroundScheduler()

    scheduler.add_job(
        func=run_pipeline,
        trigger=IntervalTrigger(hours=SCHEDULER_INTERVAL_HOURS),
        id="main_pipeline",
        name="OrbitWatch main pipeline",
        replace_existing=True
    )

    scheduler.start()
    log.info(f"Scheduler started — pipeline runs every {SCHEDULER_INTERVAL_HOURS} hours.")
    return scheduler


def run_pipeline_timed():
    """
    Measures each pipeline stage independently with correct isolation.

    Key fixes over previous version:
      - TLE network time is separated from parse/store time
      - Propagation is called only ONCE (previous version called it twice)
      - n_satellites reflects what is in the database, not just what was fetched
      - All timings exclude each other — no overlap

    Run manually for paper data. Never called by the scheduler.

    Usage:
      python -c "import sys; sys.path.insert(0,'.'); 
                 from backend.scheduler import run_pipeline_timed; 
                 run_pipeline_timed()"

    Output:
      Prints formatted table to terminal.
      Saves pipeline_timing.json to project root.
    """
    from backend.services.tle_fetcher import fetch_tles_from_celestrak, store_tles
    from backend.services.propagator import get_all_satellite_positions
    from backend.services.conjunction import screen_conjunctions, store_conjunctions
    from datetime import datetime, timezone

    timings = {}
    results = {}

    # ── Stage 1a: TLE network fetch ────────────────────────
    # This is a NETWORK operation — not your system's performance.
    # Reported separately so reviewers can distinguish.
    log.info("Timing Stage 1a: TLE network fetch...")
    t0 = time.perf_counter()
    satellites = fetch_tles_from_celestrak()
    timings["tle_network_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    results["n_fetched"] = len(satellites)
    log.info(f"Stage 1a done: {len(satellites)} TLEs fetched in {timings['tle_network_ms']} ms")

    # ── Stage 1b: TLE parse and database write ─────────────
    # This IS your system's performance — parsing and storing.
    log.info("Timing Stage 1b: TLE parse + store...")
    t0 = time.perf_counter()
    store_tles(satellites)
    timings["tle_store_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    log.info(f"Stage 1b done in {timings['tle_store_ms']} ms")

    # ── Stage 2: SGP4 propagation ──────────────────────────
    # Propagates every satellite to current UTC using python-sgp4.
    # Called ONCE here — positions passed directly to Stage 3.
    # (Previous version caused double propagation — now fixed.)
    log.info("Timing Stage 2: SGP4 propagation...")
    dt = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    positions = get_all_satellite_positions(dt)
    timings["sgp4_propagation_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    results["n_propagated"] = len(positions)
    log.info(f"Stage 2 done: {len(positions)} satellites in {timings['sgp4_propagation_ms']} ms")

    # ── Stage 3: Conjunction screening ────────────────────
    # Uses positions already computed above — no re-propagation.
    # Includes: altitude shell filter + k-d tree + risk scoring + DB write.
    log.info("Timing Stage 3: Conjunction screening...")
    t0 = time.perf_counter()
    conjunctions = screen_conjunctions(positions, dt)
    store_conjunctions(conjunctions)
    timings["screening_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    results["n_conjunctions"] = len(conjunctions)
    log.info(f"Stage 3 done: {len(conjunctions)} conjunctions in {timings['screening_ms']} ms")

    # ── Stage 4: Maneuver generation ──────────────────────
    # Reads conjunctions from DB, computes delta-V, writes maneuvers.
    log.info("Timing Stage 4: Maneuver generation...")
    t0 = time.perf_counter()
    generate_maneuvers()
    timings["maneuver_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    log.info(f"Stage 4 done in {timings['maneuver_ms']} ms")

    # ── Totals ─────────────────────────────────────────────
    # Total excluding network: what YOUR SYSTEM does
    # Total including network: full wall-clock time
    timings["total_excl_network_ms"] = round(
        timings["tle_store_ms"] +
        timings["sgp4_propagation_ms"] +
        timings["screening_ms"] +
        timings["maneuver_ms"],
        1
    )
    timings["total_incl_network_ms"] = round(
        timings["tle_network_ms"] +
        timings["total_excl_network_ms"],
        1
    )

    # ── Save and print ─────────────────────────────────────
    output = {**results, **timings}

    with open("pipeline_timing.json", "w") as f:
        json.dump(output, f, indent=2)

    print("\n" + "=" * 56)
    print("  ORBITWATCH PIPELINE TIMING — TABLE 3 DATA")
    print("=" * 56)
    print(f"  Satellites fetched:       {results.get('n_fetched', 0):>8,}")
    print(f"  Satellites propagated:    {results.get('n_propagated', 0):>8,}")
    print(f"  Conjunctions detected:    {results.get('n_conjunctions', 0):>8,}")
    print("-" * 56)
    print(f"  TLE network fetch:        {timings['tle_network_ms']:>8.1f} ms  ← network, excluded")
    print(f"  TLE parse + DB write:     {timings['tle_store_ms']:>8.1f} ms")
    print(f"  SGP4 propagation:         {timings['sgp4_propagation_ms']:>8.1f} ms")
    print(f"  Conjunction screening:    {timings['screening_ms']:>8.1f} ms")
    print(f"  Maneuver generation:      {timings['maneuver_ms']:>8.1f} ms")
    print("-" * 56)
    print(f"  TOTAL (excl. network):    {timings['total_excl_network_ms']:>8.1f} ms  ← paper result")
    print(f"  TOTAL (incl. network):    {timings['total_incl_network_ms']:>8.1f} ms")
    print("=" * 56)
    print("  Saved: pipeline_timing.json")
    print()

    return output


if __name__ == "__main__":
    log.info("Running pipeline once for testing...")
    run_pipeline()
    print("\nPipeline test complete.")