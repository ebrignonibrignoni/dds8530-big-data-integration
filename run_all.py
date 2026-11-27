"""End-to-end lab: sources -> ingest -> clean -> Spark -> integrate -> stream -> visualize."""
import json
import subprocess
import sys
import time

from bdi import clean, ingest, integrate, spark_jobs, streaming, visualize
from bdi.config import RESULTS, get_logger

log = get_logger("run_all")


def main():
    t0, timings, summary = time.perf_counter(), {}, {}

    def step(name, fn):
        t = time.perf_counter()
        out = fn()
        timings[name] = round(time.perf_counter() - t, 2)
        return out

    step("build_sources", lambda: subprocess.run([sys.executable, "scripts/build_sources.py"], check=True))
    summary["ingest"] = step("ingest", ingest.ingest)
    summary["clean"] = step("clean", clean.clean)
    summary["spark"] = step("spark", spark_jobs.run)
    summary["schema_matching"] = step("schema_matching", integrate.schema_matching)
    summary["entity_resolution"] = step("entity_resolution", integrate.entity_resolution)
    summary["streaming"] = step("streaming", streaming.run)
    timings["total"] = round(time.perf_counter() - t0, 2)
    summary["timings_seconds"] = timings
    (RESULTS / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    visualize.run()
    log.info("Done in %.1f s. Results: results/summary.json, figures/*.png", timings["total"])


if __name__ == "__main__":
    main()
