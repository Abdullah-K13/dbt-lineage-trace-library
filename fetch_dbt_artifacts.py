"""
fetch_dbt_artifacts.py
======================
Downloads manifest.json and catalog.json from the most recent run of your
dbt Cloud job that produced BOTH files.

SETUP
-----
Set these in your .env file (or as environment variables):

    DBT_CLOUD_ACCOUNT_ID   Your numeric account ID  (Settings -> Account)
    DBT_CLOUD_API_TOKEN    A personal or service account token
    DBT_CLOUD_JOB_NAME     Job name to search for, e.g. "dbt partial production"
                           Case-insensitive partial match. Overridden by JOB_ID.
    DBT_CLOUD_JOB_ID       (optional) Exact numeric job ID. Skips name search.
    DBT_CLOUD_HOST         (optional) Default: cloud.getdbt.com
    DBT_ARTIFACTS_DIR      (optional) Where to save files. Default: demo/

.env EXAMPLE
------------
    DBT_CLOUD_ACCOUNT_ID=123456
    DBT_CLOUD_API_TOKEN=dbtc_xxxxxxxxxxxx
    DBT_CLOUD_JOB_NAME=dbt partial production
    DBT_ARTIFACTS_DIR=demo/

USAGE
-----
    pip install requests python-dotenv   # one-time
    python fetch_dbt_artifacts.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is not installed.  Run:  pip install requests")
    sys.exit(1)

# ── Load .env if present ──────────────────────────────────────────────────────
_dotenv_path = Path(__file__).parent / ".env"
if _dotenv_path.exists():
    for line in _dotenv_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

# ── Config ────────────────────────────────────────────────────────────────────
ACCOUNT_ID   = os.environ.get("DBT_CLOUD_ACCOUNT_ID", "").strip()
API_TOKEN    = os.environ.get("DBT_CLOUD_API_TOKEN",  "").strip()
JOB_ID       = os.environ.get("DBT_CLOUD_JOB_ID",    "").strip()
JOB_NAME     = os.environ.get("DBT_CLOUD_JOB_NAME",  "").strip()
HOST         = os.environ.get("DBT_CLOUD_HOST", "cloud.getdbt.com").strip()
OUTPUT_DIR   = Path(os.environ.get("DBT_ARTIFACTS_DIR", "demo")).resolve()

# How many recent successful runs to scan when looking for one with both artifacts.
# Increase this if your job sometimes skips dbt docs generate.
MAX_RUNS_TO_SCAN = 10

LINE = "-" * 68
_AUTH_SCHEME: str | None = None


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _headers(scheme: str) -> dict:
    return {"Authorization": f"{scheme} {API_TOKEN}", "Content-Type": "application/json"}


def _get(path: str, params: dict | None = None) -> dict:
    """GET JSON from dbt Cloud API. Auto-detects Token vs Bearer auth."""
    global _AUTH_SCHEME
    url = f"https://{HOST}/api/v2{path}"
    schemes = [_AUTH_SCHEME] if _AUTH_SCHEME else ["Token", "Bearer"]

    for scheme in schemes:
        r = requests.get(url, headers=_headers(scheme), params=params, timeout=30)
        if r.status_code == 200:
            _AUTH_SCHEME = scheme
            return r.json()
        if r.status_code == 401:
            continue
        if r.status_code == 404:
            print(f"  ERROR: 404 Not Found — {url}")
            sys.exit(1)
        r.raise_for_status()

    print("  ERROR: 401 Unauthorized — both 'Token' and 'Bearer' schemes failed.")
    print(f"  Token prefix : {API_TOKEN[:10]}...  length={len(API_TOKEN)}")
    print(f"  Account ID   : {ACCOUNT_ID}")
    print(f"  URL          : https://{HOST}/api/v2{path}")
    sys.exit(1)


def _get_bytes(path: str) -> bytes:
    """GET raw bytes for artifact download. Returns empty bytes on 404."""
    url = f"https://{HOST}/api/v2{path}"
    r = requests.get(url, headers=_headers(_AUTH_SCHEME or "Token"), timeout=60)
    if r.status_code == 404:
        return b""
    r.raise_for_status()
    return r.content


def _artifact_exists(run_id: int | str, name: str) -> bool:
    """HEAD check — True if the artifact file is present for this run."""
    url = f"https://{HOST}/api/v2/accounts/{ACCOUNT_ID}/runs/{run_id}/artifacts/{name}"
    r = requests.head(url, headers=_headers(_AUTH_SCHEME or "Token"), timeout=15)
    return r.status_code == 200


# ── Step 1: Validate config ───────────────────────────────────────────────────

def validate_config() -> None:
    missing = []
    if not ACCOUNT_ID: missing.append("DBT_CLOUD_ACCOUNT_ID")
    if not API_TOKEN:  missing.append("DBT_CLOUD_API_TOKEN")
    if missing:
        print(f"  ERROR: Missing required env variables: {', '.join(missing)}")
        print()
        print("  Add them to your .env file:")
        print("    DBT_CLOUD_ACCOUNT_ID=123456")
        print("    DBT_CLOUD_API_TOKEN=dbtc_xxxxxxxxxxxx")
        sys.exit(1)


# ── Step 2: Resolve job ID ────────────────────────────────────────────────────

def _list_all_jobs() -> list[dict]:
    data = _get(f"/accounts/{ACCOUNT_ID}/jobs/")
    return data.get("data", [])


def resolve_job_id() -> tuple[str, str]:
    """
    Returns (job_id, job_name).

    Resolution order:
      1. DBT_CLOUD_JOB_ID env var  — use directly, skip name search
      2. DBT_CLOUD_JOB_NAME env var — find job by partial name match
      3. Neither set               — list all jobs, prompt user to pick
    """
    # --- Option 1: explicit ID ---
    if JOB_ID:
        print(f"  Job ID (from env)  : {JOB_ID}")
        # Fetch job name for display
        try:
            data = _get(f"/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/")
            name = data.get("data", {}).get("name", "")
        except Exception:
            name = ""
        if name:
            print(f"  Job name           : {name}")
        return JOB_ID, name

    jobs = _list_all_jobs()
    if not jobs:
        print("  ERROR: No jobs found for this account.")
        sys.exit(1)

    # --- Option 2: match by name ---
    if JOB_NAME:
        query = JOB_NAME.lower()
        matches = [j for j in jobs if query in (j.get("name") or "").lower()]

        if len(matches) == 1:
            jid  = str(matches[0]["id"])
            name = matches[0].get("name", "")
            print(f"  Job name (matched) : {name}")
            print(f"  Job ID             : {jid}")
            print(f"  Tip: set DBT_CLOUD_JOB_ID={jid} to skip name search next time.")
            return jid, name

        if len(matches) > 1:
            print(f"  '{JOB_NAME}' matched {len(matches)} jobs — please pick one:")
            print()
            print(f"  {'#':<4}  {'ID':<10}  Name")
            print(f"  {'-'*4}  {'-'*10}  {'-'*40}")
            for i, j in enumerate(matches):
                print(f"  {i+1:<4}  {j['id']:<10}  {j.get('name','')}")
            print()
            choice = input("  Enter # : ").strip()
            try:
                selected = matches[int(choice) - 1]
            except (ValueError, IndexError):
                print("  ERROR: Invalid selection.")
                sys.exit(1)
            return str(selected["id"]), selected.get("name", "")

        # No match — warn and fall through to interactive list
        print(f"  WARNING: No job found matching '{JOB_NAME}'. Showing all jobs...")

    # --- Option 3: interactive ---
    def _priority(j: dict) -> int:
        n = (j.get("name") or "").lower()
        if "prod" in n: return 0
        if "daily" in n or "nightly" in n: return 1
        return 2

    jobs.sort(key=_priority)
    print()
    print(f"  {'#':<4}  {'ID':<10}  {'Name':<44}  Environment")
    print(f"  {'-'*4}  {'-'*10}  {'-'*44}  -----------")
    for i, j in enumerate(jobs):
        env = j.get("environment", {}).get("name", "") or str(j.get("environment_id", ""))
        print(f"  {i+1:<4}  {j['id']:<10}  {(j.get('name') or '')[:44]:<44}  {env}")

    print()
    choice = input("  Enter # of job to use (default 1): ").strip() or "1"
    try:
        selected = jobs[int(choice) - 1]
    except (ValueError, IndexError):
        print("  ERROR: Invalid selection.")
        sys.exit(1)

    jid  = str(selected["id"])
    name = selected.get("name", "")
    print(f"  Selected: {name} (ID: {jid})")
    print(f"  Tip: add  DBT_CLOUD_JOB_NAME={name}  to your .env to auto-select next time.")
    return jid, name


# ── Step 3: Find latest run that has both artifacts ───────────────────────────

_STATUS = {1: "Queued", 2: "Starting", 3: "Running", 10: "Success", 20: "Error", 30: "Cancelled"}


def get_best_run(job_id: str) -> tuple[dict, bool, bool]:
    """
    Scan the most recent successful runs (up to MAX_RUNS_TO_SCAN) and return
    the latest one that produced manifest.json + catalog.json.

    Falls back to:
      - latest run with only manifest.json   (catalog missing — job may lack docs generate)
      - latest finished run of any status    (last resort)

    Returns (run_dict, has_manifest, has_catalog).
    """
    print(f"\n  Scanning last {MAX_RUNS_TO_SCAN} successful runs for job {job_id}...")

    data = _get(
        f"/accounts/{ACCOUNT_ID}/runs/",
        params={
            "job_definition_id": job_id,
            "status":            10,              # Success only
            "order_by":          "-created_at",   # newest first
            "limit":             MAX_RUNS_TO_SCAN,
            "include_related":   ["trigger"],
        },
    )
    runs = data.get("data", [])

    if not runs:
        # No successful runs — try any finished run
        print("  No successful runs found. Trying most recent finished run...")
        data = _get(
            f"/accounts/{ACCOUNT_ID}/runs/",
            params={
                "job_definition_id": job_id,
                "order_by":          "-created_at",
                "limit":             5,
            },
        )
        runs = [r for r in data.get("data", []) if r.get("finished_at")]
        if not runs:
            print("  ERROR: No finished runs found for this job.")
            sys.exit(1)
        run = runs[0]
        status = _STATUS.get(run.get("status", 0), "Unknown")
        print(f"  WARNING: Using most recent finished run (status: {status}).")
        print(f"  Artifacts may be incomplete.")
        return run, False, False

    # Scan runs newest-first — stop at first one that has both files
    best_manifest_only: dict | None = None

    print(f"  {'Run ID':<12}  {'Started':<22}  manifest  catalog")
    print(f"  {'-'*12}  {'-'*22}  --------  -------")

    for run in runs:
        run_id = run["id"]
        started = run.get("trigger", {}).get("created_at") or run.get("created_at", "")
        started_short = started[:19].replace("T", " ") if started else "unknown"

        has_m = _artifact_exists(run_id, "manifest.json")
        has_c = _artifact_exists(run_id, "catalog.json")

        m_mark = "yes" if has_m else "no"
        c_mark = "yes" if has_c else "no"
        note   = " <-- BEST" if (has_m and has_c) else (" <-- manifest only" if has_m else "")
        print(f"  {run_id:<12}  {started_short:<22}  {m_mark:<8}  {c_mark}{note}")

        if has_m and has_c:
            print()
            return run, True, True

        if has_m and best_manifest_only is None:
            best_manifest_only = run

    # No run had both — use the one with at least manifest
    if best_manifest_only:
        print()
        print("  WARNING: No run found with BOTH files.")
        print("  Using most recent run that has manifest.json.")
        print("  Add 'dbt docs generate --no-compile --select *' as a step in your")
        print("  job to get catalog.json on future runs.")
        return best_manifest_only, True, False

    print()
    print("  ERROR: No successful run produced manifest.json.")
    print("  Make sure your job includes 'dbt compile' or 'dbt run'.")
    sys.exit(1)


# ── Step 4: Download ──────────────────────────────────────────────────────────

def download(run_id: int | str, artifact: str, dest: Path) -> bool:
    """Download one artifact. Returns True if saved successfully."""
    print(f"  Downloading {artifact} ...", end=" ", flush=True)
    content = _get_bytes(f"/accounts/{ACCOUNT_ID}/runs/{run_id}/artifacts/{artifact}")

    if not content:
        print("not found")
        return False

    try:
        json.loads(content)
    except json.JSONDecodeError:
        print("ERROR: response is not valid JSON")
        return False

    dest.write_bytes(content)
    print(f"saved  ({len(content)/1024:.0f} KB)  ->  {dest}")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"\n{LINE}")
    print("  dbt Cloud Artifact Fetcher")
    print(LINE)

    validate_config()

    print(f"  Account    : {ACCOUNT_ID}")
    print(f"  Host       : {HOST}")
    print(f"  Output dir : {OUTPUT_DIR}")

    job_id, job_name = resolve_job_id()

    run, has_manifest, has_catalog = get_best_run(job_id)
    run_id  = run["id"]
    started = run.get("trigger", {}).get("created_at") or run.get("created_at", "unknown")

    print(f"  Using run  : {run_id}  (started: {started[:19].replace('T',' ')})")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\n{LINE}")
    print("  Downloading")
    print(LINE)

    got_manifest = download(run_id, "manifest.json", OUTPUT_DIR / "manifest.json")
    got_catalog  = download(run_id, "catalog.json",  OUTPUT_DIR / "catalog.json")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{LINE}")
    print("  Summary")
    print(LINE)

    if got_manifest:
        print(f"  manifest.json  : {OUTPUT_DIR / 'manifest.json'}")
    else:
        print("  manifest.json  : NOT AVAILABLE")
        print("    -> Make sure your job runs  dbt compile  or  dbt run")

    if got_catalog:
        print(f"  catalog.json   : {OUTPUT_DIR / 'catalog.json'}")
    else:
        print("  catalog.json   : NOT AVAILABLE")
        print("    -> Add this step to your job:")
        print("         dbt docs generate --no-compile --select '*'")
        print("    -> Without it, SELECT * models won't have column lineage")

    print(LINE)

    if got_manifest:
        print()
        print("  Run lineage analysis:")
        print(f"    from dbt_lineage import LineageGraph")
        print(f"    g = LineageGraph(\"{OUTPUT_DIR / 'manifest.json'}\")")
        print(f"    result = g.trace(\"your_model\", \"your_column\")")
    print()


if __name__ == "__main__":
    main()
