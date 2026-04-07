"""
Targeted column-lineage trace for fct_petpay_snapshot
======================================================
Run:  python demo/trace_petpay.py

Two modes:
  1. DIRECT MODE  — calls analyze_model_columns() on the raw SQL below
                    (Jinja refs replaced with short table names).
                    Works without manifest.json. Good for quick iteration.

  2. GRAPH MODE   — loads the full LineageGraph for fct_petpay_snapshot
                    and calls g.trace() / g.impact() on each column.
                    Requires manifest.json + catalog.json in demo/.
"""

from __future__ import annotations

import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("demo")

# ── Config ─────────────────────────────────────────────────────────────────────
MANIFEST_PATH = "demo/manifest.json"
CATALOG_PATH  = "demo/catalog.json"
MODEL_NAME    = "fct_petpay_snapshot"     # change if actual dbt model name differs
DIALECT       = "bigquery"

# Columns to trace (covers all interesting transform types in the SQL)
TRACE_COLUMNS = [
    "cat_petpay_status",            # COALESCE from two CTEs → two source tables
    "amt_petpay_balance",           # passthrough through two CTE hops
    "tm_petpay_kyc_pending",        # MIN(CASE WHEN ...) aggregation
    "tm_petpay_payment_completed",  # another MIN(CASE WHEN ...)
    "cat_account_group",            # straight passthrough from seller_classification
    "id_petpay",                    # simple passthrough through all CTEs
]

LINE = "-" * 70

# ── The SQL (Jinja refs replaced with short table names for direct analysis) ───
# The final SELECT is SELECT * FROM joined_with_seller_classification.
# We rewrite it as explicit columns so analyze_model_columns can resolve them.

COMPILED_SQL = """
WITH petpay_hist AS (
    SELECT
        *,
        first_value(cat_petpay_status) OVER (
            PARTITION BY id_market, id_listing, id_chat_channel, id_petpay
            ORDER BY tm_created DESC
        ) AS cat_petpay_status_latest
    FROM fct_petpay_states
),

pet_payment_latest_status AS (
    SELECT id_petpay, cat_petpay_status
    FROM stg_owp__pet_payment
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_petpay
        ORDER BY tm_updated DESC
    ) = 1
),

seller_classification AS (
    SELECT
        id_date,
        id_market,
        id_user,
        cat_account_age_group,
        cat_account_group,
        cat_account_group_highest,
        tm_valid_from,
        tm_valid_to
    FROM dim_user_seller_classification
    WHERE id_date >= 20220101
),

acm_snap AS (
    SELECT
        id_market,
        id_listing,
        id_listing_details,
        id_user_seller,
        id_user_buyer,
        id_chat_channel,
        id_petpay,

        cat_petpay_status_latest AS cat_petpay_status,
        petpay_initiated_by,

        key_external_id,
        amt_petpay_balance,
        amt_petpay_deposit,
        amt_petpay_fee,
        amt_petpay_fee_incl_vat,

        tm_petpay_requested,

        MIN(CASE WHEN cat_petpay_status = 'AwaitingKycVerification'      THEN tm_created ELSE NULL END) AS tm_petpay_kyc_pending,
        MIN(CASE WHEN cat_petpay_status = 'KycVerificationPassed'        THEN tm_created ELSE NULL END) AS tm_petpay_kyc_passed,
        MIN(CASE WHEN cat_petpay_status = 'Accepted'                     THEN tm_created ELSE NULL END) AS tm_petpay_accepted,

        tm_petpay_created,

        MIN(CASE WHEN cat_petpay_action = 'TermsAccepted'                THEN tm_created ELSE NULL END) AS tm_petpay_terms_accepted,
        MIN(CASE WHEN cat_petpay_action = 'PaymentInitiated'             THEN tm_created ELSE NULL END) AS tm_petpay_payment_initiated,
        MIN(CASE WHEN cat_petpay_status = 'PaymentCompleted'             THEN tm_created ELSE NULL END) AS tm_petpay_payment_completed,
        MIN(CASE WHEN cat_petpay_status = 'DepositPayoutCompleted'       THEN tm_created ELSE NULL END) AS tm_petpay_deposit_completed,
        MIN(CASE WHEN cat_petpay_status = 'PendingBuyerConfirmation'     THEN tm_created ELSE NULL END) AS tm_petpay_confirmation_pending,
        MIN(CASE WHEN cat_petpay_status = 'BalancePayoutRequested'       THEN tm_created ELSE NULL END) AS tm_petpay_balance_requested,
        MIN(CASE WHEN cat_petpay_status = 'Completed'                    THEN tm_created ELSE NULL END) AS tm_petpay_completed,
        MIN(CASE WHEN cat_petpay_status = 'BalancePayoutCompleted'       THEN tm_created ELSE NULL END) AS tm_petpay_balance_completed,
        MIN(CASE WHEN cat_petpay_status = 'BalanceOnlyPaymentCompleted'  THEN tm_created ELSE NULL END) AS tm_petpay_balance_only_payment_completed,
        MIN(CASE WHEN cat_petpay_status = 'FullPaymentCompleted'         THEN tm_created ELSE NULL END) AS tm_petpay_full_payment_completed,
        MIN(CASE WHEN cat_petpay_status = 'DepositFirstPaymentCompleted' THEN tm_created ELSE NULL END) AS tm_petpay_deposit_first_payment_completed,
        MIN(CASE WHEN cat_petpay_status = 'FullPaymentDepositPayoutFailure'    THEN tm_created ELSE NULL END) AS tm_petpay_full_payment_deposit_payout_failure,
        MIN(CASE WHEN cat_petpay_status = 'FullPaymentDepositPayoutRequested'  THEN tm_created ELSE NULL END) AS tm_petpay_full_payment_deposit_payout_requested,
        MIN(CASE WHEN cat_petpay_status = 'DepositFirstDepositPayoutRequested' THEN tm_created ELSE NULL END) AS tm_petpay_deposit_first_deposit_payout_requested,
        MIN(CASE WHEN cat_petpay_status = 'DepositFirstDepositPayoutFailure'   THEN tm_created ELSE NULL END) AS tm_petpay_deposit_first_deposit_payout_failure,
        MIN(CASE WHEN cat_petpay_status = 'FullPaymentDepositPayoutCompleted'  THEN tm_created ELSE NULL END) AS tm_petpay_full_payment_deposit_payout_completed,
        MIN(CASE WHEN cat_petpay_status = 'DepositFirstDepositPayoutCompleted' THEN tm_created ELSE NULL END) AS tm_petpay_deposit_first_deposit_payout_completed,
        MIN(CASE WHEN cat_petpay_status = 'DepositFirstBalancePaymentPending'  THEN tm_created ELSE NULL END) AS tm_petpay_deposit_first_balance_payment_pending,
        MIN(CASE WHEN cat_petpay_status = 'DepositFirstBalancePaymentCompleted' THEN tm_created ELSE NULL END) AS tm_petpay_deposit_first_balance_payment_completed

    FROM petpay_hist
    GROUP BY
        id_market, id_chat_channel, id_petpay, id_listing, id_listing_details,
        id_user_seller, id_user_buyer, cat_petpay_status_latest, petpay_initiated_by,
        key_external_id, amt_petpay_balance, amt_petpay_deposit, amt_petpay_fee,
        amt_petpay_fee_incl_vat, tm_petpay_requested, tm_petpay_created
),

joined_with_seller_classification AS (
    SELECT
        acm_snap.id_market,
        acm_snap.id_listing,
        acm_snap.id_listing_details,
        acm_snap.id_user_seller,
        acm_snap.id_user_buyer,
        acm_snap.id_chat_channel,
        acm_snap.id_petpay,
        acm_snap.petpay_initiated_by,
        acm_snap.key_external_id,
        acm_snap.amt_petpay_balance,
        acm_snap.amt_petpay_deposit,
        acm_snap.amt_petpay_fee,
        acm_snap.amt_petpay_fee_incl_vat,
        acm_snap.tm_petpay_requested,
        acm_snap.tm_petpay_kyc_pending,
        acm_snap.tm_petpay_kyc_passed,
        acm_snap.tm_petpay_accepted,
        acm_snap.tm_petpay_created,
        acm_snap.tm_petpay_terms_accepted,
        acm_snap.tm_petpay_payment_initiated,
        acm_snap.tm_petpay_payment_completed,
        acm_snap.tm_petpay_deposit_completed,
        acm_snap.tm_petpay_confirmation_pending,
        acm_snap.tm_petpay_balance_requested,
        acm_snap.tm_petpay_completed,
        acm_snap.tm_petpay_balance_completed,
        acm_snap.tm_petpay_balance_only_payment_completed,
        acm_snap.tm_petpay_full_payment_completed,
        acm_snap.tm_petpay_deposit_first_payment_completed,
        acm_snap.tm_petpay_full_payment_deposit_payout_failure,
        acm_snap.tm_petpay_full_payment_deposit_payout_requested,
        acm_snap.tm_petpay_deposit_first_deposit_payout_requested,
        acm_snap.tm_petpay_deposit_first_deposit_payout_failure,
        acm_snap.tm_petpay_full_payment_deposit_payout_completed,
        acm_snap.tm_petpay_deposit_first_deposit_payout_completed,
        acm_snap.tm_petpay_deposit_first_balance_payment_pending,
        acm_snap.tm_petpay_deposit_first_balance_payment_completed,

        COALESCE(pp_ls.cat_petpay_status, acm_snap.cat_petpay_status) AS cat_petpay_status,
        seller_classification.cat_account_group

    FROM acm_snap
    LEFT JOIN seller_classification
        ON  acm_snap.id_market     = seller_classification.id_market
        AND acm_snap.id_user_seller = seller_classification.id_user
        AND EXTRACT(date FROM acm_snap.tm_petpay_requested) >= EXTRACT(date FROM seller_classification.tm_valid_from)
        AND EXTRACT(date FROM acm_snap.tm_petpay_requested) <  EXTRACT(date FROM seller_classification.tm_valid_to)
    LEFT JOIN pet_payment_latest_status pp_ls
        ON pp_ls.id_petpay = acm_snap.id_petpay
)

SELECT * FROM joined_with_seller_classification
"""

# Minimal schema so the library can resolve the source tables.
# Matches the columns actually referenced in the SQL above.
SCHEMA: dict[str, dict[str, str]] = {
    "fct_petpay_states": {
        "id_market": "INT64",
        "id_listing": "INT64",
        "id_listing_details": "INT64",
        "id_user_seller": "INT64",
        "id_user_buyer": "INT64",
        "id_chat_channel": "INT64",
        "id_petpay": "INT64",
        "cat_petpay_status": "STRING",
        "cat_petpay_action": "STRING",
        "petpay_initiated_by": "STRING",
        "key_external_id": "STRING",
        "amt_petpay_balance": "NUMERIC",
        "amt_petpay_deposit": "NUMERIC",
        "amt_petpay_fee": "NUMERIC",
        "amt_petpay_fee_incl_vat": "NUMERIC",
        "tm_petpay_requested": "TIMESTAMP",
        "tm_petpay_created": "TIMESTAMP",
        "tm_created": "TIMESTAMP",
    },
    "stg_owp__pet_payment": {
        "id_petpay": "INT64",
        "cat_petpay_status": "STRING",
        "tm_updated": "TIMESTAMP",
    },
    "dim_user_seller_classification": {
        "id_date": "INT64",
        "id_market": "INT64",
        "id_user": "INT64",
        "cat_account_age_group": "STRING",
        "cat_account_group": "STRING",
        "cat_account_group_highest": "STRING",
        "tm_valid_from": "TIMESTAMP",
        "tm_valid_to": "TIMESTAMP",
    },
}


# ── Mode 1: Direct SQL analysis ────────────────────────────────────────────────

def run_direct_mode() -> None:
    """Analyze the SQL directly without loading manifest.json."""
    from dbt_lineage.sql_analyzer import analyze_model_columns

    print(f"\n{LINE}")
    print("  DIRECT MODE — analyze_model_columns() on the SQL above")
    print(LINE)

    result = analyze_model_columns(
        compiled_sql=COMPILED_SQL,
        model_name=MODEL_NAME,
        schema=SCHEMA,
        dialect=DIALECT,
    )

    print(f"\n  Total edges found : {len(result.edges)}")
    print(f"  Columns traced    : {result.columns_traced}")
    print(f"  Columns attempted : {result.columns_attempted}")

    # Group edges by target column for easy reading
    by_col: dict[str, list] = {}
    for e in result.edges:
        by_col.setdefault(e.target.column, []).append(e)

    print(f"\n  All output columns with lineage ({len(by_col)}):")
    for col in sorted(by_col):
        srcs = [f"{e.source.model}.{e.source.column}" for e in by_col[col]]
        types = {e.transform_type for e in by_col[col]}
        print(f"    {col:<50}  <- {', '.join(srcs[:3])}{'...' if len(srcs)>3 else ''}  [{', '.join(str(t) for t in types)}]")

    print(f"\n{LINE}")
    print("  COLUMN DEEP-DIVES")
    print(LINE)

    for col in TRACE_COLUMNS:
        edges = by_col.get(col)
        print(f"\n  Column: {col}")
        if not edges:
            print("    (no lineage found — column may come from SELECT * expansion)")
            continue
        for e in edges:
            print(f"    {e.source.model}.{e.source.column:<35}  [{e.transform_type}]")
            if e.transform_sql and e.transform_sql.strip().lower() != col.lower():
                # Truncate long SQL expressions for readability
                sql_preview = e.transform_sql.replace("\n", " ").strip()
                print(f"      SQL: {sql_preview[:100]}{'...' if len(sql_preview)>100 else ''}")


# ── Mode 2: Full graph trace via LineageGraph ──────────────────────────────────

def run_graph_mode() -> None:
    """Load the graph from manifest + catalog and call g.trace() per column."""
    from pathlib import Path
    from dbt_lineage import LineageGraph

    manifest = Path(MANIFEST_PATH)
    if not manifest.exists():
        print(f"\n  GRAPH MODE skipped — {MANIFEST_PATH} not found.")
        return

    print(f"\n{LINE}")
    print(f"  GRAPH MODE — LineageGraph.trace() for {MODEL_NAME}")
    print(LINE)

    catalog = Path(CATALOG_PATH) if Path(CATALOG_PATH).exists() else None
    LineageGraph.clear_cache()
    g = LineageGraph(
        manifest,
        catalog_path=catalog,
        select=MODEL_NAME,
        dialect=DIALECT,
    )

    available = g.all_columns(MODEL_NAME)
    print(f"\n  Columns in graph for {MODEL_NAME}: {len(available)}")
    if available:
        print(f"  First 10: {available[:10]}")

    for col in TRACE_COLUMNS:
        if col not in available:
            print(f"\n  {col}: not in graph (may need catalog.json for SELECT * expansion)")
            continue

        print(f"\n{LINE}")
        print(f"  TRACE: {MODEL_NAME}.{col}")
        result = g.trace(MODEL_NAME, col)
        print(f"  Root source(s): {[str(c) for c in result.source_columns]}")
        if result.edges:
            print(f"  Hops ({len(result.edges)} total):")
            for e in result.edges[:10]:
                print(f"    {e.source.model}.{e.source.column:<40} -> {e.target.model}.{e.target.column:<40} [{e.transform_type}]")
            if len(result.edges) > 10:
                print(f"    ... and {len(result.edges) - 10} more hops")

        # Impact: what downstream columns would break if this source changed?
        impact = g.impact(MODEL_NAME, col)
        if impact.affected_columns:
            print(f"  Downstream impact: {len(impact.affected_columns)} column(s) in {len(impact.affected_models)} model(s)")


if __name__ == "__main__":
    run_direct_mode()
    run_graph_mode()
