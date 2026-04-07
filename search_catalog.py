import json

manifest = json.load(open("demo/manifest.json"))
catalog  = json.load(open("demo/catalog.json"))

SEARCHES = [
    "rep_user_feedback",
]

# Column to hunt for across all related models
HUNT_COLUMN = "id_user"

manifest_nodes = {**manifest.get("nodes", {}), **manifest.get("sources", {})}
catalog_nodes  = {**catalog.get("nodes",  {}), **catalog.get("sources",  {})}

# ── Catalog metadata ──────────────────────────────────────────────────────────
cat_meta = catalog.get("metadata", {})
print(f"Catalog generated : {cat_meta.get('generated_at', 'unknown')}")
print(f"Total catalog nodes: {len(catalog_nodes)}\n")

# ── Per-model check ───────────────────────────────────────────────────────────
print(f"{'Model':<40} {'Manifest':>10} {'Catalog':>10} {'Cat cols':>10}")
print("-" * 74)

for search in SEARCHES:
    in_manifest = any(
        search.lower() in uid.lower() or search.lower() == node.get("name", "").lower()
        for uid, node in manifest_nodes.items()
    )
    cat_match = next(
        (
            (uid, node)
            for uid, node in catalog_nodes.items()
            if search.lower() in uid.lower()
            or search.lower() == node.get("metadata", {}).get("name", "").lower()
        ),
        None,
    )
    in_catalog = cat_match is not None
    col_count  = len(cat_match[1].get("columns", {})) if cat_match else 0

    manifest_str = "YES" if in_manifest else "MISSING"
    catalog_str  = "YES" if in_catalog  else "MISSING"
    print(f"  {search:<38} {manifest_str:>10} {catalog_str:>10} {col_count:>10}")

    # ── Detailed column dump ──────────────────────────────────────────────────
    print()

    # Manifest: compiled SQL + documented columns
    m_node = next(
        (n for uid, n in manifest_nodes.items()
         if search.lower() == n.get("name", "").lower()),
        None,
    )
    if m_node:
        compiled = m_node.get("compiled_code") or m_node.get("compiled_sql") or ""
        has_jinja = "{{" in compiled or "{%" in compiled
        print(f"  Manifest node found:")
        print(f"    unique_id    : {m_node.get('unique_id', 'n/a')}")
        print(f"    resource_type: {m_node.get('resource_type', 'n/a')}")
        print(f"    compiled SQL : {len(compiled)} chars  jinja={has_jinja}")
        doc_cols = list(m_node.get("columns", {}).keys())
        print(f"    documented columns ({len(doc_cols)}): {doc_cols}")
        if HUNT_COLUMN.lower() in [c.lower() for c in doc_cols]:
            print(f"    -> '{HUNT_COLUMN}' IS in manifest documented columns")
        else:
            print(f"    -> '{HUNT_COLUMN}' NOT in manifest documented columns")

        # Check compiled SQL
        if HUNT_COLUMN.lower() in compiled.lower():
            print(f"    -> '{HUNT_COLUMN}' appears in compiled SQL")
        else:
            print(f"    -> '{HUNT_COLUMN}' NOT found in compiled SQL")
    else:
        print(f"  Manifest node: NOT FOUND for '{search}'")

    print()

    # Catalog: physical columns
    if cat_match:
        cat_uid, cat_node = cat_match
        cat_cols = {k.lower(): v for k, v in cat_node.get("columns", {}).items()}
        print(f"  Catalog node found:")
        print(f"    unique_id : {cat_uid}")
        all_cols = sorted(cat_cols.keys())
        print(f"    columns ({len(all_cols)}): {all_cols}")
        if HUNT_COLUMN.lower() in cat_cols:
            print(f"    -> '{HUNT_COLUMN}' IS in catalog columns")
        else:
            print(f"    -> '{HUNT_COLUMN}' NOT in catalog columns")
    else:
        print(f"  Catalog node: NOT FOUND for '{search}'")
