"""
cap_vs_sku_reconciler.py
==========================
Reconciles temp.cap_prediction_temp against temp.sku_prediction_temp for one
iteration_id, on a per-SHELF (iterationtranid) basis — not just per image
name.

Business rule (as specified)
-----------------------------
For every (iteration_id, iterationtranid) shelf:

    * sku_cnt is temp.sku_prediction_temp's row count for that shelf's
      image, EXCLUDING non-product classes (see "SKU count exclusions"
      below) — not the raw row count.

    * If sku_cnt does NOT exceed cap_cnt by at least SKU_CAP_EXCESS_THRESHOLD
      (70% by default, i.e. sku_cnt < cap_cnt * 1.70) -> do nothing. The
      cap-derived rows already sitting in orgi.coolermetricstransaction
      (written by cap_pipeline_runner.py's Steps 4/5) are trusted as-is.

    * If sku_cnt >= cap_cnt * (1 + SKU_CAP_EXCESS_THRESHOLD) -> the cap
      model under-counted relative to the SKU model for that shelf by more
      than the threshold. DELETE the existing orgi.coolermetricstransaction
      rows for that iterationtranid and INSERT fresh rows built directly
      from temp.sku_prediction_temp instead (the INSERT itself still uses
      every temp.sku_prediction_temp row for that image, exclusions apply
      only to the sku_cnt used for this comparison — see below).
      orgi.coolermetricsmaster is only inserted if the (iterationid,
      iterationtranid) row is somehow missing — never overwritten
      otherwise.

SKU count exclusions ("ignore Shelf detections and others")
-------------------------------------------------------------
When computing sku_cnt for the threshold comparison, rows whose
prod_class_id is in EXCLUDED_SKU_CLASS_IDS_FROM_COUNT are not counted:

    * 33 — Shelf-detection (not a product at all)
    * 28-32 — Other Brand Glass / Other CAN / Other PET / Other TPK /
      Other Water Bottle (unidentified-brand "Other *" classes)

These ids are in the SKU model's own raw class-id space (0-41), the same
space CLASS_ID_REMAP_CTE below documents by name. This exclusion affects
ONLY the threshold comparison (fetch_sku_image_counts()) — it does not
change what reconcile_shelf() inserts; a qualifying shelf still gets every
temp.sku_prediction_temp row for that image inserted, exclusions and all.

Why iterationtranid, not just image_file_name
------------------------------------------------
orgi.coolermetricsmaster / orgi.coolermetricstransaction are keyed on
(iterationid, iterationtranid) — cap_pipeline_runner.py's Steps 4/5 assign
that iterationtranid via:

    DENSE_RANK() OVER (
        PARTITION BY iteration_id
        ORDER BY store_id, image_file_name, s3path_annotated_file
    )

computed over temp.cap_prediction_temp. Grouping the count comparison by
image_file_name alone (an earlier version of this script did that) is not
quite right: if temp.cap_prediction_temp holds more than one distinct
s3path_annotated_file for the same (store_id, image_file_name) — e.g. the
image was re-processed and re-annotated within the same iteration — DENSE_RANK
assigns those rows to DIFFERENT iterationtranid values even though they share
a filename. Comparing/deleting by image name alone would then either miss one
of those shelves or delete the wrong one.

This version reproduces that exact DENSE_RANK in Python (assign_iterationtranids
below) so the tranid values it works with are IDENTICAL to what's already in
orgi.coolermetricsmaster/coolermetricstransaction, then does the count
comparison AND the delete+insert per resolved iterationtranid:

    * Unambiguous case (the common one): a (store_id, image_file_name) maps
      to exactly one iterationtranid -> compared and, if needed, fixed
      directly against that tranid.
    * Ambiguous case: a (store_id, image_file_name) maps to MORE THAN ONE
      iterationtranid (multiple distinct s3path_annotated_file values in
      cap_prediction_temp). temp.sku_prediction_temp has no
      s3path_annotated_file to disambiguate which shelf a given sku row
      belongs to, so this script does NOT guess — it reports these as
      "ambiguous, needs manual review" and leaves them untouched rather
      than risking a delete against the wrong shelf.
    * Orphan case: an image has sku_prediction_temp rows but ZERO rows in
      cap_prediction_temp for this iteration at all (so it never got a
      tranid from the DENSE_RANK above). Falls back to reading an existing
      tranid off orgi.coolermetricstransaction.imagefilename if one is
      already there, otherwise mints a brand-new iterationtranid and
      creates the master row for it.

Known caveat
------------
temp.cap_prediction_temp row counts include the "no cap detected"
(cap_class_id=10010) and "nothing detected at all" (cap_class_id=10028)
sentinel rows that visicooler.py / cap_pipeline_runner.py insert so every
image is trackable. Those sentinels count as 1 row each toward cap_cnt, same
as a real detection — this script compares RAW row counts exactly as
specified. Filter cap_class_id NOT IN (10010, 10028) in
fetch_cap_tranid_groups() below if you want sentinel-free comparison instead.

Class-ID remap on insert
-------------------------
temp.sku_prediction_temp.prod_class_id is stored in the SKU YOLO model's OWN
raw class-id space (0-41) — visicooler.py inserts `cls_id` straight from the
model's box output, with no translation. This is different from
temp.cap_prediction_temp.prod_class_id, which cap_sku_mapper.py already
remaps into the catalog/orgi.productmaster id space (via
_NEW_TO_OLD_CLASS_MAP / _remap_to_old_class_ids) before ever writing a row.
orgi.coolermetricstransaction.productclassid is expected to be in that same
catalog space (it's what Step 5 of cap_pipeline_runner.py writes from the
already-remapped cap rows).

So when this script builds transaction rows straight from
temp.sku_prediction_temp, it has to apply the identical remap
cap_sku_mapper.py uses, or productclassid ends up wrong for every reconciled
shelf. CLASS_ID_REMAP_CTE below is that same table (values copied from
cap_sku_mapper.py's _NEW_TO_OLD_CLASS_MAP), and reconcile_shelf() applies it
with the same rules _remap_to_old_class_ids() does: ids >= 9000 (unresolved-
fallback classes) and ids with no entry in the table are kept as-is; ids
found in the table are replaced with the mapped catalog id; NULL passes
through unchanged.

Safe to re-run: every run re-evaluates every shelf fresh, and DELETE+INSERT
is naturally idempotent for a given temp.sku_prediction_temp snapshot.

Usage:
    python cap_vs_sku_reconciler.py 260
    python cap_vs_sku_reconciler.py 260 --dry-run
"""

import sys
import argparse
import logging
from collections import defaultdict

import psycopg2

from app.config_loader import load_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Threshold gate: sku_cnt must exceed cap_cnt by at least this fraction
# before a shelf is reconciled (caps cleared, SKU rows inserted). 0.70 means
# sku_cnt must be at least 70% higher than cap_cnt, i.e.
# sku_cnt >= cap_cnt * (1 + SKU_CAP_EXCESS_THRESHOLD) == cap_cnt * 1.70.
# For orphan images (cap_cnt implicitly 0, see resolve_orphan_tranid()) this
# reduces to sku_cnt >= 0, so any sku_cnt > 0 still qualifies as before.
# ─────────────────────────────────────────────────────────────────────────────
SKU_CAP_EXCESS_THRESHOLD = 0.70


def sku_exceeds_cap_threshold(sku_cnt: int, cap_cnt: int) -> bool:
    """True if sku_cnt exceeds cap_cnt by at least SKU_CAP_EXCESS_THRESHOLD."""
    return sku_cnt >= cap_cnt * (1 + SKU_CAP_EXCESS_THRESHOLD)


# ─────────────────────────────────────────────────────────────────────────────
# SKU-model raw class ids (0-41 space) excluded from sku_cnt when it's used
# for the threshold comparison above — "ignore Shelf detections and others
# (the 'Other *' unidentified-brand classes) from SKU for counting in total
# detections". Names match the comments in CLASS_ID_REMAP_CTE below. This
# does NOT affect what reconcile_shelf() inserts — only the count used to
# decide whether a shelf qualifies for reconciliation.
# ─────────────────────────────────────────────────────────────────────────────
EXCLUDED_SKU_CLASS_IDS_FROM_COUNT = {
    28,  # Other Brand Glass
    29,  # Other CAN
    30,  # Other PET
    31,  # Other TPK
    32,  # Other Water Bottle
    33,  # Shelf-detection
}


# ─────────────────────────────────────────────────────────────────────────────
# SKU-model raw class id -> catalog (orgi.productmaster) productclassid.
# Copied verbatim from cap_sku_mapper.py's _NEW_TO_OLD_CLASS_MAP (values
# unchanged; column names per the mapping as given). Any id NOT in this
# table, or >= 9000 (unresolved-fallback classes), is left as-is by the CASE
# expression in reconcile_shelf() below — see _remap_to_old_class_ids() in
# cap_sku_mapper.py for the same rule set applied to cap rows.
# ─────────────────────────────────────────────────────────────────────────────
CLASS_ID_REMAP_CTE = """
    class_map (old_class_id, new_class_id) AS (
        VALUES
            (0,  25),  -- Alcohol Bottle
            (1,  0),   -- Coca-Cola 1000ml PET
            (2,  1),   -- Coca-Cola 1500ml PET
            (3,  4),   -- Coca-Cola 175ml Glass
            (4,  2),   -- Coca-Cola 2000ml PET
            (5,  3),   -- Coca-Cola 2250ml PET
            (6,  4),   -- Coca-Cola 250ml Glass
            (7,  5),   -- Coca-Cola 250ml PET
            (8,  6),   -- Coca-Cola 500ml PET
            (9,  36),  -- Coke Zero 250ml PET
            (10, 37),  -- Coke Zero 500ml PET
            (11, 15),  -- Fanta 175ml Glass
            (12, 7),   -- Fanta Lemon 1000ml
            (13, 8),   -- Fanta Lemon 2000ml PET
            (14, 9),   -- Fanta Lemon 2250ml PET
            (15, 10),  -- Fanta Lemon 250ml
            (16, 11),  -- Fanta Orange 1000ml PET
            (17, 12),  -- Fanta Orange 1500ml PET
            (18, 13),  -- Fanta Orange 2000ml PET
            (19, 14),  -- Fanta Orange 2250ml PET
            (20, 15),  -- Fanta Orange 250ml Glass
            (21, 16),  -- Fanta Orange 250ml PET
            (22, 17),  -- Fanta Orange 500ml PET
            (23, 18),  -- Kinley Soda 250ml Glass
            (24, 19),  -- Kinley Soda 250ml PET
            (25, 20),  -- Kinley Soda 500ml PET
            (26, 21),  -- Kinley Water 1000ml PET
            (27, 22),  -- Kinley Water 500ml PET
            (28, 25),  -- Other Brand Glass
            (29, 23),  -- Other CAN
            (30, 24),  -- Other PET
            (31, 26),  -- Other TPK
            (32, 27),  -- Other Water Bottle
            (33, 28),  -- Shelf-detection
            (34, 29),  -- Sprite 1000ml PET
            (35, 30),  -- Sprite 1500ml PET
            (36, 34),  -- Sprite 175ml Glass
            (37, 31),  -- Sprite 2000ml PET
            (38, 32),  -- Sprite 2250ml PET
            (39, 33),  -- Sprite 250ml Glass
            (40, 34),  -- Sprite 250ml PET
            (41, 35)   -- Sprite 500ml PET
    )
"""


# ─────────────────────────────────────────────────────────────────────────────
# iterationtranid derivation — mirrors cap_pipeline_runner.py Steps 4/5
# ─────────────────────────────────────────────────────────────────────────────

def fetch_cap_tranid_groups(cur, iteration_id: int) -> list:
    """
    One row per distinct (store_id, image_file_name, s3path_annotated_file)
    combo in temp.cap_prediction_temp for this iteration, with its row
    count. This is exactly the grain cap_pipeline_runner.py Steps 4/5
    DENSE_RANK over — see assign_iterationtranids().
    """
    cur.execute("""
        SELECT store_id, image_file_name, s3path_annotated_file, COUNT(*) AS cap_cnt
        FROM temp.cap_prediction_temp
        WHERE iteration_id = %s
        GROUP BY store_id, image_file_name, s3path_annotated_file
    """, (iteration_id,))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _s3_sort_key(s3path):
    """Postgres ORDER BY ... ASC default: NULLS LAST."""
    return (1, '') if s3path is None else (0, s3path)


def assign_iterationtranids(cap_groups: list) -> list:
    """
    Reproduces cap_pipeline_runner.py Steps 4/5's exact:

        DENSE_RANK() OVER (
            PARTITION BY iteration_id
            ORDER BY store_id, image_file_name, s3path_annotated_file
        )

    in Python, so the 'iterationtranid' this function stamps onto each
    group is identical to what's already sitting in
    orgi.coolermetricsmaster / orgi.coolermetricstransaction for this
    iteration. Mutates and returns cap_groups (list of dicts) with
    'iterationtranid' added, ordered by rank.
    """
    ordered = sorted(
        cap_groups,
        key=lambda g: (g['store_id'], g['image_file_name'], _s3_sort_key(g['s3path_annotated_file'])),
    )
    tranid = 0
    prev_key = None
    for g in ordered:
        key = (g['store_id'], g['image_file_name'], g['s3path_annotated_file'])
        if key != prev_key:
            tranid += 1
            prev_key = key
        g['iterationtranid'] = tranid
    return ordered


def fetch_sku_image_counts(cur, iteration_id: int) -> dict:
    """
    Returns {(store_id, image_file_name): sku_cnt} for this iteration.

    sku_cnt EXCLUDES rows whose prod_class_id is in
    EXCLUDED_SKU_CLASS_IDS_FROM_COUNT (Shelf-detection and the "Other *"
    unidentified-brand classes) — this is the count used for the
    SKU_CAP_EXCESS_THRESHOLD comparison, not a raw row count.
    """
    cur.execute("""
        SELECT store_id, image_file_name, COUNT(*) AS sku_cnt
        FROM temp.sku_prediction_temp
        WHERE iteration_id = %s
          AND (prod_class_id IS NULL OR prod_class_id != ALL(%s))
        GROUP BY store_id, image_file_name
    """, (iteration_id, list(EXCLUDED_SKU_CLASS_IDS_FROM_COUNT)))
    return {(store_id, image_file_name): sku_cnt for store_id, image_file_name, sku_cnt in cur.fetchall()}


def build_shelf_comparison(cur, iteration_id: int) -> dict:
    """
    Ties it all together. Returns:
        {
            'clean':     [ {store_id, image_file_name, iterationtranid, cap_cnt, sku_cnt}, ... ],
            'ambiguous': [ {store_id, image_file_name, iterationtranids: [...]}, ... ],
            'orphans':   [ {store_id, image_file_name, sku_cnt}, ... ],
        }

    'clean'      — (store_id, image_file_name) maps to exactly one
                   iterationtranid; cap_cnt/sku_cnt (sku_cnt already
                   excludes EXCLUDED_SKU_CLASS_IDS_FROM_COUNT) compared
                   against SKU_CAP_EXCESS_THRESHOLD for that shelf.
    'ambiguous'  — maps to more than one iterationtranid (multiple
                   s3path_annotated_file values) — reported, not touched.
    'orphans'    — sku_prediction_temp has rows for this image but
                   cap_prediction_temp has none at all for this iteration,
                   so it never got a tranid from the DENSE_RANK above.
    """
    cap_groups = assign_iterationtranids(fetch_cap_tranid_groups(cur, iteration_id))
    sku_counts = fetch_sku_image_counts(cur, iteration_id)

    tranids_by_image = defaultdict(list)
    for g in cap_groups:
        tranids_by_image[(g['store_id'], g['image_file_name'])].append(g)

    clean, ambiguous = [], []
    seen_images = set()
    for (store_id, image_file_name), groups in tranids_by_image.items():
        seen_images.add((store_id, image_file_name))
        sku_cnt = sku_counts.get((store_id, image_file_name), 0)
        if len(groups) == 1:
            g = groups[0]
            clean.append({
                'store_id': store_id,
                'image_file_name': image_file_name,
                'iterationtranid': g['iterationtranid'],
                'cap_cnt': g['cap_cnt'],
                'sku_cnt': sku_cnt,
            })
        else:
            ambiguous.append({
                'store_id': store_id,
                'image_file_name': image_file_name,
                'iterationtranids': sorted(g['iterationtranid'] for g in groups),
                'cap_cnt_by_tranid': {g['iterationtranid']: g['cap_cnt'] for g in groups},
                'sku_cnt': sku_cnt,
            })

    orphans = [
        {'store_id': store_id, 'image_file_name': image_file_name, 'sku_cnt': sku_cnt}
        for (store_id, image_file_name), sku_cnt in sku_counts.items()
        if (store_id, image_file_name) not in seen_images
    ]

    return {'clean': clean, 'ambiguous': ambiguous, 'orphans': orphans}


# ─────────────────────────────────────────────────────────────────────────────
# Shelf (iterationtranid) resolution for orphan images
# ─────────────────────────────────────────────────────────────────────────────

def resolve_orphan_tranid(cur, iteration_id: int, image_file_name: str):
    """
    For an image with sku_prediction_temp rows but NO cap_prediction_temp
    rows at all this iteration: check whether orgi.coolermetricstransaction
    already knows a tranid for it (from a previous run); return None if not
    — caller must mint a new one via get_next_tranid().
    """
    cur.execute("""
        SELECT DISTINCT iterationtranid
        FROM orgi.coolermetricstransaction
        WHERE iterationid = %s AND imagefilename = %s
    """, (iteration_id, image_file_name))
    rows = cur.fetchall()
    if not rows:
        return None
    tranids = sorted(r[0] for r in rows)
    if len(tranids) > 1:
        logger.warning(
            f"Orphan image '{image_file_name}' (iteration {iteration_id}) maps to "
            f"{len(tranids)} distinct iterationtranid values in "
            f"orgi.coolermetricstransaction {tranids} — using the lowest ({tranids[0]})."
        )
    return tranids[0]


def get_next_tranid(cur, iteration_id: int) -> int:
    cur.execute(
        "SELECT COALESCE(MAX(iterationtranid), 0) + 1 "
        "FROM orgi.coolermetricsmaster WHERE iterationid = %s",
        (iteration_id,),
    )
    return cur.fetchone()[0]


def ensure_master_row(cur, iteration_id: int, iterationtranid: int, store_id) -> None:
    """
    Insert orgi.coolermetricsmaster row only if missing — never overwrites
    an existing one. caserid is resolved the same way Steps 4/8 of
    cap_pipeline_runner.py do (storemaster.cooler <-> puritymapping.casername).
    """
    cur.execute("""
        INSERT INTO orgi.coolermetricsmaster
            (iterationid, iterationtranid, storeid, caserid, modelrun, processed_flag)
        SELECT %s, %s, %s, p.caserid, NOW(), 'N'
        FROM orgi.storemaster s
        JOIN orgi.puritymapping p ON lower(p.casername) = lower(s.cooler)
        WHERE s.storeid = %s
        LIMIT 1
        ON CONFLICT (iterationid, iterationtranid) DO NOTHING
    """, (iteration_id, iterationtranid, store_id, store_id))


# ─────────────────────────────────────────────────────────────────────────────
# Delete + re-insert for one shelf
# ─────────────────────────────────────────────────────────────────────────────

def reconcile_shelf(cur, iteration_id: int, store_id, image_file_name: str,
                     iterationtranid: int) -> tuple:
    """
    DELETE every orgi.coolermetricstransaction row currently under this
    (iterationid, iterationtranid) shelf, then INSERT fresh rows built
    straight from temp.sku_prediction_temp for the matching
    (iteration_id, store_id, image_file_name).

    productsequenceno is a clean ROW_NUMBER() starting at 1 per shelfnumber
    — no MAX()-offset needed, since everything previously under this tranid
    was just deleted.

    Returns (deleted_count, inserted_count).
    """
    cur.execute("""
        DELETE FROM orgi.coolermetricstransaction
        WHERE iterationid = %s AND iterationtranid = %s
    """, (iteration_id, iterationtranid))
    deleted = cur.rowcount

    cur.execute(f"""
        WITH {CLASS_ID_REMAP_CTE}
        INSERT INTO orgi.coolermetricstransaction (
            iterationid,
            iterationtranid,
            shelfnumber,
            productsequenceno,
            productclassid,
            x1,
            y1,
            x2,
            y2,
            confidence,
            imagefilename,
            s3path_actual_file,
            s3path_annotated_file
        )
        SELECT
            %s AS iterationid,
            %s AS iterationtranid,
            COALESCE(s.shelfnumber, 0) AS shelfnumber,
            ROW_NUMBER() OVER (
                PARTITION BY COALESCE(s.shelfnumber, 0)
                ORDER BY s.x1, s.y1
            ) AS productsequenceno,
            -- Remap the SKU model's own raw class id into the catalog
            -- (orgi.productmaster) id space, same rules as
            -- cap_sku_mapper.py's _remap_to_old_class_ids(): NULL passes
            -- through, >= 9000 (unresolved-fallback classes) is kept as-is,
            -- anything found in class_map is translated, anything else
            -- (< 9000, no entry) is kept as-is.
            CASE
                WHEN s.prod_class_id IS NULL THEN NULL
                WHEN s.prod_class_id >= 9000 THEN s.prod_class_id
                ELSE COALESCE(cm.new_class_id, s.prod_class_id)
            END AS productclassid,
            s.x1, s.y1, s.x2, s.y2,
            1.0000 AS confidence,
            s.image_file_name,
            'June_store_images/' || s.image_file_name AS s3path_actual_file,
            s.s3path_annotated_file
        FROM temp.sku_prediction_temp s
        LEFT JOIN class_map cm ON cm.old_class_id = s.prod_class_id
        WHERE s.iteration_id = %s
          AND s.store_id = %s
          AND s.image_file_name = %s
        ON CONFLICT (iterationid, iterationtranid, shelfnumber, productsequenceno)
        DO NOTHING
    """, (iteration_id, iterationtranid, iteration_id, store_id, image_file_name))
    inserted = cur.rowcount

    return deleted, inserted


# ─────────────────────────────────────────────────────────────────────────────
# Preflight
# ─────────────────────────────────────────────────────────────────────────────

def preflight_checks(cur, iteration_id: int) -> None:
    cur.execute(
        "SELECT COUNT(*) FROM temp.cap_prediction_temp WHERE iteration_id = %s",
        (iteration_id,),
    )
    cap_total = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM temp.sku_prediction_temp WHERE iteration_id = %s",
        (iteration_id,),
    )
    sku_total = cur.fetchone()[0]

    logger.info(f"temp.cap_prediction_temp rows for iteration {iteration_id}: {cap_total}")
    logger.info(f"temp.sku_prediction_temp rows for iteration {iteration_id}: {sku_total}")

    if cap_total == 0 and sku_total == 0:
        raise RuntimeError(
            f"No rows in either temp.cap_prediction_temp or "
            f"temp.sku_prediction_temp for iteration_id={iteration_id}. "
            f"Nothing to reconcile — check the iteration_id or whether the "
            f"temp tables were cleared since that run."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def run_reconciliation(db_config: dict, iteration_id: int, dry_run: bool = False) -> dict:
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
    )
    conn.autocommit = False
    result = {'reconciled': [], 'skipped': 0, 'ambiguous': [], 'orphans_reconciled': []}
    try:
        with conn.cursor() as cur:
            preflight_checks(cur, iteration_id)

            comparison = build_shelf_comparison(cur, iteration_id)
            clean, ambiguous, orphans = comparison['clean'], comparison['ambiguous'], comparison['orphans']

            logger.info(
                f"Shelves compared for iteration_id={iteration_id}: "
                f"{len(clean)} unambiguous, {len(ambiguous)} ambiguous "
                f"(multiple s3path variants — skipped, see below), "
                f"{len(orphans)} orphan image(s) (sku rows with no cap rows at all)"
            )

            if ambiguous:
                for a in ambiguous:
                    logger.warning(
                        f"AMBIGUOUS — skipped: image '{a['image_file_name']}' "
                        f"(store={a['store_id']}) maps to iterationtranids "
                        f"{a['iterationtranids']} (cap_cnt per tranid: "
                        f"{a['cap_cnt_by_tranid']}), sku_cnt={a['sku_cnt']}. "
                        f"No single s3path_annotated_file to pick the right shelf — "
                        f"needs manual review."
                    )
                result['ambiguous'] = ambiguous

            # ── Unambiguous shelves ──────────────────────────────────────────
            for r in clean:
                if not sku_exceeds_cap_threshold(r['sku_cnt'], r['cap_cnt']):
                    logger.debug(
                        f"[{r['image_file_name']}] store={r['store_id']} "
                        f"tranid={r['iterationtranid']} cap_cnt={r['cap_cnt']} "
                        f"sku_cnt={r['sku_cnt']} (exclusions applied) — below "
                        f"{SKU_CAP_EXCESS_THRESHOLD:.0%} excess threshold, skipping."
                    )
                    result['skipped'] += 1
                    continue

                store_id = r['store_id']
                image_file_name = r['image_file_name']
                tranid = r['iterationtranid']

                ensure_master_row(cur, iteration_id, tranid, store_id)
                deleted, inserted = reconcile_shelf(
                    cur, iteration_id, store_id, image_file_name, tranid
                )
                logger.info(
                    f"[{image_file_name}] store={store_id} tranid={tranid} "
                    f"cap_cnt={r['cap_cnt']} sku_cnt={r['sku_cnt']} (exclusions applied) "
                    f"-> deleted {deleted} existing transaction row(s), "
                    f"inserted {inserted} row(s) from temp.sku_prediction_temp"
                )
                result['reconciled'].append({**r, 'deleted': deleted, 'inserted': inserted})

            # ── Orphan images (no cap rows at all this iteration) ───────────
            for o in orphans:
                store_id = o['store_id']
                image_file_name = o['image_file_name']
                sku_cnt = o['sku_cnt']
                # cap_cnt is implicitly 0 here, so sku_exceeds_cap_threshold()
                # reduces to sku_cnt >= 0 — any sku_cnt > 0 still qualifies.
                if not sku_exceeds_cap_threshold(sku_cnt, 0):
                    result['skipped'] += 1
                    continue

                tranid = resolve_orphan_tranid(cur, iteration_id, image_file_name)
                minted_new = tranid is None
                if minted_new:
                    tranid = get_next_tranid(cur, iteration_id)
                    logger.info(
                        f"Orphan image '{image_file_name}' has no cap_prediction_temp "
                        f"rows and no existing iterationtranid — minting new tranid {tranid}"
                    )

                ensure_master_row(cur, iteration_id, tranid, store_id)
                deleted, inserted = reconcile_shelf(
                    cur, iteration_id, store_id, image_file_name, tranid
                )
                logger.info(
                    f"[{image_file_name}] store={store_id} tranid={tranid} "
                    f"cap_cnt=0 sku_cnt={sku_cnt} "
                    f"-> deleted {deleted} existing transaction row(s), "
                    f"inserted {inserted} row(s) from temp.sku_prediction_temp"
                    f"{' [new tranid]' if minted_new else ''}"
                )
                result['orphans_reconciled'].append({
                    'store_id': store_id, 'image_file_name': image_file_name,
                    'iterationtranid': tranid, 'sku_cnt': sku_cnt,
                    'deleted': deleted, 'inserted': inserted, 'minted_new': minted_new,
                })

            total_fixed = len(result['reconciled']) + len(result['orphans_reconciled'])

            if dry_run:
                logger.info(f"[DRY RUN] Would reconcile {total_fixed} shelf(s) — rolling back.")
                conn.rollback()
            else:
                conn.commit()
                logger.info(f"Reconciled {total_fixed} shelf(s) for iteration_id={iteration_id}.")

            return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Compare temp.cap_prediction_temp vs temp.sku_prediction_temp row "
            "counts per shelf (iterationtranid) for one iteration_id (SKU count "
            "excludes Shelf-detection and 'Other *' classes, see "
            "EXCLUDED_SKU_CLASS_IDS_FROM_COUNT). Where sku_cnt exceeds cap_cnt by "
            f"at least {SKU_CAP_EXCESS_THRESHOLD:.0%} (sku_cnt >= cap_cnt * "
            f"{1 + SKU_CAP_EXCESS_THRESHOLD:.2f}), delete that shelf's "
            "orgi.coolermetricstransaction rows and re-insert them from "
            "temp.sku_prediction_temp."
        )
    )
    parser.add_argument("iteration_id", type=int, help="Iteration ID to reconcile, e.g. 260")
    parser.add_argument("--dry-run", action="store_true", help="Run inside a transaction and roll back (no writes).")
    parser.add_argument("--config", default="config.json", help="Path to config.json (default: ./config.json)")
    args = parser.parse_args()

    config = load_config(args.config)
    db_config = config["db_config"]

    try:
        result = run_reconciliation(db_config, args.iteration_id, dry_run=args.dry_run)
        total_fixed = len(result['reconciled']) + len(result['orphans_reconciled'])
        logger.info(
            f"Done. shelves_reconciled={total_fixed} skipped={result['skipped']} "
            f"ambiguous_needs_review={len(result['ambiguous'])}"
        )
    except Exception as e:
        logger.error(f"Reconciliation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()