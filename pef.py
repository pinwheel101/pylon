#!/usr/bin/env python3
"""
Trino Performance Benchmark: Array vs Flat Table Structure
===========================================================

Compares query performance between two table designs:
  - Array table:  1 row = 1 entity, stat values in ARRAY<STRING> columns
  - Flat table:   1 row = 1 item,   stat values as scalar DOUBLE columns

Setup:
  1. pip install trino
  2. Create schema_mapping.json (see SCHEMA_KEYS below for required keys)
  3. Edit CONFIG section (host, port, data scope)
  4. python perf-benchmark.py [--dry-run] [--rounds N]

Output:
  benchmark_output/benchmark_results.json
  benchmark_output/benchmark_report.md
"""

import argparse
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import trino
    from trino.dbapi import connect
except ImportError:
    print("Error: trino package required. Install with: pip install trino")
    sys.exit(1)


# ================================================================
#  SCHEMA KEYS — mapping key -> description
#  schema_mapping.json must provide real values for all of these.
# ================================================================

SCHEMA_KEYS = {
    # Tables
    "t01": "array source table (catalog.schema.table)",
    "t02": "flat source table (catalog.schema.table)",
    # Columns k01-k30
    "k01": "partition column A",
    "k02": "partition column B",
    "k03": "entity identifier 1",
    "k04": "entity identifier 2",
    "k05": "entity identifier 3",
    "k06": "entity identifier 4",
    "k07": "entity identifier 5",
    "k08": "entity identifier 6",
    "k09": "entity identifier 7",
    "k10": "entity identifier 8",
    "k11": "entity identifier 9",
    "k12": "entity identifier 10",
    "k13": "entity identifier 11",
    "k14": "item column A (ARRAY in t01, STRING in t02)",
    "k15": "item column B",
    "k16": "item column C",
    "k17": "stat column 1 (ARRAY<STRING> in t01, DOUBLE in t02)",
    "k18": "stat column 2",
    "k19": "stat column 3",
    "k20": "stat column 4",
    "k21": "stat column 5",
    "k22": "stat column 6",
    "k23": "stat column 7",
    "k24": "stat column 8",
    "k25": "stat column 9",
    "k26": "stat column 10",
    "k27": "stat column 11",
    "k28": "stat column 12",
    "k29": "timestamp column (t01)",
    "k30": "timestamp column (t02)",
}


# ================================================================
#  CONFIGURATION
# ================================================================

CONFIG = {
    # -- Trino connection --
    "host":        os.getenv("TRINO_HOST", "localhost"),
    "port":        int(os.getenv("TRINO_PORT", "443")),
    "user":        os.getenv("TRINO_USER", "benchmark"),
    "http_scheme": os.getenv("TRINO_SCHEME", "https"),

    # -- Schema mapping file --
    "mapping_file": os.path.join(os.path.dirname(__file__) or ".", "schema_mapping.json"),

    # -- Data scope (edit to match your migrated data) --
    "fab_value":  "CHANGEME",
    "dt_start":   "2025-12-01",
    "dt_end":     "2025-12-31",

    # -- Benchmark parameters --
    "warmup_rounds":  1,
    "measure_rounds": 3,

    # -- Output --
    "output_dir": "benchmark_output",
}


# ================================================================
#  Schema mapping loader
# ================================================================

def load_schema_mapping() -> Dict[str, str]:
    """Load schema_mapping.json and validate all required keys exist."""
    path = CONFIG["mapping_file"]
    if not os.path.exists(path):
        print(f"ERROR: Schema mapping file not found: {path}")
        print(f"\nCreate it with the following keys:")
        print(json.dumps({"tables": {"t_arr": "...", "t_flat": "..."},
                          "columns": {k: "..." for k in SCHEMA_KEYS if k.startswith("c_")}},
                         indent=2))
        sys.exit(1)

    with open(path) as f:
        raw = json.load(f)

    # Flatten into a single dict
    m = {}
    for section in raw.values():
        if isinstance(section, dict) and not section.get("_comment"):
            m.update(section)

    # Validate
    missing = [k for k in SCHEMA_KEYS if k not in m]
    if missing:
        print(f"ERROR: Missing keys in schema_mapping.json: {missing}")
        sys.exit(1)

    return m


# ================================================================
#  Data structures
# ================================================================

@dataclass
class RunStats:
    wall_time_ms: float = 0.0
    cpu_time_ms: float = 0.0
    physical_input_rows: int = 0
    physical_input_bytes: int = 0
    peak_memory_bytes: int = 0
    result_rows: int = 0
    query_id: str = ""
    error: str = ""

    def to_dict(self):
        d = {
            "wall_time_ms": round(self.wall_time_ms, 1),
            "cpu_time_ms": round(self.cpu_time_ms, 1),
            "physical_input_rows": self.physical_input_rows,
            "physical_input_bytes": self.physical_input_bytes,
            "peak_memory_bytes": self.peak_memory_bytes,
            "result_rows": self.result_rows,
            "query_id": self.query_id,
        }
        if self.error:
            d["error"] = self.error
        return d


@dataclass
class QueryBenchmark:
    name: str
    description: str
    category: str
    array_sql: str
    flat_sql: str
    array_runs: List[RunStats] = field(default_factory=list)
    flat_runs: List[RunStats] = field(default_factory=list)


# ================================================================
#  Trino helpers
# ================================================================

def get_connection():
    kwargs = {
        "host": CONFIG["host"],
        "port": CONFIG["port"],
        "user": CONFIG["user"],
        "http_scheme": CONFIG["http_scheme"],
    }
    password = os.getenv("TRINO_PASSWORD")
    if password:
        kwargs["auth"] = trino.auth.BasicAuthentication(CONFIG["user"], password)
    return connect(**kwargs)


def _parse_duration_ms(duration_str) -> float:
    """Parse Trino duration (e.g. '1.23s', '456.78ms', '2.00m') to ms."""
    if not duration_str:
        return 0.0
    s = str(duration_str).strip()
    try:
        if s.endswith("ms"):
            return float(s[:-2])
        elif s.endswith("s"):
            return float(s[:-1]) * 1000
        elif s.endswith("m"):
            return float(s[:-1]) * 60000
        elif s.endswith("h"):
            return float(s[:-1]) * 3600000
        elif s.endswith("ns"):
            return float(s[:-2]) / 1_000_000
        elif s.endswith("us"):
            return float(s[:-2]) / 1_000
    except ValueError:
        pass
    return 0.0


def _get_query_id(cursor) -> str:
    """Extract query_id from cursor (trino-python-client internals)."""
    # Try known attribute paths across client versions
    for attr_path in [
        lambda c: c._query.query_id,
        lambda c: c._result.query_id,
        lambda c: c.stats.get("queryId", ""),
    ]:
        try:
            qid = attr_path(cursor)
            if qid:
                return str(qid)
        except Exception:
            pass
    return ""


def run_query(conn, sql: str) -> RunStats:
    """Execute query, then fetch accurate stats from system.runtime.queries."""
    cursor = conn.cursor()
    t0 = time.monotonic()
    cursor.execute(sql.strip())
    rows = cursor.fetchall()
    wall_ms = (time.monotonic() - t0) * 1000.0

    qid = _get_query_id(cursor)
    result = RunStats(
        wall_time_ms=wall_ms,
        result_rows=len(rows),
        query_id=qid,
    )

    # Fetch accurate stats from Trino system table
    if qid:
        try:
            sc = conn.cursor()
            sc.execute(f"""
                SELECT
                    total_cpu_time,
                    physical_input_bytes,
                    physical_input_rows,
                    peak_user_memory_reservations
                FROM system.runtime.queries
                WHERE query_id = '{qid}'
            """)
            row = sc.fetchone()
            if row:
                result.cpu_time_ms = _parse_duration_ms(row[0])
                result.physical_input_bytes = row[1] or 0
                result.physical_input_rows = row[2] or 0
                result.peak_memory_bytes = row[3] or 0
        except Exception:
            # Fallback: try peak_memory_reservations (older Trino)
            try:
                sc2 = conn.cursor()
                sc2.execute(f"""
                    SELECT
                        total_cpu_time,
                        physical_input_bytes,
                        physical_input_rows
                    FROM system.runtime.queries
                    WHERE query_id = '{qid}'
                """)
                row2 = sc2.fetchone()
                if row2:
                    result.cpu_time_ms = _parse_duration_ms(row2[0])
                    result.physical_input_bytes = row2[1] or 0
                    result.physical_input_rows = row2[2] or 0
            except Exception:
                # Last fallback: use cursor.stats
                cs = getattr(cursor, "stats", None) or {}
                result.cpu_time_ms = cs.get("cpuTimeMillis", 0)
                result.physical_input_rows = cs.get("processedRows", 0)
                result.physical_input_bytes = cs.get("processedBytes", 0)

    return result


def fetch_rows(conn, sql: str) -> list:
    cursor = conn.cursor()
    cursor.execute(sql.strip())
    return cursor.fetchall()


def fetch_scalar(conn, sql: str):
    cursor = conn.cursor()
    cursor.execute(sql.strip())
    row = cursor.fetchone()
    return row[0] if row else None


# ================================================================
#  Discovery
# ================================================================

def discover_sample_data(conn, m: Dict[str, str]) -> Dict[str, Any]:
    """Find usable filter values from the flat table."""
    fv = CONFIG["fab_value"]
    ds, de = CONFIG["dt_start"], CONFIG["dt_end"]

    print("[Discovery] Finding sample data...")

    top_eqp = fetch_rows(conn, f"""
        SELECT {m['k03']}, {m['k04']}, {m['k05']}, COUNT(*) AS cnt
        FROM {m['t02']}
        WHERE {m['k01']} = '{fv}' AND {m['k02']} BETWEEN '{ds}' AND '{de}'
        GROUP BY {m['k03']}, {m['k04']}, {m['k05']}
        ORDER BY cnt DESC
        LIMIT 5
    """)
    if not top_eqp:
        print("  ERROR: No data found. Check mapping + CONFIG.")
        sys.exit(1)

    eqp_val, mod_val, rcp_val, row_cnt = top_eqp[0]
    print(f"  top entity: {eqp_val}, {mod_val}, {rcp_val} ({row_cnt:,} rows)")

    param_rows = fetch_rows(conn, f"""
        SELECT {m['k14']}, COUNT(*) AS cnt
        FROM {m['t02']}
        WHERE {m['k01']} = '{fv}' AND {m['k02']} BETWEEN '{ds}' AND '{de}'
          AND {m['k03']} = '{eqp_val}' AND {m['k05']} = '{rcp_val}'
        GROUP BY {m['k14']}
        ORDER BY cnt DESC
        LIMIT 10
    """)
    prm_val = param_rows[0][0] if param_rows else "UNKNOWN"
    prm_top5 = [r[0] for r in param_rows[:5]]
    print(f"  top item: {prm_val}")

    lot_rows = fetch_rows(conn, f"""
        SELECT DISTINCT {m['k06']} FROM {m['t02']}
        WHERE {m['k01']} = '{fv}' AND {m['k02']} BETWEEN '{ds}' AND '{de}'
          AND {m['k03']} = '{eqp_val}' AND {m['k05']} = '{rcp_val}'
        LIMIT 20
    """)
    lot_vals = [r[0] for r in lot_rows]
    print(f"  entities: {len(lot_vals)} found")

    step_rows = fetch_rows(conn, f"""
        SELECT DISTINCT {m['k09']} FROM {m['t02']}
        WHERE {m['k01']} = '{fv}' AND {m['k02']} BETWEEN '{ds}' AND '{de}'
          AND {m['k03']} = '{eqp_val}' AND {m['k05']} = '{rcp_val}'
        LIMIT 10
    """)
    step_vals = [r[0] for r in step_rows]
    step_val = step_vals[0] if step_vals else "1"
    print(f"  sub-ids: {step_vals}")

    meta = fetch_rows(conn, f"""
        SELECT {m['k11']}, {m['k07']} FROM {m['t02']}
        WHERE {m['k01']} = '{fv}' AND {m['k02']} BETWEEN '{ds}' AND '{de}'
          AND {m['k03']} = '{eqp_val}' AND {m['k05']} = '{rcp_val}'
        LIMIT 1
    """)
    oper_val = meta[0][0] if meta else "UNKNOWN"
    lotcd_val = meta[0][1] if meta else "UNKNOWN"

    arr_cnt = fetch_scalar(conn, f"""
        SELECT COUNT(*) FROM {m['t01']}
        WHERE {m['k01']} = '{fv}' AND {m['k02']} BETWEEN '{ds}' AND '{de}'
    """)
    flat_cnt = fetch_scalar(conn, f"""
        SELECT COUNT(*) FROM {m['t02']}
        WHERE {m['k01']} = '{fv}' AND {m['k02']} BETWEEN '{ds}' AND '{de}'
    """)
    print(f"  array total: {arr_cnt:,} rows  |  flat total: {flat_cnt:,} rows\n")

    return {
        "eqp": eqp_val, "mod": mod_val, "rcp": rcp_val,
        "prm": prm_val, "prm_top5": prm_top5,
        "lots": lot_vals, "step": step_val, "steps": step_vals,
        "oper": oper_val, "lotcd": lotcd_val,
        "array_total_rows": arr_cnt, "flat_total_rows": flat_cnt,
    }


# ================================================================
#  Query definitions (uses mapping keys only — no real names)
# ================================================================

def _in_list(values: list) -> str:
    return "', '".join(str(v) for v in values)


def build_queries(m: Dict[str, str], s: Dict[str, Any]) -> List[QueryBenchmark]:
    # Resolve all schema references to short opaque aliases
    c = {k: v for k, v in m.items()}  # t01, t02, k01..k30

    # Data scope
    dv0 = CONFIG["fab_value"]
    dv1 = CONFIG["dt_start"]
    dv2 = CONFIG["dt_end"]

    # Discovered sample values
    sv0 = s["eqp"]
    sv1 = s["mod"]
    sv2 = s["rcp"]
    sv3 = s["prm"]
    sv4 = _in_list(s["prm_top5"])
    sv5 = _in_list(s["lots"][:10])
    sv6 = s["step"]
    sv7 = s["oper"]
    sv8 = s["lotcd"]

    queries = []

    # ── Q01: Step Lookup ─────────────────────────────────────────
    queries.append(QueryBenchmark(
        name="Q01_step_lookup",
        description="DISTINCT id/name for entity+filter (row-level only)",
        category="metadata_lookup",
        array_sql=f"""
SELECT COUNT(*) FROM (
    SELECT DISTINCT
        CAST({c['k09']} AS INTEGER) AS sid,
        {c['k10']},
        {c['k09']} || ':' || {c['k10']} AS combined
    FROM {c['t01']}
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k05']} = '{sv2}'
      AND {c['k10']} IS NOT NULL
)""",
        flat_sql=f"""
SELECT COUNT(*) FROM (
    SELECT DISTINCT
        CAST({c['k09']} AS INTEGER) AS sid,
        {c['k10']},
        {c['k09']} || ':' || {c['k10']} AS combined
    FROM {c['t02']}
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k05']} = '{sv2}'
      AND {c['k10']} IS NOT NULL
)""",
    ))

    # ── Q02: Item Stats (14 arrays UNNEST vs direct) ─────────────
    queries.append(QueryBenchmark(
        name="Q02_item_stats",
        description="All stat columns for specific item — UNNEST vs direct",
        category="unnest_vs_direct",
        array_sql=f"""
SELECT COUNT(*) FROM (
    SELECT {c['k03']}, {c['k04']}, {c['k06']}, {c['k08']}, {c['k09']}, {c['k10']},
           t.{c['k14']}, t.{c['k16']},
           CAST(t.{c['k19']} AS DOUBLE) AS v19,
           CAST(t.{c['k20']} AS DOUBLE) AS v20,
           CAST(t.{c['k17']} AS DOUBLE) AS v17,
           CAST(t.{c['k18']} AS DOUBLE) AS v18,
           CAST(t.{c['k22']} AS DOUBLE) AS v22,
           CAST(t.{c['k21']} AS DOUBLE) AS v21,
           CAST(t.{c['k23']} AS DOUBLE) AS v23,
           CAST(t.{c['k24']} AS DOUBLE) AS v24,
           CAST(t.{c['k25']} AS DOUBLE) AS v25,
           CAST(t.{c['k26']} AS DOUBLE) AS v26,
           CAST(t.{c['k27']} AS DOUBLE) AS v27,
           CAST(t.{c['k28']} AS DOUBLE) AS v28
    FROM {c['t01']}
    CROSS JOIN UNNEST(
        {c['k14']}, {c['k16']},
        {c['k19']}, {c['k20']}, {c['k17']}, {c['k18']},
        {c['k22']}, {c['k21']}, {c['k23']}, {c['k24']},
        {c['k25']}, {c['k26']}, {c['k27']}, {c['k28']}
    ) AS t(
        {c['k14']}, {c['k16']},
        {c['k19']}, {c['k20']}, {c['k17']}, {c['k18']},
        {c['k22']}, {c['k21']}, {c['k23']}, {c['k24']},
        {c['k25']}, {c['k26']}, {c['k27']}, {c['k28']}
    )
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k04']} IN ('{sv1}')
      AND t.{c['k14']} = '{sv3}'
    ORDER BY {c['k29']}
    LIMIT 1000
)""",
        flat_sql=f"""
SELECT COUNT(*) FROM (
    SELECT {c['k03']}, {c['k04']}, {c['k06']}, {c['k08']}, {c['k09']}, {c['k10']},
           {c['k14']}, {c['k16']},
           {c['k19']}, {c['k20']}, {c['k17']}, {c['k18']},
           {c['k22']}, {c['k21']}, {c['k23']}, {c['k24']},
           {c['k25']}, {c['k26']}, {c['k27']}, {c['k28']}
    FROM {c['t02']}
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k04']} IN ('{sv1}')
      AND {c['k14']} = '{sv3}'
    ORDER BY {c['k30']}
    LIMIT 1000
)""",
    ))

    # ── Q03: Distinct Item Listing ───────────────────────────────
    queries.append(QueryBenchmark(
        name="Q03_item_listing",
        description="DISTINCT items — UNNEST array vs direct column",
        category="unnest_vs_direct",
        array_sql=f"""
SELECT COUNT(*) FROM (
    SELECT DISTINCT t.v14
    FROM {c['t01']}
    CROSS JOIN UNNEST({c['k14']}) AS t(v14)
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k04']} IN ('{sv1}')
)""",
        flat_sql=f"""
SELECT COUNT(*) FROM (
    SELECT DISTINCT {c['k14']}
    FROM {c['t02']}
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k04']} IN ('{sv1}')
)""",
    ))

    # ── Q04: Entity Pair Listing ─────────────────────────────────
    queries.append(QueryBenchmark(
        name="Q04_entity_pair_listing",
        description="DISTINCT entity pairs — row-level columns only",
        category="metadata_lookup",
        array_sql=f"""
SELECT COUNT(*) FROM (
    SELECT DISTINCT {c['k12']}, {c['k08']}
    FROM {c['t01']}
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k04']} = '{sv1}'
      AND {c['k11']} = '{sv7}'
)""",
        flat_sql=f"""
SELECT COUNT(*) FROM (
    SELECT DISTINCT {c['k12']}, {c['k08']}
    FROM {c['t02']}
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k04']} = '{sv1}'
      AND {c['k11']} = '{sv7}'
)""",
    ))

    # ── Q05: Daily Count ─────────────────────────────────────────
    queries.append(QueryBenchmark(
        name="Q05_daily_count",
        description="COUNT/MIN/MAX grouped by partition col",
        category="scan_aggregate",
        array_sql=f"""
SELECT COUNT(*) FROM (
    SELECT {c['k02']},
           COUNT(*) AS data_count,
           SUBSTR(MIN(DATE_FORMAT({c['k29']}, '%Y-%m-%d %T')), 1, 16) || ':00' AS min_ts
    FROM {c['t01']}
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k04']} = '{sv1}'
    GROUP BY {c['k02']}
)""",
        flat_sql=f"""
SELECT COUNT(*) FROM (
    SELECT {c['k02']},
           COUNT(*) AS data_count,
           SUBSTR(MIN(DATE_FORMAT({c['k30']}, '%Y-%m-%d %T')), 1, 16) || ':00' AS min_ts
    FROM {c['t02']}
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k04']} = '{sv1}'
    GROUP BY {c['k02']}
)""",
    ))

    # ── Q06: Ranking/Dedup (UNNEST + ROW_NUMBER) ─────────────────
    queries.append(QueryBenchmark(
        name="Q06_ranking_dedup",
        description="ROW_NUMBER dedup — UNNEST+window vs direct+window",
        category="unnest_vs_direct",
        array_sql=f"""
SELECT COUNT(*) FROM (
    SELECT * FROM (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY {c['k11']}, t.{c['k14']}, {c['k09']}, {c['k13']}
                ORDER BY {c['k29']} DESC
            ) AS rk,
            {c['k11']}, t.{c['k14']}, {c['k09']},
            {c['k12']}, {c['k08']},
            CAST(t.{c['k18']} AS DOUBLE) AS yvalue
        FROM {c['t01']}
        CROSS JOIN UNNEST({c['k14']}, {c['k18']}) AS t({c['k14']}, {c['k18']})
        WHERE {c['k01']} = '{dv0}'
          AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
          AND {c['k07']} = '{sv8}'
          AND {c['k11']} = '{sv7}'
          AND t.{c['k14']} IN ('{sv4}')
          AND {c['k09']} = '{sv6}'
          AND t.{c['k18']} IS NOT NULL
    ) WHERE rk = 1
)""",
        flat_sql=f"""
SELECT COUNT(*) FROM (
    SELECT * FROM (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY {c['k11']}, {c['k14']}, {c['k09']}, {c['k13']}
                ORDER BY {c['k30']} DESC
            ) AS rk,
            {c['k11']}, {c['k14']}, {c['k09']},
            {c['k12']}, {c['k08']},
            {c['k18']} AS yvalue
        FROM {c['t02']}
        WHERE {c['k01']} = '{dv0}'
          AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
          AND {c['k07']} = '{sv8}'
          AND {c['k11']} = '{sv7}'
          AND {c['k14']} IN ('{sv4}')
          AND {c['k09']} = '{sv6}'
          AND {c['k18']} IS NOT NULL
    ) WHERE rk = 1
)""",
    ))

    # ── Q07: Multi-UNNEST + Aggregation (heaviest) ───────────────
    queries.append(QueryBenchmark(
        name="Q07_multi_unnest_agg",
        description="UNNEST 6 stat arrays + ROW_NUMBER + AVG — heaviest pattern",
        category="unnest_vs_direct",
        array_sql=f"""
SELECT COUNT(*) FROM (
    SELECT
        r.a1, r.a2, r.a3, r.a4, r.a5, r.a6,
        AVG(r.v1) AS ag1, AVG(r.v2) AS ag2,
        AVG(r.v3) AS ag3, AVG(r.v4) AS ag4, AVG(r.v5) AS ag5
    FROM (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY raw.{c['k01']}, raw.{c['k07']}, raw.{c['k11']}, arr.a5,
                             raw.{c['k09']}, raw.{c['k13']}
                ORDER BY raw.{c['k29']} DESC
            ) AS rk,
            raw.{c['k01']} AS a1, raw.{c['k07']} AS a2, raw.{c['k06']} AS a3,
            raw.{c['k11']} AS a4, arr.a5, raw.{c['k09']} AS a6,
            CAST(arr.v1 AS DOUBLE) AS v1,
            CAST(arr.v2 AS DOUBLE) AS v2,
            CAST(arr.v3 AS DOUBLE) AS v3,
            CAST(arr.v4 AS DOUBLE) AS v4,
            CAST(arr.v5 AS DOUBLE) AS v5
        FROM {c['t01']} raw
        CROSS JOIN UNNEST(
            raw.{c['k14']}, raw.{c['k17']}, raw.{c['k19']},
            raw.{c['k20']}, raw.{c['k21']}, raw.{c['k22']}
        ) AS arr(a5, v1, v2, v3, v4, v5)
        WHERE raw.{c['k01']} = '{dv0}'
          AND raw.{c['k02']} BETWEEN '{dv1}' AND '{dv2}'
          AND raw.{c['k03']} = '{sv0}'
          AND raw.{c['k07']} = '{sv8}'
          AND raw.{c['k11']} = '{sv7}'
          AND arr.a5 IN ('{sv4}')
          AND arr.v1 IS NOT NULL
    ) r
    WHERE r.rk = 1
    GROUP BY r.a1, r.a2, r.a3, r.a4, r.a5, r.a6
)""",
        flat_sql=f"""
SELECT COUNT(*) FROM (
    SELECT
        r.a1, r.a2, r.a3, r.a4, r.a5, r.a6,
        AVG(r.v1) AS ag1, AVG(r.v2) AS ag2,
        AVG(r.v3) AS ag3, AVG(r.v4) AS ag4, AVG(r.v5) AS ag5
    FROM (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY {c['k01']}, {c['k07']}, {c['k11']}, {c['k14']},
                             {c['k09']}, {c['k13']}
                ORDER BY {c['k30']} DESC
            ) AS rk,
            {c['k01']} AS a1, {c['k07']} AS a2, {c['k06']} AS a3,
            {c['k11']} AS a4, {c['k14']} AS a5, {c['k09']} AS a6,
            {c['k17']} AS v1, {c['k19']} AS v2,
            {c['k20']} AS v3, {c['k21']} AS v4, {c['k22']} AS v5
        FROM {c['t02']}
        WHERE {c['k01']} = '{dv0}'
          AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
          AND {c['k03']} = '{sv0}'
          AND {c['k07']} = '{sv8}'
          AND {c['k11']} = '{sv7}'
          AND {c['k14']} IN ('{sv4}')
          AND {c['k17']} IS NOT NULL
    ) r
    WHERE r.rk = 1
    GROUP BY r.a1, r.a2, r.a3, r.a4, r.a5, r.a6
)""",
    ))

    # ── Q08: Spec Aggregation (AVG/STDDEV) ───────────────────────
    queries.append(QueryBenchmark(
        name="Q08_spec_aggregation",
        description="AVG/STDDEV grouped by item+sub-id — UNNEST+CAST vs direct",
        category="scan_aggregate",
        array_sql=f"""
SELECT COUNT(*) FROM (
    SELECT
        t.{c['k14']}, {c['k09']},
        ROUND(AVG(CAST(t.{c['k17']} AS DOUBLE)), 2) AS target,
        ROUND(AVG(CAST(t.{c['k17']} AS DOUBLE)) + STDDEV_POP(CAST(t.{c['k17']} AS DOUBLE)) * 5.0, 2) AS hi,
        ROUND(AVG(CAST(t.{c['k17']} AS DOUBLE)) - STDDEV_POP(CAST(t.{c['k17']} AS DOUBLE)) * 5.0, 2) AS lo,
        ROUND(AVG(CAST(t.{c['k18']} AS DOUBLE)), 2) AS mid,
        STDDEV_POP(CAST(t.{c['k17']} AS DOUBLE)) AS sigma
    FROM {c['t01']}
    CROSS JOIN UNNEST({c['k14']}, {c['k17']}, {c['k18']})
        AS t({c['k14']}, {c['k17']}, {c['k18']})
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k05']} = '{sv2}'
      AND t.{c['k14']} IN ('{sv4}')
    GROUP BY t.{c['k14']}, {c['k09']}
)""",
        flat_sql=f"""
SELECT COUNT(*) FROM (
    SELECT
        {c['k14']}, {c['k09']},
        ROUND(AVG({c['k17']}), 2) AS target,
        ROUND(AVG({c['k17']}) + STDDEV_POP({c['k17']}) * 5.0, 2) AS hi,
        ROUND(AVG({c['k17']}) - STDDEV_POP({c['k17']}) * 5.0, 2) AS lo,
        ROUND(AVG({c['k18']}), 2) AS mid,
        STDDEV_POP({c['k17']}) AS sigma
    FROM {c['t02']}
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} BETWEEN '{dv1}' AND '{dv2}'
      AND {c['k03']} = '{sv0}'
      AND {c['k05']} = '{sv2}'
      AND {c['k14']} IN ('{sv4}')
    GROUP BY {c['k14']}, {c['k09']}
)""",
    ))

    # ── Q09: Wide Scan (single day) ──────────────────────────────
    queries.append(QueryBenchmark(
        name="Q09_wide_scan_1day",
        description="Full scan for 1 day — raw I/O + UNNEST overhead",
        category="full_scan",
        array_sql=f"""
SELECT COUNT(*) FROM (
    SELECT {c['k03']}, {c['k04']}, {c['k06']}, {c['k09']},
           t.v14,
           CAST(t.v17 AS DOUBLE) AS v17
    FROM {c['t01']}
    CROSS JOIN UNNEST({c['k14']}, {c['k17']})
        AS t(v14, v17)
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} = '{dv1}'
)""",
        flat_sql=f"""
SELECT COUNT(*) FROM (
    SELECT {c['k03']}, {c['k04']}, {c['k06']}, {c['k09']},
           {c['k14']},
           {c['k17']}
    FROM {c['t02']}
    WHERE {c['k01']} = '{dv0}'
      AND {c['k02']} = '{dv1}'
)""",
    ))

    return queries


# ================================================================
#  Benchmark runner
# ================================================================

def run_benchmark(conn, queries: List[QueryBenchmark], dry_run: bool = False):
    warmup = CONFIG["warmup_rounds"]
    measure = CONFIG["measure_rounds"]
    total = warmup + measure

    for qi, q in enumerate(queries, 1):
        hdr = f"[{qi}/{len(queries)}] {q.name}: {q.description}"
        print(hdr)
        print("-" * len(hdr))

        if dry_run:
            print("  ARRAY:\n    " + q.array_sql.strip()[:200] + " ...")
            print("  FLAT:\n    " + q.flat_sql.strip()[:200] + " ...")
            print()
            continue

        for rnd in range(total):
            is_warm = rnd < warmup
            tag = "W" if is_warm else f"{rnd - warmup + 1}"

            for label, sql, run_list in [
                ("array", q.array_sql, q.array_runs),
                ("flat ", q.flat_sql,  q.flat_runs),
            ]:
                try:
                    st = run_query(conn, sql)
                    if not is_warm:
                        run_list.append(st)
                    print(f"  [{tag}] {label} | wall={st.wall_time_ms:>9,.0f}ms"
                          f" | cpu={st.cpu_time_ms:>9,.0f}ms"
                          f" | phys_rows={st.physical_input_rows:>12,}"
                          f" | phys_bytes={st.physical_input_bytes:>14,}"
                          f" | peak_mem={st.peak_memory_bytes:>14,}"
                          f" | result={st.result_rows}"
                          f" | qid={st.query_id}")
                except Exception as e:
                    err = str(e)[:120]
                    print(f"  [{tag}] {label} | ERROR: {err}")
                    if not is_warm:
                        run_list.append(RunStats(error=err))
        print()


# ================================================================
#  Report generation
# ================================================================

def _avg(runs: List[RunStats], attr: str) -> float:
    vals = [getattr(r, attr) for r in runs if not r.error]
    return statistics.mean(vals) if vals else 0.0


def _pct_change(base: float, new: float) -> str:
    if base == 0:
        return "N/A"
    pct = (new - base) / base * 100
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.0f}%"


def generate_report(queries: List[QueryBenchmark], sample: Dict[str, Any]) -> str:
    lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines.append("# Performance Benchmark: Array vs Flat\n")
    lines.append(f"Generated: {now}\n")

    lines.append("## Environment\n")
    lines.append(f"| Item | Value |")
    lines.append(f"|------|-------|")
    lines.append(f"| Date range | {CONFIG['dt_start']} ~ {CONFIG['dt_end']} |")
    lines.append(f"| Array rows (period) | {sample.get('array_total_rows', 'N/A'):,} |")
    lines.append(f"| Flat rows (period) | {sample.get('flat_total_rows', 'N/A'):,} |")
    lines.append(f"| Warmup rounds | {CONFIG['warmup_rounds']} |")
    lines.append(f"| Measure rounds | {CONFIG['measure_rounds']} |")
    lines.append("")

    lines.append("## Summary\n")
    lines.append("| Query | Category | Wall ms (A) | Wall ms (F) | Wall Diff "
                 "| CPU ms (A) | CPU ms (F) | CPU Diff "
                 "| Bytes (A) | Bytes (F) | Bytes Diff | Speedup |")
    lines.append("|-------|----------|------------:|------------:|----------:"
                 "|-----------:|-----------:|---------:"
                 "|----------:|----------:|-----------:|--------:|")

    for q in queries:
        if not q.array_runs or not q.flat_runs:
            continue
        aw = _avg(q.array_runs, "wall_time_ms")
        fw = _avg(q.flat_runs, "wall_time_ms")
        ac = _avg(q.array_runs, "cpu_time_ms")
        fc = _avg(q.flat_runs, "cpu_time_ms")
        ab = _avg(q.array_runs, "physical_input_bytes")
        fb = _avg(q.flat_runs, "physical_input_bytes")
        speedup = aw / fw if fw > 0 else 0

        lines.append(
            f"| {q.name} | {q.category} "
            f"| {aw:,.0f} | {fw:,.0f} | {_pct_change(aw, fw)} "
            f"| {ac:,.0f} | {fc:,.0f} | {_pct_change(ac, fc)} "
            f"| {ab:,.0f} | {fb:,.0f} | {_pct_change(ab, fb)} "
            f"| {speedup:.2f}x |"
        )

    lines.append("")
    lines.append("> **Diff**: negative = flat is faster/smaller, positive = array is faster/smaller")
    lines.append("> **Speedup**: >1 = flat is faster (wall time basis)")
    lines.append("")

    lines.append("## Detailed Results\n")
    for q in queries:
        lines.append(f"### {q.name}\n")
        lines.append(f"**{q.description}** (category: `{q.category}`)\n")

        if not q.array_runs:
            lines.append("_No results_\n")
            continue

        lines.append("| Run | Type | Wall (ms) | CPU (ms) | Phys Input Rows | Phys Input Bytes | Peak Mem | Result |")
        lines.append("|----:|------|----------:|---------:|----------------:|-----------------:|---------:|-------:|")

        for i, r in enumerate(q.array_runs, 1):
            lines.append(f"| {i} | array | {r.wall_time_ms:,.0f} | {r.cpu_time_ms:,.0f} "
                         f"| {r.physical_input_rows:,} | {r.physical_input_bytes:,} "
                         f"| {r.peak_memory_bytes:,} | {r.result_rows} |")
        for i, r in enumerate(q.flat_runs, 1):
            lines.append(f"| {i} | flat | {r.wall_time_ms:,.0f} | {r.cpu_time_ms:,.0f} "
                         f"| {r.physical_input_rows:,} | {r.physical_input_bytes:,} "
                         f"| {r.peak_memory_bytes:,} | {r.result_rows} |")

        aw = _avg(q.array_runs, "wall_time_ms")
        fw = _avg(q.flat_runs, "wall_time_ms")
        ac = _avg(q.array_runs, "cpu_time_ms")
        fc = _avg(q.flat_runs, "cpu_time_ms")
        sp_w = aw / fw if fw > 0 else 0
        sp_c = ac / fc if fc > 0 else 0

        faster_w = "(flat faster)" if sp_w > 1 else "(array faster)"
        faster_c = "(flat faster)" if sp_c > 1 else "(array faster)"
        lines.append(f"\n- **Wall time**: array {aw:,.0f}ms vs flat {fw:,.0f}ms = "
                     f"**{sp_w:.2f}x** {faster_w}")
        lines.append(f"- **CPU time**: array {ac:,.0f}ms vs flat {fc:,.0f}ms = "
                     f"**{sp_c:.2f}x** {faster_c}")
        lines.append("")

    return "\n".join(lines)


def save_results(queries: List[QueryBenchmark], sample: Dict[str, Any]):
    out_dir = CONFIG["output_dir"]
    os.makedirs(out_dir, exist_ok=True)

    results = {
        "timestamp": datetime.now().isoformat(),
        "config": {
            "dt_start": CONFIG["dt_start"],
            "dt_end": CONFIG["dt_end"],
            "warmup_rounds": CONFIG["warmup_rounds"],
            "measure_rounds": CONFIG["measure_rounds"],
        },
        "sample_data": {
            "array_total_rows": sample.get("array_total_rows"),
            "flat_total_rows": sample.get("flat_total_rows"),
        },
        "queries": [
            {
                "name": q.name,
                "description": q.description,
                "category": q.category,
                "array_runs": [r.to_dict() for r in q.array_runs],
                "flat_runs": [r.to_dict() for r in q.flat_runs],
            }
            for q in queries
        ],
    }
    json_path = os.path.join(out_dir, "benchmark_results.json")
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"  JSON   -> {json_path}")

    report = generate_report(queries, sample)
    md_path = os.path.join(out_dir, "benchmark_report.md")
    with open(md_path, "w") as f:
        f.write(report)
    print(f"  Report -> {md_path}")


# ================================================================
#  Console summary
# ================================================================

def print_summary(queries: List[QueryBenchmark]):
    print()
    print("=" * 80)
    print(f"{'Query':<28} {'Wall(A)':>9} {'Wall(F)':>9} {'Speedup':>8}"
          f"  {'CPU(A)':>9} {'CPU(F)':>9} {'Speedup':>8}")
    print("-" * 80)

    for q in queries:
        if not q.array_runs or not q.flat_runs:
            continue
        aw = _avg(q.array_runs, "wall_time_ms")
        fw = _avg(q.flat_runs, "wall_time_ms")
        ac = _avg(q.array_runs, "cpu_time_ms")
        fc = _avg(q.flat_runs, "cpu_time_ms")
        sp_w = aw / fw if fw > 0 else 0
        sp_c = ac / fc if fc > 0 else 0
        print(f"{q.name:<28} {aw:>8,.0f}ms {fw:>8,.0f}ms {sp_w:>7.2f}x"
              f"  {ac:>8,.0f}ms {fc:>8,.0f}ms {sp_c:>7.2f}x")

    print("=" * 80)
    print("  Speedup >1.0 = flat is faster | <1.0 = array is faster")


# ================================================================
#  Main
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Trino benchmark: Array vs Flat table performance")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print queries without executing")
    parser.add_argument("--rounds", type=int, default=None,
                        help="Override number of measured rounds")
    parser.add_argument("--warmup", type=int, default=None,
                        help="Override number of warmup rounds")
    parser.add_argument("--mapping", type=str, default=None,
                        help="Path to schema_mapping.json")
    args = parser.parse_args()

    if args.rounds:
        CONFIG["measure_rounds"] = args.rounds
    if args.warmup:
        CONFIG["warmup_rounds"] = args.warmup
    if args.mapping:
        CONFIG["mapping_file"] = args.mapping

    print("=" * 60)
    print(" Trino Benchmark: Array vs Flat")
    print("=" * 60)
    print(f"  Mapping: {CONFIG['mapping_file']}")
    print(f"  Scope:   {CONFIG['dt_start']} ~ {CONFIG['dt_end']}")
    print(f"  Rounds:  {CONFIG['warmup_rounds']} warmup + {CONFIG['measure_rounds']} measured")
    print()

    # Load schema mapping
    m = load_schema_mapping()
    print(f"  Array table: {m['t01']}")
    print(f"  Flat table:  {m['t02']}")
    print()

    conn = get_connection()

    if not args.dry_run:
        sample = discover_sample_data(conn, m)
    else:
        sample = {
            "eqp": "<EQP>", "mod": "<MOD>", "rcp": "<RCP>",
            "prm": "<PARAM>", "prm_top5": ["P1", "P2", "P3", "P4", "P5"],
            "lots": ["LOT1", "LOT2"], "step": "1", "steps": ["1"],
            "oper": "<OPER>", "lotcd": "<LC>",
            "array_total_rows": 0, "flat_total_rows": 0,
        }

    queries = build_queries(m, sample)

    print(f"Running {len(queries)} query pairs ...\n")
    run_benchmark(conn, queries, dry_run=args.dry_run)

    if not args.dry_run:
        print("\nSaving results:")
        save_results(queries, sample)
        print_summary(queries)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
