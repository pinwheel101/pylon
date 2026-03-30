"""
Parquet 컬럼 메타데이터 비교 스크립트
- Array 구조 (fdc_summary) vs Flat 구조 (y1_summary_v1)
- 대표 파일 3개씩, 총 6개 분석

사용법:
  pip install pyarrow
  python parquet_inspect.py
"""

import os
import sys
from pathlib import Path

import pyarrow.parquet as pq

# ──────────────────────────────────────────────
# 설정: 로컬 다운로드 경로
# ──────────────────────────────────────────────
ARRAY_DIR = "/home/dkim/temp/sum-arr"
FLAT_DIR = "/home/dkim/temp/sum-flat"


def collect_parquet_files(directory: str) -> list[str]:
    """디렉토리 내 .parquet 파일 목록 반환"""
    d = Path(directory)
    if not d.exists():
        print(f"[ERROR] 디렉토리 없음: {directory}")
        sys.exit(1)
    files = sorted(d.glob("*.parquet"))
    if not files:
        print(f"[ERROR] parquet 파일 없음: {directory}")
        sys.exit(1)
    return [str(f) for f in files]


def inspect_file(filepath: str) -> dict:
    """단일 Parquet 파일의 컬럼별 메타데이터 수집"""
    meta = pq.read_metadata(filepath)
    file_info = {
        "file": os.path.basename(filepath),
        "num_rows": meta.num_rows,
        "num_row_groups": meta.num_row_groups,
        "num_columns": meta.num_columns,
        "created_by": meta.created_by,
        "file_size": os.path.getsize(filepath),
        "columns": [],
    }

    # 전체 row group 합산
    col_stats = {}
    for i in range(meta.num_row_groups):
        rg = meta.row_group(i)
        for j in range(rg.num_columns):
            col = rg.column(j)
            name = col.path_in_schema
            if name not in col_stats:
                col_stats[name] = {
                    "name": name,
                    "physical_type": str(col.physical_type),
                    "encodings": set(),
                    "compression": str(col.compression),
                    "compressed": 0,
                    "uncompressed": 0,
                    "num_values": 0,
                    "has_dict": False,
                }
            s = col_stats[name]
            s["encodings"].update(str(e) for e in col.encodings)
            s["compressed"] += col.total_compressed_size
            s["uncompressed"] += col.total_uncompressed_size
            s["num_values"] += col.num_values
            if col.has_dictionary_page:
                s["has_dict"] = True

    file_info["columns"] = list(col_stats.values())
    return file_info


def print_file_report(info: dict):
    """단일 파일 리포트 출력"""
    print(f"\n  File: {info['file']}")
    print(f"  Rows: {info['num_rows']:,}  |  Row Groups: {info['num_row_groups']}  |  "
          f"Columns: {info['num_columns']}  |  File Size: {info['file_size']/1024/1024:.1f} MB")
    print(f"  Created by: {info['created_by']}")

    print(f"\n  {'Column':<35} {'Type':<12} {'Encoding':<25} {'Comp.':<8} "
          f"{'Compressed':>12} {'Uncompressed':>14} {'Ratio':>7} {'Dict':>5}")
    print(f"  {'-'*120}")

    for col in info["columns"]:
        ratio = (col["compressed"] / col["uncompressed"]
                 if col["uncompressed"] > 0 else 0)
        enc_str = ",".join(sorted(col["encodings"]))
        print(f"  {col['name']:<35} {col['physical_type']:<12} {enc_str:<25} {col['compression']:<8} "
              f"{col['compressed']:>10,} B {col['uncompressed']:>12,} B "
              f"{ratio:>6.2f} {'Y' if col['has_dict'] else 'N':>5}")


def print_summary_comparison(array_files: list[dict], flat_files: list[dict]):
    """두 구조의 집계 비교"""
    print("\n")
    print("=" * 100)
    print("  SUMMARY COMPARISON: Array vs Flat")
    print("=" * 100)

    for label, files in [("Array (fdc_summary)", array_files), ("Flat (y1_summary_v1)", flat_files)]:
        total_rows = sum(f["num_rows"] for f in files)
        total_size = sum(f["file_size"] for f in files)
        total_compressed = sum(c["compressed"] for f in files for c in f["columns"])
        total_uncompressed = sum(c["uncompressed"] for f in files for c in f["columns"])
        overall_ratio = total_compressed / total_uncompressed if total_uncompressed > 0 else 0

        print(f"\n  [{label}]  files={len(files)}  rows={total_rows:,}  "
              f"size={total_size/1024/1024:.1f} MB  "
              f"bytes/row={total_size/total_rows:.1f}  "
              f"compression_ratio={overall_ratio:.3f}")

    # 컬럼별 딕셔너리 적용률 (flat만)
    print(f"\n  [Flat 딕셔너리 적용 현황]")
    print(f"  {'Column':<35} {'Dict':>5} {'Compressed (avg)':>16} {'Ratio (avg)':>12}")
    print(f"  {'-'*75}")

    col_agg = {}
    for f in flat_files:
        for col in f["columns"]:
            name = col["name"]
            if name not in col_agg:
                col_agg[name] = {"has_dict": [], "compressed": [], "ratio": []}
            col_agg[name]["has_dict"].append(col["has_dict"])
            col_agg[name]["compressed"].append(col["compressed"])
            r = col["compressed"] / col["uncompressed"] if col["uncompressed"] > 0 else 0
            col_agg[name]["ratio"].append(r)

    for name, agg in sorted(col_agg.items(), key=lambda x: -sum(x[1]["compressed"])):
        dict_pct = sum(agg["has_dict"]) / len(agg["has_dict"])
        avg_comp = sum(agg["compressed"]) / len(agg["compressed"])
        avg_ratio = sum(agg["ratio"]) / len(agg["ratio"])
        dict_label = "ALL" if dict_pct == 1.0 else ("NONE" if dict_pct == 0 else f"{dict_pct:.0%}")
        print(f"  {name:<35} {dict_label:>5} {avg_comp:>14,.0f} B {avg_ratio:>11.3f}")


def main():
    print("=" * 100)
    print("  Parquet Column Metadata Inspector")
    print("  Array 구조 (fdc_summary) vs Flat 구조 (y1_summary_v1)")
    print("=" * 100)

    array_files_paths = collect_parquet_files(ARRAY_DIR)
    flat_files_paths = collect_parquet_files(FLAT_DIR)

    print(f"\n  Array files ({ARRAY_DIR}): {len(array_files_paths)}")
    print(f"  Flat  files ({FLAT_DIR}): {len(flat_files_paths)}")

    # ── Array 구조 분석 ──
    print(f"\n{'='*100}")
    print(f"  ARRAY 구조 (fdc_summary)")
    print(f"{'='*100}")

    array_results = []
    for fp in array_files_paths:
        info = inspect_file(fp)
        array_results.append(info)
        print_file_report(info)

    # ── Flat 구조 분석 ──
    print(f"\n{'='*100}")
    print(f"  FLAT 구조 (y1_summary_v1)")
    print(f"{'='*100}")

    flat_results = []
    for fp in flat_files_paths:
        info = inspect_file(fp)
        flat_results.append(info)
        print_file_report(info)

    # ── 비교 요약 ──
    print_summary_comparison(array_results, flat_results)

    print("\n")


if __name__ == "__main__":
    main()