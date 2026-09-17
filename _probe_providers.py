#!/usr/bin/env python3
"""Тимчасовий скрипт: довільний SQL до Databricks для розвідки даних."""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent
spec = importlib.util.spec_from_file_location(
    "gen", ROOT / "MBR Bella Mozarella Pinkman Bar" / "generate_mbr.py")
gen = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen)

QUERIES = [q.strip() for q in os.getenv("SQL", "").split(";;") if q.strip()]

if not gen.DATABRICKS_TOKEN:
    print("NO TOKEN"); sys.exit(1)

gen.CLUSTER = gen.pick_cluster()
ctx = gen.create_ctx()
try:
    schema = gen.pick_schema(ctx)
    for q in QUERIES:
        q = q.replace("{schema}", schema)
        print(f"\n===== {q[:120]} =====")
        try:
            rows = gen.run_query(ctx, q)
            for r in rows:
                print(" | ".join(str(x) for x in r))
            print(f"total: {len(rows)}")
        except Exception as exc:
            print(f"ERROR: {exc}")
finally:
    gen.destroy_ctx(ctx)
