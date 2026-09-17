#!/usr/bin/env python3
"""Тимчасовий скрипт: пошук локацій бренду в Databricks за шматком назви."""
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

PATTERNS = [p.strip() for p in os.getenv("PATTERNS", "баранчик,baranchyk,baranchik,баран").split(",") if p.strip()]

if not gen.DATABRICKS_TOKEN:
    print("NO TOKEN"); sys.exit(1)

gen.CLUSTER = gen.pick_cluster()
ctx = gen.create_ctx()
try:
    schema = gen.pick_schema(ctx)
    for pattern in PATTERNS:
        print(f"\n===== pattern: {pattern} =====")
        rows = gen.run_query(ctx, f"""
            SELECT provider_id, provider_name, city_name, zone_name, country_name
            FROM {schema}.dim_provider_v2
            WHERE LOWER(provider_name) LIKE LOWER('%{pattern}%')
            ORDER BY city_name, provider_name
        """)
        for r in rows:
            print(" | ".join(str(x) for x in r))
        print(f"total: {len(rows)}")
finally:
    gen.destroy_ctx(ctx)
