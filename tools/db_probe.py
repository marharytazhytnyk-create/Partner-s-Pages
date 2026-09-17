#!/usr/bin/env python3
"""Діагностика доступу CI-токена до Databricks: хто ми, що бачимо, що можемо запустити."""

import json
import os
import sys

import requests

HOST = (os.getenv("DATABRICKS_HOST") or "https://bolt-incentives.cloud.databricks.com").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN", "").strip()
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

REPORT_CLUSTERS = ["0221-081903-9ag4bh69", "0505-112942-d3yviznw"]


def probe(method: str, path: str, **kw):
    try:
        r = requests.request(method, f"{HOST}{path}", headers=HEADERS, timeout=60, **kw)
        body = r.text[:600]
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, body
    except Exception as exc:
        return "EXC", str(exc)


def main():
    if not TOKEN:
        print("ERROR: DATABRICKS_TOKEN not set")
        sys.exit(1)

    code, me = probe("GET", "/api/2.0/preview/scim/v2/Me")
    who = me.get("userName") if isinstance(me, dict) else me
    print(f"[identity] {code} → {who}")

    code, whs = probe("GET", "/api/2.0/sql/warehouses")
    print(f"[warehouses] {code} → keys={list(whs) if isinstance(whs, dict) else whs}")
    if isinstance(whs, dict):
        for w in whs.get("warehouses", []):
            print(f"    · {w.get('id')} {w.get('name')} state={w.get('state')} "
                  f"serverless={w.get('enable_serverless_compute')}")

    code, cls = probe("GET", "/api/2.0/clusters/list")
    if isinstance(cls, dict):
        items = cls.get("clusters", [])
        print(f"[clusters/list] {code} → {len(items)} шт.")
        for c in items[:40]:
            print(f"    · {c.get('cluster_id')} {c.get('cluster_name')!r} "
                  f"state={c.get('state')} source={c.get('cluster_source')}")
    else:
        print(f"[clusters/list] {code} → {cls}")

    for cid in REPORT_CLUSTERS:
        code, st = probe("GET", "/api/2.0/clusters/get", params={"cluster_id": cid})
        state = st.get("state") if isinstance(st, dict) else st
        print(f"[cluster {cid}] get={code} state={state}")
        code, lvl = probe("GET", f"/api/2.0/permissions/clusters/{cid}/permissionLevels")
        levels = ([p.get("permission_level") for p in lvl.get("permission_levels", [])]
                  if isinstance(lvl, dict) else lvl)
        print(f"[cluster {cid}] my permission levels={code} {levels}")

    code, res = probe("GET", "/api/2.0/permissions/warehouses")
    print(f"[warehouse perms] {code} → {json.dumps(res)[:200] if isinstance(res, dict) else res}")


if __name__ == "__main__":
    main()
