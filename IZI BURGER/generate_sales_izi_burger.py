#!/usr/bin/env python3
"""
IZI BURGER — короткий звіт про продажі та знижки (одна сторінка).

Фокус — один місяць (FOCUS_MONTH) у порівнянні з попередніми: що сталося з
продажами, які знижки ми давали, як вони вплинули і що варто покращити.
Валюта UAH, мова українська.
"""

from __future__ import annotations

import datetime
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import requests

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST") or "https://bolt-incentives.cloud.databricks.com"
CLUSTER_ID_ENV = os.getenv("DATABRICKS_CLUSTER_ID") or ""
CLUSTER_CANDIDATES = ["0505-112942-d3yviznw", "0221-081903-9ag4bh69"]
SCHEMA_CANDIDATES = ["main.ng_delivery", "ng_delivery_spark"]
SCHEMA_TABLES = [
    "dim_provider_v2",
    "fact_provider_monthly",
    "fact_provider_weekly",
    "dim_order_campaign_delivery",
    "dim_campaign_delivery_v2",
    "delivery_order_order",
]

PROVIDER_IDS = [139923, 169006, 184043]
PARTNER_TITLE = "IZI BURGER"
CITY = "Харків"

# Місяць, на якому фокус звіту, і скільки місяців показувати для контексту.
FOCUS_MONTH = os.getenv("FOCUS_MONTH") or "2026-08"
N_MONTHS = 6

SCRIPT_DIR = Path(__file__).parent
OUTPUT_HTML = SCRIPT_DIR / "Zvit_Prodazhi_Serpen_IziBurger.html"

POLL_INTERVAL_S = 4
MAX_POLL_S = 600
CLUSTER_START_S = 900
USABLE_STATES = {"RUNNING"}
PENDING_STATES = {"PENDING", "RESTARTING", "RESIZING"}

MONTHS_SHORT = ["Січ", "Лют", "Бер", "Квіт", "Трав", "Черв",
                "Лип", "Серп", "Вер", "Жовт", "Лист", "Груд"]
MONTHS_NOM = ["Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
              "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"]
MONTHS_GEN = ["січня", "лютого", "березня", "квітня", "травня", "червня",
              "липня", "серпня", "вересня", "жовтня", "листопада", "грудня"]
MONTHS_LOC = ["січні", "лютому", "березні", "квітні", "травні", "червні",
              "липні", "серпні", "вересні", "жовтні", "листопаді", "грудні"]

# Назви кампаній у Bolt — це довгі технічні рядки з тегами сегментів.
# Групуємо їх у зрозумілі партнеру категорії за характерним маркером.
CAMPAIGN_BUCKETS = [
    ("MD 10%", "Smart Promo 10%", "Плюс-підписники та лояльні клієнти"),
    ("MD 30%", "Меню-знижка 30%", "Ті, хто давно не замовляв, і рідкі клієнти"),
    ("FFD", "Безкоштовна доставка", "Ті ж сегменти, що й меню-знижка"),
    ("Visa", "Visa / ПриватБанк 50%", "Клієнти банку, знижка поверх інших"),
    ("New Users", "20% для нових клієнтів", "Ті, хто вперше замовляє"),
]


def _load_token() -> str:
    token = os.getenv("DATABRICKS_TOKEN", "").strip()
    if token:
        return token
    for profile in ("bolt-incentives-temp", "DEFAULT", "bolt-incentives"):
        try:
            out = subprocess.check_output(
                ["databricks", "auth", "token", "-p", profile],
                text=True, stderr=subprocess.DEVNULL, timeout=30,
            )
            tok = json.loads(out).get("access_token", "").strip()
            if tok:
                return tok
        except Exception:
            pass
    cfg = Path.home() / ".databrickscfg"
    if cfg.exists():
        section = None
        for line in cfg.read_text().splitlines():
            s = line.strip()
            if s.startswith("[") and s.endswith("]"):
                section = s[1:-1]
            elif s.lower().startswith("token") and "=" in s and section:
                tok = s.split("=", 1)[1].strip()
                if tok:
                    return tok
    return ""


DATABRICKS_TOKEN = _load_token()
HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
CLUSTER = CLUSTER_ID_ENV
SCHEMA = ""


# ─── DATABRICKS ────────────────────────────────────────────────────────────────

def _post(path: str, payload: dict) -> dict:
    r = requests.post(f"{DATABRICKS_HOST}{path}", headers=HEADERS, json=payload, timeout=90)
    r.raise_for_status()
    return r.json()


def _get(path: str, params: dict) -> dict:
    r = requests.get(f"{DATABRICKS_HOST}{path}", headers=HEADERS, params=params, timeout=90)
    r.raise_for_status()
    return r.json()


def _cluster_state(cluster_id: str) -> str:
    try:
        return _get("/api/2.0/clusters/get", {"cluster_id": cluster_id}).get("state", "")
    except Exception:
        return ""


def pick_cluster() -> str:
    wanted = ([CLUSTER_ID_ENV] if CLUSTER_ID_ENV else []) + CLUSTER_CANDIDATES
    for cid in wanted:
        if _cluster_state(cid) in USABLE_STATES:
            print(f"  кластер: {cid}")
            return cid
    try:
        listed = _get("/api/2.0/clusters/list", {}).get("clusters", [])
    except Exception:
        listed = []
    for c in listed:
        if c.get("state") in USABLE_STATES and c.get("cluster_source") != "JOB":
            print(f"  кластер: {c['cluster_id']} ({c.get('cluster_name')})")
            return c["cluster_id"]
    for cid in wanted:
        state = _cluster_state(cid)
        if not state:
            continue
        if state not in PENDING_STATES:
            try:
                _post("/api/2.0/clusters/start", {"cluster_id": cid})
            except Exception as exc:
                print(f"    не вдалося запустити {cid}: {exc}")
                continue
        deadline = time.time() + CLUSTER_START_S
        while time.time() < deadline:
            time.sleep(15)
            if _cluster_state(cid) in USABLE_STATES:
                return cid
    raise RuntimeError("Немає доступного кластера Databricks")


def create_context() -> str:
    """Спільний кластер часто віддає 500 на створення контексту — наполягаємо."""
    last = None
    for attempt in range(8):
        try:
            return _post("/api/1.2/contexts/create", {"language": "sql", "clusterId": CLUSTER})["id"]
        except Exception as exc:
            last = exc
            wait = min(15 + attempt * 10, 60)
            print(f"    контекст не створився ({attempt + 1}/8), чекаємо {wait}с…")
            time.sleep(wait)
    raise RuntimeError(f"Не вдалося створити контекст: {last}")


def run_query(ctx_id: str, sql: str) -> list[list]:
    cmd_id = _post("/api/1.2/commands/execute",
                   {"language": "sql", "clusterId": CLUSTER, "contextId": ctx_id, "command": sql})["id"]
    deadline = time.time() + MAX_POLL_S
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL_S)
        resp = _get("/api/1.2/commands/status",
                    {"clusterId": CLUSTER, "contextId": ctx_id, "commandId": cmd_id})
        status = resp.get("status")
        if status == "Finished":
            result = resp.get("results", {})
            if result.get("resultType") == "error":
                raise RuntimeError(result.get("summary", "Query error"))
            return result.get("data", [])
        if status in ("Cancelled", "Error"):
            raise RuntimeError(f"Command {status}: {resp}")
    raise TimeoutError(f"Query timed out after {MAX_POLL_S}s")


def pick_schema(ctx_id: str) -> str:
    """Схему беремо лише якщо читаються всі таблиці звіту.

    Кластер спільний і час від часу віддає тимчасові помилки, тому кожну
    перевірку повторюємо — інакше справна схема виглядала б як недоступна.
    """
    global SCHEMA
    if SCHEMA:
        return SCHEMA
    errors = []
    for schema in SCHEMA_CANDIDATES:
        for attempt in range(3):
            try:
                for table in SCHEMA_TABLES:
                    run_query(ctx_id, f"SELECT 1 FROM {schema}.{table} LIMIT 1")
                print(f"  схема: {schema}")
                SCHEMA = schema
                return SCHEMA
            except Exception as exc:
                errors.append(f"{schema} (спроба {attempt + 1}): {str(exc)[:120]}")
                time.sleep(5)
    raise RuntimeError("Не знайдено схему з усіма потрібними таблицями:\n  " + "\n  ".join(errors))


def destroy_context(ctx_id: str) -> None:
    try:
        _post("/api/1.2/contexts/destroy", {"clusterId": CLUSTER, "contextId": ctx_id})
    except Exception:
        pass


def _sf(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _si(v, default=0) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


# ─── DATES ─────────────────────────────────────────────────────────────────────

def month_range(focus: str, n: int) -> list[str]:
    """Список 'yyyy-MM' — n місяців, останній з яких focus."""
    y, m = int(focus[:4]), int(focus[5:7])
    keys = []
    for _ in range(n):
        keys.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return list(reversed(keys))


def month_short(key: str) -> str:
    return f"{MONTHS_SHORT[int(key[5:7]) - 1]} {key[:4]}"


def month_nom(key: str) -> str:
    return MONTHS_NOM[int(key[5:7]) - 1]


def month_gen(key: str) -> str:
    """Родовий відмінок: «до серпня», «по тижнях серпня»."""
    return MONTHS_GEN[int(key[5:7]) - 1]


def month_loc(key: str) -> str:
    """Місцевий відмінок: «у серпні»."""
    return MONTHS_LOC[int(key[5:7]) - 1]


def last_day(key: str) -> str:
    y, m = int(key[:4]), int(key[5:7])
    nm_y, nm_m = (y + 1, 1) if m == 12 else (y, m + 1)
    return (datetime.date(nm_y, nm_m, 1) - datetime.timedelta(days=1)).isoformat()


# ─── FETCH ─────────────────────────────────────────────────────────────────────

def fetch_data() -> dict:
    keys = month_range(FOCUS_MONTH, N_MONTHS)
    start = f"{keys[0]}-01"
    end = last_day(keys[-1])
    pids_sql = ", ".join(str(p) for p in PROVIDER_IDS)
    pids_str = ", ".join(f"'{p}'" for p in PROVIDER_IDS)

    global CLUSTER
    CLUSTER = pick_cluster()
    ctx = create_context()
    try:
        pick_schema(ctx)
        print(f"  період: {start} → {end}, фокус: {FOCUS_MONTH}")

        fact = run_query(ctx, f"""
        SELECT DATE_FORMAT(metric_timestamp_partition,'yyyy-MM') AS m,
               SUM(delivered_orders_count) AS orders,
               SUM(total_gmv_before_discounts) AS gross,
               SUM(total_gmv_after_discounts) AS net,
               SUM(total_campaign_discount) AS discounts,
               SUM(total_campaign_spend_bolt) AS camp_bolt,
               SUM(total_campaign_spend_provider) AS camp_merch,
               SUM(users_activated_vendor_count) AS new_users,
               SUM(provider_rating_per_order_value*provider_rating_per_order_weight)
                 /NULLIF(SUM(provider_rating_per_order_weight),0) AS rating,
               SUM(provider_preparation_minutes_per_order_value*provider_preparation_minutes_per_order_weight)
                 /NULLIF(SUM(provider_preparation_minutes_per_order_weight),0) AS prep,
               SUM(customer_refunded_order_rate_value*customer_refunded_order_rate_weight)
                 /NULLIF(SUM(customer_refunded_order_rate_weight),0)*100 AS refunds
        FROM {SCHEMA}.fact_provider_monthly
        WHERE provider_id IN ({pids_sql})
          AND metric_timestamp_partition >= '{start}' AND metric_timestamp_partition <= '{end}'
        GROUP BY 1 ORDER BY 1
        """)

        users = run_query(ctx, f"""
        SELECT DATE_FORMAT(metric_timestamp_partition,'yyyy-MM') AS m,
               SUM(provider_deliveries_unique_user_count) AS au
        FROM {SCHEMA}.int_provider_metrics_non_additive
        WHERE entity_id IN ({pids_str}) AND timeframe_name='month'
          AND metric_timestamp_partition >= '{start}' AND metric_timestamp_partition <= '{end}'
        GROUP BY 1 ORDER BY 1
        """)

        promo_share = run_query(ctx, f"""
        SELECT DATE_FORMAT(DATE_TRUNC('month', o.created_date),'yyyy-MM') AS m,
               COUNT(*) AS delivered,
               COUNT(DISTINCT p.order_id) AS promo_orders
        FROM {SCHEMA}.delivery_order_order o
        LEFT JOIN (SELECT DISTINCT order_id FROM {SCHEMA}.dim_order_campaign_delivery
                   WHERE provider_id IN ({pids_sql})
                     AND order_created_date >= '{start}' AND order_created_date <= '{end}') p
               ON p.order_id = o.id
        WHERE o.provider_id IN ({pids_sql}) AND o.state='delivered'
          AND o.created_date >= '{start}' AND o.created_date <= '{end}'
        GROUP BY 1 ORDER BY 1
        """)

        campaigns = run_query(ctx, f"""
        SELECT COALESCE(c.campaign_name,'—') AS name,
               COUNT(DISTINCT d.order_id) AS orders,
               SUM(d.campaign_discount) AS discount,
               SUM(d.campaign_spend_provider) AS provider_pays
        FROM {SCHEMA}.dim_order_campaign_delivery d
        LEFT JOIN {SCHEMA}.dim_campaign_delivery_v2 c ON c.campaign_id = d.campaign_id
        WHERE d.provider_id IN ({pids_sql})
          AND d.order_created_date >= '{FOCUS_MONTH}-01'
          AND d.order_created_date <= '{last_day(FOCUS_MONTH)}'
        GROUP BY 1
        """)

        weekly = run_query(ctx, f"""
        SELECT DATE_FORMAT(DATE_TRUNC('week', metric_timestamp_partition),'yyyy-MM-dd') AS w,
               SUM(delivered_orders_count) AS orders,
               SUM(total_gmv_before_discounts) AS gross,
               SUM(total_campaign_discount) AS discounts
        FROM {SCHEMA}.fact_provider_weekly
        WHERE provider_id IN ({pids_sql})
          AND metric_timestamp_partition >= '{FOCUS_MONTH}-01'
          AND metric_timestamp_partition <= '{last_day(FOCUS_MONTH)}'
        GROUP BY 1 ORDER BY 1
        """)

        bad = run_query(ctx, f"""
        SELECT DATE_FORMAT(DATE_TRUNC('month', o.created_date),'yyyy-MM') AS m,
               SUM(CASE WHEN f.is_bad_order AND LOWER(COALESCE(a.bad_order_actor_at_fault,''))='provider'
                        THEN 1 ELSE 0 END) AS bad_provider
        FROM {SCHEMA}.delivery_order_order o
        INNER JOIN {SCHEMA}.fact_order_delivery f ON f.order_id=o.id
        LEFT JOIN {SCHEMA}.int_order_bad_order_attribution a ON a.order_id=o.id
        WHERE o.provider_id IN ({pids_sql})
          AND o.created_date >= '{start}' AND o.created_date <= '{end}'
        GROUP BY 1 ORDER BY 1
        """)

        locs = run_query(ctx, f"""
        SELECT d.provider_name,
               DATE_FORMAT(f.metric_timestamp_partition,'yyyy-MM') AS m,
               SUM(f.delivered_orders_count) AS orders,
               SUM(f.total_gmv_before_discounts) AS gross
        FROM {SCHEMA}.fact_provider_monthly f
        JOIN {SCHEMA}.dim_provider_v2 d ON d.provider_id = f.provider_id
        WHERE f.provider_id IN ({pids_sql})
          AND f.metric_timestamp_partition >= '{start}' AND f.metric_timestamp_partition <= '{end}'
        GROUP BY 1,2
        """)
    finally:
        destroy_context(ctx)

    months: dict[str, dict] = {k: {"key": k, "label": month_short(k)} for k in keys}
    for r in fact:
        k = str(r[0])
        if k not in months:
            continue
        months[k].update({
            "orders": _si(r[1]), "gross": _sf(r[2]), "net": _sf(r[3]),
            "discounts": _sf(r[4]), "camp_bolt": _sf(r[5]), "camp_merch": _sf(r[6]),
            "new_users": _si(r[7]), "rating": _sf(r[8]), "prep": _sf(r[9]),
            "refunds": _sf(r[10]),
        })
    for r in users:
        if str(r[0]) in months:
            months[str(r[0])]["active_users"] = _si(r[1])
    for r in promo_share:
        if str(r[0]) in months:
            d, p = _si(r[1]), _si(r[2])
            months[str(r[0])]["promo_share"] = round(p / d * 100, 1) if d else 0.0
    for r in bad:
        if str(r[0]) in months:
            months[str(r[0])]["bad_provider"] = _si(r[1])

    series = []
    for k in keys:
        m = months[k]
        m.setdefault("orders", 0)
        for f in ("gross", "net", "discounts", "camp_bolt", "camp_merch", "rating", "prep", "refunds"):
            m.setdefault(f, 0.0)
        for f in ("new_users", "active_users", "bad_provider"):
            m.setdefault(f, 0)
        m.setdefault("promo_share", 0.0)
        m["aov"] = round(m["gross"] / m["orders"]) if m["orders"] else 0
        m["net_per_order"] = round(m["net"] / m["orders"]) if m["orders"] else 0
        m["disc_share"] = round(m["discounts"] / m["gross"] * 100, 1) if m["gross"] else 0.0
        m["bad_provider_pct"] = round(m["bad_provider"] / m["orders"] * 100, 2) if m["orders"] else 0.0
        series.append(m)

    # Кампанії фокусного місяця → зрозумілі категорії
    buckets: dict[str, dict] = {}
    for r in campaigns:
        name = str(r[0])
        label, audience = "Інші кампанії", "Різні сегменти"
        for marker, lbl, aud in CAMPAIGN_BUCKETS:
            if marker in name:
                label, audience = lbl, aud
                break
        b = buckets.setdefault(label, {
            "label": label, "audience": audience,
            "orders": 0, "discount": 0.0, "provider_pays": 0.0,
        })
        b["orders"] += _si(r[1])
        b["discount"] += _sf(r[2])
        b["provider_pays"] += _sf(r[3])
    campaign_list = sorted(buckets.values(), key=lambda b: b["discount"], reverse=True)
    total_disc = sum(b["discount"] for b in campaign_list) or 1.0
    for b in campaign_list:
        b["share"] = round(b["discount"] / total_disc * 100)
        b["provider_pct"] = round(b["provider_pays"] / b["discount"] * 100) if b["discount"] else 0

    weeks = [{"start": str(r[0]), "orders": _si(r[1]),
              "gross": _sf(r[2]), "discounts": _sf(r[3])} for r in weekly]

    by_loc: dict[str, dict] = {}
    for r in locs:
        by_loc.setdefault(str(r[0]), {})[str(r[1])] = {"orders": _si(r[2]), "gross": _sf(r[3])}
    loc_list = []
    prev_key = keys[-2]
    for name, data in by_loc.items():
        cur = data.get(FOCUS_MONTH, {"orders": 0, "gross": 0.0})
        prv = data.get(prev_key, {"orders": 0, "gross": 0.0})
        loc_list.append({
            "name": name, "orders": cur["orders"], "gross": cur["gross"],
            "growth": round((cur["gross"] / prv["gross"] - 1) * 100) if prv["gross"] else None,
        })
    loc_list.sort(key=lambda x: x["gross"], reverse=True)

    return {
        "series": series,
        "campaigns": campaign_list,
        "weeks": weeks,
        "locations": loc_list,
        "generated_at": datetime.datetime.now().strftime("%d.%m.%Y"),
    }


# ─── HTML ───────────────────────────────────────────────────────────────────────

def num(v: float, digits: int = 0) -> str:
    return f"{v:,.{digits}f}".replace(",", "\u202f")


def dec(v: float, digits: int = 1) -> str:
    """Дробові числа українською — через кому."""
    return f"{v:.{digits}f}".replace(".", ",")


def delta(cur: float, prev: float) -> tuple[str, str]:
    """Повертає (текст, клас) для зміни місяць-до-місяця."""
    if not prev:
        return "—", "flat"
    pct = (cur - prev) / prev * 100
    cls = "up" if pct > 1 else ("down" if pct < -1 else "flat")
    return f"{pct:+.0f}%", cls


def bar_chart(series: list[dict], key: str, fmt, focus_key: str, unit: str = "") -> str:
    vals = [float(m[key]) for m in series]
    top = max(vals) if vals and max(vals) > 0 else 1.0
    bars = ""
    for m in series:
        v = float(m[key])
        h = max(3, round(v / top * 100))
        is_focus = m["key"] == focus_key
        bars += f"""
        <div class="bar-col{' is-focus' if is_focus else ''}">
          <div class="bar-val">{fmt(v)}</div>
          <div class="bar" style="height:{h}%"></div>
          <div class="bar-lbl">{m['label']}</div>
        </div>"""
    return f'<div class="bars">{bars}</div><div class="chart-unit">{unit}</div>'


def dual_chart(series: list[dict], focus_key: str) -> str:
    """Gross і Net поруч — видно, скільки обороту зʼїдає знижка."""
    top = max([m["gross"] for m in series] or [1]) or 1
    bars = ""
    for m in series:
        gh = max(3, round(m["gross"] / top * 100))
        nh = max(3, round(m["net"] / top * 100))
        focus = " is-focus" if m["key"] == focus_key else ""
        bars += f"""
        <div class="bar-col pair{focus}">
          <div class="bar-val">{num(m['gross'] / 1000)}к</div>
          <div class="pair-bars">
            <div class="bar bar-gross" style="height:{gh}%"></div>
            <div class="bar bar-net" style="height:{nh}%"></div>
          </div>
          <div class="bar-lbl">{m['label']}</div>
        </div>"""
    return f'<div class="bars">{bars}</div>'


def week_chart(weeks: list[dict]) -> str:
    top = max([w["orders"] for w in weeks] or [1]) or 1
    bars = ""
    for w in weeks:
        d = datetime.date.fromisoformat(w["start"])
        h = max(3, round(w["orders"] / top * 100))
        disc_h = max(2, round(w["discounts"] / (w["gross"] or 1) * 100))
        bars += f"""
        <div class="bar-col">
          <div class="bar-val">{w['orders']}</div>
          <div class="bar" style="height:{h}%"></div>
          <div class="week-disc" title="частка знижок від обороту">{disc_h}%</div>
          <div class="bar-lbl">{d.day:02d}.{d.month:02d}</div>
        </div>"""
    return f'<div class="bars weeks">{bars}</div>'


def generate_html(data: dict) -> str:
    s = data["series"]
    cur, prev = s[-1], s[-2]
    focus_name = month_nom(FOCUS_MONTH)
    focus_gen, focus_loc = month_gen(FOCUS_MONTH), month_loc(FOCUS_MONTH)
    prev_gen, prev_loc = month_gen(prev["key"]), month_loc(prev["key"])
    first = s[0]

    # Ефективність власних витрат на промо: скільки чистих продажів на 1 ₴
    eff_cur = cur["net"] / cur["camp_merch"] if cur["camp_merch"] else 0
    eff_prev = prev["net"] / prev["camp_merch"] if prev["camp_merch"] else 0

    kpis = [
        ("Замовлення", num(cur["orders"]), "", *delta(cur["orders"], prev["orders"])),
        ("Оборот (Gross)", num(cur["gross"]), "₴", *delta(cur["gross"], prev["gross"])),
        ("Після знижок (Net)", num(cur["net"]), "₴", *delta(cur["net"], prev["net"])),
        ("Середній чек", num(cur["aov"]), "₴", *delta(cur["aov"], prev["aov"])),
        ("Унікальні клієнти", num(cur["active_users"]), "", *delta(cur["active_users"], prev["active_users"])),
        ("Нові клієнти", num(cur["new_users"]), "", *delta(cur["new_users"], prev["new_users"])),
    ]
    kpi_html = ""
    for label, value, unit, dtxt, dcls in kpis:
        kpi_html += f"""
      <div class="kpi">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}<span class="kpi-unit">{unit}</span></div>
        <div class="kpi-delta {dcls}">{dtxt} до {prev_gen}</div>
      </div>"""

    camp_rows = ""
    for c in data["campaigns"]:
        camp_rows += f"""
        <tr>
          <td><b>{c['label']}</b><span class="camp-aud">{c['audience']}</span></td>
          <td class="ta-r">{num(c['orders'])}</td>
          <td class="ta-r">{num(c['discount'])} ₴</td>
          <td class="ta-r"><span class="share-bar" style="--w:{c['share']}%"></span>{c['share']}%</td>
          <td class="ta-r">{c['provider_pct']}%</td>
        </tr>"""

    loc_rows = ""
    for l in data["locations"]:
        g = l["growth"]
        gcls = "up" if (g or 0) > 1 else ("down" if (g or 0) < -1 else "flat")
        loc_rows += f"""
        <tr>
          <td>{l['name']}</td>
          <td class="ta-r">{num(l['orders'])}</td>
          <td class="ta-r">{num(l['gross'])} ₴</td>
          <td class="ta-r {gcls}">{f'{g:+d}%' if g is not None else '—'}</td>
        </tr>"""

    orders_growth = (cur["orders"] / prev["orders"] - 1) * 100 if prev["orders"] else 0
    gross_growth = (cur["gross"] / prev["gross"] - 1) * 100 if prev["gross"] else 0
    net_growth = (cur["net"] / prev["net"] - 1) * 100 if prev["net"] else 0
    npo_drop = (cur["net_per_order"] / prev["net_per_order"] - 1) * 100 if prev["net_per_order"] else 0
    merch_growth = (cur["camp_merch"] / prev["camp_merch"] - 1) * 100 if prev["camp_merch"] else 0

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>{PARTNER_TITLE} · Продажі та знижки · {focus_name} {FOCUS_MONTH[:4]}</title>
<style>
  :root {{
    --green:#34d186; --green-d:#0d8a52; --ink:#16181a; --gray:#6b7280;
    --line:#e8eaed; --bg:#f7f8fa; --warn:#e67e22; --danger:#c0392b; --net:#9fe3c2;
  }}
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    background:var(--bg);color:var(--ink);font-size:13px;line-height:1.5;
    -webkit-font-smoothing:antialiased}}
  .page{{max-width:1180px;margin:0 auto;padding:28px 26px 40px}}

  header{{display:flex;align-items:flex-end;justify-content:space-between;gap:20px;
    flex-wrap:wrap;padding-bottom:14px;border-bottom:3px solid var(--green);margin-bottom:18px}}
  .brand{{display:inline-flex;align-items:center;gap:7px;padding:4px 10px;border-radius:999px;
    background:rgba(52,209,134,.13);color:var(--green-d);font-size:10px;font-weight:700;
    letter-spacing:.08em;text-transform:uppercase;margin-bottom:9px}}
  h1{{font-size:24px;font-weight:700;letter-spacing:-.02em}}
  .sub{{color:var(--gray);margin-top:3px;font-size:12.5px}}
  .head-meta{{text-align:right;color:var(--gray);font-size:11px;line-height:1.8}}
  .head-meta b{{color:var(--ink)}}

  h2{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;
    color:var(--gray);margin:22px 0 10px;display:flex;align-items:center;gap:8px}}
  h2::after{{content:"";flex:1;height:1px;background:var(--line)}}

  .kpis{{display:grid;grid-template-columns:repeat(6,1fr);gap:9px}}
  .kpi{{background:#fff;border:1px solid var(--line);border-radius:11px;padding:11px 12px}}
  .kpi-label{{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.04em;color:var(--gray)}}
  .kpi-value{{font-size:20px;font-weight:700;margin-top:3px;letter-spacing:-.02em}}
  .kpi-unit{{font-size:12px;font-weight:600;color:var(--gray);margin-left:2px}}
  .kpi-delta{{font-size:10.5px;margin-top:2px;font-weight:600}}
  .up{{color:var(--green-d)}} .down{{color:var(--danger)}} .flat{{color:var(--gray)}}

  .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:14px}}
  .grid3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px}}
  .card{{background:#fff;border:1px solid var(--line);border-radius:13px;padding:14px 16px}}
  .card h3{{font-size:12.5px;font-weight:700;margin-bottom:2px}}
  .card .hint{{font-size:11px;color:var(--gray);margin-bottom:10px}}

  .bars{{display:flex;align-items:flex-end;gap:7px;height:122px;margin-top:6px}}
  .bar-col{{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%}}
  .bar-val{{font-size:9.5px;font-weight:700;color:var(--gray);margin-bottom:3px;white-space:nowrap}}
  .bar{{width:100%;max-width:44px;background:#cfe9db;border-radius:4px 4px 0 0;min-height:3px}}
  .bar-lbl{{font-size:9.5px;color:var(--gray);margin-top:4px;white-space:nowrap}}
  .is-focus .bar{{background:var(--green-d)}}
  .is-focus .bar-val{{color:var(--green-d)}}
  .is-focus .bar-lbl{{color:var(--ink);font-weight:700}}
  .pair-bars{{display:flex;align-items:flex-end;gap:2px;width:100%;max-width:44px;height:100%}}
  .bar-gross{{background:#cfe9db}} .bar-net{{background:#8fd9b6}}
  .is-focus .bar-gross{{background:var(--green-d)}} .is-focus .bar-net{{background:var(--green)}}
  .chart-unit{{font-size:10px;color:var(--gray);margin-top:7px}}
  .legend{{display:flex;gap:12px;font-size:10.5px;color:var(--gray);margin-top:8px}}
  .legend i{{width:9px;height:9px;border-radius:2px;display:inline-block;margin-right:4px}}
  .weeks .bar{{background:#f0c9a0}} .weeks .is-focus .bar{{background:var(--warn)}}
  .week-disc{{font-size:9px;color:var(--warn);font-weight:700;margin-top:3px}}

  table{{width:100%;border-collapse:collapse;font-size:12px}}
  th{{text-align:left;font-size:9.5px;text-transform:uppercase;letter-spacing:.05em;color:var(--gray);
    padding:6px 8px;border-bottom:1px solid var(--line);font-weight:700}}
  td{{padding:7px 8px;border-bottom:1px solid #f4f5f6;vertical-align:top}}
  tr:last-child td{{border-bottom:none}}
  .ta-r{{text-align:right;white-space:nowrap}}
  .camp-aud{{display:block;font-size:10px;color:var(--gray);font-weight:400;margin-top:1px}}
  .share-bar{{display:inline-block;width:38px;height:5px;border-radius:3px;background:#eef0f2;
    margin-right:6px;vertical-align:middle;position:relative;overflow:hidden}}
  .share-bar::after{{content:"";position:absolute;inset:0;width:var(--w);background:var(--green)}}

  .takeaway{{background:#f3faf6;border-left:3px solid var(--green);border-radius:0 8px 8px 0;
    padding:9px 12px;font-size:12px;margin-top:11px}}
  .takeaway.warn{{background:#fff8f0;border-left-color:var(--warn)}}

  .advice{{display:grid;grid-template-columns:repeat(2,1fr);gap:12px}}
  .tip{{background:#fff;border:1px solid var(--line);border-radius:13px;padding:13px 15px;
    border-left:3px solid var(--green)}}
  .tip.b{{border-left-color:var(--warn)}}
  .tip-h{{display:flex;align-items:baseline;justify-content:space-between;gap:10px;margin-bottom:5px}}
  .tip-h h4{{font-size:12.5px;font-weight:700}}
  .tip-num{{font-size:11px;font-weight:700;color:var(--warn);white-space:nowrap}}
  .tip p{{font-size:11.5px;color:#41464b}}
  .tip .why{{font-size:10.5px;color:var(--gray);margin-top:5px;padding-top:5px;border-top:1px dashed var(--line)}}

  footer{{margin-top:22px;padding-top:12px;border-top:1px solid var(--line);
    font-size:10.5px;color:var(--gray);display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap}}

  @media(max-width:900px){{
    .kpis{{grid-template-columns:repeat(3,1fr)}}
    .grid2,.grid3,.advice{{grid-template-columns:1fr}}
  }}
  @media print{{
    body{{background:#fff;font-size:11px}}
    .page{{padding:0;max-width:none}}
    .card,.tip,.kpi{{break-inside:avoid}}
    h2{{margin:12px 0 7px}}
  }}
</style>
</head>
<body>
<div class="page">

  <header>
    <div>
      <div class="brand">Bolt Food · {CITY}</div>
      <h1>{PARTNER_TITLE} — продажі та знижки</h1>
      <p class="sub">Фокус: <b>{focus_name} {FOCUS_MONTH[:4]}</b> · порівняння з попередніми місяцями</p>
    </div>
    <div class="head-meta">
      <div>Локацій: <strong>{len(data['locations'])}</strong></div>
      <div>Період: <strong>{first['label']} — {cur['label']}</strong></div>
      <div>Оновлено: <strong>{data['generated_at']}</strong></div>
    </div>
  </header>

  <div class="kpis">{kpi_html}</div>

  <h2>1. Продажі місяць до місяця</h2>
  <div class="grid2">
    <div class="card">
      <h3>Оборот і сума після знижок</h3>
      <p class="hint">Темна колонка — {focus_name.lower()}. Розрив між колонками — це знижки.</p>
      {dual_chart(s, FOCUS_MONTH)}
      <div class="legend">
        <span><i style="background:#cfe9db"></i>Оборот (Gross)</span>
        <span><i style="background:#8fd9b6"></i>Після знижок (Net)</span>
      </div>
    </div>
    <div class="card">
      <h3>Доставлені замовлення</h3>
      <p class="hint">Скільки замовлень виконано за місяць</p>
      {bar_chart(s, 'orders', lambda v: num(v), FOCUS_MONTH, 'штук на місяць')}
    </div>
  </div>
  <div class="takeaway">
    <b>{focus_name} — найкращий місяць за пів року.</b> Замовлення {num(prev['orders'])} → {num(cur['orders'])}
    ({orders_growth:+.0f}%), оборот {num(prev['gross'])} → {num(cur['gross'])} ₴ ({gross_growth:+.0f}%).
    Але сума після знижок зросла лише на {net_growth:+.0f}%, бо знижок теж стало більше.
  </div>

  <h2>2. Що ми робили зі знижками</h2>
  <div class="grid2">
    <div class="card">
      <h3>Знижки в кампаніях за {focus_name.lower()}</h3>
      <p class="hint">Останній стовпець — скільки з цієї знижки оплатив заклад, решту доклав Bolt</p>
      <table>
        <thead><tr><th>Кампанія</th><th class="ta-r">Замовлень</th><th class="ta-r">Знижка</th>
        <th class="ta-r">Частка</th><th class="ta-r">Платить заклад</th></tr></thead>
        <tbody>{camp_rows}</tbody>
      </table>
    </div>
    <div class="card">
      <h3>Частка замовлень зі знижкою</h3>
      <p class="hint">Скільки відсотків усіх замовлень пройшло з промо</p>
      {bar_chart(s, 'promo_share', lambda v: f'{v:.0f}%', FOCUS_MONTH, '% замовлень із промо')}
    </div>
  </div>
  <div class="takeaway warn">
    <b>Знижки стали основним двигуном.</b> Промо було в {cur['promo_share']:.0f}% замовлень проти
    {prev['promo_share']:.0f}% у {prev_loc}. Знижки зʼїли {cur['disc_share']:.0f}% обороту
    (було {prev['disc_share']:.0f}%), а витрати самого закладу на промо зросли з
    {num(prev['camp_merch'])} до {num(cur['camp_merch'])} ₴ ({merch_growth:+.0f}%).
  </div>

  <h2>3. Як це вплинуло — по тижнях {focus_gen}</h2>
  <div class="grid2">
    <div class="card">
      <h3>Замовлення по тижнях</h3>
      <p class="hint">Помаранчевим під колонкою — частка знижок від обороту тижня</p>
      {week_chart(data['weeks'])}
    </div>
    <div class="card">
      <h3>Продажі по локаціях за {focus_name.lower()}</h3>
      <p class="hint">Зростання обороту до {prev_gen}</p>
      <table>
        <thead><tr><th>Локація</th><th class="ta-r">Замовлень</th><th class="ta-r">Оборот</th>
        <th class="ta-r">Динаміка</th></tr></thead>
        <tbody>{loc_rows}</tbody>
      </table>
    </div>
  </div>
  <div class="takeaway">
    Пік припав на тиждень із найбільшою знижкою, далі замовлення плавно знижувалися разом із промо.
    Попит реагує на знижку майже миттєво — тому головне питання не «чи працює промо»,
    а <b>чи повернуться ці клієнти без нього</b>.
  </div>

  <h2>4. Що покращити далі</h2>
  <div class="advice">

    <div class="tip">
      <div class="tip-h"><h4>1. Утримати нових клієнтів</h4>
        <span class="tip-num">{num(cur['new_users'])} нових</span></div>
      <p>Знижки привели {num(cur['new_users'])} нових клієнтів — найбільше за пів року.
      Головна цінність {focus_gen} не в обороті, а в цій базі.
      Просіть менеджера Bolt запустити окремий офер «на друге замовлення» саме для них.</p>
      <p class="why">Чому: утримати клієнта дешевше, ніж купувати нового знижкою щомісяця.</p>
    </div>

    <div class="tip b">
      <div class="tip-h"><h4>2. Повернути маржу</h4>
        <span class="tip-num">{num(prev['net_per_order'])} → {num(cur['net_per_order'])} ₴ / замовлення</span></div>
      <p>Чистими з одного замовлення стало на {abs(npo_drop):.0f}% менше.
      Промо було вже в {cur['promo_share']:.0f}% замовлень — це майже кожне.
      Варто знижувати глибину поступово: почати зі Smart Promo 10%, де заклад платить усі 100%.</p>
      <p class="why">Чому: на 1 ₴ власних витрат на промо припадало {eff_prev:.0f} ₴ чистих продажів
      у {prev_loc} і лише {dec(eff_cur)} ₴ у {focus_loc}.</p>
    </div>

    <div class="tip b">
      <div class="tip-h"><h4>3. Втримати якість під навантаженням</h4>
        <span class="tip-num">погані замовлення {prev['bad_provider']} → {cur['bad_provider']}</span></div>
      <p>Обсяг зріс майже вдвічі, і кухня почала не встигати: приготування
      {dec(prev['prep'])} → {dec(cur['prep'])} хв, компенсації клієнтам
      {dec(prev['refunds'])}% → {dec(cur['refunds'])}%, рейтинг {dec(prev['rating'], 2)} → {dec(cur['rating'], 2)}.
      Додайте людину на кухню в пікові години перед наступною акцією.</p>
      <p class="why">Чому: рейтинг нижче 4,7 помітно зменшує показ закладу в застосунку.</p>
    </div>

    <div class="tip">
      <div class="tip-h"><h4>4. Підняти середній чек</h4>
        <span class="tip-num">{num(prev['aov'])} → {num(cur['aov'])} ₴</span></div>
      <p>Середній чек не зрушив попри всі знижки — клієнти брали те саме, тільки дешевше.
      Замість «мінус 30% на все» спробуйте комбо-набори та поріг безкоштовної доставки
      трохи вище поточного чека.</p>
      <p class="why">Чому: це піднімає оборот без додаткових витрат на знижку.</p>
    </div>

  </div>

  <footer>
    <span>Bolt Food · {PARTNER_TITLE} · {CITY} · дані Bolt за {first['label']} — {cur['label']}</span>
    <span>Питання по звіту — до вашого менеджера Bolt Food</span>
  </footer>

</div>
</body>
</html>"""


def main() -> None:
    global DATABRICKS_TOKEN, HEADERS
    if not DATABRICKS_TOKEN:
        DATABRICKS_TOKEN = _load_token()
        HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN не задано.", file=sys.stderr)
        sys.exit(1)

    print(f"{PARTNER_TITLE} — звіт про продажі, фокус {FOCUS_MONTH}")
    data = fetch_data()
    OUTPUT_HTML.write_text(generate_html(data), encoding="utf-8")
    print(f"\n→ {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
