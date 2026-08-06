#!/usr/bin/env python3
"""
MBR IZI BURGER — щомісячний звіт (Харків).
Останні 6 завершених місяців, HTML українською, валюта UAH.
Автооновлення: 1-го числа кожного місяця о 14:00 (Київ) через GitHub Actions.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import requests

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST") or "https://bolt-incentives.cloud.databricks.com"
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID") or "0221-081903-9ag4bh69"

PROVIDER_IDS = [139923, 169006, 184043]
PARTNER_TITLE = "IZI BURGER"
CITY = "Kharkiv"
N_MONTHS = 6
SCRIPT_DIR = Path(__file__).parent
OUTPUT_HTML = SCRIPT_DIR / "MBR_IziBurger.html"
POLL_INTERVAL_S = 4
MAX_POLL_S = 600

MONTH_BAR_COLORS = [
    "#0a5c38", "#0d8a52", "#12a35f", "#34D186", "#5cdb9a", "#7ee0ad",
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

CHART_SECTIONS: list[tuple[str, list[str]]] = [
    ("1. Продажі", ["gross", "net", "orders", "aov"]),
    ("2. Операційні показники", [
        "avail", "accept", "refunds",
        "del_time", "acc_time", "prep_time", "wait_time",
    ]),
    ("2a. Погані замовлення з вини закладу", ["bad_provider_count", "bad_provider_pct"]),
    ("3. Клієнти", ["active_users", "freq", "new_users", "sessions", "imp_menu", "menu_prod", "rating"]),
    ("4. Знижки", ["discounts", "camp_bolt", "camp_merch"]),
]

BAD_ORDERS_EXPLAIN_UA = (
    "Погані замовлення (Bad Orders) — доставлені замовлення, з якими у клієнта виникла проблема: "
    "затримка з вини закладу, неповний склад, холодна/неякісна їжа тощо. "
    "Високий показник знижує рейтинг і зменшує шанс повторного замовлення."
)

METRIC_UK: dict[str, tuple[str, str, str]] = {
    "gross": ("Gross Sales", "Сума до знижок", "₴"),
    "net": ("Net Sales", "Сума після знижок", "₴"),
    "orders": ("Delivered Orders", "Доставлені замовлення", "шт."),
    "aov": ("AOV (середній чек)", "Середня сума замовлення", "₴"),
    "avail": ("Availability", "Частка часу онлайн", "%"),
    "accept": ("Acceptance Rate", "Прийняття замовлень вчасно", "%"),
    "refunds": ("Refunds Rate", "Компенсації клієнтам", "%"),
    "del_time": ("Avg. Delivery Time", "Середній час доставки", "хв"),
    "acc_time": ("Avg. Acceptance Time", "Середній час прийняття", "хв"),
    "prep_time": ("Avg. Prep Time", "Середній час приготування", "хв"),
    "wait_time": ("Courier Wait Time", "Очікування курʼєром", "хв"),
    "active_users": ("Active Users", "Унікальних клієнтів", "осіб"),
    "freq": ("Order Frequency", "Замовлень на клієнта", "зам."),
    "new_users": ("New Users", "Нові клієнти", "осіб"),
    "sessions": ("Sessions", "Перегляди в застосунку", "сесій"),
    "imp_menu": ("Impression → Menu", "Відкрили меню з переглядів", "%"),
    "menu_prod": ("Menu → Cart", "Додали в кошик з меню", "%"),
    "rating": ("Rating", "Середня оцінка", "з 5"),
    "discounts": ("Total Discounts", "Знижки для клієнтів", "₴"),
    "camp_bolt": ("Campaigns by Bolt", "Витрати Bolt на промо", "₴"),
    "camp_merch": ("Campaigns by Merchant", "Витрати партнера на промо", "₴"),
    "bad_provider_count": ("Погані замовлення (шт.)", "Кількість поганих замовлень з вини закладу", "шт."),
    "bad_provider_pct": ("Погані замовлення (%)", "Відсоток поганих замовлень з вини закладу", "%"),
}

EMPTY_MONTH = {
    "orders": 0, "gross": 0, "net": 0, "aov": 0,
    "avail": 0, "accept": 0, "refunds": 0,
    "del_time": 0, "acc_time": 0, "prep_time": 0, "wait_time": 0,
    "new_users": 0, "sessions": 0, "imp_menu": 0, "menu_prod": 0, "rating": 0,
    "discounts": 0, "camp_bolt": 0, "camp_merch": 0, "active_users": 0, "freq": 0,
    "bad_provider_count": 0, "bad_provider_pct": 0.0, "rating_weight": 0.0,
}


# ─── DATES ─────────────────────────────────────────────────────────────────────

def last_n_completed_months(n: int = N_MONTHS) -> list[tuple[datetime.date, datetime.date]]:
    today = datetime.date.today()
    # Start from the beginning of current month, go back
    first_of_current = today.replace(day=1)
    months = []
    for i in range(n):
        last_day_of_month = first_of_current - datetime.timedelta(days=1)
        first_day_of_month = last_day_of_month.replace(day=1)
        months.append((first_day_of_month, last_day_of_month))
        first_of_current = first_day_of_month
    return list(reversed(months))


def month_label(start: datetime.date) -> str:
    MONTHS_UA = ["Січ", "Лют", "Бер", "Квіт", "Трав", "Черв",
                 "Лип", "Серп", "Вер", "Жовт", "Лист", "Груд"]
    return f"{MONTHS_UA[start.month - 1]} {start.year}"


# ─── DATABRICKS ────────────────────────────────────────────────────────────────

def _post(path: str, payload: dict) -> dict:
    r = requests.post(f"{DATABRICKS_HOST}{path}", headers=HEADERS, json=payload, timeout=90)
    r.raise_for_status()
    return r.json()


def _get(path: str, params: dict) -> dict:
    r = requests.get(f"{DATABRICKS_HOST}{path}", headers=HEADERS, params=params, timeout=90)
    r.raise_for_status()
    return r.json()


def ensure_cluster_running() -> None:
    st = _get("/api/2.0/clusters/get", {"cluster_id": CLUSTER_ID})
    state = st.get("state")
    if state == "RUNNING":
        return
    if state in ("TERMINATED", "TERMINATING"):
        _post("/api/2.0/clusters/start", {"cluster_id": CLUSTER_ID})
    deadline = time.time() + 900
    while time.time() < deadline:
        time.sleep(10)
        state = _get("/api/2.0/clusters/get", {"cluster_id": CLUSTER_ID}).get("state")
        print(f"  cluster: {state}")
        if state == "RUNNING":
            return
    raise TimeoutError("Cluster did not start in time")


def create_context() -> str:
    return _post("/api/1.2/contexts/create", {"language": "sql", "clusterId": CLUSTER_ID})["id"]


def run_query(ctx_id: str, sql: str) -> list[list]:
    cmd_id = _post("/api/1.2/commands/execute",
                   {"language": "sql", "clusterId": CLUSTER_ID, "contextId": ctx_id, "command": sql})["id"]
    deadline = time.time() + MAX_POLL_S
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL_S)
        resp = _get("/api/1.2/commands/status",
                    {"clusterId": CLUSTER_ID, "contextId": ctx_id, "commandId": cmd_id})
        status = resp.get("status")
        if status == "Finished":
            result = resp.get("results", {})
            if result.get("resultType") == "error":
                raise RuntimeError(result.get("summary", "Query error"))
            return result.get("data", [])
        if status in ("Cancelled", "Error"):
            raise RuntimeError(f"Command {status}: {resp}")
    raise TimeoutError(f"Query timed out after {MAX_POLL_S}s")


def destroy_context(ctx_id: str) -> None:
    try:
        _post("/api/1.2/contexts/destroy", {"clusterId": CLUSTER_ID, "contextId": ctx_id})
    except Exception:
        pass


def _sf(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _si(v, default=0) -> int:
    return int(round(_sf(v, default)))


# ─── FETCH ─────────────────────────────────────────────────────────────────────

def fetch_data() -> dict:
    months = last_n_completed_months(N_MONTHS)
    global_start = months[0][0].isoformat()
    global_end = months[-1][1].isoformat()
    month_keys = [m[0].isoformat() for m in months]
    month_labels_list = [month_label(m[0]) for m in months]

    pids_sql = ", ".join(str(p) for p in PROVIDER_IDS)
    pids_str = ", ".join(f"'{p}'" for p in PROVIDER_IDS)

    ensure_cluster_running()
    ctx = create_context()
    try:
        # Get provider names
        loc_rows = run_query(ctx, f"""
        SELECT provider_id, provider_name, brand_name, city_name, zone_name
        FROM ng_delivery_spark.dim_provider_v2
        WHERE provider_id IN ({pids_sql})
        ORDER BY provider_name
        """)

        providers = [
            {"provider_id": int(r[0]), "name": str(r[1]) if r[1] else f"ID {r[0]}",
             "brand": str(r[2]), "city": str(r[3] or ""), "zone": str(r[4] or "")}
            for r in loc_rows
        ]
        print(f"  локацій: {len(providers)}, період: {global_start} → {global_end}")

        # Monthly grain: weekly rows straddle month boundaries, so a month would get
        # 4 or 5 whole weeks depending on where Mondays fall. Use the monthly fact table.
        fact_rows = run_query(ctx, f"""
        SELECT
            f.provider_id,
            d.provider_name,
            DATE_FORMAT(DATE_TRUNC('month', f.metric_timestamp_partition), 'yyyy-MM-dd') AS month_start,
            SUM(f.delivered_orders_count) AS orders,
            SUM(f.total_gmv_before_discounts) AS gross,
            SUM(f.total_gmv_after_discounts) AS net,
            SUM(f.provider_active_rate_value * f.provider_active_rate_weight) /
              NULLIF(SUM(f.provider_active_rate_weight), 0) * 100 AS avail,
            SUM(f.provider_acceptance_rate_value * f.provider_acceptance_rate_weight) /
              NULLIF(SUM(f.provider_acceptance_rate_weight), 0) * 100 AS accept,
            SUM(f.customer_refunded_order_rate_value * f.customer_refunded_order_rate_weight) /
              NULLIF(SUM(f.customer_refunded_order_rate_weight), 0) * 100 AS refunds,
            SUM(f.order_total_minutes_per_order_value * f.order_total_minutes_per_order_weight) /
              NULLIF(SUM(f.order_total_minutes_per_order_weight), 0) AS del_time,
            SUM(f.provider_acceptance_minutes_per_order_value * f.provider_acceptance_minutes_per_order_weight) /
              NULLIF(SUM(f.provider_acceptance_minutes_per_order_weight), 0) AS acc_time,
            SUM(f.provider_preparation_minutes_per_order_value * f.provider_preparation_minutes_per_order_weight) /
              NULLIF(SUM(f.provider_preparation_minutes_per_order_weight), 0) AS prep_time,
            SUM(f.courier_total_wait_minutes_per_order_value * f.courier_total_wait_minutes_per_order_weight) /
              NULLIF(SUM(f.courier_total_wait_minutes_per_order_weight), 0) AS wait_time,
            SUM(f.users_activated_vendor_count) AS new_users,
            SUM(f.provider_impressions_sessions_count) AS sessions,
            SUM(f.provider_menu_viewed_sessions_count) AS menu_viewed,
            SUM(f.provider_product_added_from_menu_viewed_rate_value * f.provider_product_added_from_menu_viewed_rate_weight) /
              NULLIF(SUM(f.provider_product_added_from_menu_viewed_rate_weight), 0) * 100 AS menu_prod,
            SUM(f.provider_rating_per_order_value * f.provider_rating_per_order_weight) /
              NULLIF(SUM(f.provider_rating_per_order_weight), 0) AS rating,
            SUM(f.total_campaign_discount) AS discounts,
            SUM(f.total_campaign_spend_bolt) AS camp_bolt,
            SUM(f.total_campaign_spend_provider) AS camp_merch,
            SUM(f.provider_rating_per_order_weight) AS rating_weight
        FROM ng_delivery_spark.fact_provider_monthly f
        JOIN ng_delivery_spark.dim_provider_v2 d ON f.provider_id = d.provider_id
        WHERE f.provider_id IN ({pids_sql})
          AND f.metric_timestamp_partition >= '{global_start}'
          AND f.metric_timestamp_partition <= '{global_end}'
        GROUP BY 1, 2, 3
        ORDER BY d.provider_name, 3
        """)

        users_rows = run_query(ctx, f"""
        SELECT
            entity_id AS provider_id,
            DATE_FORMAT(DATE_TRUNC('month', metric_timestamp_partition), 'yyyy-MM-dd') AS month_start,
            SUM(provider_deliveries_unique_user_count) AS active_users
        FROM ng_delivery_spark.int_provider_metrics_non_additive
        WHERE entity_id IN ({pids_str})
          AND timeframe_name = 'month'
          AND metric_timestamp_partition >= '{global_start}'
          AND metric_timestamp_partition <= '{global_end}'
        GROUP BY 1, 2
        ORDER BY 1, 2
        """)

        try:
            bad_rows = run_query(ctx, f"""
            SELECT
                o.provider_id,
                DATE_FORMAT(DATE_TRUNC('month', o.created_date), 'yyyy-MM-dd') AS month_start,
                SUM(CASE WHEN o.state = 'delivered' THEN 1 ELSE 0 END) AS delivered,
                SUM(CASE
                    WHEN f.is_bad_order = true
                     AND LOWER(COALESCE(a.bad_order_actor_at_fault, '')) = 'provider'
                    THEN 1 ELSE 0
                END) AS bad_provider
            FROM ng_delivery_spark.delivery_order_order o
            INNER JOIN ng_delivery_spark.fact_order_delivery f ON f.order_id = o.id
            LEFT JOIN ng_delivery_spark.int_order_bad_order_attribution a ON a.order_id = o.id
            WHERE o.provider_id IN ({pids_sql})
              AND o.created_date >= '{global_start}'
              AND o.created_date <= '{global_end}'
            GROUP BY 1, 2
            ORDER BY 1, 2
            """)
        except Exception as e:
            print(f"  ⚠ bad_orders query failed ({e}), using zeros")
            bad_rows = []

    finally:
        destroy_context(ctx)

    # Build lookup maps
    active_map: dict[tuple[int, str], int] = {}
    for row in users_rows:
        active_map[(int(row[0]), str(row[1])[:7])] = _si(row[2])

    bad_map: dict[tuple[int, str], tuple[int, float]] = {}
    for row in bad_rows:
        pid = int(row[0])
        mk = str(row[1])[:7]
        delivered = _si(row[2])
        bad_n = _si(row[3])
        bad_pct = round(bad_n / delivered * 100, 2) if delivered else 0.0
        bad_map[(pid, mk)] = (bad_n, bad_pct)

    # Build per-location monthly data
    by_pid: dict[int, dict] = {p["provider_id"]: {**p, "by_month": {}} for p in providers}
    for row in fact_rows:
        pid = int(row[0])
        mk = str(row[2])[:7]  # YYYY-MM
        if pid not in by_pid:
            continue
        orders = _si(row[3])
        gross = _sf(row[4])
        net = _sf(row[5])
        sessions = _si(row[14])
        menu_viewed = _si(row[15])
        au = active_map.get((pid, mk), 0) or orders
        bad_n, bad_pct = bad_map.get((pid, mk), (0, 0.0))
        by_pid[pid]["by_month"][mk] = {
            "orders": orders,
            "gross": round(gross, 0),
            "net": round(net, 0),
            "aov": round(gross / orders, 0) if orders else 0,
            "avail": round(_sf(row[6]), 2),
            "accept": round(_sf(row[7]), 2),
            "refunds": round(_sf(row[8]), 2),
            "del_time": round(_sf(row[9]), 1),
            "acc_time": round(_sf(row[10]), 1),
            "prep_time": round(_sf(row[11]), 1),
            "wait_time": round(_sf(row[12]), 1),
            "new_users": _si(row[13]),
            "sessions": sessions,
            "imp_menu": round(menu_viewed / sessions * 100, 2) if sessions else 0,
            "menu_prod": round(_sf(row[16]), 2),
            "rating": round(_sf(row[17]), 2),
            "discounts": round(_sf(row[18]), 0),
            "camp_bolt": round(_sf(row[19]), 0),
            "camp_merch": round(_sf(row[20]), 0),
            "rating_weight": _sf(row[21]) if len(row) > 21 else 0.0,
            "active_users": au,
            "freq": round(orders / au, 2) if au else 0,
            "bad_provider_count": bad_n,
            "bad_provider_pct": bad_pct,
        }

    locations: list[dict] = []
    for pid in sorted(by_pid.keys(), key=lambda x: by_pid[x]["name"].lower()):
        loc = by_pid[pid]
        months_data = []
        for mk, label in zip(month_keys, month_labels_list):
            rec = dict(loc["by_month"].get(mk[:7], EMPTY_MONTH))
            rec["month_key"] = mk
            rec["label"] = label
            months_data.append(rec)
        loc["months"] = months_data
        # Rolling rating
        total_w = sum(m.get("rating_weight", 0) for m in months_data)
        total_s = sum(m.get("rating", 0) * m.get("rating_weight", 0) for m in months_data if m.get("rating"))
        loc["rolling_rating"] = round(total_s / total_w, 2) if total_w else 0.0
        locations.append(loc)

    # Brand totals per month
    brand_months = []
    for i, (mk, label) in enumerate(zip(month_keys, month_labels_list)):
        agg = dict(EMPTY_MONTH)
        for loc in locations:
            m = loc["months"][i]
            for k in ("orders", "gross", "net", "new_users", "sessions",
                      "discounts", "camp_bolt", "camp_merch", "active_users",
                      "bad_provider_count", "rating_weight"):
                agg[k] += m.get(k, 0)
        agg["bad_provider_pct"] = round(
            agg["bad_provider_count"] / agg["orders"] * 100, 2
        ) if agg["orders"] else 0.0
        weighted_keys = [
            ("avail", "orders"), ("accept", "orders"), ("refunds", "orders"),
            ("del_time", "orders"), ("acc_time", "orders"), ("prep_time", "orders"),
            ("wait_time", "orders"), ("imp_menu", "sessions"), ("menu_prod", "sessions"),
        ]
        for metric, wk in weighted_keys:
            total_w = sum(loc["months"][i].get(wk, 0) for loc in locations)
            if total_w:
                agg[metric] = round(
                    sum(loc["months"][i].get(metric, 0) * loc["months"][i].get(wk, 0)
                        for loc in locations) / total_w, 2)
        total_rw = sum(loc["months"][i].get("rating_weight", 0) for loc in locations)
        if total_rw:
            agg["rating"] = round(
                sum(loc["months"][i].get("rating", 0) * loc["months"][i].get("rating_weight", 0)
                    for loc in locations) / total_rw, 2)
        agg["aov"] = round(agg["gross"] / agg["orders"], 0) if agg["orders"] else 0
        agg["freq"] = round(agg["orders"] / agg["active_users"], 2) if agg["active_users"] else 0
        agg["month_key"] = mk
        agg["label"] = label
        brand_months.append(agg)

    # Brand rolling rating
    total_rw_all = sum(sum(m.get("rating_weight", 0) for m in loc["months"]) for loc in locations)
    total_rs_all = sum(
        sum(m.get("rating", 0) * m.get("rating_weight", 0) for m in loc["months"] if m.get("rating"))
        for loc in locations)
    brand_rolling_rating = round(total_rs_all / total_rw_all, 2) if total_rw_all else 0.0

    return {
        "locations": locations,
        "brand_months": brand_months,
        "brand_rolling_rating": brand_rolling_rating,
        "month_keys": month_keys,
        "month_labels": month_labels_list,
        "period_label": f"{month_labels_list[0]} — {month_labels_list[-1]}",
        "generated_at": datetime.datetime.now().strftime("%d.%m.%Y %H:%M"),
        "city": CITY,
    }


# ─── HTML ───────────────────────────────────────────────────────────────────────

def _fmt(val: float, key: str) -> str:
    if key in ("avail", "accept", "refunds", "imp_menu", "menu_prod", "bad_provider_pct"):
        return f"{val:.1f}%"
    if key in ("rating", "freq"):
        return f"{val:.2f}"
    if key in ("del_time", "acc_time", "prep_time", "wait_time"):
        return f"{val:.1f}"
    return f"{val:,.0f}".replace(",", "\u202f")


def _histogram(key: str, months: list[dict], chart_id: str) -> str:
    title, desc, unit = METRIC_UK[key]
    vals = [float(m.get(key, 0)) for m in months]
    max_v = max(vals) if vals and max(vals) > 0 else 1.0
    bars = ""
    for i, (m, val) in enumerate(zip(months, vals)):
        h = max(4, round(val / max_v * 100))
        color = MONTH_BAR_COLORS[i % len(MONTH_BAR_COLORS)]
        bars += f"""
        <div class="bar-col">
          <div class="bar-val">{_fmt(val, key)}</div>
          <div class="bar" style="height:{h}%;background:{color}"></div>
          <div class="bar-lbl">{m['label']}</div>
        </div>"""
    return f"""
    <div class="chart-card" id="{chart_id}">
      <h3>{title}</h3>
      <p class="metric-desc">{desc}</p>
      <p class="unit">Одиниця: {unit} · {N_MONTHS} місяців</p>
      <div class="bars-scroll"><div class="bars">{bars}</div></div>
    </div>"""


def _location_analysis_block(loc: dict, analysis: dict) -> str:
    months = loc["months"]
    if len(months) < 2:
        return ""
    prev, last = months[-2], months[-1]
    sev = analysis["severity"]
    sev_cls = "high" if sev >= 3 else ("mid" if sev >= 1 else "ok")
    issues = "".join(f"<li>{i}</li>" for i in analysis["issues"]) or "<li>Критичних відхилень немає.</li>"
    advice = "".join(f"<li>{i}</li>" for i in analysis["advice"]) or "<li>Підтримуйте поточний рівень.</li>"
    rolling_r = loc.get("rolling_rating", 0)
    return f"""
    <div class="loc-analysis sev-{sev_cls}">
      <div class="loc-analysis-head">
        <h3>Що помітили і що зробити</h3>
        <span class="sev-badge">{"Потребує уваги" if sev >= 2 else "Огляд"}</span>
      </div>
      <p class="bad-explain">{BAD_ORDERS_EXPLAIN_UA}</p>
      <div class="analysis-kpi">
        <span>Замовлення: <b>{prev['orders']}</b> → <b>{last['orders']}</b></span>
        <span>Доступність: <b>{prev['avail']:.1f}%</b> → <b>{last['avail']:.1f}%</b></span>
        <span>Рейтинг (6 міс.): <b>{rolling_r:.2f}</b></span>
        <span>Компенсації: <b>{prev['refunds']:.1f}%</b> → <b>{last['refunds']:.1f}%</b></span>
        <span>Погані замовлення: <b>{last['bad_provider_count']}</b> · <b>{last['bad_provider_pct']:.1f}%</b></span>
      </div>
      <h4>Що відбувається</h4>
      <ul>{issues}</ul>
      <h4>Поради</h4>
      <ul class="advice">{advice}</ul>
    </div>"""


def _analyze(loc: dict) -> dict:
    months = loc["months"]
    if len(months) < 2:
        return {"severity": 0, "issues": [], "advice": [], "trend": "stable"}
    prev, last = months[-2], months[-1]
    issues, advice = [], []
    severity = 0

    def pct_chg(old, new):
        return (new - old) / old * 100 if old else None

    o_chg = pct_chg(prev["orders"], last["orders"])
    if last["orders"] < 50:
        issues.append(f"Мало замовлень за останній місяць — {last['orders']} шт.")
        severity += 2
    elif o_chg is not None and o_chg <= -20:
        issues.append(f"Падіння замовлень: {prev['orders']} → {last['orders']} ({o_chg:.0f}%).")
        severity += 2
    elif o_chg is not None and o_chg >= 15:
        issues.append(f"Зростання замовлень: {prev['orders']} → {last['orders']} (+{o_chg:.0f}%).")

    if last["avail"] < 90:
        issues.append(f"Низька доступність — {last['avail']:.1f}%.")
        advice.append("Тримайте заклад онлайн в пікові години.")
        severity += 2

    if last["accept"] < 97:
        issues.append(f"Не всі замовлення прийняті вчасно — {last['accept']:.1f}%.")
        severity += 1

    rolling_r = loc.get("rolling_rating", 0)
    if rolling_r and rolling_r < 4.4:
        issues.append(f"Рейтинг нижче комфортного рівня — {rolling_r:.2f} з 5.")
        advice.append("Зверніть увагу на відгуки: якість страв, час доставки, комплектація.")
        severity += 2

    if last["bad_provider_pct"] >= 10:
        issues.append(f"Багато поганих замовлень — {last['bad_provider_pct']:.1f}% ({last['bad_provider_count']} шт.).")
        severity += 2

    if last["refunds"] >= 5:
        issues.append(f"Висока частка компенсацій — {last['refunds']:.1f}%.")
        severity += 1

    trend = "stable"
    if o_chg is not None:
        trend = "up" if o_chg >= 10 else ("down" if o_chg <= -10 else "stable")

    return {"severity": severity, "issues": issues, "advice": advice, "trend": trend, "o_chg": o_chg}


def _location_block(loc: dict) -> str:
    pid = loc["provider_id"]
    analysis = _analyze(loc)
    charts = ""
    for section_title, keys in CHART_SECTIONS:
        charts += f'<div class="loc-section-title">{section_title}</div>'
        charts += '<div class="charts-grid">'
        for key in keys:
            charts += _histogram(key, loc["months"], f"c-{pid}-{key}")
        charts += "</div>"
    return f"""
    <section class="loc-card" id="loc-{pid}">
      <div class="loc-row">
        <div class="loc-row-info">
          <h2>{loc['name']}</h2>
          <p class="loc-meta">{loc.get('city','')} · {loc.get('zone','')} · ID {pid}</p>
        </div>
        <button type="button" class="loc-open-btn" data-loc-id="{pid}" aria-expanded="false">
          Відкрити деталі
        </button>
      </div>
      <div class="loc-body" id="loc-body-{pid}" hidden>
        {charts}
        {_location_analysis_block(loc, analysis)}
      </div>
    </section>"""


def generate_html(data: dict) -> str:
    locations = data["locations"]
    brand_months = data["brand_months"]
    last = brand_months[-1] if brand_months else EMPTY_MONTH
    period = data["period_label"]
    gen = data["generated_at"]
    brand_rolling_rating = data.get("brand_rolling_rating", 0)
    n = len(locations)

    brand_charts = ""
    for section_title, keys in CHART_SECTIONS:
        brand_charts += f'<div class="section-title">{section_title} — увесь бренд</div>'
        brand_charts += f'<p class="section-hint">Сума / середнє по всіх {n} локаціях · {N_MONTHS} місяців</p>'
        brand_charts += '<div class="charts-grid">'
        for key in keys:
            brand_charts += _histogram(key, brand_months, f"brand-{key}")
        brand_charts += "</div>"

    loc_blocks = "\n".join(_location_block(loc) for loc in locations)

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>MBR {PARTNER_TITLE} · {period}</title>
  <style>
    :root {{
      --green:#34D186; --green-d:#0d8a52; --black:#0d0d0d;
      --gray-700:#4a4a4a; --gray-400:#9a9a9a; --gray-100:#f5f5f5;
      --positive:#1aad6a; --warning:#e67e22; --danger:#c0392b;
    }}
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
      font-size:14px;line-height:1.55;color:#1a1a1a;background:var(--gray-100)}}
    .header{{background:var(--black);padding:20px 40px;display:flex;align-items:flex-start;
      justify-content:space-between;border-bottom:4px solid var(--green);flex-wrap:wrap;gap:16px}}
    .header-left{{display:flex;align-items:center;gap:14px}}
    .bolt-logo{{width:44px;height:44px;background:var(--green);border-radius:10px;
      display:flex;align-items:center;justify-content:center}}
    .header-title h1{{font-size:22px;font-weight:700;color:#fff}}
    .header-title p{{font-size:11px;color:var(--green);text-transform:uppercase;
      letter-spacing:1.2px;font-weight:600;margin-top:4px}}
    .header-meta{{text-align:right;color:var(--gray-400);font-size:12px;line-height:1.9}}
    .header-meta strong{{color:var(--green)}}
    .container{{max-width:1320px;margin:0 auto;padding:28px 40px 48px}}
    .period-bar{{background:#fff;border-radius:12px;padding:14px 20px;margin-bottom:20px;
      display:flex;align-items:center;gap:12px;flex-wrap:wrap;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .section-title{{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;
      color:var(--gray-700);padding-bottom:10px;border-bottom:2px solid var(--green);margin:28px 0 10px}}
    .section-hint{{font-size:12px;color:var(--gray-400);margin-bottom:14px}}
    .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px;margin-bottom:8px}}
    .kpi-card{{background:#fff;border-radius:12px;padding:14px 16px;border-top:3px solid var(--green);
      box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .kpi-label{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--gray-400);margin-bottom:4px}}
    .kpi-value{{font-size:20px;font-weight:700}}
    .charts-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px;margin-bottom:12px}}
    .chart-card{{background:#fff;border-radius:12px;padding:16px 18px;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .chart-card h3{{font-size:12px;font-weight:700;color:var(--gray-700);margin-bottom:4px}}
    .metric-desc{{font-size:11px;color:var(--gray-700);margin-bottom:4px}}
    .unit{{font-size:10px;color:var(--gray-400);margin-bottom:8px}}
    .bars-scroll{{overflow-x:auto;padding-bottom:4px}}
    .bars{{display:flex;gap:6px;align-items:flex-end;min-height:120px;padding-top:6px}}
    .bar-col{{display:flex;flex-direction:column;align-items:center;min-width:42px;flex-shrink:0;
      height:110px;justify-content:flex-end}}
    .bar-val{{font-size:8px;font-weight:700;color:var(--gray-700);margin-bottom:3px;text-align:center;max-width:48px}}
    .bar{{width:36px;border-radius:5px 5px 0 0;min-height:4px}}
    .bar-lbl{{font-size:9px;color:var(--gray-400);margin-top:3px;text-align:center}}
    .loc-card{{background:#fff;border-radius:12px;margin:0 0 10px;
      box-shadow:0 1px 4px rgba(0,0,0,.06);border:1px solid #eee;overflow:hidden}}
    .loc-row{{display:flex;align-items:center;justify-content:space-between;gap:16px;
      padding:14px 18px;flex-wrap:wrap}}
    .loc-row-info h2{{font-size:15px;font-weight:700}}
    .loc-open-btn{{padding:9px 16px;border:none;border-radius:8px;background:var(--green-d);
      color:#fff;font-size:13px;font-weight:600;cursor:pointer}}
    .loc-open-btn:hover{{background:var(--green);color:var(--black)}}
    .loc-open-btn[aria-expanded="true"]{{background:#eee;color:var(--gray-700)}}
    .loc-body{{padding:0 18px 20px;border-top:1px solid #f0f0f0}}
    .loc-body[hidden]{{display:none}}
    .loc-section-title{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;
      color:var(--green-d);margin:18px 0 10px}}
    .loc-meta{{font-size:12px;color:var(--gray-400);margin-top:2px}}
    .loc-analysis{{background:var(--gray-100);border-radius:10px;padding:16px 18px;margin-top:20px;border-left:4px solid var(--gray-400)}}
    .loc-analysis.sev-high{{border-left-color:var(--danger);background:#fff8f6}}
    .loc-analysis.sev-mid{{border-left-color:var(--warning);background:#fffaf3}}
    .loc-analysis.sev-ok{{border-left-color:var(--positive)}}
    .loc-analysis-head{{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px}}
    .loc-analysis h4{{font-size:12px;margin:10px 0 4px;color:var(--gray-700)}}
    .loc-analysis ul{{margin-left:18px;font-size:13px}}
    .loc-analysis ul.advice{{color:var(--green-d)}}
    .bad-explain{{font-size:12px;color:var(--gray-700);padding:10px 12px;background:#fff;
      border-radius:8px;border:1px solid #eee;margin-bottom:12px}}
    .sev-badge{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--warning)}}
    .analysis-kpi{{display:flex;flex-wrap:wrap;gap:10px;font-size:11px;color:var(--gray-400);
      margin-bottom:10px;padding:8px 0;border-top:1px solid #f0f0f0;border-bottom:1px solid #f0f0f0}}
    .analysis-kpi b{{color:var(--gray-700)}}
    .footer{{background:var(--black);color:var(--gray-400);font-size:11px;padding:22px 40px;text-align:center}}
    .footer span{{color:var(--green)}}
    @media(max-width:700px){{
      .container{{padding:16px}} .charts-grid{{grid-template-columns:1fr}} .header{{padding:16px}}
    }}
  </style>
</head>
<body>
<header class="header">
  <div class="header-left">
    <div class="bolt-logo">
      <svg viewBox="0 0 24 24" width="26" height="26"><path d="M13 2L4.5 13.5H11L10 22L19.5 10.5H13V2Z" fill="#0d0d0d"/></svg>
    </div>
    <div class="header-title">
      <h1>MBR {PARTNER_TITLE}</h1>
      <p>Bolt Food · Щомісячний звіт · {CITY}</p>
    </div>
  </div>
  <div class="header-meta">
    <div>Період: <strong>{period}</strong></div>
    <div>Місяців: <strong>{N_MONTHS}</strong> · Локацій: <strong>{n}</strong></div>
    <div>Оновлено: <strong>{gen}</strong></div>
  </div>
</header>

<div class="container">
  <div class="period-bar">
    <span style="font-size:11px;font-weight:700;text-transform:uppercase;color:var(--gray-700)">Період:</span>
    <span style="font-size:12px;color:var(--gray-700)">{period} · валюта UAH (₴)</span>
    <span style="margin-left:auto;font-size:11px;color:var(--gray-400)">Останній місяць: {last.get('label','')}</span>
  </div>

  <div class="section-title">Огляд бренду — останній місяць</div>
  <div class="kpi-grid">
    <div class="kpi-card"><div class="kpi-label">Gross Sales</div><div class="kpi-value">{_fmt(last['gross'],'gross')} ₴</div></div>
    <div class="kpi-card"><div class="kpi-label">Net Sales</div><div class="kpi-value">{_fmt(last['net'],'net')} ₴</div></div>
    <div class="kpi-card"><div class="kpi-label">Delivered Orders</div><div class="kpi-value">{last['orders']}</div></div>
    <div class="kpi-card"><div class="kpi-label">AOV</div><div class="kpi-value">{_fmt(last['aov'],'aov')} ₴</div></div>
    <div class="kpi-card"><div class="kpi-label">Availability</div><div class="kpi-value">{last['avail']:.1f}%</div></div>
    <div class="kpi-card"><div class="kpi-label">Acceptance</div><div class="kpi-value">{last['accept']:.1f}%</div></div>
    <div class="kpi-card"><div class="kpi-label">Active Users</div><div class="kpi-value">{last['active_users']}</div></div>
    <div class="kpi-card"><div class="kpi-label">Rating (6 міс.)</div><div class="kpi-value">{brand_rolling_rating:.2f}</div></div>
    <div class="kpi-card"><div class="kpi-label">Погані замовлення</div><div class="kpi-value">{last['bad_provider_count']} · {last['bad_provider_pct']:.1f}%</div></div>
  </div>
  <p class="section-hint" style="margin-top:8px">{BAD_ORDERS_EXPLAIN_UA}</p>

  {brand_charts}

  <div class="section-title">Локації бренду</div>
  <p class="section-hint">Натисніть «Відкрити деталі» для графіків і порад по локації</p>
  <div id="locations">
    {loc_blocks}
  </div>
</div>

<footer class="footer">
  <span>Bolt Food</span> · MBR {PARTNER_TITLE} · Автооновлення: 1-го числа кожного місяця о 14:00 (Київ) ·
  <a href="https://github.com/marharytazhytnyk-create/Partner-s-Pages/tree/main/IZI%20BURGER" style="color:var(--green)">GitHub</a>
</footer>

<script>
(function() {{
  document.querySelectorAll('.loc-open-btn').forEach(btn => {{
    btn.addEventListener('click', () => {{
      const pid = btn.dataset.locId;
      const body = document.getElementById('loc-body-' + pid);
      const open = body.hidden;
      body.hidden = !open;
      btn.setAttribute('aria-expanded', open ? 'true' : 'false');
      btn.textContent = open ? 'Згорнути' : 'Відкрити деталі';
      if (open) document.getElementById('loc-' + pid).scrollIntoView({{behavior:'smooth',block:'start'}});
    }});
  }});
}})();
</script>
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

    months = last_n_completed_months(N_MONTHS)
    print(f"MBR {PARTNER_TITLE} — {N_MONTHS} завершених місяців: {months[0][0]} → {months[-1][1]}")
    print(f"Provider IDs: {PROVIDER_IDS}\n")

    data = fetch_data()
    html = generate_html(data)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"\n→ {OUTPUT_HTML}")
    print(f"Локацій: {len(data['locations'])}, згенеровано: {data['generated_at']}")


if __name__ == "__main__":
    main()
