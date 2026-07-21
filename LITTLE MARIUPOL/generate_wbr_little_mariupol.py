#!/usr/bin/env python3
"""
WBR Little Mariupol Network — щотижневий звіт мережі (Полтава).
Бренди: #LITTLE MARIUPOL, #DOMASHNYA YIZA, #OBID TYT, #MAMYNY DERUNY, #PANI KARTOPLA
Останні 8 завершених тижнів, HTML українською, валюта UAH.
Автооновлення: щопонеділка о 14:00 (Київ) через GitHub Actions.
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
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")

BRAND_NAMES = [
    "LITTLE MARIUPOL",
    "DOMASHNYA YIZA",
    "OBID TYT",
    "MAMYNY DERUNY",
    "PANI KARTOPLA",
]
PARTNER_TITLE = "Little Mariupol Network"
N_WEEKS = 8
SCRIPT_DIR = Path(__file__).parent
OUTPUT_HTML = SCRIPT_DIR / "WBR_LittleMariupol.html"
POLL_INTERVAL_S = 4
MAX_POLL_S = 600


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
    for env_path in (
        SCRIPT_DIR.parent / "databricks-setup" / ".env",
        Path.home() / "Library" / "CloudStorage"
        / "GoogleDrive-marharyta.zhytnyk@bolt.eu" / "My Drive"
        / "Events project" / "databricks-setup" / ".env",
    ):
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("DATABRICKS_TOKEN="):
                    return line.split("=", 1)[1].strip()
    return ""


DATABRICKS_TOKEN = _load_token()
HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

CHART_SECTIONS: list[tuple[str, list[str]]] = [
    ("1. Продажі", ["gross", "net", "orders", "aov"]),
    ("2. Операційні показники", [
        "avail", "accept", "refunds",
        "del_time", "acc_time", "prep_time", "wait_time", "c2m_time", "c2e_time",
    ]),
    ("2a. Погані замовлення з вини закладу", ["bad_provider_count", "bad_provider_pct"]),
    ("3. Клієнти та їх поведінка", [
        "active_users", "freq", "new_users", "sessions", "imp_menu", "menu_prod", "rating",
    ]),
    ("4. Знижки", ["discounts", "camp_bolt", "camp_merch"]),
]

BAD_ORDERS_EXPLAIN_UA = (
    "Погані замовлення (Bad Orders) — це доставлені замовлення, з якими у клієнта виникла "
    "проблема: затримка з вини закладу, неповний склад, холодна/неякісна їжа, відмова чи "
    "інша скарга, яку система віднесла до відповідальності ресторану. "
    "Високий показник знижує рейтинг і зменшує шанс, що гість замовить знову."
)

METRIC_UK: dict[str, tuple[str, str, str]] = {
    "gross": ("Gross Sales", "Сума вартості доставлених замовлень до знижок", "₴"),
    "net": ("Net Sales", "Сума після застосування знижок", "₴"),
    "orders": ("Delivered Orders", "Кількість успішно доставлених замовлень", "шт."),
    "aov": ("AOV (середній чек)", "Середня сума одного замовлення до знижок", "₴"),
    "avail": ("Availability Rate", "Частка часу, коли заклад був онлайн", "%"),
    "accept": ("Acceptance Rate", "Частка замовлень, прийнятих вчасно", "%"),
    "refunds": ("Orders with Refunds", "Частка замовлень із поверненням коштів", "%"),
    "del_time": ("Avg. Delivery Time", "Середній повний час доставки", "хв"),
    "acc_time": ("Avg. Acceptance Time", "Середній час прийняття замовлення", "хв"),
    "prep_time": ("Avg. Preparation Time", "Середній час приготування", "хв"),
    "wait_time": ("Avg. Courier Wait Time", "Середній час очікування курʼєром", "хв"),
    "c2m_time": ("Courier to Merchant", "Середній час шляху курʼєра до закладу", "хв"),
    "c2e_time": ("Courier to Eater", "Середній час шляху курʼєра до клієнта", "хв"),
    "active_users": ("Active Users", "Унікальні клієнти з доставленим замовленням", "осіб"),
    "freq": ("Order Frequency", "Середня кількість замовлень на клієнта", "зам./корист."),
    "new_users": ("New Users", "Клієнти, які вперше замовили", "осіб"),
    "sessions": ("Sessions", "Перегляди закладу у стрічці / пошуку", "сесій"),
    "imp_menu": ("Impression → Menu", "Частка переглядів, з яких відкрили меню", "%"),
    "menu_prod": ("Menu → Product Added", "Частка переглядів меню з додаванням у кошик", "%"),
    "rating": ("Rating", "Середня оцінка закладу", "з 5"),
    "discounts": ("Total Discounts", "Загальна сума знижок для клієнтів", "₴"),
    "camp_bolt": ("Campaigns by Bolt", "Витрати Bolt на знижки та промо", "₴"),
    "camp_merch": ("Campaigns by Merchant", "Витрати партнера на знижки", "₴"),
    "bad_provider_count": ("Погані замовлення (заклад)", "Кількість поганих замовлень з вини закладу", "шт."),
    "bad_provider_pct": ("Погані замовлення (%)", "Відсоток поганих замовлень з вини закладу", "%"),
}

WEEK_BAR_COLORS = [
    "#0a5c38", "#0d8a52", "#12a35f", "#34D186",
    "#5cdb9a", "#7ee0ad", "#a3e8c4", "#c8f0da",
]

EMPTY_WEEK = {
    "orders": 0, "gross": 0, "net": 0, "aov": 0,
    "avail": 0, "accept": 0, "refunds": 0,
    "del_time": 0, "acc_time": 0, "prep_time": 0, "wait_time": 0, "c2m_time": 0, "c2e_time": 0,
    "new_users": 0, "sessions": 0, "imp_menu": 0, "menu_prod": 0, "rating": 0,
    "discounts": 0, "camp_bolt": 0, "camp_merch": 0, "active_users": 0, "freq": 0,
    "bad_provider_count": 0, "bad_provider_pct": 0.0,
}


# ─── DATES ─────────────────────────────────────────────────────────────────────

def last_n_completed_weeks(n: int = N_WEEKS) -> list[tuple[datetime.date, datetime.date]]:
    today = datetime.date.today()
    last_sunday = today - datetime.timedelta(days=today.weekday() + 1)
    weeks = []
    for i in range(n):
        end = last_sunday - datetime.timedelta(weeks=i)
        start = end - datetime.timedelta(days=6)
        weeks.append((start, end))
    return list(reversed(weeks))


def week_label(start: datetime.date, end: datetime.date) -> str:
    return f"{start.strftime('%d.%m')}–{end.strftime('%d.%m')}"


def week_key(d) -> str:
    return str(d)[:10]


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
    cmd_id = _post(
        "/api/1.2/commands/execute",
        {"language": "sql", "clusterId": CLUSTER_ID, "contextId": ctx_id, "command": sql},
    )["id"]
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


def _parse_loc_week(row: list, active_users: int = 0) -> dict:
    orders = _si(row[3])
    gross = _sf(row[4])
    net = _sf(row[5])
    sessions = _si(row[16])
    menu_viewed = _si(row[17])
    au = active_users or orders
    return {
        "orders": orders, "gross": round(gross, 0), "net": round(net, 0),
        "aov": round(gross / orders, 0) if orders else 0,
        "avail": round(_sf(row[6]), 2), "accept": round(_sf(row[7]), 2),
        "refunds": round(_sf(row[8]), 2), "del_time": round(_sf(row[9]), 1),
        "acc_time": round(_sf(row[10]), 1), "prep_time": round(_sf(row[11]), 1),
        "wait_time": round(_sf(row[12]), 1), "c2m_time": round(_sf(row[13]), 1),
        "c2e_time": round(_sf(row[14]), 1), "new_users": _si(row[15]),
        "sessions": sessions,
        "imp_menu": round(menu_viewed / sessions * 100, 2) if sessions else 0,
        "menu_prod": round(_sf(row[18]), 2), "rating": round(_sf(row[19]), 2),
        "discounts": round(_sf(row[20]), 0), "camp_bolt": round(_sf(row[21]), 0),
        "camp_merch": round(_sf(row[22]), 0), "active_users": au,
        "freq": round(orders / au, 2) if au else 0,
        "bad_provider_count": 0, "bad_provider_pct": 0.0,
    }


# ─── FETCH ─────────────────────────────────────────────────────────────────────

def fetch_data() -> dict:
    weeks = last_n_completed_weeks(N_WEEKS)
    global_start = weeks[0][0].isoformat()
    global_end = weeks[-1][1].isoformat()
    week_keys = [w[0].isoformat() for w in weeks]
    week_labels_list = [week_label(s, e) for s, e in weeks]

    brands_sql = ", ".join(f"'{b}'" for b in BRAND_NAMES)

    ensure_cluster_running()
    ctx = create_context()
    try:
        loc_rows = run_query(ctx, f"""
        SELECT provider_id, provider_name, brand_name, city_name, zone_name
        FROM ng_delivery_spark.dim_provider_v2
        WHERE brand_name IN ({brands_sql})
        ORDER BY brand_name, city_name, provider_name
        """)
        if not loc_rows:
            raise RuntimeError(f"Не знайдено локацій для брендів: {brands_sql}")

        providers = [
            {"provider_id": int(r[0]), "name": str(r[1]),
             "brand": str(r[2]), "city": str(r[3] or ""), "zone": str(r[4] or "")}
            for r in loc_rows
        ]
        pids = [p["provider_id"] for p in providers]
        pids_sql = ", ".join(str(p) for p in pids)
        pids_str = ", ".join(f"'{p}'" for p in pids)

        print(f"  локацій: {len(providers)}, брендів: {len(set(p['brand'] for p in providers))}, період: {global_start} → {global_end}")

        fact_rows = run_query(ctx, f"""
        SELECT
            f.provider_id, d.provider_name,
            DATE_FORMAT(DATE_TRUNC('week', f.metric_timestamp_partition), 'yyyy-MM-dd') AS week_start,
            SUM(f.delivered_orders_count) AS orders,
            SUM(f.total_gmv_before_discounts) AS gross,
            SUM(f.total_gmv_after_discounts) AS net,
            AVG(f.provider_active_rate_value) * 100 AS avail,
            AVG(f.provider_acceptance_rate_value) * 100 AS accept,
            AVG(f.customer_refunded_order_rate_value) * 100 AS refunds,
            AVG(f.order_total_minutes_per_order_value) AS del_time,
            AVG(f.provider_acceptance_minutes_per_order_value) AS acc_time,
            AVG(f.provider_preparation_minutes_per_order_value) AS prep_time,
            AVG(f.courier_total_wait_minutes_per_order_value) AS wait_time,
            AVG(f.courier_to_provider_actual_minutes_per_order_value) AS c2m,
            AVG(f.courier_to_eater_actual_minutes_per_order_value) AS c2e,
            SUM(f.users_activated_vendor_count) AS new_users,
            SUM(f.provider_impressions_sessions_count) AS sessions,
            SUM(f.provider_menu_viewed_sessions_count) AS menu_viewed,
            AVG(f.provider_product_added_from_menu_viewed_rate_value) * 100 AS menu_prod,
            AVG(f.provider_rating_per_order_value) AS rating,
            SUM(f.total_campaign_discount) AS discounts,
            SUM(f.total_campaign_spend_bolt) AS camp_bolt,
            SUM(f.total_campaign_spend_provider) AS camp_merch
        FROM ng_delivery_spark.fact_provider_weekly f
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
            DATE_FORMAT(DATE_TRUNC('week', metric_timestamp_partition), 'yyyy-MM-dd') AS week_start,
            SUM(provider_deliveries_unique_user_count) AS active_users
        FROM ng_delivery_spark.int_provider_metrics_non_additive
        WHERE entity_id IN ({pids_str})
          AND timeframe_name = 'week'
          AND metric_timestamp_partition >= '{global_start}'
          AND metric_timestamp_partition <= '{global_end}'
        GROUP BY 1, 2
        ORDER BY 1, 2
        """)

        bad_rows = run_query(ctx, f"""
        SELECT
            o.provider_id,
            DATE_FORMAT(DATE_TRUNC('week', o.created_date), 'yyyy-MM-dd') AS week_start,
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

    finally:
        destroy_context(ctx)

    active_map: dict[tuple[int, str], int] = {}
    for row in users_rows:
        active_map[(int(row[0]), week_key(str(row[1])))] = _si(row[2])

    bad_map: dict[tuple[int, str], tuple[int, float]] = {}
    for row in bad_rows:
        pid = int(row[0])
        wk = week_key(str(row[1]))
        delivered = _si(row[2])
        bad_n = _si(row[3])
        bad_pct = round(bad_n / delivered * 100, 2) if delivered else 0.0
        bad_map[(pid, wk)] = (bad_n, bad_pct)

    by_pid: dict[int, dict] = {p["provider_id"]: {**p, "by_week": {}} for p in providers}
    for row in fact_rows:
        pid = int(row[0])
        wk = week_key(str(row[2]))
        if pid not in by_pid:
            continue
        au = active_map.get((pid, wk), 0)
        rec = _parse_loc_week(row, au)
        bad_n, bad_pct = bad_map.get((pid, wk), (0, 0.0))
        rec["bad_provider_count"] = bad_n
        rec["bad_provider_pct"] = bad_pct
        by_pid[pid]["by_week"][wk] = rec

    # Build locations list with weekly data
    locations: list[dict] = []
    for pid in sorted(by_pid.keys(), key=lambda x: (by_pid[x]["brand"], by_pid[x]["name"].lower())):
        loc = by_pid[pid]
        weeks_data = []
        for wk, label in zip(week_keys, week_labels_list):
            rec = dict(loc["by_week"].get(wk, EMPTY_WEEK))
            rec["week_key"] = wk
            rec["label"] = label
            weeks_data.append(rec)
        loc["weeks"] = weeks_data
        locations.append(loc)

    # Group by brand
    brands_data: dict[str, dict] = {}
    for brand in BRAND_NAMES:
        brand_locs = [l for l in locations if l["brand"] == brand]
        brand_weeks_agg = _aggregate_brand_weeks(brand_locs, week_keys, week_labels_list)
        brands_data[brand] = {
            "brand": brand,
            "locations": brand_locs,
            "brand_weeks": brand_weeks_agg,
        }

    # Network totals
    network_weeks = _aggregate_brand_weeks(locations, week_keys, week_labels_list)

    return {
        "brands_data": brands_data,
        "network_weeks": network_weeks,
        "locations": locations,
        "week_keys": week_keys,
        "week_labels": week_labels_list,
        "period_label": f"{week_labels_list[0]} — {week_labels_list[-1]}",
        "period_dates": f"{weeks[0][0].strftime('%d.%m.%Y')} — {weeks[-1][1].strftime('%d.%m.%Y')}",
        "generated_at": datetime.datetime.now().strftime("%d.%m.%Y %H:%M"),
        "city": locations[0]["city"] if locations else "Полтава",
    }


def _aggregate_brand_weeks(locations: list[dict], week_keys: list, week_labels_list: list) -> list[dict]:
    brand_weeks = []
    for i, (wk, label) in enumerate(zip(week_keys, week_labels_list)):
        agg = dict(EMPTY_WEEK)
        for loc in locations:
            w = loc["weeks"][i]
            for k in ("orders", "gross", "net", "new_users", "sessions",
                      "discounts", "camp_bolt", "camp_merch", "active_users", "bad_provider_count"):
                agg[k] += w.get(k, 0)
        agg["bad_provider_pct"] = round(
            agg["bad_provider_count"] / agg["orders"] * 100, 2
        ) if agg["orders"] else 0.0
        weighted_keys = [
            ("avail", "orders"), ("accept", "orders"), ("refunds", "orders"),
            ("del_time", "orders"), ("acc_time", "orders"), ("prep_time", "orders"),
            ("wait_time", "orders"), ("c2m_time", "orders"), ("c2e_time", "orders"),
            ("rating", "orders"), ("imp_menu", "sessions"), ("menu_prod", "sessions"),
        ]
        for metric, weight_key in weighted_keys:
            total_w = sum(loc["weeks"][i].get(weight_key, 0) for loc in locations)
            if total_w:
                agg[metric] = round(
                    sum(loc["weeks"][i].get(metric, 0) * loc["weeks"][i].get(weight_key, 0)
                        for loc in locations) / total_w, 2)
        agg["aov"] = round(agg["gross"] / agg["orders"], 0) if agg["orders"] else 0
        agg["freq"] = round(agg["orders"] / agg["active_users"], 2) if agg["active_users"] else 0
        agg["week_key"] = wk
        agg["label"] = label
        brand_weeks.append(agg)
    return brand_weeks


# ─── ANALYSIS ──────────────────────────────────────────────────────────────────

def _pct_change(old: float, new: float):
    if old == 0:
        return None
    return (new - old) / old * 100


def analyze_location(loc: dict) -> dict:
    weeks = loc["weeks"]
    if len(weeks) < 2:
        return {"name": loc["name"], "provider_id": loc["provider_id"],
                "severity": 0, "issues": [], "advice": [], "trend": "stable",
                "prev": weeks[-1] if weeks else EMPTY_WEEK,
                "last": weeks[-1] if weeks else EMPTY_WEEK, "o_chg": None}

    prev, last = weeks[-2], weeks[-1]
    first = weeks[0]
    issues: list[str] = []
    advice: list[str] = []
    severity = 0

    o_chg = _pct_change(prev["orders"], last["orders"])
    o_trend = _pct_change(first["orders"], last["orders"])

    if last["orders"] < 15:
        issues.append(f"За останній тиждень дуже мало замовлень — лише {last['orders']} (тиждень раніше: {prev['orders']}).")
        advice.append("Перевірте години роботи в застосунку, головне фото та опис меню.")
        severity += 3
    elif o_chg is not None and o_chg <= -25:
        issues.append(f"Різке падіння замовлень: {prev['orders']} → {last['orders']} ({o_chg:.0f}%).")
        advice.append("Перегляньте, чи не було довгих пауз офлайн або змін у меню.")
        severity += 2
    elif o_chg is not None and o_chg <= -10:
        issues.append(f"Замовлень менше, ніж тиждень тому: {prev['orders']} → {last['orders']} ({o_chg:.0f}%).")
        severity += 1

    if o_chg is not None and o_chg >= 20:
        issues.append(f"Гарне зростання замовлень: {prev['orders']} → {last['orders']} (+{o_chg:.0f}%).")

    if o_trend is not None and o_trend <= -30:
        issues.append(f"За 8 тижнів замовлення зменшилися з {first['orders']} до {last['orders']} ({o_trend:.0f}%).")
        severity += 2

    if last["avail"] < 90:
        issues.append(f"Заклад доступний лише {last['avail']:.1f}% часу.")
        advice.append("Тримайте заклад увімкненим в обід і ввечері.")
        severity += 2

    if last["accept"] < 97:
        issues.append(f"Не всі замовлення приймаються вчасно — {last['accept']:.1f}%.")
        advice.append("Приймайте замовлення якомога швидше (орієнтир < 1 хв).")
        severity += 2

    if last["refunds"] >= 5:
        issues.append(f"Часто компенсації клієнтам — {last['refunds']:.1f}%.")
        advice.append("Перевірте актуальність меню та правильність збірки замовлень.")
        severity += 2

    if last["prep_time"] >= 30:
        issues.append(f"Страви готуються довго — {last['prep_time']:.1f} хв.")
        severity += 1

    if last["rating"] and last["rating"] < 4.4:
        issues.append(f"Середня оцінка нижча за комфортну — {last['rating']:.2f} з 5.")
        advice.append("Перегляньте останні низькі відгуки: комплектація, температура, запізнення.")
        severity += 2

    if last["bad_provider_pct"] >= 12:
        issues.append(f"Багато поганих замовлень з вини закладу — {last['bad_provider_pct']:.1f}% ({last['bad_provider_count']} зам.).")
        advice.append("Зверніть увагу на причини скарг: довге приготування, неповний склад, відмова.")
        severity += 2
    elif last["bad_provider_pct"] >= 8 and last["bad_provider_pct"] > prev["bad_provider_pct"] + 2:
        issues.append(f"Поганих замовлень стало більше: {prev['bad_provider_pct']:.1f}% → {last['bad_provider_pct']:.1f}%.")
        severity += 1

    if prev["bad_provider_count"] and last["bad_provider_count"] == 0:
        issues.append("За останній тиждень — жодного поганого замовлення з вини закладу. Відмінно!")

    trend = "stable"
    if o_chg is not None:
        if o_chg >= 10:
            trend = "up"
        elif o_chg <= -10:
            trend = "down"

    return {
        "name": loc["name"], "provider_id": loc["provider_id"],
        "zone": loc.get("zone", ""), "brand": loc.get("brand", ""),
        "severity": severity, "issues": issues, "advice": advice, "trend": trend,
        "prev": prev, "last": last, "o_chg": o_chg,
    }


# ─── HTML HELPERS ───────────────────────────────────────────────────────────────

def _fmt(val: float, key: str) -> str:
    if key in ("avail", "accept", "refunds", "imp_menu", "menu_prod", "bad_provider_pct"):
        return f"{val:.1f}%"
    if key in ("rating", "freq"):
        return f"{val:.2f}"
    if key in ("del_time", "acc_time", "prep_time", "wait_time", "c2m_time", "c2e_time"):
        return f"{val:.1f}"
    return f"{val:,.0f}".replace(",", "\u202f")


def _histogram(key: str, weeks: list[dict], chart_id: str) -> str:
    title, desc, unit = METRIC_UK[key]
    vals = [float(w.get(key, 0)) for w in weeks]
    max_v = max(vals) if vals and max(vals) > 0 else 1.0
    bars = ""
    for i, (w, val) in enumerate(zip(weeks, vals)):
        h = max(4, round(val / max_v * 100))
        color = WEEK_BAR_COLORS[i % len(WEEK_BAR_COLORS)]
        bars += f"""
        <div class="bar-col">
          <div class="bar-val">{_fmt(val, key)}</div>
          <div class="bar" style="height:{h}%;background:{color}"></div>
          <div class="bar-lbl">{w['label']}</div>
        </div>"""
    return f"""
    <div class="chart-card" id="{chart_id}">
      <h3>{title}</h3>
      <p class="metric-desc">{desc}</p>
      <p class="unit">Одиниця: {unit} · 8 тижнів</p>
      <div class="bars-scroll"><div class="bars">{bars}</div></div>
    </div>"""


def _location_analysis_block(analysis: dict) -> str:
    prev, last = analysis["prev"], analysis["last"]
    sev = "high" if analysis["severity"] >= 3 else ("mid" if analysis["severity"] >= 1 else "ok")
    issues = "".join(f"<li>{i}</li>" for i in analysis["issues"]) or "<li>Критичних відхилень немає.</li>"
    advice = "".join(f"<li>{i}</li>" for i in analysis["advice"]) or "<li>Підтримуйте поточний рівень сервісу.</li>"
    badge = "Потребує уваги" if analysis["severity"] >= 2 else "Огляд"
    return f"""
    <div class="loc-analysis sev-{sev}">
      <div class="loc-analysis-head">
        <h3>Що помітили і що зробити</h3>
        <span class="sev-badge">{badge}</span>
      </div>
      <p class="bad-explain">{BAD_ORDERS_EXPLAIN_UA}</p>
      <div class="analysis-kpi">
        <span>Замовлення: <b>{prev['orders']}</b> → <b>{last['orders']}</b></span>
        <span>Доступність: <b>{prev['avail']:.1f}%</b> → <b>{last['avail']:.1f}%</b></span>
        <span>Рейтинг: <b>{prev['rating']:.2f}</b> → <b>{last['rating']:.2f}</b></span>
        <span>Компенсації: <b>{prev['refunds']:.1f}%</b> → <b>{last['refunds']:.1f}%</b></span>
        <span>Погані замовлення: <b>{last['bad_provider_count']}</b> · <b>{last['bad_provider_pct']:.1f}%</b></span>
      </div>
      <h4>Що відбувається</h4>
      <ul>{issues}</ul>
      <h4>Поради</h4>
      <ul class="advice">{advice}</ul>
    </div>"""


def _location_block(loc: dict, analysis: dict) -> str:
    pid = loc["provider_id"]
    search_blob = f"{loc['name']} {loc.get('zone','')} {loc.get('city','')} {loc.get('brand','')} {pid}".lower()
    charts = ""
    for section_title, keys in CHART_SECTIONS:
        charts += f'<div class="loc-section-title">{section_title}</div>'
        charts += '<div class="charts-grid">'
        for key in keys:
            charts += _histogram(key, loc["weeks"], f"c-{pid}-{key}")
        charts += "</div>"
    return f"""
    <section class="loc-card" data-search="{search_blob}" data-name="{loc['name']}" id="loc-{pid}">
      <div class="loc-row">
        <div class="loc-row-info">
          <h2>{loc['name']}</h2>
          <p class="loc-meta">{loc.get('city','')} · {loc.get('zone','')} · ID {pid}</p>
        </div>
        <button type="button" class="loc-open-btn" data-loc-id="{pid}" aria-expanded="false">
          Відкрити інформацію
        </button>
      </div>
      <div class="loc-body" id="loc-body-{pid}" hidden>
        {charts}
        {_location_analysis_block(analysis)}
      </div>
    </section>"""


def _kpi_grid(last: dict) -> str:
    return f"""
    <div class="kpi-grid">
      <div class="kpi-card"><div class="kpi-label">Gross Sales</div><div class="kpi-value">{_fmt(last['gross'],'gross')} ₴</div></div>
      <div class="kpi-card"><div class="kpi-label">Net Sales</div><div class="kpi-value">{_fmt(last['net'],'net')} ₴</div></div>
      <div class="kpi-card"><div class="kpi-label">Delivered Orders</div><div class="kpi-value">{last['orders']}</div></div>
      <div class="kpi-card"><div class="kpi-label">AOV</div><div class="kpi-value">{_fmt(last['aov'],'aov')} ₴</div></div>
      <div class="kpi-card"><div class="kpi-label">Availability</div><div class="kpi-value">{last['avail']:.1f}%</div></div>
      <div class="kpi-card"><div class="kpi-label">Acceptance</div><div class="kpi-value">{last['accept']:.1f}%</div></div>
      <div class="kpi-card"><div class="kpi-label">Active Users</div><div class="kpi-value">{last['active_users']}</div></div>
      <div class="kpi-card"><div class="kpi-label">Rating</div><div class="kpi-value">{last['rating']:.2f}</div></div>
      <div class="kpi-card"><div class="kpi-label">Погані замовлення</div><div class="kpi-value">{last['bad_provider_count']} · {last['bad_provider_pct']:.1f}%</div></div>
    </div>"""


def _brand_charts(brand_weeks: list[dict], brand_slug: str) -> str:
    html = ""
    for section_title, keys in CHART_SECTIONS:
        html += f'<div class="section-title">{section_title}</div>'
        html += '<div class="charts-grid">'
        for key in keys:
            html += _histogram(key, brand_weeks, f"{brand_slug}-{key}")
        html += "</div>"
    return html


def generate_html(data: dict) -> str:
    brands_data = data["brands_data"]
    network_weeks = data["network_weeks"]
    period = data["period_label"]
    gen = data["generated_at"]
    city = data.get("city", "Полтава")
    last_net = network_weeks[-1] if network_weeks else EMPTY_WEEK

    total_locs = len(data["locations"])
    total_brands = len([b for b in BRAND_NAMES if brands_data.get(b, {}).get("locations")])

    # Build brand overview cards for "all brands" tab
    brand_summary_cards = ""
    for brand in BRAND_NAMES:
        bd = brands_data.get(brand, {})
        bw = bd.get("brand_weeks", [])
        if not bw:
            continue
        last_b = bw[-1]
        slug = re.sub(r"[^a-z0-9]", "_", brand.lower().lstrip("#"))
        n_locs = len(bd.get("locations", []))
        brand_summary_cards += f"""
        <div class="brand-summary-card" onclick="showBrandTab('{slug}')">
          <div class="brand-summary-name">{brand}</div>
          <div class="brand-summary-kpi">
            <span>Замовлення: <b>{last_b['orders']}</b></span>
            <span>Gross: <b>{_fmt(last_b['gross'],'gross')} ₴</b></span>
            <span>Availability: <b>{last_b['avail']:.1f}%</b></span>
            <span>Rating: <b>{last_b['rating']:.2f}</b></span>
            <span>Погані: <b>{last_b['bad_provider_pct']:.1f}%</b></span>
            <span>Локацій: <b>{n_locs}</b></span>
          </div>
          <div class="brand-summary-link">Детальніше →</div>
        </div>"""

    # Build per-brand tabs content
    brand_tabs_html = ""
    brand_tab_buttons = ""

    for brand in BRAND_NAMES:
        bd = brands_data.get(brand, {})
        locs = bd.get("locations", [])
        bw = bd.get("brand_weeks", [])
        slug = re.sub(r"[^a-z0-9]", "_", brand.lower().lstrip("#"))
        display = brand.lstrip("#")
        brand_tab_buttons += f'<button type="button" class="report-tab" data-tab="{slug}">{display}</button>\n'

        if not locs or not bw:
            brand_tabs_html += f'<div class="tab-panel" id="tab-{slug}"><div class="problem-none">Дані відсутні для бренду {brand}.</div></div>'
            continue

        last_b = bw[-1]
        analyses = [analyze_location(loc) for loc in locs]

        loc_blocks = "\n".join(
            _location_block(loc, next(a for a in analyses if a["provider_id"] == loc["provider_id"]))
            for loc in locs
        )

        brand_tabs_html += f"""
        <div class="tab-panel" id="tab-{slug}">
          <div class="section-title">Огляд бренду {display} — останній тиждень</div>
          {_kpi_grid(last_b)}
          <p class="section-hint" style="margin-top:8px">{BAD_ORDERS_EXPLAIN_UA}</p>
          {_brand_charts(bw, slug)}
          <div class="section-title">Локації — {display}</div>
          <p class="section-hint">Натисніть «Відкрити інформацію», щоб побачити гістограми та поради</p>
          <div class="loc-list">{loc_blocks}</div>
        </div>"""

    all_search_items = json.dumps(
        [{"id": loc["provider_id"], "name": loc["name"],
          "zone": loc.get("zone", ""), "city": loc.get("city", ""), "brand": loc.get("brand", "")}
         for loc in data["locations"]],
        ensure_ascii=False,
    )

    # Network charts
    net_charts = ""
    for section_title, keys in CHART_SECTIONS:
        net_charts += f'<div class="section-title">{section_title} — вся мережа</div>'
        net_charts += '<p class="section-hint">Сума / середнє по всіх брендах та локаціях · 8 тижнів</p>'
        net_charts += '<div class="charts-grid">'
        for key in keys:
            net_charts += _histogram(key, network_weeks, f"net-{key}")
        net_charts += "</div>"

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>WBR {PARTNER_TITLE} · {period}</title>
  <style>
    :root {{
      --green:#34D186; --green-d:#0d8a52; --black:#0d0d0d;
      --gray-700:#4a4a4a; --gray-400:#9a9a9a; --gray-100:#f5f5f5;
      --positive:#1aad6a; --warning:#e67e22; --danger:#c0392b; --info:#2980b9;
    }}
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
      font-size:14px;line-height:1.55;color:#1a1a1a;background:var(--gray-100)}}
    .header{{background:var(--black);padding:20px 40px;display:flex;align-items:flex-start;
      justify-content:space-between;border-bottom:4px solid var(--green);flex-wrap:wrap;gap:16px}}
    .header-left{{display:flex;align-items:center;gap:14px;flex:1;min-width:240px}}
    .header-right{{display:flex;flex-direction:column;align-items:flex-end;gap:10px}}
    .header-search-wrap{{position:relative;width:min(320px,100%)}}
    .header-search-btn{{width:100%;padding:9px 14px;border:1px solid #333;border-radius:8px;background:#1a1a1a;
      color:var(--gray-400);font-size:13px;text-align:left;cursor:pointer;display:flex;align-items:center;gap:8px}}
    .header-search-btn:hover{{border-color:var(--green);color:#fff}}
    .search-panel{{display:none;position:absolute;top:calc(100% + 6px);right:0;left:0;background:#fff;
      border-radius:10px;box-shadow:0 8px 28px rgba(0,0,0,.18);padding:10px;z-index:50;border:1px solid #e8e8e8}}
    .search-panel.open{{display:block}}
    .search-panel input{{width:100%;padding:10px 12px;border:1px solid #ddd;border-radius:8px;font-size:14px;outline:none}}
    .search-panel input:focus{{border-color:var(--green-d)}}
    .search-results{{max-height:280px;overflow-y:auto;margin-top:8px}}
    .search-results:empty{{display:none}}
    .search-result-item{{display:block;width:100%;text-align:left;padding:9px 10px;border:none;background:transparent;
      border-radius:6px;cursor:pointer;font-size:13px;color:var(--gray-700)}}
    .search-result-item:hover,.search-result-item.active{{background:#e6faf2}}
    .search-result-item small{{display:block;font-size:11px;color:var(--gray-400);margin-top:2px}}
    .search-empty{{font-size:12px;color:var(--gray-400);padding:8px 4px;display:none}}
    .search-empty.visible{{display:block}}
    .bolt-logo{{width:44px;height:44px;background:var(--green);border-radius:10px;
      display:flex;align-items:center;justify-content:center}}
    .header-title h1{{font-size:20px;font-weight:700;color:#fff}}
    .header-title p{{font-size:11px;color:var(--green);text-transform:uppercase;letter-spacing:1.2px;font-weight:600;margin-top:4px}}
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
    .metric-desc{{font-size:11px;color:var(--gray-700);margin-bottom:4px;line-height:1.4}}
    .unit{{font-size:10px;color:var(--gray-400);margin-bottom:8px}}
    .bars-scroll{{overflow-x:auto;padding-bottom:4px}}
    .bars{{display:flex;gap:6px;align-items:flex-end;min-height:120px;padding-top:6px}}
    .bar-col{{display:flex;flex-direction:column;align-items:center;min-width:42px;flex-shrink:0;
      height:110px;justify-content:flex-end}}
    .bar-val{{font-size:8px;font-weight:700;color:var(--gray-700);margin-bottom:3px;text-align:center;max-width:48px;line-height:1.15}}
    .bar{{width:36px;border-radius:5px 5px 0 0;min-height:4px}}
    .bar-lbl{{font-size:8px;color:var(--gray-400);margin-top:3px;text-align:center;line-height:1.2}}
    .loc-card{{background:#fff;border-radius:12px;padding:0;margin:0 0 10px;
      box-shadow:0 1px 4px rgba(0,0,0,.06);border:1px solid #eee;overflow:hidden}}
    .loc-row{{display:flex;align-items:center;justify-content:space-between;gap:16px;
      padding:14px 18px;flex-wrap:wrap}}
    .loc-row-info{{flex:1;min-width:180px}}
    .loc-row-info h2{{font-size:15px;color:var(--black);font-weight:700}}
    .loc-open-btn{{flex-shrink:0;padding:9px 16px;border:none;border-radius:8px;background:var(--green-d);
      color:#fff;font-size:13px;font-weight:600;cursor:pointer;white-space:nowrap}}
    .loc-open-btn:hover{{background:var(--green);color:var(--black)}}
    .loc-open-btn[aria-expanded="true"]{{background:#eee;color:var(--gray-700)}}
    .loc-body{{padding:0 18px 20px;border-top:1px solid #f0f0f0}}
    .loc-body[hidden]{{display:none}}
    .loc-analysis{{background:var(--gray-100);border-radius:10px;padding:16px 18px;margin-top:20px;border-left:4px solid var(--gray-400)}}
    .loc-analysis.sev-high{{border-left-color:var(--danger);background:#fff8f6}}
    .loc-analysis.sev-mid{{border-left-color:var(--warning);background:#fffaf3}}
    .loc-analysis.sev-ok{{border-left-color:var(--positive)}}
    .loc-analysis-head{{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px}}
    .loc-analysis h4{{font-size:12px;margin:10px 0 4px;color:var(--gray-700)}}
    .loc-analysis ul{{margin-left:18px;font-size:13px}}
    .loc-analysis ul.advice{{color:var(--green-d)}}
    .bad-explain{{font-size:12px;color:var(--gray-700);line-height:1.45;margin:0 0 12px;
      padding:10px 12px;background:#fff;border-radius:8px;border:1px solid #eee}}
    .loc-meta{{font-size:12px;color:var(--gray-400);margin-top:2px}}
    .loc-list{{display:flex;flex-direction:column;gap:0}}
    .loc-section-title{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;
      color:var(--green-d);margin:18px 0 10px}}
    .sev-badge{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--warning)}}
    .analysis-kpi{{display:flex;flex-wrap:wrap;gap:10px;font-size:11px;color:var(--gray-400);
      margin-bottom:10px;padding:8px 0;border-top:1px solid #f0f0f0;border-bottom:1px solid #f0f0f0}}
    .analysis-kpi b{{color:var(--gray-700)}}
    .report-tabs{{display:flex;gap:8px;flex-wrap:wrap;margin:20px 0 14px;overflow-x:auto;padding-bottom:4px}}
    .report-tab{{padding:10px 18px;border:1px solid #ddd;border-radius:999px;background:#fff;
      font-size:13px;font-weight:600;color:var(--gray-700);cursor:pointer;white-space:nowrap;flex-shrink:0}}
    .report-tab:hover{{border-color:var(--green-d);color:var(--green-d)}}
    .report-tab.active{{background:var(--green-d);border-color:var(--green-d);color:#fff}}
    .tab-panel{{display:none}}
    .tab-panel.active{{display:block}}
    .brand-summary-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:14px;margin-bottom:24px}}
    .brand-summary-card{{background:#fff;border-radius:12px;padding:18px;box-shadow:0 1px 4px rgba(0,0,0,.06);
      border-left:4px solid var(--green);cursor:pointer;transition:box-shadow .15s,transform .15s}}
    .brand-summary-card:hover{{box-shadow:0 4px 16px rgba(0,0,0,.1);transform:translateY(-2px)}}
    .brand-summary-name{{font-size:15px;font-weight:700;margin-bottom:10px;color:var(--black)}}
    .brand-summary-kpi{{display:flex;flex-wrap:wrap;gap:8px;font-size:11px;color:var(--gray-400);margin-bottom:10px}}
    .brand-summary-kpi b{{color:var(--gray-700)}}
    .brand-summary-link{{font-size:12px;color:var(--green-d);font-weight:600}}
    .problem-none{{background:#e6faf2;border-radius:12px;padding:18px;color:var(--positive)}}
    .footer{{background:var(--black);color:var(--gray-400);font-size:11px;padding:22px 40px;text-align:center}}
    .footer span{{color:var(--green)}}
    @media(max-width:700px){{
      .container{{padding:16px}} .charts-grid{{grid-template-columns:1fr}}
      .header{{padding:16px}} .header-right{{align-items:stretch;width:100%}}
      .report-tabs{{gap:6px}} .report-tab{{padding:8px 12px;font-size:12px}}
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
      <h1>WBR {PARTNER_TITLE}</h1>
      <p>Bolt Food · Щотижневий звіт · {city}</p>
    </div>
  </div>
  <div class="header-right">
    <div class="header-meta">
      <div>Період: <strong>{data['period_dates']}</strong></div>
      <div>Тижнів: <strong>{N_WEEKS} завершених</strong> · Брендів: <strong>{total_brands}</strong> · Локацій: <strong>{total_locs}</strong></div>
      <div>Оновлено: <strong>{gen}</strong></div>
    </div>
    <div class="header-search-wrap" id="header-search-wrap">
      <button type="button" class="header-search-btn" id="search-open-btn" aria-expanded="false">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3-3"/></svg>
        Пошук локації…
      </button>
      <div class="search-panel" id="search-panel">
        <input id="loc-search" type="search" placeholder="Назва, бренд, зона або ID…" autocomplete="off"/>
        <div class="search-empty" id="search-empty">Нічого не знайдено</div>
        <div class="search-results" id="search-results"></div>
      </div>
    </div>
  </div>
</header>

<div class="container">
  <div class="period-bar">
    <span style="font-size:11px;font-weight:700;text-transform:uppercase;color:var(--gray-700)">Період:</span>
    <span style="font-size:12px;color:var(--gray-700)">{period} · валюта UAH (₴)</span>
    <span style="margin-left:auto;font-size:11px;color:var(--gray-400)">Останній тиждень: {last_net.get('label','')}</span>
  </div>

  <div class="report-tabs" role="tablist">
    <button type="button" class="report-tab active" data-tab="network">Вся мережа</button>
    {brand_tab_buttons}
  </div>

  <!-- Вся мережа -->
  <div class="tab-panel active" id="tab-network">
    <div class="section-title">Огляд мережі — останній тиждень</div>
    {_kpi_grid(last_net)}
    <p class="section-hint" style="margin-top:8px">{BAD_ORDERS_EXPLAIN_UA}</p>

    {net_charts}

    <div class="section-title">Бренди мережі</div>
    <p class="section-hint">Натисніть на картку бренду, щоб перейти до детального звіту</p>
    <div class="brand-summary-grid">
      {brand_summary_cards}
    </div>
  </div>

  <!-- Per-brand tabs -->
  {brand_tabs_html}

</div>

<footer class="footer">
  <span>Bolt Food</span> · WBR {PARTNER_TITLE} · Автооновлення: щопонеділка о 14:00 (Київ) ·
  <a href="https://github.com/marharytazhytnyk-create/Partner-s-Pages/tree/main/LITTLE%20MARIUPOL" style="color:var(--green)">GitHub</a>
</footer>

<script>
(function() {{
  const LOCATIONS = {all_search_items};

  function toggleLocation(id, forceOpen) {{
    const card = document.getElementById('loc-' + id);
    const body = document.getElementById('loc-body-' + id);
    const btn = card && card.querySelector('.loc-open-btn[data-loc-id]');
    if (!card || !body || !btn) return;
    const open = forceOpen !== undefined ? forceOpen : body.hidden;
    body.hidden = !open;
    btn.setAttribute('aria-expanded', open ? 'true' : 'false');
    btn.textContent = open ? 'Згорнути' : 'Відкрити інформацію';
    if (open) card.scrollIntoView({{behavior: 'smooth', block: 'start'}});
  }}

  window.showBrandTab = function(name) {{
    document.querySelectorAll('.report-tab').forEach(t => {{
      const on = t.dataset.tab === name;
      t.classList.toggle('active', on);
      t.setAttribute('aria-selected', on ? 'true' : 'false');
    }});
    document.querySelectorAll('.tab-panel').forEach(p => {{
      p.classList.toggle('active', p.id === ('tab-' + name));
    }});
    window.scrollTo({{top: 0, behavior: 'smooth'}});
  }};

  document.querySelectorAll('.report-tab').forEach(tab => {{
    tab.addEventListener('click', () => showBrandTab(tab.dataset.tab));
  }});

  document.querySelectorAll('.loc-open-btn[data-loc-id]').forEach(btn => {{
    btn.addEventListener('click', () => toggleLocation(btn.dataset.locId));
  }});

  const wrap = document.getElementById('header-search-wrap');
  const panel = document.getElementById('search-panel');
  const openBtn = document.getElementById('search-open-btn');
  const input = document.getElementById('loc-search');
  const resultsEl = document.getElementById('search-results');
  const emptyEl = document.getElementById('search-empty');
  let activeIdx = -1;

  function closePanel() {{
    panel.classList.remove('open');
    openBtn.setAttribute('aria-expanded', 'false');
    input.value = '';
    resultsEl.innerHTML = '';
    emptyEl.classList.remove('visible');
    activeIdx = -1;
  }}

  function openPanel() {{
    panel.classList.add('open');
    openBtn.setAttribute('aria-expanded', 'true');
    input.focus();
  }}

  function renderResults(q) {{
    const query = (q || '').trim().toLowerCase();
    resultsEl.innerHTML = '';
    emptyEl.classList.remove('visible');
    activeIdx = -1;
    if (!query) return;
    const matches = LOCATIONS.filter(loc => {{
      const blob = [loc.name, loc.zone, loc.city, loc.brand, loc.id].join(' ').toLowerCase();
      return blob.includes(query);
    }}).slice(0, 12);
    if (!matches.length) {{ emptyEl.classList.add('visible'); return; }}
    matches.forEach(loc => {{
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'search-result-item';
      btn.dataset.id = loc.id;
      btn.innerHTML = loc.name + '<small>' + loc.brand + ' · ' + (loc.zone || loc.city) + ' · ID ' + loc.id + '</small>';
      btn.addEventListener('click', () => {{
        closePanel();
        const brand_slug = loc.brand.replace(/^#/, '').toLowerCase().replace(/[^a-z0-9]/g, '_');
        showBrandTab(brand_slug);
        setTimeout(() => toggleLocation(loc.id, true), 100);
      }});
      resultsEl.appendChild(btn);
    }});
  }}

  openBtn.addEventListener('click', e => {{
    e.stopPropagation();
    panel.classList.contains('open') ? closePanel() : openPanel();
  }});
  input.addEventListener('input', () => renderResults(input.value));
  input.addEventListener('keydown', e => {{
    const items = Array.from(resultsEl.querySelectorAll('.search-result-item'));
    if (e.key === 'Escape') {{ closePanel(); return; }}
    if (!items.length) return;
    if (e.key === 'ArrowDown') {{ e.preventDefault(); activeIdx = Math.min(activeIdx + 1, items.length - 1); }}
    else if (e.key === 'ArrowUp') {{ e.preventDefault(); activeIdx = Math.max(activeIdx - 1, 0); }}
    else return;
    items.forEach((it, i) => it.classList.toggle('active', i === activeIdx));
  }});
  document.addEventListener('click', e => {{ if (!wrap.contains(e.target)) closePanel(); }});
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

    weeks = last_n_completed_weeks(N_WEEKS)
    print(f"WBR {PARTNER_TITLE} — {N_WEEKS} завершених тижнів: {weeks[0][0]} → {weeks[-1][1]}")
    print(f"Бренди: {', '.join(BRAND_NAMES)}\n")

    data = fetch_data()
    html = generate_html(data)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"\n→ {OUTPUT_HTML}")
    total_locs = len(data["locations"])
    print(f"Локацій: {total_locs}, згенеровано: {data['generated_at']}")


if __name__ == "__main__":
    main()
