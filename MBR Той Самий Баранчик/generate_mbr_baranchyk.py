#!/usr/bin/env python3
"""
MBR — Той Самий Баранчик (мережа, кілька міст)
Місячний звіт, останні 12 повних місяців.
Структура: загальні дані по бренду + окремі вкладки по містах.
Автооновлення: 3-го числа кожного місяця (резервний запуск 4-го) через GitHub Actions.
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
# `or` rather than a getenv default: CI passes these as empty strings when the
# repository secret is unset, and an empty host builds a scheme-less URL.
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST") or "https://bolt-incentives.cloud.databricks.com"
CLUSTER_ID_ENV  = os.getenv("DATABRICKS_CLUSTER_ID") or ""

CLUSTER_CANDIDATES = ["0505-112942-d3yviznw", "0221-081903-9ag4bh69"]
SCHEMA_CANDIDATES  = ["main.ng_delivery", "ng_delivery_spark"]
N_MONTHS        = 12
SCRIPT_DIR      = Path(__file__).parent
OUTPUT_HTML     = SCRIPT_DIR / "MBR Той Самий Баранчик.html"
POLL_INTERVAL_S = 5
MAX_POLL_S      = 600
CLUSTER_START_S = 900
FETCH_ATTEMPTS  = 3
# RESIZING — кластер працює і лише додає/знімає воркери, запити на ньому виконуються.
USABLE_STATES   = {"RUNNING", "RESIZING"}
PENDING_STATES  = {"PENDING", "RESTARTING"}
RETRY_DELAY_S   = 20

BRAND_TITLE   = "Той Самий Баранчик"
BRAND_EMOJI   = "🐑"
BRAND_COLOR   = "#B5651D"
# Локації шукаються за назвою, а не за списком ID: коли мережа відкриває новий
# заклад, він потрапляє у звіт автоматично, без правок у коді.
PROVIDER_NAME_LIKE = "той самий баранчик"

CITY_UK = {
    "Kyiv":        "Київ",
    "Kharkiv":     "Харків",
    "Lviv":        "Львів",
    "Poltava":     "Полтава",
    "Cherkasy":    "Черкаси",
    "Kremenchuk":  "Кременчук",
    "Dnipro":      "Дніпро",
    "Odesa":       "Одеса",
    "Odessa":      "Одеса",
    "Zaporizhzhia":"Запоріжжя",
    "Vinnytsia":   "Вінниця",
    "Chernihiv":   "Чернігів",
    "Chernivtsi":  "Чернівці",
    "Rivne":       "Рівне",
    "Ternopil":    "Тернопіль",
    "Ivano-Frankivsk": "Івано-Франківськ",
    "Khmelnytskyi":"Хмельницький",
    "Zhytomyr":    "Житомир",
    "Sumy":        "Суми",
    "Mykolaiv":    "Миколаїв",
    "Kryvyi Rih":  "Кривий Ріг",
    "Uzhhorod":    "Ужгород",
    "Lutsk":       "Луцьк",
}

UK_MONTHS_SHORT = ["","Січ","Лют","Бер","Кві","Тра","Чер","Лип","Сер","Вер","Жов","Лис","Гру"]
UK_MONTHS_FULL  = ["","Січень","Лютий","Березень","Квітень","Травень","Червень",
                    "Липень","Серпень","Вересень","Жовтень","Листопад","Грудень"]
UK_MONTHS_GEN   = ["","січня","лютого","березня","квітня","травня","червня",
                    "липня","серпня","вересня","жовтня","листопада","грудня"]

TOP_ITEMS_LIMIT = 5

CHART_SECTIONS = [
    ("1. Продажі",                    ["gross","net","orders","aov"]),
    ("2. Операційні показники",       ["avail","accept","refunds","prep_time","acc_time","del_time"]),
    ("3. Клієнти та поведінка",       ["active_users","freq","new_users","sessions","imp_menu","menu_prod","rating"]),
    ("4. Знижки",                     ["discounts","camp_bolt","camp_merch"]),
]

METRIC_UK = {
    "gross":      ("Gross Sales (продажі)",         "Сума вартості доставлених замовлень до знижок",   "₴"),
    "net":        ("Net Sales (чисті продажі)",      "Сума після застосування знижок клієнтам",         "₴"),
    "orders":     ("Delivered Orders",               "Кількість успішно доставлених замовлень",         "шт."),
    "aov":        ("AOV — середній чек",             "Середня сума одного доставленого замовлення",     "₴"),
    "avail":      ("Availability Rate",              "Частка часу, коли заклад був онлайн",             "%"),
    "accept":     ("Acceptance Rate",                "Частка замовлень, прийнятих вчасно",              "%"),
    "refunds":    ("Orders with Refunds",            "Частка замовлень з компенсацією клієнту",         "%"),
    "del_time":   ("Average Delivery Time",          "Середній повний час доставки від закладу до гостя","хв"),
    "acc_time":   ("Merchant Acceptance Time",       "Середній час прийняття замовлення партнером",     "хв"),
    "prep_time":  ("Preparation Time",               "Середній час приготування страв на кухні",        "хв"),
    "active_users":("Active Users",                  "Унікальні клієнти з доставленим замовленням",     "осіб"),
    "freq":       ("Order Frequency",                "Середня кількість замовлень на активного гостя",  "зам./гість"),
    "new_users":  ("New Users",                      "Гості, які вперше замовили в цьому закладі",      "осіб"),
    "sessions":   ("Sessions (покази)",              "Перегляди закладу в стрічці або пошуку",          "сесій"),
    "imp_menu":   ("Impression → Menu",              "Частка переглядів закладу, де відкрили меню",     "%"),
    "menu_prod":  ("Menu → Cart",                    "Частка переглядів меню з додаванням у кошик",     "%"),
    "rating":     ("Average Rating",                 "Середня оцінка закладу від гостей",               "з 5"),
    "discounts":  ("Total Discounts",                "Загальна сума знижок для клієнтів",               "₴"),
    "camp_bolt":  ("Campaigns Spend by Bolt",        "Витрати Bolt на знижки та промо",                 "₴"),
    "camp_merch": ("Campaigns Spend by Merchant",    "Сума, яку партнер вклав у знижки та промо",       "₴"),
}

MONTH_BAR_COLORS = [
    "#4E2600","#6B3410","#82421A","#985024",
    "#AE5E2E","#C06A34","#CE7A44","#D98C57",
    "#E29E6C","#EAB083","#F0C29B","#F5D4B5",
]

EMPTY_MONTH = {
    "orders":0,"gross":0,"net":0,"aov":0,
    "avail":0,"accept":0,"refunds":0,
    "del_time":0,"acc_time":0,"prep_time":0,
    "new_users":0,"sessions":0,"imp_menu":0,"menu_prod":0,"rating":0,
    "discounts":0,"camp_bolt":0,"camp_merch":0,
    "active_users":0,"freq":0,
}

SUM_KEYS      = ("orders","gross","net","new_users","sessions",
                 "discounts","camp_bolt","camp_merch","active_users")
WEIGHTED_KEYS = (("avail","orders"),("accept","orders"),("refunds","orders"),
                 ("del_time","orders"),("acc_time","orders"),("prep_time","orders"),
                 ("rating","orders"),("imp_menu","sessions"),("menu_prod","sessions"))


# ─── DATE HELPERS ──────────────────────────────────────────────────────────────

def last_n_full_months(n: int = N_MONTHS) -> list[tuple[int, int]]:
    d = datetime.date.today().replace(day=1)
    months = []
    for _ in range(n):
        d -= datetime.timedelta(days=1)
        months.append((d.year, d.month))
        d = d.replace(day=1)
    return list(reversed(months))


def month_label(y: int, m: int, short: bool = False) -> str:
    name = UK_MONTHS_SHORT[m] if short else UK_MONTHS_FULL[m]
    return f"{name} {y}"


def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"


def month_range(y: int, m: int) -> tuple[str, str]:
    start = datetime.date(y, m, 1)
    end = (datetime.date(y + 1, 1, 1) if m == 12 else datetime.date(y, m + 1, 1))
    return start.isoformat(), end.isoformat()


def date_uk(iso: str) -> str:
    """'2025-07-03' → '3 липня 2025'."""
    try:
        d = datetime.date.fromisoformat(str(iso)[:10])
    except (TypeError, ValueError):
        return str(iso or "")
    return f"{d.day} {UK_MONTHS_GEN[d.month]} {d.year}"


# ─── TOKEN ─────────────────────────────────────────────────────────────────────

def _load_token() -> str:
    tok = os.getenv("DATABRICKS_TOKEN", "").strip()
    if tok:
        return tok
    for profile in ("bolt-incentives-temp", "bolt-incentives"):
        try:
            out = subprocess.check_output(
                ["databricks", "auth", "token", "-p", profile],
                text=True, stderr=subprocess.DEVNULL, timeout=30)
            t = json.loads(out).get("access_token", "").strip()
            if t:
                return t
        except Exception:
            pass
    cfg = Path.home() / ".databrickscfg"
    if cfg.exists():
        for line in cfg.read_text().splitlines():
            if line.lower().startswith("token") and "=" in line:
                t = line.split("=", 1)[1].strip()
                if t:
                    return t
    return ""


DATABRICKS_TOKEN = _load_token()
HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

# Обидва визначаються на старті й далі використовуються всіма запитами.
CLUSTER = CLUSTER_ID_ENV
SCHEMA  = ""


# ─── DATABRICKS ────────────────────────────────────────────────────────────────

def _post(path, payload):
    r = requests.post(f"{DATABRICKS_HOST}{path}", headers=HEADERS, json=payload, timeout=90)
    r.raise_for_status()
    return r.json()

def _get(path, params):
    r = requests.get(f"{DATABRICKS_HOST}{path}", headers=HEADERS, params=params, timeout=90)
    r.raise_for_status()
    return r.json()

def _cluster_state(cluster_id: str) -> str:
    try:
        return _get("/api/2.0/clusters/get", {"cluster_id": cluster_id}).get("state", "")
    except Exception:
        return ""


def pick_cluster() -> str:
    """Знайти кластер, на якому можна виконувати запити.

    Спочатку явно заданий, далі відомі кандидати, далі будь-який доступний
    all-purpose кластер. Права на `clusters/start` є не в усіх, тому вже
    піднятий кластер завжди пріоритетніший за той, що треба стартувати.
    """
    wanted = ([CLUSTER_ID_ENV] if CLUSTER_ID_ENV else []) + CLUSTER_CANDIDATES

    for cid in wanted:
        state = _cluster_state(cid)
        if state in USABLE_STATES:
            print(f"  кластер: {cid} ({state})")
            return cid

    try:
        listed = _get("/api/2.0/clusters/list", {}).get("clusters", [])
    except Exception:
        listed = []
    for c in listed:
        if c.get("state") in USABLE_STATES and c.get("cluster_source") != "JOB":
            print(f"  кластер: {c['cluster_id']} ({c.get('cluster_name')}) — вже доступний")
            return c["cluster_id"]

    for cid in wanted:
        state = _cluster_state(cid)
        if not state:
            continue
        if state not in PENDING_STATES:
            print(f"  кластер {cid} у стані {state}, пробуємо запустити…")
            try:
                _post("/api/2.0/clusters/start", {"cluster_id": cid})
            except Exception as exc:
                print(f"    не вдалося: {exc}")
                continue
        else:
            print(f"  кластер {cid} у стані {state}, чекаємо…")
        deadline = time.time() + CLUSTER_START_S
        while time.time() < deadline:
            time.sleep(15)
            if _cluster_state(cid) in USABLE_STATES:
                print(f"  кластер: {cid} (готовий)")
                return cid
    raise RuntimeError("Немає доступного кластера Databricks")


def create_ctx() -> str:
    return _post("/api/1.2/contexts/create", {"language": "sql", "clusterId": CLUSTER})["id"]

def run_query(ctx: str, sql: str) -> list[list]:
    cmd_id = _post("/api/1.2/commands/execute",
        {"language": "sql", "clusterId": CLUSTER, "contextId": ctx, "command": sql})["id"]
    deadline = time.time() + MAX_POLL_S
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL_S)
        resp = _get("/api/1.2/commands/status",
            {"clusterId": CLUSTER, "contextId": ctx, "commandId": cmd_id})
        s = resp.get("status")
        if s == "Finished":
            res = resp.get("results", {})
            if res.get("resultType") == "error":
                raise RuntimeError(res.get("summary") or res.get("cause") or "Query error")
            return res.get("data", [])
        if s in ("Cancelled", "Error"):
            raise RuntimeError(f"Query {s}")
    raise TimeoutError("Query timed out")

def pick_schema(ctx: str) -> str:
    """Той самий набір таблиць живе і в Unity Catalog, і в старому hive_metastore."""
    global SCHEMA
    if SCHEMA:
        return SCHEMA
    for schema in SCHEMA_CANDIDATES:
        try:
            run_query(ctx, f"SELECT 1 FROM {schema}.dim_provider_v2 LIMIT 1")
            print(f"  схема: {schema}")
            SCHEMA = schema
            return SCHEMA
        except Exception:
            continue
    raise RuntimeError("Не знайдено схему з dim_provider_v2")

def destroy_ctx(ctx: str):
    try:
        _post("/api/1.2/contexts/destroy", {"clusterId": CLUSTER, "contextId": ctx})
    except Exception:
        pass

def _sf(v, d=0.0):
    try:
        return float(v) if v is not None else d
    except (TypeError, ValueError):
        return d

def _si(v, d=0):
    return int(round(_sf(v, d)))


# ─── AGGREGATION ───────────────────────────────────────────────────────────────

def aggregate_months(month_lists: list[list[dict]]) -> list[dict]:
    """Скласти місячні ряди кількох локацій в один.

    Суми складаються, відсотки й тривалості усереднюються з вагою (замовлення
    або сесії) — інакше маленький заклад тягнув би середнє мережі на себе.
    """
    if not month_lists:
        return []
    n = len(month_lists[0])
    out = []
    for i in range(n):
        slice_i = [ml[i] for ml in month_lists]
        agg = dict(EMPTY_MONTH)
        for k in SUM_KEYS:
            agg[k] = sum(m.get(k, 0) for m in slice_i)
        for metric, wkey in WEIGHTED_KEYS:
            total_w = sum(m.get(wkey, 0) for m in slice_i)
            if total_w:
                agg[metric] = round(
                    sum(m.get(metric, 0) * m.get(wkey, 0) for m in slice_i) / total_w, 2)
        agg["aov"]  = round(agg["gross"] / agg["orders"], 0) if agg["orders"] else 0
        agg["freq"] = round(agg["orders"] / agg["active_users"], 2) if agg["active_users"] else 0
        agg["month_key"] = slice_i[0].get("month_key", "")
        agg["label"]     = slice_i[0].get("label", "")
        agg["label_s"]   = slice_i[0].get("label_s", "")
        out.append(agg)
    return out


# ─── DATA FETCH ────────────────────────────────────────────────────────────────

def fetch_fun_facts(ctx, schema: str, pids_sql: str) -> dict:
    """Підсумки за весь час роботи мережі на платформі — для розділу «Цікаві цифри».

    Свідомо без обмеження за датою: на відміну від решти звіту, тут рахуємо всю
    історію, включно з поточним незавершеним місяцем. Період підписуємо явно,
    щоб цифри не плутали з помісячними графіками за останні N місяців.
    """
    facts: dict = {}

    totals = run_query(ctx, f"""
        SELECT
            COUNT(*)                            AS orders,
            SUM(order_gmv)                      AS gross,
            SUM(order_gmv_after_discount)       AS net,
            MIN(order_created_date_local)       AS first_date,
            MAX(order_created_date_local)       AS last_date
        FROM {schema}.fact_order_delivery
        WHERE provider_id IN ({pids_sql})
          AND order_state = 'delivered'
    """)
    if totals and totals[0]:
        r = totals[0]
        facts["orders"]     = _si(r[0])
        facts["gross"]      = round(_sf(r[1]), 0)
        facts["net"]        = round(_sf(r[2]), 0)
        facts["first_date"] = str(r[3])[:10]
        facts["last_date"]  = str(r[4])[:10]

    # Позиції меню однакові в різних містах, тому групуємо за назвою, а не за
    # product_id: інакше та сама страва розпалася б на шість рядків.
    # total_order_dish_amount рахує лише страви, без додатків та опцій.
    items = run_query(ctx, f"""
        SELECT
            d.menu_item_name,
            SUM(p.total_order_dish_amount)   AS dishes,
            COUNT(DISTINCT p.provider_id)    AS locations
        FROM {schema}.fact_provider_product_monthly p
        JOIN {schema}.dim_provider_product_delivery d
          ON d.provider_id = p.provider_id AND d.product_id = p.product_id
        WHERE p.provider_id IN ({pids_sql})
        GROUP BY 1
        HAVING SUM(p.total_order_dish_amount) > 0
        ORDER BY dishes DESC
        LIMIT {TOP_ITEMS_LIMIT}
    """)
    facts["top_items"] = [
        {"name": str(r[0]), "qty": _si(r[1]), "locations": _si(r[2])} for r in items
    ]

    top_order = run_query(ctx, f"""
        SELECT provider_id, order_created_date_local, order_gmv, order_gmv_after_discount
        FROM {schema}.fact_order_delivery
        WHERE provider_id IN ({pids_sql})
          AND order_state = 'delivered'
        ORDER BY order_gmv DESC
        LIMIT 1
    """)
    if top_order and top_order[0]:
        r = top_order[0]
        facts["top_order"] = {
            "provider_id": int(r[0]),
            "date":        str(r[1])[:10],
            "gross":       round(_sf(r[2]), 0),
            "net":         round(_sf(r[3]), 0),
        }

    return facts


def fetch_data() -> dict:
    months = last_n_full_months(N_MONTHS)
    y0, m0 = months[0]
    y1, m1 = months[-1]
    global_start, _ = month_range(y0, m0)
    _, global_end   = month_range(y1, m1)
    month_keys     = [month_key(y, m) for y, m in months]
    month_labels   = [month_label(y, m) for y, m in months]
    month_labels_s = [month_label(y, m, short=True) for y, m in months]

    print(f"  fetching {N_MONTHS} months {global_start} → {global_end}")
    ctx = create_ctx()
    try:
        schema = pick_schema(ctx)

        loc_rows = run_query(ctx, f"""
            SELECT provider_id, provider_name, city_name, zone_name
            FROM {schema}.dim_provider_v2
            WHERE LOWER(provider_name) LIKE '%{PROVIDER_NAME_LIKE}%'
            ORDER BY city_name, provider_name
        """)
        if not loc_rows:
            raise RuntimeError(f"не знайдено жодної локації за назвою '{PROVIDER_NAME_LIKE}'")

        pids = [int(r[0]) for r in loc_rows]
        pids_sql = ", ".join(str(p) for p in pids)
        pids_str = ", ".join(f"'{p}'" for p in pids)

        # Monthly grain: a weekly row is stamped with its Monday and lands whole in that
        # Monday's month, so a month collected 4 or 5 entire weeks depending on where the
        # Mondays fell. The monthly fact table gives calendar months, 1st to last day.
        fact_rows = run_query(ctx, f"""
            SELECT
                f.provider_id,
                DATE_FORMAT(DATE_TRUNC('month', f.metric_timestamp_partition), 'yyyy-MM-dd') AS mstart,
                SUM(f.delivered_orders_count)                                        AS orders,
                SUM(f.total_gmv_before_discounts)                                    AS gross,
                SUM(f.total_gmv_after_discounts)                                     AS net,
                SUM(f.provider_active_rate_value * f.provider_active_rate_weight)
                    / NULLIF(SUM(f.provider_active_rate_weight), 0) * 100            AS avail,
                SUM(f.provider_acceptance_rate_value * f.provider_acceptance_rate_weight)
                    / NULLIF(SUM(f.provider_acceptance_rate_weight), 0) * 100        AS accept,
                SUM(f.customer_refunded_order_rate_value * f.customer_refunded_order_rate_weight)
                    / NULLIF(SUM(f.customer_refunded_order_rate_weight), 0) * 100    AS refunds,
                SUM(f.order_total_minutes_per_order_value * f.order_total_minutes_per_order_weight)
                    / NULLIF(SUM(f.order_total_minutes_per_order_weight), 0)         AS del_time,
                SUM(f.provider_acceptance_minutes_per_order_value * f.provider_acceptance_minutes_per_order_weight)
                    / NULLIF(SUM(f.provider_acceptance_minutes_per_order_weight), 0) AS acc_time,
                SUM(f.provider_preparation_minutes_per_order_value * f.provider_preparation_minutes_per_order_weight)
                    / NULLIF(SUM(f.provider_preparation_minutes_per_order_weight), 0) AS prep_time,
                SUM(f.users_activated_vendor_count)                                  AS new_users,
                SUM(f.provider_impressions_sessions_count)                           AS sessions,
                SUM(f.provider_menu_viewed_sessions_count)                           AS menu_views,
                SUM(f.provider_product_added_from_menu_viewed_rate_value * f.provider_product_added_from_menu_viewed_rate_weight)
                    / NULLIF(SUM(f.provider_product_added_from_menu_viewed_rate_weight), 0) * 100 AS menu_prod,
                SUM(f.provider_rating_per_order_value * f.provider_rating_per_order_weight)
                    / NULLIF(SUM(f.provider_rating_per_order_weight), 0)             AS rating,
                SUM(f.total_campaign_discount)                                       AS discounts,
                SUM(f.total_campaign_spend_bolt)                                     AS camp_bolt,
                SUM(f.total_campaign_spend_provider)                                 AS camp_merch
            FROM {schema}.fact_provider_monthly f
            WHERE f.provider_id IN ({pids_sql})
              AND f.metric_timestamp_partition >= '{global_start}'
              AND f.metric_timestamp_partition <  '{global_end}'
            GROUP BY 1, 2
            ORDER BY 1, 2
        """)

        # Unique customers can't be summed across weeks, so take the monthly figure.
        # entity_id here is a STRING holding providers, zones and countries alike, so
        # comparing it against bare numbers makes Spark cast the whole column and fail.
        users_rows = run_query(ctx, f"""
            SELECT
                entity_id AS provider_id,
                DATE_FORMAT(DATE_TRUNC('month', metric_timestamp_partition), 'yyyy-MM-dd') AS mstart,
                SUM(provider_deliveries_unique_user_count) AS active_users
            FROM {schema}.int_provider_metrics_non_additive
            WHERE entity_id IN ({pids_str})
              AND timeframe_name = 'month'
              AND metric_timestamp_partition >= '{global_start}'
              AND metric_timestamp_partition <  '{global_end}'
            GROUP BY 1, 2
        """)

        # «Цікаві цифри» — приємний бонус, а не основа звіту. Якщо ці запити
        # впадуть, решта звіту має вийти як зазвичай.
        try:
            fun = fetch_fun_facts(ctx, schema, pids_sql)
        except Exception as exc:
            print(f"  ⚠️  не вдалося зібрати «Цікаві цифри»: {exc}")
            fun = {}

    finally:
        destroy_ctx(ctx)

    loc_map = {int(r[0]): {"name": str(r[1]), "city": str(r[2] or ""), "zone": str(r[3] or "")}
               for r in loc_rows}

    users_map: dict[tuple[int, str], int] = {}
    for row in users_rows:
        users_map[(int(row[0]), str(row[1])[:7])] = _si(row[2])

    by_pid: dict[int, dict] = {}
    for row in fact_rows:
        pid = int(row[0])
        mk  = str(row[1])[:7]
        orders    = _si(row[2])
        gross     = _sf(row[3])
        net       = _sf(row[4])
        avail     = round(_sf(row[5]), 1)
        accept    = round(_sf(row[6]), 1)
        refunds   = round(_sf(row[7]), 1)
        del_time  = round(_sf(row[8]),  1)
        acc_time  = round(_sf(row[9]),  1)
        prep_time = round(_sf(row[10]), 1)
        new_users = _si(row[11])
        sessions  = _si(row[12])
        menu_views= _si(row[13])
        menu_prod = round(_sf(row[14]), 1)
        rating    = round(_sf(row[15]), 2)
        discounts = round(_sf(row[16]), 0)
        camp_bolt = round(_sf(row[17]), 0)
        camp_merch= round(_sf(row[18]), 0)
        active_u  = users_map.get((pid, mk)) or orders
        aov       = round(gross / orders, 0) if orders else 0
        freq      = round(orders / active_u, 2) if active_u else 0
        imp_menu  = round(menu_views / sessions * 100, 1) if sessions else 0

        by_pid.setdefault(pid, {})[mk] = {
            "orders": orders, "gross": round(gross, 0), "net": round(net, 0),
            "aov": aov, "avail": avail, "accept": accept, "refunds": refunds,
            "del_time": del_time, "acc_time": acc_time, "prep_time": prep_time,
            "new_users": new_users, "sessions": sessions, "imp_menu": imp_menu,
            "menu_prod": menu_prod, "rating": rating,
            "discounts": discounts, "camp_bolt": camp_bolt, "camp_merch": camp_merch,
            "active_users": active_u, "freq": freq,
        }

    locations = []
    for pid in pids:
        info = loc_map[pid]
        months_data = []
        for mk, lbl, lbls in zip(month_keys, month_labels, month_labels_s):
            rec = dict(by_pid.get(pid, {}).get(mk, EMPTY_MONTH))
            rec["month_key"] = mk
            rec["label"]     = lbl
            rec["label_s"]   = lbls
            months_data.append(rec)
        city_en = info["city"] or "—"
        locations.append({
            "provider_id": pid,
            "name": info["name"],
            "city_en": city_en,
            "city": CITY_UK.get(city_en, city_en),
            "zone": info["zone"],
            "months": months_data,
        })

    # Групування по містах: одне місто може мати кілька закладів мережі.
    cities: dict[str, dict] = {}
    for loc in locations:
        c = cities.setdefault(loc["city"], {
            "city": loc["city"],
            "city_en": loc["city_en"],
            "slug": re.sub(r"[^a-z0-9]+", "-", loc["city_en"].lower()).strip("-") or f"c{loc['provider_id']}",
            "locations": [],
        })
        c["locations"].append(loc)
    for c in cities.values():
        c["months"] = aggregate_months([l["months"] for l in c["locations"]])

    # Найбільші міста першими — партнер бачить основний обсяг одразу.
    cities_list = sorted(cities.values(),
                         key=lambda c: c["months"][-1]["orders"] if c["months"] else 0,
                         reverse=True)

    brand_months = aggregate_months([l["months"] for l in locations])

    # Найдорожче замовлення показуємо разом із містом, а не з ID закладу.
    if fun.get("top_order"):
        pid = fun["top_order"]["provider_id"]
        info = loc_map.get(pid, {})
        city_en = info.get("city", "")
        fun["top_order"]["city"] = CITY_UK.get(city_en, city_en or f"ID {pid}")

    return {
        "locations": locations,
        "cities": cities_list,
        "fun": fun,
        "brand_months": brand_months,
        "month_keys": month_keys,
        "month_labels": month_labels,
        "month_labels_s": month_labels_s,
        "period_label": f"{month_labels[0]} — {month_labels[-1]}" if month_labels else "",
    }


def fetch_data_checked() -> dict:
    """Retry the fetch and reject empty results.

    A result with no locations or no orders at all means the query or the
    cluster misbehaved, not that the brand stopped selling — accepting it would
    publish a report full of zeros.
    """
    last_exc: Exception | None = None
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            data = fetch_data()
            if not data["locations"]:
                raise RuntimeError("запит не повернув жодної локації")
            if not any(m.get("orders") for m in data["brand_months"]):
                raise RuntimeError("запит не повернув замовлень ні за один місяць")
            return data
        except Exception as exc:
            last_exc = exc
            print(f"  спроба {attempt}/{FETCH_ATTEMPTS} не вдалася: {exc}")
            if attempt < FETCH_ATTEMPTS:
                time.sleep(RETRY_DELAY_S)
    raise last_exc


# ─── ANALYSIS ──────────────────────────────────────────────────────────────────

def _pct_chg(old, new):
    if not old:
        return None
    return (new - old) / old * 100


def analyze(months: list[dict], title: str) -> dict:
    if len(months) < 2:
        return {"severity": 0, "issues": [], "advice": [], "trend": "stable", "prev": {}, "last": {}}
    prev, last = months[-2], months[-1]
    first = months[0]
    issues, advice, severity = [], [], 0

    o_chg   = _pct_chg(prev["orders"], last["orders"])
    o_trend = _pct_chg(first["orders"], last["orders"])

    if last["orders"] < 30:
        issues.append(f"Дуже мало замовлень за місяць — {last['orders']} (минулого: {prev['orders']}).")
        advice.append("Перевірте години роботи, фото та опис меню. Переконайтеся, що заклад видно в зоні доставки.")
        severity += 3
    elif o_chg is not None and o_chg <= -20:
        issues.append(f"Різке падіння замовлень: {prev['orders']} → {last['orders']} ({o_chg:.0f}%).")
        advice.append("Перевірте, чи не було довгих пауз офлайн, змін у меню або знижок.")
        severity += 2
    elif o_chg is not None and o_chg <= -10:
        issues.append(f"Замовлень менше, ніж місяць тому: {prev['orders']} → {last['orders']} ({o_chg:.0f}%).")
        severity += 1
    if o_chg is not None and o_chg >= 15:
        issues.append(f"Приємне зростання замовлень: {prev['orders']} → {last['orders']} (+{o_chg:.0f}%).")

    if o_trend is not None and o_trend <= -25:
        issues.append(f"За {N_MONTHS} місяців замовлення впали з {first['orders']} до {last['orders']} ({o_trend:.0f}%).")
        advice.append("Розгляньте підключення Розумних акцій або спонсорованих оголошень для відновлення трафіку.")
        severity += 2

    if last["avail"] < 90:
        issues.append(f"Заклад доступний лише {last['avail']:.1f}% часу — гості часто не знаходять вас онлайн.")
        advice.append("Тримайте заклад увімкненим в обідні та вечірні пікові години.")
        severity += 2

    if last["accept"] < 97:
        issues.append(f"Замовлення не завжди приймаються вчасно — {last['accept']:.1f}%.")
        advice.append("Приймайте замовлення в застосунку якомога швидше, орієнтир — до 1 хвилини.")
        severity += 2

    if last["refunds"] >= 5:
        issues.append(f"Часті компенсації клієнтам — {last['refunds']:.1f}% замовлень.")
        advice.append("Перевірте меню на актуальність, правильність збірки та час приготування.")
        severity += 2
    elif last["refunds"] >= 3 and last["refunds"] > prev["refunds"] + 1:
        issues.append(f"Компенсацій стало більше: {prev['refunds']:.1f}% → {last['refunds']:.1f}%.")
        severity += 1

    if last["prep_time"] >= 35:
        issues.append(f"Тривалий час приготування — {last['prep_time']:.1f} хв.")
        advice.append("Оновіть час приготування у порталі або оптимізуйте кухонний процес у пікові години.")
        severity += 1

    if last["rating"] and last["rating"] < 4.4:
        issues.append(f"Рейтинг нижчий за комфортний — {last['rating']:.2f} з 5.")
        advice.append("Перегляньте останні негативні відгуки та усуньте часті причини.")
        severity += 2

    if last["imp_menu"] < 8 and last["sessions"] > 500:
        issues.append(f"Лише {last['imp_menu']:.1f}% переглядів у стрічці переходять до меню.")
        advice.append("Оновіть головне фото та бейджі акцій — щоб гість охочіше натискав на заклад.")
        severity += 1

    trend = "stable"
    if o_chg is not None:
        trend = "up" if o_chg >= 10 else "down" if o_chg <= -10 else "stable"

    return {
        "severity": severity, "issues": issues, "advice": advice, "title": title,
        "trend": trend, "prev": prev, "last": last, "o_chg": o_chg,
    }


# ─── HTML HELPERS ──────────────────────────────────────────────────────────────

def _fmt(v, unit="₴", decimals=0) -> str:
    try:
        f = float(v)
        if unit == "%":
            return f"{f:.1f}%"
        if unit == "з 5":
            return f"{f:.2f}"
        if unit in ("хв", "зам./гість"):
            return f"{f:.1f}"
        s = f"{int(round(f)):,}".replace(",", "\u202f")
        return f"{s}\u202f{unit}" if unit else s
    except (TypeError, ValueError):
        return "—"


def _pct_badge(old, new) -> str:
    ch = _pct_chg(old, new)
    if ch is None:
        return ""
    sign = "▲" if ch >= 0 else "▼"
    cls  = "positive" if ch >= 0 else "danger"
    return f'<span class="delta {cls}">{sign}\u202f{abs(ch):.1f}%</span>'


def _bar_chart(values: list, labels: list, unit: str, colors: list, max_val=None) -> str:
    nums = [float(v or 0) for v in values]
    m = max_val or (max(nums) if nums else 1) or 1
    bars = ""
    for i, (v, lbl) in enumerate(zip(nums, labels)):
        h = max(4, int(v / m * 110))
        col = colors[i % len(colors)]
        display = _fmt(v, unit)
        bars += (
            f'<div class="bar-col">'
            f'<div class="bar-val">{display}</div>'
            f'<div class="bar" style="height:{h}px;background:{col}"></div>'
            f'<div class="bar-lbl">{lbl}</div>'
            f'</div>'
        )
    return f'<div class="bars-scroll"><div class="bars">{bars}</div></div>'


def _kpi_card(label: str, value: str, delta: str = "", color: str = "var(--green)") -> str:
    return (
        f'<div class="kpi-card" style="border-top-color:{color}">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}{delta}</div>'
        f'</div>'
    )


def _severity_cls(sev: int) -> str:
    if sev >= 4: return "sev-high"
    if sev >= 2: return "sev-mid"
    return "sev-ok"


def _sev_label(sev: int) -> str:
    return ['OK','помірно','помірно','увага','увага','критично'][min(sev, 5)]


def _plural_uk(n, one: str, few: str, many: str) -> str:
    """1 порція / 2 порції / 5 порцій."""
    n = abs(int(n))
    if 11 <= n % 100 <= 19:
        return many
    last = n % 10
    if last == 1:
        return one
    if 2 <= last <= 4:
        return few
    return many


def _trend_icon(trend: str) -> str:
    return {"up": "↑", "down": "↓", "stable": "→"}.get(trend, "→")


def _kpi_block(months: list[dict], color: str) -> str:
    last = months[-1] if months else {}
    prev = months[-2] if len(months) > 1 else {}
    return (
        _kpi_card("Delivered Orders", _fmt(last.get("orders"), "шт."),
                  _pct_badge(prev.get("orders",0), last.get("orders",0)), color) +
        _kpi_card("Gross Sales", _fmt(last.get("gross"), "₴"),
                  _pct_badge(prev.get("gross",0), last.get("gross",0)), color) +
        _kpi_card("Net Sales", _fmt(last.get("net"), "₴"),
                  _pct_badge(prev.get("net",0), last.get("net",0)), color) +
        _kpi_card("AOV", _fmt(last.get("aov"), "₴"),
                  _pct_badge(prev.get("aov",0), last.get("aov",0))) +
        _kpi_card("Availability", _fmt(last.get("avail"), "%"),
                  _pct_badge(prev.get("avail",0), last.get("avail",0))) +
        _kpi_card("Acceptance", _fmt(last.get("accept"), "%"),
                  _pct_badge(prev.get("accept",0), last.get("accept",0))) +
        _kpi_card("Refund Rate", _fmt(last.get("refunds"), "%"),
                  _pct_badge(prev.get("refunds",0), last.get("refunds",0)), "#c0392b") +
        _kpi_card("Rating", _fmt(last.get("rating"), "з 5"),
                  _pct_badge(prev.get("rating",0), last.get("rating",0)), "#e67e22") +
        _kpi_card("Active Users", _fmt(last.get("active_users"), "осіб"),
                  _pct_badge(prev.get("active_users",0), last.get("active_users",0))) +
        _kpi_card("Знижки (Bolt)", _fmt(last.get("camp_bolt"), "₴"),
                  _pct_badge(prev.get("camp_bolt",0), last.get("camp_bolt",0))) +
        _kpi_card("Знижки (партнер)", _fmt(last.get("camp_merch"), "₴"),
                  _pct_badge(prev.get("camp_merch",0), last.get("camp_merch",0)))
    )


def _charts_block(months: list[dict], labels_s: list[str]) -> str:
    html = ""
    for sec_title, metric_keys in CHART_SECTIONS:
        charts = ""
        for mk in metric_keys:
            name, desc, unit = METRIC_UK.get(mk, (mk, "", ""))
            vals = [m.get(mk, 0) for m in months]
            charts += (
                f'<div class="chart-card">'
                f'<h3>{name}</h3>'
                f'<div class="metric-desc">{desc}</div>'
                f'<div class="unit">{unit}</div>'
                f'{_bar_chart(vals, labels_s, unit, MONTH_BAR_COLORS)}'
                f'</div>'
            )
        html += f'<div class="section-title">{sec_title}</div><div class="charts-grid">{charts}</div>'
    return html


def _analysis_block(anal: dict, heading: str = "Аналіз") -> str:
    issues_html = "".join(f"<li>{i}</li>" for i in anal["issues"]) if anal["issues"] else "<li>Без зауважень</li>"
    advice_html = "".join(f"<li>{a}</li>" for a in anal["advice"])
    advice_part = f'<h4>Рекомендації:</h4><ul class="advice">{advice_html}</ul>' if advice_html else ""
    return f"""
    <div class="loc-analysis {_severity_cls(anal['severity'])}">
      <div class="loc-analysis-head">
        <h3>{heading}</h3>
        <span class="sev-badge">{_sev_label(anal['severity'])}</span>
      </div>
      <h4>Що варто знати:</h4>
      <ul>{issues_html}</ul>
      {advice_part}
    </div>"""


# ─── HTML BUILDERS ─────────────────────────────────────────────────────────────

def build_fun_facts(data: dict) -> str:
    """Розділ «Цікаві цифри»: підсумки за весь час роботи мережі на платформі."""
    fun = data.get("fun") or {}
    if not fun.get("orders"):
        return ""

    period = ""
    if fun.get("first_date") and fun.get("last_date"):
        period = f"{date_uk(fun['first_date'])} — {date_uk(fun['last_date'])}"

    cards = (
        f'<div class="fun-card">'
        f'<div class="fun-label">Замовлень за весь час</div>'
        f'<div class="fun-value">{_fmt(fun["orders"], "")}</div>'
        f'<div class="fun-sub">доставлених замовлень по всій мережі</div>'
        f'</div>'
        f'<div class="fun-card">'
        f'<div class="fun-label">Gross Sales за весь час</div>'
        f'<div class="fun-value">{_fmt(fun.get("gross"), "₴")}</div>'
        f'<div class="fun-sub">до застосування знижок</div>'
        f'</div>'
        f'<div class="fun-card">'
        f'<div class="fun-label">Net Sales за весь час</div>'
        f'<div class="fun-value">{_fmt(fun.get("net"), "₴")}</div>'
        f'<div class="fun-sub">після знижок клієнтам</div>'
        f'</div>'
    )

    top = fun.get("top_order")
    if top:
        cards += (
            f'<div class="fun-card">'
            f'<div class="fun-label">Найдорожче замовлення</div>'
            f'<div class="fun-value">{_fmt(top.get("gross"), "₴")}</div>'
            f'<div class="fun-sub">{top.get("city","")} &nbsp;·&nbsp; {date_uk(top.get("date",""))}</div>'
            f'</div>'
        )

    items = fun.get("top_items") or []
    items_html = ""
    if items:
        max_qty = max(i["qty"] for i in items) or 1
        rows = ""
        for idx, it in enumerate(items, 1):
            width = max(6, int(it["qty"] / max_qty * 100))
            rows += (
                f'<div class="item-row">'
                f'<div class="item-rank">{idx}</div>'
                f'<div class="item-body">'
                f'<div class="item-name">{it["name"]}</div>'
                f'<div class="item-bar"><span style="width:{width}%"></span></div>'
                f'</div>'
                f'<div class="item-qty">{_fmt(it["qty"], "")} '
                f'<span>{_plural_uk(it["qty"], "порція", "порції", "порцій")}</span></div>'
                f'</div>'
            )
        leader = items[0]
        items_html = f"""
        <div class="fun-panel">
          <h3>Топ-{len(items)} позицій меню за весь час</h3>
          <p class="fun-panel-lead">Найчастіше замовляють «{leader['name']}» —
             {_fmt(leader['qty'], '')} {_plural_uk(leader['qty'], 'порція', 'порції', 'порцій')}
             у {leader['locations']} {_plural_uk(leader['locations'], 'місті', 'містах', 'містах')}.</p>
          {rows}
          <p class="fun-note">Рахуються страви з меню; напої-додатки та опції до страв
             (як-от «з зеленню») у підрахунок не входять.</p>
        </div>"""

    top_note = ""
    if top:
        discount_note = ""
        if top.get("net") and abs(top["net"] - top["gross"]) >= 1:
            discount_note = (f" Зі знижкою клієнт сплатив {_fmt(top['net'], '₴')}.")
        top_note = (
            f'<p class="fun-note">Найбільший чек — {_fmt(top.get("gross"), "₴")} '
            f'({top.get("city","")}, {date_uk(top.get("date",""))}).{discount_note}</p>'
        )

    return f"""
    <div class="section-title">✨ Цікаві цифри — за весь час на Bolt Food</div>
    <div class="fun-period">Період: {period} &nbsp;·&nbsp; уся мережа, всі міста</div>
    <div class="fun-grid">{cards}</div>
    {items_html}
    {top_note}
    """


def build_cities_table(data: dict) -> str:
    """Порівняння міст за останній місяць — одразу видно, де мережа росте."""
    rows = ""
    brand_last = data["brand_months"][-1] if data["brand_months"] else {}
    total_orders = brand_last.get("orders", 0) or 1
    for c in data["cities"]:
        last = c["months"][-1] if c["months"] else {}
        prev = c["months"][-2] if len(c["months"]) > 1 else {}
        share = last.get("orders", 0) / total_orders * 100
        rows += (
            f'<tr>'
            f'<td class="city-cell"><button class="city-link" onclick="switchTab(\'{c["slug"]}\')">{c["city"]}</button></td>'
            f'<td>{len(c["locations"])}</td>'
            f'<td>{_fmt(last.get("orders",0),"шт.")} {_pct_badge(prev.get("orders",0), last.get("orders",0))}</td>'
            f'<td>{_fmt(last.get("gross",0),"₴")} {_pct_badge(prev.get("gross",0), last.get("gross",0))}</td>'
            f'<td>{_fmt(last.get("aov",0),"₴")}</td>'
            f'<td>{_fmt(last.get("avail",0),"%")}</td>'
            f'<td>{_fmt(last.get("rating",0),"з 5")}</td>'
            f'<td>{share:.0f}%</td>'
            f'</tr>'
        )
    return f"""
    <div class="table-card">
      <table class="cmp-table">
        <thead><tr>
          <th>Місто</th><th>Закладів</th><th>Замовлення</th><th>Gross Sales</th>
          <th>AOV</th><th>Availability</th><th>Рейтинг</th><th>Частка мережі</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
    </div>"""


def build_brand_panel(data: dict) -> str:
    brand_months = data["brand_months"]
    labels_s     = data["month_labels_s"]
    if not brand_months:
        return '<p style="color:#999;padding:40px">Немає даних</p>'

    cities_str = ", ".join(c["city"] for c in data["cities"])
    anal = analyze(brand_months, "Мережа")

    return f"""
    <div class="period-bar">
      <span class="period-label">Місяці:</span>
      <span>{data['period_label']} &nbsp;·&nbsp; валюта UAH (₴) &nbsp;·&nbsp; {len(data['locations'])} закладів у {len(data['cities'])} містах</span>
      <span style="margin-left:auto;font-size:11px;color:var(--gray-400)">Останній місяць: {data['month_labels'][-1]}</span>
    </div>

    {build_fun_facts(data)}

    <div class="section-title">Мережа загалом — останній місяць</div>
    <div class="kpi-grid">{_kpi_block(brand_months, BRAND_COLOR)}</div>

    {_charts_block(brand_months, labels_s)}

    {_analysis_block(anal, "Аналіз мережі")}

    <div class="section-title">Міста — порівняння за {data['month_labels'][-1]}</div>
    {build_cities_table(data)}
    <div class="table-note">Міста: {cities_str}. Детальні графіки по кожному місту — у вкладках угорі.</div>
    """


def build_city_panel(city: dict, data: dict) -> str:
    months   = city["months"]
    labels_s = data["month_labels_s"]
    anal     = analyze(months, city["city"])

    loc_items = ""
    # Один заклад у місті — його графіки дублювали б міські, тож показуємо лише картку.
    show_loc_charts = len(city["locations"]) > 1
    for loc in city["locations"]:
        l_anal = analyze(loc["months"], loc["name"])
        last_loc = l_anal.get("last", {})
        prev_loc = l_anal.get("prev", {})
        loc_id = f'{city["slug"]}_{loc["provider_id"]}'
        body = ""
        if show_loc_charts:
            body = f"""
          <div class="loc-body" id="loc_{loc_id}" hidden>
            {_charts_block(loc['months'], labels_s)}
            {_analysis_block(l_anal, 'Аналіз закладу')}
          </div>"""
        btn = (f'<button class="loc-open-btn" aria-expanded="false" '
               f'onclick="toggleLoc(\'{loc_id}\', this)">Детальніше ▾</button>') if show_loc_charts else ""
        loc_items += f"""
        <div class="loc-card">
          <div class="loc-row">
            <div class="loc-row-info">
              <h2>{_trend_icon(l_anal['trend'])} {loc['name']}</h2>
              <div class="loc-meta">
                {loc['zone']} &nbsp;·&nbsp; ID {loc['provider_id']} &nbsp;·&nbsp;
                {_fmt(last_loc.get('orders',0),'шт.')} зам. &nbsp;·&nbsp;
                {_fmt(last_loc.get('gross',0),'₴')}
                {_pct_badge(prev_loc.get('orders',0), last_loc.get('orders',0))}
              </div>
            </div>
            {btn}
          </div>{body}
        </div>"""

    return f"""
    <div class="period-bar">
      <span class="period-label">Місяці:</span>
      <span>{data['period_label']} &nbsp;·&nbsp; валюта UAH (₴) &nbsp;·&nbsp; {city['city']} &nbsp;·&nbsp; {len(city['locations'])} заклад(ів)</span>
      <span style="margin-left:auto;font-size:11px;color:var(--gray-400)">Останній місяць: {data['month_labels'][-1]}</span>
    </div>

    <div class="section-title">{city['city']} — останній місяць</div>
    <div class="kpi-grid">{_kpi_block(months, BRAND_COLOR)}</div>

    {_charts_block(months, labels_s)}

    {_analysis_block(anal, f"Аналіз — {city['city']}")}

    <div class="section-title">Заклади в місті</div>
    <div class="loc-list">{loc_items}</div>
    """


def build_html(data: dict) -> str:
    today  = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
    period = data["period_label"]

    tabs   = (f'<button class="brand-tab active" id="btab_brand" onclick="switchTab(\'brand\')" '
              f'style="--bc:{BRAND_COLOR}">{BRAND_EMOJI} ВЕСЬ БРЕНД</button>')
    panels = f'<div id="bpanel_brand" style="display:block">{build_brand_panel(data)}</div>'

    for city in data["cities"]:
        tabs += (f'<button class="brand-tab" id="btab_{city["slug"]}" '
                 f'onclick="switchTab(\'{city["slug"]}\')" style="--bc:{BRAND_COLOR}">'
                 f'📍 {city["city"].upper()}</button>')
        panels += (f'<div id="bpanel_{city["slug"]}" style="display:none">'
                   f'{build_city_panel(city, data)}</div>')

    cities_line = " · ".join(c["city"] for c in data["cities"])

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>MBR · Той Самий Баранчик</title>
  <style>
    :root{{
      --green:#34D186;--green-d:#0d8a52;--black:#0d0d0d;
      --gray-700:#4a4a4a;--gray-400:#9a9a9a;--gray-100:#f5f5f5;
      --positive:#1aad6a;--warning:#e67e22;--danger:#c0392b;
    }}
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
      font-size:14px;line-height:1.55;color:#1a1a1a;background:var(--gray-100)}}
    .header{{background:var(--black);padding:20px 40px;display:flex;align-items:flex-start;
      justify-content:space-between;border-bottom:4px solid var(--green);flex-wrap:wrap;gap:16px}}
    .header-left{{display:flex;align-items:center;gap:14px;flex:1;min-width:240px}}
    .bolt-logo{{width:44px;height:44px;background:var(--green);border-radius:10px;
      display:flex;align-items:center;justify-content:center}}
    .header-title h1{{font-size:22px;font-weight:700;color:#fff}}
    .header-title p{{font-size:11px;color:var(--green);text-transform:uppercase;
      letter-spacing:1.2px;font-weight:600;margin-top:4px}}
    .header-meta{{text-align:right;color:var(--gray-400);font-size:12px;line-height:1.9}}
    .header-meta strong{{color:var(--green)}}

    .brand-tabs{{background:#fff;padding:0 40px;display:flex;gap:4px;overflow-x:auto;
      border-bottom:2px solid #eee;position:sticky;top:0;z-index:50;box-shadow:0 2px 6px rgba(0,0,0,.06)}}
    .brand-tab{{padding:14px 20px;border:none;background:transparent;cursor:pointer;
      font-size:13px;font-weight:700;color:var(--gray-400);border-bottom:3px solid transparent;
      transition:all .2s;white-space:nowrap}}
    .brand-tab:hover{{color:var(--bc,var(--green-d))}}
    .brand-tab.active{{color:var(--bc,var(--green-d));border-bottom-color:var(--bc,var(--green-d))}}

    .container{{max-width:1320px;margin:0 auto;padding:28px 40px 48px}}
    .period-bar{{background:#fff;border-radius:12px;padding:14px 20px;margin-bottom:20px;
      display:flex;align-items:center;gap:12px;flex-wrap:wrap;
      box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .period-label{{font-size:11px;font-weight:700;text-transform:uppercase;color:var(--gray-700)}}
    .section-title{{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;
      color:var(--gray-700);padding-bottom:10px;border-bottom:2px solid var(--green);margin:28px 0 10px}}
    .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;margin-bottom:8px}}
    .kpi-card{{background:#fff;border-radius:12px;padding:14px 16px;border-top:3px solid var(--green);
      box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .kpi-label{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--gray-400);margin-bottom:4px}}
    .kpi-value{{font-size:19px;font-weight:700}}
    .delta{{font-size:11px;font-weight:600;margin-left:4px}}
    .delta.positive{{color:var(--positive)}}
    .delta.danger{{color:var(--danger)}}
    .charts-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px;margin-bottom:12px}}
    .chart-card{{background:#fff;border-radius:12px;padding:16px 18px;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .chart-card h3{{font-size:12px;font-weight:700;color:var(--gray-700);margin-bottom:4px}}
    .metric-desc{{font-size:11px;color:var(--gray-700);margin-bottom:4px;line-height:1.4}}
    .unit{{font-size:10px;color:var(--gray-400);margin-bottom:8px}}
    .bars-scroll{{overflow-x:auto;padding-bottom:4px}}
    .bars{{display:flex;gap:6px;align-items:flex-end;min-height:120px;padding-top:6px}}
    .bar-col{{display:flex;flex-direction:column;align-items:center;min-width:44px;flex-shrink:0;
      height:110px;justify-content:flex-end}}
    .bar-val{{font-size:8px;font-weight:700;color:var(--gray-700);margin-bottom:3px;
      text-align:center;max-width:52px;line-height:1.15}}
    .bar{{width:36px;border-radius:5px 5px 0 0;min-height:4px}}
    .bar-lbl{{font-size:8px;color:var(--gray-400);margin-top:3px;text-align:center;line-height:1.2}}

    /* Цікаві цифри */
    .fun-period{{font-size:11px;color:var(--gray-400);margin:0 0 10px}}
    .fun-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(215px,1fr));gap:12px;margin-bottom:14px}}
    .fun-card{{background:linear-gradient(135deg,#3b1c00 0%,#7a4416 55%,#b5651d 100%);
      color:#fff;border-radius:14px;padding:18px 20px;box-shadow:0 2px 10px rgba(0,0,0,.12)}}
    .fun-label{{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.7px;
      color:rgba(255,255,255,.72);margin-bottom:6px}}
    .fun-value{{font-size:27px;font-weight:800;line-height:1.15}}
    .fun-sub{{font-size:11px;color:rgba(255,255,255,.75);margin-top:6px}}
    .fun-panel{{background:#fff;border-radius:12px;padding:18px 20px;
      box-shadow:0 1px 4px rgba(0,0,0,.06);margin-bottom:10px}}
    .fun-panel h3{{font-size:13px;font-weight:700;color:var(--gray-700);margin-bottom:4px}}
    .fun-panel-lead{{font-size:12px;color:var(--gray-700);margin-bottom:12px}}
    .item-row{{display:flex;align-items:center;gap:12px;padding:7px 0;border-bottom:1px solid #f4f4f4}}
    .item-row:last-of-type{{border-bottom:none}}
    .item-rank{{flex-shrink:0;width:22px;height:22px;border-radius:50%;background:var(--gray-100);
      color:var(--gray-700);font-size:11px;font-weight:700;display:flex;align-items:center;
      justify-content:center}}
    .item-body{{flex:1;min-width:0}}
    .item-name{{font-size:13px;font-weight:600;margin-bottom:4px}}
    .item-bar{{height:7px;background:var(--gray-100);border-radius:4px;overflow:hidden}}
    .item-bar span{{display:block;height:100%;border-radius:4px;
      background:linear-gradient(90deg,#b5651d,#e29e6c)}}
    .item-qty{{flex-shrink:0;font-size:13px;font-weight:700;white-space:nowrap}}
    .item-qty span{{font-size:10px;font-weight:400;color:var(--gray-400)}}
    .fun-note{{font-size:11px;color:var(--gray-400);margin-top:10px}}

    .table-card{{background:#fff;border-radius:12px;padding:6px 6px;box-shadow:0 1px 4px rgba(0,0,0,.06);overflow-x:auto}}
    .cmp-table{{width:100%;border-collapse:collapse;font-size:13px;min-width:720px}}
    .cmp-table th{{text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.6px;
      color:var(--gray-400);padding:12px 14px;border-bottom:1px solid #eee;white-space:nowrap}}
    .cmp-table td{{padding:12px 14px;border-bottom:1px solid #f4f4f4;white-space:nowrap}}
    .cmp-table tr:last-child td{{border-bottom:none}}
    .city-cell{{font-weight:700}}
    .city-link{{border:none;background:transparent;font:inherit;font-weight:700;color:var(--green-d);
      cursor:pointer;padding:0}}
    .city-link:hover{{text-decoration:underline}}
    .table-note{{font-size:11px;color:var(--gray-400);margin-top:10px}}

    .loc-card{{background:#fff;border-radius:12px;margin:0 0 10px;
      box-shadow:0 1px 4px rgba(0,0,0,.06);border:1px solid #eee;overflow:hidden}}
    .loc-row{{display:flex;align-items:center;justify-content:space-between;gap:16px;
      padding:14px 18px;flex-wrap:wrap}}
    .loc-row-info{{flex:1;min-width:180px}}
    .loc-row-info h2{{font-size:15px;color:var(--black);font-weight:700}}
    .loc-open-btn{{flex-shrink:0;padding:9px 16px;border:none;border-radius:8px;
      background:var(--green-d);color:#fff;font-size:13px;font-weight:600;cursor:pointer}}
    .loc-open-btn:hover{{background:var(--green);color:var(--black)}}
    .loc-body{{padding:0 18px 20px;border-top:1px solid #f0f0f0}}
    .loc-meta{{font-size:12px;color:var(--gray-400);margin-top:2px}}
    .loc-list{{display:flex;flex-direction:column;gap:0}}
    .loc-analysis{{background:#fff;border-radius:10px;padding:16px 18px;
      margin-top:20px;border-left:4px solid var(--gray-400);box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .loc-analysis.sev-high{{border-left-color:var(--danger);background:#fff8f6}}
    .loc-analysis.sev-mid{{border-left-color:var(--warning);background:#fffaf3}}
    .loc-analysis.sev-ok{{border-left-color:var(--positive)}}
    .loc-analysis-head{{display:flex;justify-content:space-between;align-items:center;
      gap:8px;margin-bottom:8px}}
    .loc-analysis-head h3{{font-size:14px;color:var(--gray-700)}}
    .loc-analysis h4{{font-size:12px;margin:10px 0 4px;color:var(--gray-700)}}
    .loc-analysis ul{{margin-left:18px;font-size:13px}}
    .loc-analysis ul.advice{{color:var(--green-d)}}
    .sev-badge{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--warning)}}
    .footer{{background:var(--black);color:var(--gray-400);font-size:11px;padding:22px 40px;text-align:center}}
    .footer span{{color:var(--green)}}
    @media(max-width:700px){{
      .container{{padding:16px}}.charts-grid{{grid-template-columns:1fr}}
      .header{{padding:16px}}.brand-tabs{{padding:0 16px}}
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
      <h1>MBR · {BRAND_TITLE}</h1>
      <p>Bolt Food &nbsp;·&nbsp; Місячний звіт &nbsp;·&nbsp; {cities_line}</p>
    </div>
  </div>
  <div class="header-meta">
    <div>Період: <strong>{period}</strong></div>
    <div>Місяців: <strong>{N_MONTHS}</strong> &nbsp;·&nbsp; Оновлено: <strong>{today}</strong></div>
  </div>
</header>

<div class="brand-tabs">
  {tabs}
</div>

<div class="container">
  {panels}
</div>

<div class="footer">
  Автоматично оновлюється 3-го числа кожного місяця — додається попередній повний місяць &nbsp;·&nbsp;
  <span>Bolt Food MBR</span> &nbsp;·&nbsp; {BRAND_TITLE}
</div>

<script>
function switchTab(slug) {{
  document.querySelectorAll('.brand-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('[id^="bpanel_"]').forEach(p => p.style.display = 'none');
  document.getElementById('btab_' + slug).classList.add('active');
  document.getElementById('bpanel_' + slug).style.display = 'block';
  window.scrollTo({{top: 0, behavior: 'smooth'}});
}}

function toggleLoc(id, btn) {{
  const body = document.getElementById('loc_' + id);
  const open = btn.getAttribute('aria-expanded') === 'true';
  body.hidden = open;
  btn.setAttribute('aria-expanded', !open);
  btn.textContent = open ? 'Детальніше ▾' : 'Згорнути ▴';
}}
</script>
</body>
</html>"""


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    today = datetime.date.today().isoformat()
    print(f"=== MBR {BRAND_TITLE} [{today}] ===\n")
    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN not set"); sys.exit(1)

    global CLUSTER
    print("🔌 Кластер Databricks...")
    CLUSTER = pick_cluster()

    print(f"📊 {BRAND_TITLE}...")
    try:
        data = fetch_data_checked()
    except Exception as exc:
        print(f"\n❌ Не вдалося отримати дані з Databricks: {exc}")
        print(f"Звіт НЕ перезаписано, попередня версія збережена:\n   {OUTPUT_HTML}")
        sys.exit(1)

    print(f"  → {len(data['brand_months'])} місяців, "
          f"{len(data['locations'])} локацій, {len(data['cities'])} міст")

    html = build_html(data)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"\n✅ Saved → {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
