#!/usr/bin/env python3
"""
MBR — MARCELLO (Харків)
Місячний звіт партнера: продажі, операційні показники, воронка гостя,
знижки та платне просування (Sponsored Listing, Smart Promotion).

Дані помісячно, лише повні календарні місяці. Автооновлення 4-го числа
кожного місяця через GitHub Actions: у звіт додається щойно закритий місяць.
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
# `or` замість getenv-default: CI передає порожній рядок, коли секрет не заданий,
# а порожній host складається у URL без схеми.
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST") or "https://bolt-incentives.cloud.databricks.com"
CLUSTER_ID_ENV  = os.getenv("DATABRICKS_CLUSTER_ID") or ""

# Legacy-кластер (hive_metastore) вивели з експлуатації, актуальний — Unity Catalog.
# Обидва тримаємо у списку: скрипт бере перший, до якого може підключитися.
CLUSTER_CANDIDATES = ["0505-112942-d3yviznw", "0221-081903-9ag4bh69"]
SCHEMA_CANDIDATES  = ["main.ng_delivery", "ng_delivery_spark"]

BRAND_NAME   = "MARCELLO KHARKIV"
BRAND_TITLE  = "MARCELLO"
CITY_UK      = "Харків"
CITY_DB      = "Kharkiv"
NAME_STRIP   = r"Marcello\s*"

MAX_MONTHS   = 14      # скільки повних місяців максимум показуємо
SCRIPT_DIR   = Path(__file__).parent
OUTPUT_HTML  = SCRIPT_DIR / "MBR Marcello.html"

POLL_INTERVAL_S = 5
MAX_POLL_S      = 600
FETCH_ATTEMPTS  = 3
RETRY_DELAY_S   = 20

UK_MONTHS_SHORT = ["", "Січ", "Лют", "Бер", "Кві", "Тра", "Чер",
                   "Лип", "Сер", "Вер", "Жов", "Лис", "Гру"]
UK_MONTHS_FULL  = ["", "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
                   "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"]

METRIC_UK = {
    "orders":       ("Доставлені замовлення",      "Скільки замовлень успішно доїхало до гостя",          "шт."),
    "gross":        ("Gross Sales",                "Вартість доставлених замовлень до знижок",            "₴"),
    "net":          ("Net Sales",                  "Сума після того, як застосували знижки гостям",       "₴"),
    "aov":          ("Середній чек (AOV)",         "Середня сума одного доставленого замовлення",         "₴"),
    "avail":        ("Час онлайн",                 "Частка часу, коли заклад був доступний для замовлень","%"),
    "accept":       ("Прийняття замовлень",        "Частка замовлень, які прийняли вчасно",               "%"),
    "refunds":      ("Компенсації гостям",         "Частка замовлень, за які гостю повернули кошти",      "%"),
    "prep_time":    ("Час приготування",           "Скільки хвилин кухня збирає замовлення",              "хв"),
    "rating":       ("Рейтинг",                    "Середня оцінка закладу від гостей",                   "з 5"),
    "sessions":     ("Покази у застосунку",        "Скільки разів заклад показали гостям у стрічці/пошуку","показів"),
    "menu_views":   ("Відкриття меню",             "Скільки гостей натиснули на заклад і відкрили меню",  "сесій"),
    "imp_menu":     ("Показ → меню (CTR)",         "З кожних 100 показів стільки гостей відкрили меню",   "%"),
    "menu_prod":    ("Меню → кошик",               "Частка відкриттів меню, де гість додав страву",       "%"),
    "active_users": ("Активні гості",              "Унікальні гості, які зробили замовлення",             "осіб"),
    "new_users":    ("Нові гості",                 "Гості, які замовили у вас уперше",                    "осіб"),
    "freq":         ("Частота замовлень",          "Скільки разів у середньому замовляє один гість",       "зам./гість"),
    "discounts":    ("Знижки гостям — усього",     "Загальна сума знижок, яку отримали гості",            "₴"),
    "camp_merch":   ("Знижки за кошт партнера",    "Скільки у знижки вклали ви",                          "₴"),
    "camp_bolt":    ("Знижки за кошт Bolt",        "Скільки у знижки вклав Bolt Food",                    "₴"),
    "camp_orders":  ("Замовлень зі знижкою",       "Скільки замовлень прийшло з акцією",                  "шт."),
    "camp_share":   ("Частка замовлень з акцією",  "Яка частка всіх замовлень прийшла завдяки знижці",    "%"),
    "sl_hours":     ("Sponsored Listing — години", "Скільки годин працювала платна реклама закладу",      "год"),
    "sl_orders":    ("Sponsored Listing — замовлення", "Замовлення, які привела платна реклама",          "шт."),
    "sl_cost_per":  ("Годин реклами на 1 замовлення", "Скільки годин показів витрачено на одне замовлення","год"),
    "sp_orders":    ("Smart Promotion — замовлення", "Замовлення від Розумних акцій",                     "шт."),
}

CHART_SECTIONS = [
    ("Продажі",              ["orders", "gross", "net", "aov"]),
    ("Операційні показники", ["avail", "accept", "refunds", "prep_time", "rating"]),
    ("Гості та воронка",     ["sessions", "menu_views", "imp_menu", "menu_prod",
                              "active_users", "new_users", "freq"]),
    ("Знижки та просування", ["discounts", "camp_merch", "camp_bolt", "camp_orders",
                              "camp_share", "sl_hours", "sl_orders"]),
]

NUMERIC_KEYS = [
    "orders", "gross", "net", "aov", "avail", "accept", "refunds", "prep_time",
    "rating", "rating_w",
    "sessions", "menu_views", "imp_menu", "menu_prod", "active_users", "new_users", "freq",
    "discounts", "camp_merch", "camp_bolt", "camp_orders", "camp_share",
    "sl_hours", "sl_orders", "sp_orders", "product_added", "order_placed",
]
EMPTY_MONTH = {k: 0 for k in NUMERIC_KEYS}

SUMMABLE = ["orders", "gross", "net", "sessions", "menu_views", "active_users", "new_users",
            "discounts", "camp_merch", "camp_bolt", "camp_orders",
            "sl_hours", "sl_orders", "sp_orders", "product_added", "order_placed"]


# ─── DATE HELPERS ──────────────────────────────────────────────────────────────

def last_n_full_months(n: int) -> list[tuple[int, int]]:
    """Повні календарні місяці, найстаріший — перший. Поточний місяць не входить."""
    d = datetime.date.today().replace(day=1)
    months: list[tuple[int, int]] = []
    for _ in range(n):
        d -= datetime.timedelta(days=1)
        months.append((d.year, d.month))
        d = d.replace(day=1)
    return list(reversed(months))


def month_label(y: int, m: int, short: bool = False) -> str:
    return f"{UK_MONTHS_SHORT[m] if short else UK_MONTHS_FULL[m]} {y}"


def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"


def month_start(y: int, m: int) -> str:
    return datetime.date(y, m, 1).isoformat()


def month_after(y: int, m: int) -> str:
    return (datetime.date(y + 1, 1, 1) if m == 12 else datetime.date(y, m + 1, 1)).isoformat()


def next_update_date() -> datetime.date:
    """Наступне 4-те число — саме тоді GitHub Actions додає закритий місяць."""
    today = datetime.date.today()
    if today.day < 4:
        return today.replace(day=4)
    nxt = today.replace(day=1) + datetime.timedelta(days=31)
    return nxt.replace(day=4)


# ─── TOKEN ─────────────────────────────────────────────────────────────────────

def _load_token() -> str:
    tok = os.getenv("DATABRICKS_TOKEN", "").strip()
    if tok:
        return tok
    for profile in ("bolt-incentives-temp", "bolt-incentives", "DEFAULT"):
        try:
            out = subprocess.check_output(
                ["databricks", "auth", "token", "-p", profile],
                text=True, stderr=subprocess.DEVNULL, timeout=60)
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


# RESIZING — кластер працює і лише додає/знімає воркери, запити на ньому
# виконуються. PENDING/RESTARTING — треба дочекатися.
USABLE_STATES  = {"RUNNING", "RESIZING"}
PENDING_STATES = {"PENDING", "RESTARTING"}


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
        deadline = time.time() + 900
        while time.time() < deadline:
            time.sleep(15)
            if _cluster_state(cid) in USABLE_STATES:
                print(f"  кластер: {cid} (готовий)")
                return cid
    raise RuntimeError("Немає доступного кластера Databricks")


class Session:
    def __init__(self) -> None:
        self.cluster_id = pick_cluster()
        self.ctx = _post("/api/1.2/contexts/create",
                         {"language": "sql", "clusterId": self.cluster_id})["id"]
        self.schema = self._pick_schema()

    def _pick_schema(self) -> str:
        for schema in SCHEMA_CANDIDATES:
            try:
                self.query(f"SELECT 1 FROM {schema}.dim_provider_v2 LIMIT 1")
                print(f"  схема: {schema}")
                return schema
            except Exception:
                continue
        raise RuntimeError("Не знайдено схему з dim_provider_v2")

    def query(self, sql: str) -> list[list]:
        return self.query_named(sql)[1]

    def query_named(self, sql: str) -> tuple[list[str], list[list]]:
        cmd_id = _post("/api/1.2/commands/execute", {
            "language": "sql", "clusterId": self.cluster_id,
            "contextId": self.ctx, "command": sql,
        })["id"]
        deadline = time.time() + MAX_POLL_S
        while time.time() < deadline:
            time.sleep(POLL_INTERVAL_S)
            resp = _get("/api/1.2/commands/status", {
                "clusterId": self.cluster_id, "contextId": self.ctx, "commandId": cmd_id,
            })
            status = resp.get("status")
            if status == "Finished":
                res = resp.get("results", {})
                if res.get("resultType") == "error":
                    raise RuntimeError(res.get("summary") or res.get("cause") or "Query error")
                cols = [c.get("name") for c in (res.get("schema") or [])]
                return cols, (res.get("data") or [])
            if status in ("Cancelled", "Error"):
                raise RuntimeError(f"Query {status}: {json.dumps(resp.get('results', {}))[:600]}")
        raise TimeoutError(f"Запит не завершився за {MAX_POLL_S}s")

    def rows_as_dicts(self, sql: str) -> list[dict]:
        """Доступ до полів за іменем: порядок стовпців у SELECT більше не важливий."""
        cols, rows = self.query_named(sql)
        return [dict(zip(cols, r)) for r in rows]

    def close(self) -> None:
        try:
            _post("/api/1.2/contexts/destroy",
                  {"clusterId": self.cluster_id, "contextId": self.ctx})
        except Exception:
            pass


def _sf(v, d: float = 0.0) -> float:
    try:
        return float(v) if v is not None else d
    except (TypeError, ValueError):
        return d


def _si(v, d: int = 0) -> int:
    return int(round(_sf(v, d)))


# ─── DATA FETCH ────────────────────────────────────────────────────────────────

def fetch_data() -> dict:
    months = last_n_full_months(MAX_MONTHS)
    global_start = month_start(*months[0])
    global_end   = month_after(*months[-1])
    month_keys   = [month_key(y, m) for y, m in months]

    print(f"  вікно: {global_start} → {global_end}")
    s = Session()
    sch = s.schema
    try:
        loc_rows = s.rows_as_dicts(f"""
            SELECT provider_id, provider_name, city_name, zone_name
            FROM {sch}.dim_provider_v2
            WHERE brand_name = '{BRAND_NAME}'
              AND city_name  = '{CITY_DB}'
            ORDER BY provider_name
        """)
        if not loc_rows:
            raise RuntimeError(f"не знайдено локацій бренду {BRAND_NAME}")

        pids = [int(r["provider_id"]) for r in loc_rows]
        pids_sql = ", ".join(str(p) for p in pids)
        pids_str = ", ".join(f"'{p}'" for p in pids)
        print(f"  локацій: {len(pids)} — {pids}")

        # Тижневий ґрейн привʼязаний до понеділка і «зʼїдає» межі місяців,
        # тому беремо місячну факт-таблицю: рівно 1-ше — останнє число.
        fact_rows = s.rows_as_dicts(f"""
            SELECT
                f.provider_id                                        AS provider_id,
                DATE_FORMAT(DATE_TRUNC('month', f.metric_timestamp_partition), 'yyyy-MM') AS mk,
                SUM(f.delivered_orders_count)                        AS orders,
                SUM(f.total_gmv_before_discounts)                    AS gross,
                SUM(f.total_gmv_after_discounts)                     AS net,
                SUM(f.provider_active_rate_value * f.provider_active_rate_weight)
                    / NULLIF(SUM(f.provider_active_rate_weight), 0) * 100 AS avail,
                SUM(f.provider_acceptance_rate_value * f.provider_acceptance_rate_weight)
                    / NULLIF(SUM(f.provider_acceptance_rate_weight), 0) * 100 AS accept,
                SUM(f.customer_refunded_order_rate_value * f.customer_refunded_order_rate_weight)
                    / NULLIF(SUM(f.customer_refunded_order_rate_weight), 0) * 100 AS refunds,
                SUM(f.provider_preparation_minutes_per_order_value * f.provider_preparation_minutes_per_order_weight)
                    / NULLIF(SUM(f.provider_preparation_minutes_per_order_weight), 0) AS prep_time,
                SUM(f.provider_rating_per_order_value * f.provider_rating_per_order_weight)
                    / NULLIF(SUM(f.provider_rating_per_order_weight), 0) AS rating,
                SUM(f.provider_rating_per_order_weight)               AS rating_w,
                SUM(f.provider_impressions_sessions_count)           AS sessions,
                SUM(f.provider_menu_viewed_sessions_count)           AS menu_views,
                SUM(f.provider_product_added_sessions_count)         AS product_added,
                SUM(f.provider_order_placed_sessions_count)          AS order_placed,
                SUM(f.users_activated_vendor_count)                  AS new_users,
                SUM(f.total_campaign_discount)                       AS discounts,
                SUM(f.total_campaign_spend_bolt)                     AS camp_bolt,
                SUM(f.total_campaign_spend_provider)                 AS camp_merch,
                SUM(f.campaign_orders_count)                         AS camp_orders,
                SUM(f.sponsored_listing_duration_hours)              AS sl_hours,
                SUM(f.sponsored_listing_attributed_orders_count)     AS sl_orders,
                SUM(f.smart_promotion_campaign_orders_count)         AS sp_orders
            FROM {sch}.fact_provider_monthly f
            WHERE f.provider_id IN ({pids_sql})
              AND f.metric_timestamp_partition >= '{global_start}'
              AND f.metric_timestamp_partition <  '{global_end}'
            GROUP BY 1, 2
            ORDER BY 1, 2
        """)

        # Унікальних гостей не можна складати між місяцями — беремо місячний зріз.
        # entity_id у цій таблиці — STRING, тому порівнюємо з рядками.
        users_rows = s.rows_as_dicts(f"""
            SELECT entity_id AS provider_id,
                   DATE_FORMAT(DATE_TRUNC('month', metric_timestamp_partition), 'yyyy-MM') AS mk,
                   SUM(provider_deliveries_unique_user_count) AS active_users
            FROM {sch}.int_provider_metrics_non_additive
            WHERE entity_id IN ({pids_str})
              AND timeframe_name = 'month'
              AND metric_timestamp_partition >= '{global_start}'
              AND metric_timestamp_partition <  '{global_end}'
            GROUP BY 1, 2
        """)

        # Бенчмарк: заклади Харкова з піцою в назві. Бренд Marcello у назві піци
        # не має, тому у вибірку не потрапляє — порівняння виходить чистим.
        bench_rows = s.rows_as_dicts(f"""
            WITH pizza AS (
                SELECT DISTINCT provider_id
                FROM {sch}.dim_provider_v2
                WHERE city_name = '{CITY_DB}'
                  AND (LOWER(provider_name) LIKE '%pizz%'
                    OR LOWER(provider_name) LIKE '%піц%'
                    OR LOWER(provider_name) LIKE '%пицц%')
            )
            SELECT DATE_FORMAT(DATE_TRUNC('month', f.metric_timestamp_partition), 'yyyy-MM') AS mk,
                   COUNT(DISTINCT f.provider_id)        AS providers,
                   SUM(f.delivered_orders_count)        AS orders,
                   SUM(f.total_campaign_spend_provider) AS camp_merch,
                   SUM(f.campaign_orders_count)         AS camp_orders
            FROM {sch}.fact_provider_monthly f
            JOIN pizza p ON p.provider_id = f.provider_id
            WHERE f.metric_timestamp_partition >= '{global_start}'
              AND f.metric_timestamp_partition <  '{global_end}'
            GROUP BY 1
            ORDER BY 1
        """)

        city_rows = s.rows_as_dicts(f"""
            SELECT DATE_FORMAT(DATE_TRUNC('month', f.metric_timestamp_partition), 'yyyy-MM') AS mk,
                   SUM(f.delivered_orders_count) AS orders
            FROM {sch}.fact_provider_monthly f
            JOIN {sch}.dim_provider_v2 d ON d.provider_id = f.provider_id
            WHERE d.city_name = '{CITY_DB}'
              AND f.metric_timestamp_partition >= '{global_start}'
              AND f.metric_timestamp_partition <  '{global_end}'
            GROUP BY 1
            ORDER BY 1
        """)

        promo_rows = s.rows_as_dicts(f"""
            SELECT provider_id, promotion_type, state,
                   CAST(start AS STRING) AS started, CAST(end AS STRING) AS ended
            FROM {sch}.delivery_smart_promotion_log
            WHERE provider_id IN ({pids_sql})
            ORDER BY provider_id, start
        """)
    finally:
        s.close()

    loc_meta = {int(r["provider_id"]): {"name": str(r["provider_name"]),
                                        "zone": str(r["zone_name"] or ""),
                                        "city": str(r["city_name"] or CITY_DB)}
                for r in loc_rows}

    users_map: dict[tuple[int, str], int] = {}
    for r in users_rows:
        users_map[(int(r["provider_id"]), str(r["mk"])[:7])] = _si(r["active_users"])

    by_pid: dict[int, dict[str, dict]] = {}
    for r in fact_rows:
        pid = int(r["provider_id"])
        mk  = str(r["mk"])[:7]
        by_pid.setdefault(pid, {})[mk] = _parse_month_row(r, users_map.get((pid, mk), 0))

    # Місяці до відкриття закладу дають нулі і ламають графіки — обрізаємо їх.
    live_keys = [mk for mk in month_keys
                 if any(by_pid.get(p, {}).get(mk, {}).get("orders", 0) for p in pids)]
    if not live_keys:
        raise RuntimeError("немає жодного місяця із замовленнями")
    keys = [mk for mk in month_keys if mk >= live_keys[0]]

    labels   = [month_label(int(k[:4]), int(k[5:7])) for k in keys]
    labels_s = [month_label(int(k[:4]), int(k[5:7]), short=True) for k in keys]

    locations = []
    for pid in sorted(by_pid, key=lambda p: loc_meta.get(p, {}).get("name", "")):
        meta = loc_meta.get(pid, {"name": f"ID {pid}", "zone": "", "city": CITY_DB})
        series = []
        for mk, lbl, lbl_s in zip(keys, labels, labels_s):
            rec = dict(by_pid[pid].get(mk, EMPTY_MONTH))
            rec.update(month_key=mk, label=lbl, label_s=lbl_s)
            series.append(rec)
        short = re.sub(rf"(?i)^{NAME_STRIP}", "", meta["name"]).strip() or meta["name"]
        locations.append({
            "provider_id": pid, "name": meta["name"], "short_name": short,
            "zone": meta["zone"], "months": series,
        })

    brand_months = [_aggregate(locations, i, mk, lbl, lbl_s)
                    for i, (mk, lbl, lbl_s) in enumerate(zip(keys, labels, labels_s))]

    bench = {str(r["mk"])[:7]: {"providers": _si(r["providers"]), "orders": _si(r["orders"]),
                                "camp_merch": _sf(r["camp_merch"]),
                                "camp_orders": _si(r["camp_orders"])}
             for r in bench_rows}
    city  = {str(r["mk"])[:7]: _si(r["orders"]) for r in city_rows}

    smart_promo = [{"provider_id": int(r["provider_id"]), "type": str(r["promotion_type"]),
                    "state": str(r["state"]), "start": str(r["started"] or "")[:10],
                    "end": str(r["ended"] or "")[:10]}
                   for r in promo_rows]

    return {
        "locations": locations,
        "brand_months": brand_months,
        "month_keys": keys,
        "month_labels": labels,
        "month_labels_s": labels_s,
        "bench": bench,
        "city": city,
        "smart_promo": smart_promo,
        "period_label": f"{labels[0]} — {labels[-1]}",
    }


def _parse_month_row(r: dict, active_users: int) -> dict:
    orders     = _si(r["orders"])
    gross      = _sf(r["gross"])
    sessions   = _si(r["sessions"])
    menu_views = _si(r["menu_views"])
    added      = _si(r["product_added"])
    camp_ord   = _si(r["camp_orders"])
    au         = active_users or orders
    return {
        "orders": orders,
        "gross": round(gross, 0),
        "net": round(_sf(r["net"]), 0),
        "aov": round(gross / orders, 0) if orders else 0,
        "avail": round(_sf(r["avail"]), 1),
        "accept": round(_sf(r["accept"]), 1),
        "refunds": round(_sf(r["refunds"]), 1),
        "prep_time": round(_sf(r["prep_time"]), 1),
        "rating": round(_sf(r["rating"]), 2),
        "rating_w": _sf(r["rating_w"]),
        "sessions": sessions,
        "menu_views": menu_views,
        "imp_menu": round(menu_views / sessions * 100, 1) if sessions else 0,
        "product_added": added,
        "order_placed": _si(r["order_placed"]),
        "menu_prod": round(added / menu_views * 100, 1) if menu_views else 0,
        "new_users": _si(r["new_users"]),
        "active_users": au,
        "freq": round(orders / au, 2) if au else 0,
        "discounts": round(_sf(r["discounts"]), 0),
        "camp_bolt": round(_sf(r["camp_bolt"]), 0),
        "camp_merch": round(_sf(r["camp_merch"]), 0),
        "camp_orders": camp_ord,
        "camp_share": round(camp_ord / orders * 100, 1) if orders else 0,
        "sl_hours": round(_sf(r["sl_hours"]), 0),
        "sl_orders": _si(r["sl_orders"]),
        "sp_orders": _si(r["sp_orders"]),
    }


def _aggregate(locations: list[dict], i: int, mk: str, lbl: str, lbl_s: str) -> dict:
    agg = dict(EMPTY_MONTH)
    rows = [loc["months"][i] for loc in locations]
    for key in SUMMABLE:
        agg[key] = sum(r.get(key, 0) for r in rows)

    # Середні зважуємо тим, що їх породжує: якість — замовленнями,
    # конверсію — показами, інакше маленька локація тягне бренд.
    for metric, weight_key in [("avail", "orders"), ("accept", "orders"),
                               ("refunds", "orders"), ("prep_time", "orders"),
                               ("menu_prod", "menu_views")]:
        total_w = sum(r.get(weight_key, 0) for r in rows)
        if total_w:
            agg[metric] = round(sum(r.get(metric, 0) * r.get(weight_key, 0)
                                    for r in rows) / total_w, 1)

    rating_w = sum(r.get("rating_w", 0) for r in rows)
    if rating_w:
        agg["rating"] = round(sum(r.get("rating", 0) * r.get("rating_w", 0)
                                  for r in rows) / rating_w, 2)

    agg["aov"]        = round(agg["gross"] / agg["orders"], 0) if agg["orders"] else 0
    agg["freq"]       = round(agg["orders"] / agg["active_users"], 2) if agg["active_users"] else 0
    agg["imp_menu"]   = round(agg["menu_views"] / agg["sessions"] * 100, 1) if agg["sessions"] else 0
    agg["camp_share"] = round(agg["camp_orders"] / agg["orders"] * 100, 1) if agg["orders"] else 0
    agg.update(month_key=mk, label=lbl, label_s=lbl_s)
    return agg


def fetch_data_checked() -> dict:
    """Порожній результат — це збій запиту, а не «партнер перестав продавати».
    Публікувати такий звіт не можна, тому пробуємо ще раз."""
    last: Exception | None = None
    for attempt in range(1, FETCH_ATTEMPTS + 1):
        try:
            return fetch_data()
        except Exception as exc:
            last = exc
            print(f"  спроба {attempt}/{FETCH_ATTEMPTS} не вдалася: {exc}")
            if attempt < FETCH_ATTEMPTS:
                time.sleep(RETRY_DELAY_S)
    raise last


# ─── ANALYSIS ──────────────────────────────────────────────────────────────────

def _pct_chg(old, new):
    if not old:
        return None
    return (new - old) / old * 100


def _peak(months: list[dict], key: str) -> dict:
    return max(months, key=lambda m: m.get(key, 0)) if months else {}


def _corr(xs: list[float], ys: list[float]) -> float | None:
    """Пірсон між двома рядами однакової довжини."""
    n = len(xs)
    if n < 3:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx  = sum((x - mx) ** 2 for x in xs) ** 0.5
    dy  = sum((y - my) ** 2 for y in ys) ** 0.5
    if not dx or not dy:
        return None
    return num / (dx * dy)


def build_diagnosis(data: dict) -> dict:
    """Порахувати все, на що спирається розділ «чому падають замовлення»."""
    months = data["brand_months"]
    last   = months[-1]
    prev   = months[-2] if len(months) > 1 else last
    peak   = _peak(months, "orders")

    orders_vs_peak = _pct_chg(peak["orders"], last["orders"])
    orders_vs_prev = _pct_chg(prev["orders"], last["orders"])

    # Ринок: індексуємо від того ж місяця, що й пік партнера, — інакше
    # порівнюємо різні бази і «зростання ринку» виглядає випадковим.
    pk = peak["month_key"]
    lk = last["month_key"]
    city_chg  = _pct_chg(data["city"].get(pk, 0), data["city"].get(lk, 0))
    bench_pk  = data["bench"].get(pk, {})
    bench_lk  = data["bench"].get(lk, {})
    bench_chg = _pct_chg(bench_pk.get("orders", 0), bench_lk.get("orders", 0))
    bench_disc_chg = _pct_chg(bench_pk.get("camp_merch", 0), bench_lk.get("camp_merch", 0))

    # Знижки партнера: скільки місяців підряд по нулю на кінець періоду
    zero_streak = 0
    for m in reversed(months):
        if m["camp_merch"] <= 0:
            zero_streak += 1
        else:
            break

    disc_corr = _corr([m["camp_merch"] for m in months], [m["orders"] for m in months])

    # Sponsored Listing: скільки годин показів іде на одне замовлення.
    sl_months = [m for m in months if m["sl_hours"] > 0]
    sl_eff = [{"label_s": m["label_s"], "hours": m["sl_hours"], "orders": m["sl_orders"],
               "per_order": round(m["sl_hours"] / m["sl_orders"], 1) if m["sl_orders"] else None}
              for m in sl_months]
    sl_best  = min((e for e in sl_eff if e["per_order"]), key=lambda e: e["per_order"], default=None)
    sl_last  = sl_eff[-1] if sl_eff else None

    sp_total = sum(m["sp_orders"] for m in months)
    # У логу лишаються рядки зі state='active' від акцій, які вже закінчились,
    # тому окремо перевіряємо, що період усе ще триває.
    today = datetime.date.today().isoformat()
    sp_active = [p for p in data["smart_promo"]
                 if p["state"] == "active" and (not p["end"] or p["end"] >= today)]

    return {
        "last": last, "prev": prev, "peak": peak,
        "orders_vs_peak": orders_vs_peak, "orders_vs_prev": orders_vs_prev,
        "city_chg": city_chg, "bench_chg": bench_chg, "bench_disc_chg": bench_disc_chg,
        "bench_pk": bench_pk, "bench_lk": bench_lk,
        "sessions_chg": _pct_chg(peak["sessions"], last["sessions"]),
        "ctr_peak": peak["imp_menu"], "ctr_last": last["imp_menu"],
        "menu_prod_peak": peak["menu_prod"], "menu_prod_last": last["menu_prod"],
        "aov_chg": _pct_chg(peak["aov"], last["aov"]),
        "new_users_chg": _pct_chg(peak["new_users"], last["new_users"]),
        "disc_peak": _peak(months, "camp_merch"),
        "zero_streak": zero_streak,
        "disc_corr": disc_corr,
        "sl_eff": sl_eff, "sl_best": sl_best, "sl_last": sl_last,
        "sp_total": sp_total, "sp_active": sp_active,
        "ops_ok": (last["avail"] >= 95 and last["accept"] >= 97 and last["refunds"] <= 3),
    }


def analyze_location(loc: dict) -> dict:
    months = loc["months"]
    last, prev = months[-1], months[-2] if len(months) > 1 else months[-1]
    peak = _peak(months, "orders")
    notes: list[str] = []
    severity = 0

    chg_peak = _pct_chg(peak["orders"], last["orders"])
    if chg_peak is not None and chg_peak <= -50:
        notes.append(f"Замовлення впали з {peak['orders']} шт. у «{peak['label']}» "
                     f"до {last['orders']} шт. — це {chg_peak:.0f}%.")
        severity += 3
    elif chg_peak is not None and chg_peak <= -25:
        notes.append(f"Замовлення нижче за пік: {peak['orders']} → {last['orders']} шт. "
                     f"({chg_peak:.0f}%).")
        severity += 2

    if last["camp_merch"] <= 0:
        notes.append("У цьому місяці локація не вкладала власних коштів у знижки — "
                     "у стрічці немає бейджа акції, і гість обирає конкурента з бейджем.")
        severity += 2
    elif last["camp_share"] < 40:
        notes.append(f"Лише {last['camp_share']:.0f}% замовлень прийшли з акцією "
                     f"(було {prev['camp_share']:.0f}%).")
        severity += 1

    if last["sessions"] and last["imp_menu"] < 5:
        notes.append(f"З кожних 100 показів меню відкривають лише {last['imp_menu']:.1f} гостей — "
                     "картка закладу не чіпляє.")
        severity += 2

    if last["sl_hours"] > 0 and last["sl_orders"] == 0:
        notes.append(f"Sponsored Listing працював {_fmt(last['sl_hours'], 'год')}, "
                     "але не принiс жодного замовлення.")
        severity += 2
    elif last["sl_hours"] > 0 and last["sl_orders"]:
        notes.append(f"Sponsored Listing: {_fmt(last['sl_hours'], 'год')} показів → "
                     f"{last['sl_orders']} зам. "
                     f"({last['sl_hours'] / last['sl_orders']:.0f} год на 1 замовлення).")

    if last["avail"] < 95:
        notes.append(f"Заклад був онлайн лише {last['avail']:.1f}% часу.")
        severity += 2
    if last["refunds"] >= 3:
        notes.append(f"Компенсації гостям — {last['refunds']:.1f}% замовлень.")
        severity += 1
    if last["rating"] and last["rating"] < 4.3:
        notes.append(f"Рейтинг {last['rating']:.2f} з 5 — нижче за комфортний рівень.")
        severity += 1

    if not notes:
        notes.append("Показники в межах норми.")

    trend = "stable"
    chg_prev = _pct_chg(prev["orders"], last["orders"])
    if chg_prev is not None:
        trend = "up" if chg_prev >= 10 else "down" if chg_prev <= -10 else "stable"

    return {"severity": severity, "notes": notes, "trend": trend,
            "last": last, "prev": prev, "peak": peak}


# ─── FORMATTING ────────────────────────────────────────────────────────────────

NBSP = "\u202f"


def _fmt(v, unit: str = "₴") -> str:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return "—"
    if unit == "%":
        return f"{f:.1f}%"
    if unit == "з 5":
        return f"{f:.2f}" if f else "—"
    if unit in ("хв", "зам./гість"):
        return f"{f:.1f}"
    s = f"{int(round(f)):,}".replace(",", NBSP)
    return f"{s}{NBSP}{unit}" if unit else s


def _signed(v, unit: str = "%") -> str:
    if v is None:
        return "—"
    return f"{'+' if v >= 0 else '−'}{abs(v):.0f}{unit}"


def _delta(old, new, good_up: bool = True) -> str:
    ch = _pct_chg(old, new)
    if ch is None:
        return ""
    up = ch >= 0
    cls = "positive" if up == good_up else "danger"
    return f'<span class="delta {cls}">{"▲" if up else "▼"}{NBSP}{abs(ch):.1f}%</span>'


def _kpi(label: str, value: str, delta: str = "", accent: str = "") -> str:
    style = f' style="border-top-color:{accent}"' if accent else ""
    return (f'<div class="kpi"{style}><div class="kpi-l">{label}</div>'
            f'<div class="kpi-v">{value}{delta}</div></div>')


def _bars(values: list, labels: list, unit: str, palette: list[str]) -> str:
    nums = [float(v or 0) for v in values]
    top = max(nums) if nums else 0
    top = top or 1
    cols = ""
    for i, (v, lbl) in enumerate(zip(nums, labels)):
        h = max(3, int(v / top * 108))
        cols += (f'<div class="bcol"><div class="bval">{_fmt(v, unit)}</div>'
                 f'<div class="bar" style="height:{h}px;background:{palette[i % len(palette)]}">'
                 f'</div><div class="blbl">{lbl}</div></div>')
    return f'<div class="bscroll"><div class="bars">{cols}</div></div>'


def _dual_bars(series_a: list, series_b: list, labels: list,
               unit_a: str, unit_b: str, name_a: str, name_b: str,
               color_a: str, color_b: str) -> str:
    """Дві серії різних розмірностей: кожна масштабується до свого максимуму,
    тож читаємо форму кривих, а не абсолютну висоту."""
    a = [float(v or 0) for v in series_a]
    b = [float(v or 0) for v in series_b]
    ta, tb = (max(a) or 1), (max(b) or 1)
    cols = ""
    for i, lbl in enumerate(labels):
        ha = max(3, int(a[i] / ta * 100))
        hb = max(3, int(b[i] / tb * 100))
        cols += (
            f'<div class="dcol">'
            f'<div class="dpair">'
            f'<div class="dwrap"><div class="dval">{_fmt(a[i], unit_a)}</div>'
            f'<div class="dbar" style="height:{ha}px;background:{color_a}"></div></div>'
            f'<div class="dwrap"><div class="dval">{_fmt(b[i], unit_b)}</div>'
            f'<div class="dbar" style="height:{hb}px;background:{color_b}"></div></div>'
            f'</div><div class="blbl">{lbl}</div></div>')
    return (f'<div class="legend">'
            f'<span><i style="background:{color_a}"></i>{name_a}</span>'
            f'<span><i style="background:{color_b}"></i>{name_b}</span></div>'
            f'<div class="bscroll"><div class="dbars">{cols}</div></div>')


def _funnel(rows: list[tuple[str, int, str]]) -> str:
    """rows: (підпис, значення, пояснення). Ширина смуги — від першого кроку."""
    base = rows[0][1] or 1
    out = ""
    for i, (label, value, hint) in enumerate(rows):
        w = max(6, value / base * 100)
        share = value / base * 100
        step = ""
        if i:
            prev_v = rows[i - 1][1] or 1
            step = f'<span class="fstep">{value / prev_v * 100:.1f}% від попереднього кроку</span>'
        out += (f'<div class="frow"><div class="fhead"><b>{label}</b>'
                f'<span class="fval">{_fmt(value, "")}</span></div>'
                f'<div class="ftrack"><div class="ffill" style="width:{w:.2f}%"></div>'
                f'<span class="fshare">{share:.1f}%</span></div>'
                f'<div class="fhint">{hint} {step}</div></div>')
    return f'<div class="funnel">{out}</div>'


# ─── ARTWORK ───────────────────────────────────────────────────────────────────
# Піца намальована інлайновим SVG: звіт лишається одним файлом і відкривається
# без інтернету, а картинка не «зникне» разом із зовнішнім хостингом.

PIZZA_TOP = """
<svg class="pizza-art" viewBox="0 0 200 200" aria-hidden="true">
  <defs>
    <radialGradient id="dough" cx="45%" cy="40%">
      <stop offset="0%" stop-color="#F7D794"/><stop offset="100%" stop-color="#E0A64B"/>
    </radialGradient>
    <radialGradient id="cheese" cx="45%" cy="40%">
      <stop offset="0%" stop-color="#FFE9A8"/><stop offset="100%" stop-color="#F6C445"/>
    </radialGradient>
  </defs>
  <circle cx="100" cy="100" r="94" fill="url(#dough)"/>
  <circle cx="100" cy="100" r="78" fill="url(#cheese)"/>
  <g fill="#C6301C">
    <circle cx="72"  cy="66"  r="13"/><circle cx="130" cy="78"  r="11"/>
    <circle cx="62"  cy="120" r="12"/><circle cx="108" cy="118" r="14"/>
    <circle cx="142" cy="132" r="10"/><circle cx="96"  cy="164" r="10"/>
    <circle cx="150" cy="46"  r="7"/>
  </g>
  <g fill="#2E7D32" opacity=".85">
    <ellipse cx="92"  cy="92"  rx="9" ry="5" transform="rotate(-25 92 92)"/>
    <ellipse cx="138" cy="104" rx="8" ry="4.5" transform="rotate(15 138 104)"/>
    <ellipse cx="74"  cy="150" rx="8" ry="4.5" transform="rotate(-10 74 150)"/>
  </g>
  <g fill="#FFF6DC" opacity=".9">
    <circle cx="118" cy="52" r="5"/><circle cx="46" cy="92" r="4.5"/>
    <circle cx="124" cy="150" r="4.5"/>
  </g>
</svg>
"""

PIZZA_SLICE = """
<svg class="slice-art" viewBox="0 0 64 64" aria-hidden="true">
  <path d="M32 4 L60 54 Q32 64 4 54 Z" fill="#FFD46B"/>
  <path d="M4 54 Q32 64 60 54 L62 59 Q32 70 2 59 Z" fill="#E0A64B"/>
  <circle cx="32" cy="28" r="5" fill="#C6301C"/>
  <circle cx="20" cy="44" r="4.5" fill="#C6301C"/>
  <circle cx="44" cy="45" r="4.5" fill="#C6301C"/>
  <ellipse cx="38" cy="37" rx="4" ry="2.4" fill="#2E7D32"/>
</svg>
"""

BOLT_MARK = ('<svg viewBox="0 0 24 24" width="26" height="26" aria-hidden="true">'
             '<path d="M13 2L4.5 13.5H11L10 22L19.5 10.5H13V2Z" fill="#0d0d0d"/></svg>')

CSS = """
:root{
  --green:#34D186; --green-d:#0d8a52; --green-l:#e8faf1;
  --ink:#0d0d0d; --g700:#4a4a4a; --g400:#9a9a9a; --g100:#f5f6f8;
  --pos:#1aad6a; --warn:#e08a1e; --dang:#d0342c; --tomato:#E8501E;
  --line:#e8eaed;
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Inter,sans-serif;
  font-size:14px;line-height:1.55;color:#1a1a1a;background:var(--g100);
  -webkit-font-smoothing:antialiased}

/* ── Header ── */
.hdr{position:relative;overflow:hidden;background:var(--ink);
  padding:26px 40px;border-bottom:4px solid var(--green)}
.hdr-in{position:relative;z-index:2;display:flex;align-items:flex-start;
  justify-content:space-between;gap:20px;flex-wrap:wrap;max-width:1320px;margin:0 auto}
.hdr-l{display:flex;align-items:center;gap:14px;min-width:260px;flex:1}
.mark{width:46px;height:46px;background:var(--green);border-radius:11px;
  display:flex;align-items:center;justify-content:center;flex-shrink:0}
.hdr h1{font-size:23px;font-weight:700;color:#fff;letter-spacing:-.01em}
.hdr .sub{font-size:11px;color:var(--green);text-transform:uppercase;
  letter-spacing:1.2px;font-weight:600;margin-top:4px}
.hdr-m{text-align:right;color:var(--g400);font-size:12px;line-height:1.9}
.hdr-m strong{color:var(--green)}
.pizza-art{position:absolute;z-index:1;opacity:.16;pointer-events:none}
.hdr .pizza-art{width:230px;height:230px;right:-52px;top:-58px;transform:rotate(-12deg)}

/* ── Nav ── */
.nav{background:#fff;border-bottom:1px solid var(--line);position:sticky;top:0;z-index:60;
  box-shadow:0 2px 6px rgba(0,0,0,.05)}
.nav-in{max-width:1320px;margin:0 auto;padding:0 40px;display:flex;gap:2px;overflow-x:auto}
.nav a{padding:13px 16px;font-size:13px;font-weight:650;color:var(--g400);
  text-decoration:none;border-bottom:3px solid transparent;white-space:nowrap}
.nav a:hover{color:var(--green-d);border-bottom-color:var(--green-l)}

.wrap{max-width:1320px;margin:0 auto;padding:26px 40px 56px}
.period{background:#fff;border-radius:12px;padding:13px 18px;margin-bottom:22px;
  display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:13px;
  box-shadow:0 1px 4px rgba(0,0,0,.05)}
.period b{font-size:11px;text-transform:uppercase;letter-spacing:.6px;color:var(--g700)}

h2.sec{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;
  color:var(--g700);padding-bottom:10px;border-bottom:2px solid var(--green);
  margin:34px 0 14px;display:flex;align-items:center;gap:9px;scroll-margin-top:70px}
.slice-art{width:22px;height:22px;flex-shrink:0}
h3.sub{font-size:14px;font-weight:700;margin:22px 0 8px;color:var(--ink)}

/* ── KPI ── */
.kpis{display:grid;grid-template-columns:repeat(auto-fill,minmax(158px,1fr));gap:12px}
.kpi{background:#fff;border-radius:12px;padding:13px 15px;border-top:3px solid var(--green);
  box-shadow:0 1px 4px rgba(0,0,0,.05)}
.kpi-l{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--g400);
  margin-bottom:4px;line-height:1.3;min-height:26px}
.kpi-v{font-size:19px;font-weight:700}
.delta{font-size:11px;font-weight:600;margin-left:5px;white-space:nowrap}
.delta.positive{color:var(--pos)}
.delta.danger{color:var(--dang)}

/* ── Charts ── */
.charts{display:grid;grid-template-columns:repeat(auto-fill,minmax(330px,1fr));gap:16px}
.card{background:#fff;border-radius:12px;padding:16px 18px;box-shadow:0 1px 4px rgba(0,0,0,.05)}
.card h4{font-size:12.5px;font-weight:700;color:var(--ink);margin-bottom:3px}
.desc{font-size:11px;color:var(--g700);line-height:1.45;margin-bottom:2px}
.unit{font-size:10px;color:var(--g400);margin-bottom:9px}
.bscroll{overflow-x:auto;padding-bottom:4px}
.bars,.dbars{display:flex;gap:6px;align-items:flex-end;min-height:122px;padding-top:6px}
.bcol{display:flex;flex-direction:column;align-items:center;min-width:46px;flex-shrink:0;
  height:112px;justify-content:flex-end}
.bval{font-size:8px;font-weight:700;color:var(--g700);margin-bottom:3px;text-align:center;
  max-width:56px;line-height:1.15}
.bar{width:34px;border-radius:5px 5px 0 0;min-height:3px}
.blbl{font-size:8.5px;color:var(--g400);margin-top:4px;text-align:center;line-height:1.2}
.dcol{display:flex;flex-direction:column;align-items:center;min-width:62px;flex-shrink:0}
.dpair{display:flex;gap:3px;align-items:flex-end;height:112px}
.dwrap{display:flex;flex-direction:column;align-items:center;justify-content:flex-end;height:100%}
.dval{font-size:7.5px;font-weight:700;color:var(--g700);margin-bottom:2px;text-align:center;
  line-height:1.1;max-width:30px}
.dbar{width:16px;border-radius:4px 4px 0 0;min-height:3px}
.legend{display:flex;gap:16px;font-size:11px;color:var(--g700);margin-bottom:8px;flex-wrap:wrap}
.legend i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:5px}

/* ── Diagnosis ── */
.diag{background:#fff;border-radius:14px;padding:0;overflow:hidden;
  box-shadow:0 1px 4px rgba(0,0,0,.05);margin-bottom:16px}
.diag-h{display:flex;gap:13px;align-items:flex-start;padding:18px 20px;
  background:linear-gradient(135deg,#0d0d0d,#1f2a24);color:#fff}
.diag-n{flex-shrink:0;width:30px;height:30px;border-radius:9px;background:var(--green);
  color:var(--ink);font-weight:800;font-size:14px;display:flex;align-items:center;
  justify-content:center}
.diag-h h3{font-size:15.5px;font-weight:700;line-height:1.35}
.diag-h p{font-size:12.5px;color:#b9c4bf;margin-top:3px}
.diag-b{padding:18px 20px}
.diag-b p{margin-bottom:11px;font-size:14px}
.diag-b p:last-child{margin-bottom:0}
.diag-b b{color:var(--ink)}
.num{font-weight:700;color:var(--green-d);white-space:nowrap}
.num.bad{color:var(--dang)}
.take{margin-top:14px;padding:13px 15px;border-radius:10px;background:var(--green-l);
  border-left:4px solid var(--green);font-size:13.5px}
.take.bad{background:#fff5f4;border-left-color:var(--dang)}
.take.warn{background:#fff9ef;border-left-color:var(--warn)}
.take > b:first-child{display:block;margin-bottom:3px;font-size:11px;text-transform:uppercase;
  letter-spacing:.6px;color:var(--g700)}

/* ── Facts / tables ── */
table.t{width:100%;border-collapse:collapse;font-size:13px;margin-top:10px}
table.t th,table.t td{padding:8px 10px;text-align:right;border-bottom:1px solid var(--line)}
table.t th:first-child,table.t td:first-child{text-align:left}
table.t th{font-size:10.5px;text-transform:uppercase;letter-spacing:.5px;color:var(--g400);
  font-weight:700;background:var(--g100)}
table.t tr:last-child td{border-bottom:none}
table.t td.bad{color:var(--dang);font-weight:650}
table.t td.good{color:var(--pos);font-weight:650}
.scroll-x{overflow-x:auto}

/* ── Funnel ── */
.funnel{margin-top:6px}
.frow{margin-bottom:14px}
.fhead{display:flex;justify-content:space-between;align-items:baseline;gap:10px;
  font-size:13px;margin-bottom:4px}
.fval{font-weight:700;color:var(--ink)}
.ftrack{position:relative;height:26px;background:var(--g100);border-radius:7px;overflow:hidden}
.ffill{height:100%;border-radius:7px;background:linear-gradient(90deg,var(--green),var(--green-d))}
.fshare{position:absolute;left:9px;top:4px;font-size:11.5px;font-weight:700;color:#0b3b26}
.fhint{font-size:11.5px;color:var(--g700);margin-top:4px}
.fstep{color:var(--green-d);font-weight:650}

/* ── Locations ── */
.loc{background:#fff;border:1px solid var(--line);border-radius:12px;margin-bottom:10px;
  overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.05)}
.loc-r{display:flex;align-items:center;justify-content:space-between;gap:16px;
  padding:14px 18px;flex-wrap:wrap}
.loc-i{flex:1;min-width:200px}
.loc-i h3{font-size:15px;font-weight:700}
.loc-meta{font-size:12px;color:var(--g400);margin-top:2px}
.loc-btn{flex-shrink:0;padding:9px 16px;border:none;border-radius:8px;background:var(--green-d);
  color:#fff;font-size:13px;font-weight:650;cursor:pointer}
.loc-btn:hover{background:var(--green);color:var(--ink)}
.loc-b{padding:2px 18px 20px;border-top:1px solid #f0f0f0}
.loc-an{background:var(--g100);border-radius:10px;padding:15px 17px;margin-top:18px;
  border-left:4px solid var(--g400)}
.loc-an.sev-high{border-left-color:var(--dang);background:#fff6f5}
.loc-an.sev-mid{border-left-color:var(--warn);background:#fffaf2}
.loc-an.sev-ok{border-left-color:var(--pos);background:var(--green-l)}
.loc-an h4{font-size:13px;margin-bottom:7px;color:var(--g700)}
.loc-an ul{margin-left:18px;font-size:13px}
.loc-an li{margin-bottom:4px}
.badge{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;
  padding:3px 9px;border-radius:999px;background:#fff;border:1px solid var(--line);color:var(--g700)}

/* ── Glovo ── */
.glv{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:14px;
  align-items:start}
.glv .card{border-left:4px solid #FFC244}
.glv .card.wide{grid-column:1/-1;border-left-color:var(--green)}
.glv .card.wide h4{color:var(--green-d)}
.glv .card h4{margin-bottom:6px}
.glv ul{margin-left:17px;font-size:13px}
.glv li{margin-bottom:5px}
.src{font-size:11px;color:var(--g400);margin-top:8px;word-break:break-all}
.src a{color:var(--green-d)}
.note{background:#fffaf2;border-left:4px solid var(--warn);border-radius:10px;
  padding:13px 16px;font-size:13px;margin-bottom:14px}

.foot{background:var(--ink);color:var(--g400);font-size:11.5px;padding:24px 40px;
  text-align:center;line-height:1.8}
.foot span{color:var(--green)}

@media(max-width:760px){
  .wrap{padding:16px}.charts{grid-template-columns:1fr}
  .hdr{padding:18px 16px}.nav-in{padding:0 16px}.hdr-m{text-align:left}
  .hdr .pizza-art{width:150px;height:150px;right:-40px;top:-34px}
}
"""


# ─── DIAGNOSIS SECTION ─────────────────────────────────────────────────────────

def _loc_name(data: dict, pid: int) -> str:
    for loc in data["locations"]:
        if loc["provider_id"] == pid:
            return loc["short_name"]
    return f"ID {pid}"


def _block(n: str, title: str, lead: str, body: str, take: str = "",
           take_cls: str = "", take_label: str = "Що це означає") -> str:
    take_html = (f'<div class="take {take_cls}"><b>{take_label}</b>{take}</div>') if take else ""
    return (f'<div class="diag"><div class="diag-h"><div class="diag-n">{n}</div>'
            f'<div><h3>{title}</h3><p>{lead}</p></div></div>'
            f'<div class="diag-b">{body}{take_html}</div></div>')


def build_diagnosis_html(data: dict, dg: dict) -> str:
    months = data["brand_months"]
    labels = data["month_labels_s"]
    last, peak, prev = dg["last"], dg["peak"], dg["prev"]
    out = ""

    # 1. Масштаб падіння
    lost = peak["orders"] - last["orders"]
    body = (
        f'<p>У найкращий місяць — <b>{peak["label"]}</b> — ви отримали '
        f'<span class="num">{_fmt(peak["orders"], "шт.")}</span> замовлень. '
        f'У <b>{last["label"]}</b> їх було '
        f'<span class="num bad">{_fmt(last["orders"], "шт.")}</span> — '
        f'це <span class="num bad">{_signed(dg["orders_vs_peak"])}</span>, '
        f'тобто мінус {_fmt(lost, "замовлень")} на місяць.</p>'
        f'<p>Разом із замовленнями просіли й гроші: Gross Sales '
        f'{_fmt(peak["gross"], "₴")} → <b>{_fmt(last["gross"], "₴")}</b>, '
        f'активних гостей {_fmt(peak["active_users"], "")} → '
        f'<b>{_fmt(last["active_users"], "")}</b>, '
        f'нових гостей {_fmt(peak["new_users"], "")} → '
        f'<b>{_fmt(last["new_users"], "")}</b> '
        f'({_signed(dg["new_users_chg"])}).</p>'
        f'<p>Середній чек при цьому не впав, а навпаки — '
        f'{_fmt(peak["aov"], "₴")} → <b>{_fmt(last["aov"], "₴")}</b> '
        f'({_signed(dg["aov_chg"])}). Тобто ті гості, які замовляють, '
        f'платять навіть більше. Проблема не в чеку, а в кількості гостей.</p>'
    )
    out += _block("1", "Скільки саме замовлень втрачено",
                  "Спочатку зафіксуємо масштаб — від чого відштовхуємось",
                  body,
                  f'Ви втрачаєте близько {_fmt(lost, "замовлень")} щомісяця порівняно '
                  f'з вашим власним найкращим результатом. Гості стали рідше замовляти, '
                  f'але не стали купувати дешевше.',
                  "bad")

    # 2. Ринок vs партнер
    bench_pk, bench_lk = dg["bench_pk"], dg["bench_lk"]
    body = (
        f'<p>Найважливіше питання: справа у вас чи «просто зараз усі падають»? '
        f'Порівняємо ті самі два місяці — <b>{peak["label"]}</b> та <b>{last["label"]}</b> — '
        f'по всьому Харкову.</p>'
        f'<div class="scroll-x"><table class="t">'
        f'<tr><th>Показник</th><th>{peak["label"]}</th><th>{last["label"]}</th><th>Зміна</th></tr>'
        f'<tr><td>Marcello — замовлення</td><td>{_fmt(peak["orders"], "")}</td>'
        f'<td>{_fmt(last["orders"], "")}</td>'
        f'<td class="bad">{_signed(dg["orders_vs_peak"])}</td></tr>'
        f'<tr><td>Уся доставка Харкова — замовлення</td>'
        f'<td>{_fmt(data["city"].get(peak["month_key"], 0), "")}</td>'
        f'<td>{_fmt(data["city"].get(last["month_key"], 0), "")}</td>'
        f'<td class="good">{_signed(dg["city_chg"])}</td></tr>'
        f'<tr><td>Піцерії Харкова ({bench_lk.get("providers", 0)} закладів) — замовлення</td>'
        f'<td>{_fmt(bench_pk.get("orders", 0), "")}</td>'
        f'<td>{_fmt(bench_lk.get("orders", 0), "")}</td>'
        f'<td class="good">{_signed(dg["bench_chg"])}</td></tr>'
        f'<tr><td>Піцерії Харкова — власні знижки закладів</td>'
        f'<td>{_fmt(bench_pk.get("camp_merch", 0), "₴")}</td>'
        f'<td>{_fmt(bench_lk.get("camp_merch", 0), "₴")}</td>'
        f'<td class="good">{_signed(dg["bench_disc_chg"])}</td></tr>'
        f'</table></div>'
        f'<p style="margin-top:12px">Ринок Харкова за цей час '
        f'<b>виріс на {_signed(dg["city_chg"])}</b>, піцерії міста — '
        f'<b>{_signed(dg["bench_chg"])}</b>. І водночас вони '
        f'<b>наростили власні вкладення у знижки на {_signed(dg["bench_disc_chg"])}</b>.</p>'
    )
    out += _block("2", "Це не ринок — це конкуренція за увагу",
                  "Порівняння з усім Харковом і окремо з піцеріями міста", body,
                  "Гості з Bolt Food не поділися — їх стало більше. Вони просто пішли "
                  "до тих піцерій, які цього року активніше боролися за увагу. "
                  "Ваші замовлення забрали конкуренти, а не криза.",
                  "bad")

    # 3. Воронка
    f_rows = [
        ("Показали заклад у застосунку", last["sessions"],
         "гість побачив вашу картку у стрічці або пошуку."),
        ("Відкрили меню", last["menu_views"],
         "натиснули на картку — фото й назва у вас сильні, тут вирішує бейдж акції."),
        ("Додали страву в кошик", last["product_added"],
         "меню та ціни зайшли."),
        ("Оформили замовлення", last["order_placed"],
         "дійшли до оплати."),
    ]
    body = (
        f'<p>Тепер найцікавіше — <b>де саме</b> ми втрачаємо гостя. '
        f'Ось шлях гостя у {last["label"]}:</p>'
        f'{_funnel(f_rows)}'
        f'<p>А тепер порівняйте з місяцем-піком <b>{peak["label"]}</b>:</p>'
        f'<div class="scroll-x"><table class="t">'
        f'<tr><th>Крок воронки</th><th>{peak["label"]}</th><th>{last["label"]}</th>'
        f'<th>Зміна</th></tr>'
        f'<tr><td>Показів у застосунку</td><td>{_fmt(peak["sessions"], "")}</td>'
        f'<td>{_fmt(last["sessions"], "")}</td>'
        f'<td class="bad">{_signed(dg["sessions_chg"])}</td></tr>'
        f'<tr><td>Показ → меню (CTR)</td><td>{peak["imp_menu"]:.1f}%</td>'
        f'<td>{last["imp_menu"]:.1f}%</td>'
        f'<td class="bad">{_signed(_pct_chg(peak["imp_menu"], last["imp_menu"]))}</td></tr>'
        f'<tr><td>Меню → кошик</td><td>{peak["menu_prod"]:.1f}%</td>'
        f'<td>{last["menu_prod"]:.1f}%</td>'
        f'<td class="good">{_signed(_pct_chg(peak["menu_prod"], last["menu_prod"]))}</td></tr>'
        f'</table></div>'
        f'<p style="margin-top:12px">Читаємо просто: показів стало '
        f'<b>{_signed(dg["sessions_chg"])}</b>. Але й ті, кому вас показали, '
        f'тепер <b>рідше натискають</b> — CTR впав з '
        f'<span class="num">{peak["imp_menu"]:.1f}%</span> до '
        f'<span class="num bad">{last["imp_menu"]:.1f}%</span>. '
        f'Це подвійний удар: менше показів × менша частка кліків.</p>'
        f'<p>А от коли гість <b>уже відкрив меню</b> — у вас усе добре: '
        f'конверсія «меню → кошик» навіть краща, ніж на піку '
        f'({peak["menu_prod"]:.1f}% → <b>{last["menu_prod"]:.1f}%</b>).</p>'
    )
    out += _block("3", "Проблема — на самому вході, а не в меню",
                  "Розкладаємо шлях гостя на кроки", body,
                  "Ваше меню, ціни та фото страв працюють — хто зайшов, той купує. "
                  "Втрачаємо гостя на першому екрані: його або не показують, "
                  "або показують без причини натиснути.",
                  "warn")

    # 4. Знижки — головна причина
    disc_peak = dg["disc_peak"]
    corr = dg["disc_corr"]
    corr_txt = (f'Математично звʼязок дуже сильний: коефіцієнт кореляції між вашими '
                f'вкладеннями у знижки та кількістю замовлень — '
                f'<span class="num">{corr:.2f}</span> (1.00 — це ідеальний збіг).'
                if corr is not None and corr > 0.5 else "")
    zero_txt = (f'<p>За останні <b>{dg["zero_streak"]} міс.</b> ви не вкладали у знижки '
                f'нічого — <span class="num bad">0 ₴</span>.</p>'
                if dg["zero_streak"] else "")
    body = (
        f'<p>А тепер — головна причина. Подивіться на два стовпчики поруч: '
        f'скільки ви вкладали у знижки і скільки отримували замовлень.</p>'
        f'{_dual_bars([m["camp_merch"] for m in months], [m["orders"] for m in months], labels, "₴", "шт.", "Ваші знижки, ₴", "Замовлення, шт.", "var(--tomato)", "var(--green)")}'
        f'<p style="margin-top:14px">У <b>{disc_peak["label"]}</b> ви вклали у знижки '
        f'<span class="num">{_fmt(disc_peak["camp_merch"], "₴")}</span> — і отримали '
        f'<span class="num">{_fmt(disc_peak["orders"], "шт.")}</span> замовлень. '
        f'У <b>{last["label"]}</b> ваші вкладення — '
        f'<span class="num bad">{_fmt(last["camp_merch"], "₴")}</span>, '
        f'замовлень — <span class="num bad">{_fmt(last["orders"], "шт.")}</span>.</p>'
        f'{zero_txt}'
        f'<p>Подивіться на частку замовлень з акцією: у <b>{peak["label"]}</b> '
        f'акція була у <span class="num">{peak["camp_share"]:.0f}%</span> замовлень, '
        f'у <b>{last["label"]}</b> — <span class="num bad">{last["camp_share"]:.0f}%</span>. '
        f'{corr_txt}</p>'
        f'<p><b>Чому знижка так сильно впливає саме на показ?</b> У Bolt Food заклад із '
        f'активною акцією отримує жовтий бейдж зі знижкою у стрічці, потрапляє в добірку '
        f'«Акції» та піднімається у сортуванні. Без акції ви — звичайна картка серед '
        f'{bench_lk.get("providers", 0)}+ піцерій міста. Саме тому разом зі знижками '
        f'зникли й покази, і кліки.</p>'
    )
    out += _block("4", "Ви прибрали знижки — і разом з ними зникла видимість",
                  "Пряма залежність між вашими вкладеннями та замовленнями", body,
                  f'Знижка у Bolt Food — це не «мінус маржа», а вхідний квиток у видимість. '
                  f'Ви заощадили на знижках, але заплатили за це '
                  f'{_fmt(peak["orders"] - last["orders"], "замовленнями")} '
                  f'і {_fmt(peak["gross"] - last["gross"], "₴")} обороту на місяць.',
                  "bad")

    # 5. Sponsored Listing
    sl_rows = ""
    for e in dg["sl_eff"]:
        per = f'{e["per_order"]:.0f}' if e["per_order"] else "—"
        cls = "bad" if e["per_order"] and dg["sl_best"] and \
              e["per_order"] > dg["sl_best"]["per_order"] * 2 else ""
        sl_rows += (f'<tr><td>{e["label_s"]}</td><td>{_fmt(e["hours"], "")}</td>'
                    f'<td>{e["orders"]}</td><td class="{cls}">{per}</td></tr>')

    if dg["sl_eff"]:
        best, sl_last = dg["sl_best"], dg["sl_last"]
        cmp_txt = ""
        if best and sl_last and sl_last["per_order"] and best["per_order"]:
            times = sl_last["per_order"] / best["per_order"]
            cmp_txt = (f'<p>Тобто зараз реклама працює '
                       f'<span class="num bad">у {times:.1f} раза дорожче</span>, ніж у '
                       f'найкращий місяць (<b>{best["label_s"]}</b>: '
                       f'{best["per_order"]:.0f} год на замовлення проти '
                       f'{sl_last["per_order"]:.0f} год зараз).</p>')
        body = (
            f'<p>Ви платите за <b>Sponsored Listing</b> — це підняття закладу у стрічці. '
            f'Реклама свою роботу робить: покази ви отримуєте. Питання в тому, '
            f'<b>скільком гостям цього показу достатньо, щоб замовити</b>.</p>'
            f'<div class="scroll-x"><table class="t">'
            f'<tr><th>Місяць</th><th>Годин реклами</th><th>Замовлень від реклами</th>'
            f'<th>Годин на 1 замовлення</th></tr>{sl_rows}</table></div>'
            f'{cmp_txt}'
            f'<p><b>Чому так?</b> Sponsored Listing приводить гостя до вашої картки. '
            f'Але якщо на картці немає бейджа знижки, гість порівнює вас із сусідньою '
            f'піцерією, у якої бейдж є — і йде до неї. Ви оплачуєте показ, '
            f'а конкурент забирає замовлення.</p>'
        )
        take = ('Реклама без акції — це оплата за показ, який не конвертується. '
                'Sponsored Listing і знижка мають працювати разом: реклама приводить '
                'гостя, акція дає йому причину натиснути.')
    else:
        body = ('<p>За аналізований період Sponsored Listing на локаціях бренду '
                'не запускався — це один із найшвидших способів повернути покази.</p>')
        take = 'Sponsored Listing не використовується — є вільний резерв для зростання.'
    out += _block("5", "Sponsored Listing: платите за показ, але без пропозиції",
                  "Ефективність платного просування місяць до місяця", body, take, "warn")

    # 6. Smart Promotion
    if dg["sp_active"]:
        sp_body = ('<p>Розумні акції (Smart Promotion) активні — це добре: '
                   'Bolt Food сам підбирає, кому і яку знижку показати, '
                   'і частину вартості бере на себе.</p>')
        sp_take, sp_cls = ("Механіка працює — варто розширити її на всі локації "
                           "та не вимикати між місяцями."), ""
    else:
        history = ""
        if data["smart_promo"]:
            state_uk = {"active": "закінчилась", "disabled": "відключена",
                        "paused": "на паузі", "pending": "очікує"}
            rows = "".join(
                f'<tr><td>{_loc_name(data, p["provider_id"])}</td>'
                f'<td>{state_uk.get(p["state"], p["state"])}</td>'
                f'<td>{p["start"]}</td><td>{p["end"]}</td></tr>'
                for p in data["smart_promo"])
            history = (f'<div class="scroll-x"><table class="t">'
                       f'<tr><th>Локація</th><th>Стан</th><th>Початок</th><th>Кінець</th></tr>'
                       f'{rows}</table></div>')
        sp_body = (
            f'<p><b>Розумні акції (Smart Promotion) у вас вимкнені.</b> За весь період вони '
            f'принесли лише <span class="num bad">{_fmt(dg["sp_total"], "замовлень")}</span> — '
            f'бо працювали кілька днів і були відключені.</p>'
            f'{history}'
            f'<p>Це найдешевший для партнера інструмент: Bolt Food аналізує, який гість '
            f'вагається, і показує йому персональну знижку. Ви платите тільки за '
            f'результат — за фактичне замовлення, а частину знижки покриває Bolt.</p>'
        )
        sp_take, sp_cls = ("Найдешевший інструмент зростання просто вимкнений. "
                           "Увімкнути Розумні акції на всіх трьох локаціях — "
                           "крок №1 на вересень."), "bad"
    out += _block("6", "Smart Promotion — вимкнено" if not dg["sp_active"]
                  else "Smart Promotion — працює",
                  "Розумні акції: Bolt платить частину знижки і сам шукає гостя",
                  sp_body, sp_take, sp_cls)

    # 7. Операційка — щоб партнер не шукав проблему там
    body = (
        f'<p>Щоб не витрачати час на хибні гіпотези — <b>кухня та сервіс не є причиною</b> '
        f'падіння. Ось показники за {last["label"]}:</p>'
        f'<div class="scroll-x"><table class="t">'
        f'<tr><th>Показник</th><th>Значення</th><th>Орієнтир</th><th>Оцінка</th></tr>'
        f'<tr><td>Час онлайн</td><td>{last["avail"]:.1f}%</td><td>&ge; 95%</td>'
        f'<td class="{"good" if last["avail"] >= 95 else "bad"}">'
        f'{"добре" if last["avail"] >= 95 else "треба підтягнути"}</td></tr>'
        f'<tr><td>Прийняття замовлень</td><td>{last["accept"]:.1f}%</td><td>&ge; 97%</td>'
        f'<td class="{"good" if last["accept"] >= 97 else "bad"}">'
        f'{"добре" if last["accept"] >= 97 else "треба підтягнути"}</td></tr>'
        f'<tr><td>Компенсації гостям</td><td>{last["refunds"]:.1f}%</td><td>&le; 3%</td>'
        f'<td class="{"good" if last["refunds"] <= 3 else "bad"}">'
        f'{"добре" if last["refunds"] <= 3 else "треба підтягнути"}</td></tr>'
        f'<tr><td>Рейтинг</td><td>{_fmt(last["rating"], "з 5")}</td><td>&ge; 4.5</td>'
        f'<td class="{"good" if last["rating"] >= 4.5 else ""}">'
        f'{"добре" if last["rating"] >= 4.5 else "нормально"}</td></tr>'
        f'</table></div>'
    )
    out += _block("7", "Операційні показники: тут проблеми немає",
                  "Виключаємо хибні причини", body,
                  "Заклад онлайн, замовлення приймаються, гості задоволені. "
                  "Вкладати сили в операційку зараз не потрібно — "
                  "усе вирішується маркетингом і видимістю.",
                  "")
    return out


# ─── GLOVO SECTION ─────────────────────────────────────────────────────────────
# Дані про конкурента лежать у glovo_findings.json поруч зі скриптом: вони
# зібрані вручну з відкритих джерел і не мають перезаписуватись автооновленням.

GLOVO_JSON = SCRIPT_DIR / "glovo_findings.json"


def load_glovo() -> dict:
    if not GLOVO_JSON.exists():
        return {}
    try:
        return json.loads(GLOVO_JSON.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"  ⚠️  glovo_findings.json не прочитано: {exc}")
        return {}


def build_glovo_html(g: dict) -> str:
    if not g:
        return ('<div class="note">Дані по Glovo не додані. Заповніть '
                '<code>glovo_findings.json</code> — і блок зʼявиться у звіті.</div>')

    out = ""
    if g.get("intro"):
        out += f'<div class="note">{g["intro"]}</div>'

    cards = ""
    for card in g.get("cards", []):
        items = "".join(f"<li>{i}</li>" for i in card.get("items", []))
        srcs = ""
        if card.get("sources"):
            links = ", ".join(f'<a href="{u}" target="_blank" rel="noopener">джерело</a>'
                              for u in card["sources"])
            srcs = f'<div class="src">Перевірено: {links}</div>'
        cls = "card wide" if card.get("wide") else "card"
        cards += (f'<div class="{cls}"><h4>{card.get("title", "")}</h4>'
                  f'<div class="desc">{card.get("desc", "")}</div>'
                  f'<ul>{items}</ul>{srcs}</div>')
    out += f'<div class="glv">{cards}</div>'

    if g.get("conclusion"):
        out += (f'<div class="take warn" style="margin-top:16px">'
                f'<b>Висновок по Glovo</b>{g["conclusion"]}</div>')
    if g.get("limits"):
        out += (f'<div class="src" style="margin-top:12px">'
                f'<b>Обмеження даних:</b> {g["limits"]}</div>')
    return out


# ─── LOCATIONS ─────────────────────────────────────────────────────────────────

def _sev_cls(sev: int) -> str:
    return "sev-high" if sev >= 4 else "sev-mid" if sev >= 2 else "sev-ok"


def _sev_label(sev: int) -> str:
    return "критично" if sev >= 5 else "увага" if sev >= 3 else "помірно" if sev >= 1 else "ок"


def _charts_for(series: list[dict], labels: list[str], palette: list[str]) -> str:
    html = ""
    for title, keys in CHART_SECTIONS:
        cards = ""
        for key in keys:
            name, desc, unit = METRIC_UK[key]
            cards += (f'<div class="card"><h4>{name}</h4><div class="desc">{desc}</div>'
                      f'<div class="unit">{unit}</div>'
                      f'{_bars([m.get(key, 0) for m in series], labels, unit, palette)}</div>')
        html += f'<h3 class="sub">{title}</h3><div class="charts">{cards}</div>'
    return html


def build_locations_html(data: dict, palette: list[str]) -> str:
    labels = data["month_labels_s"]
    out = ""
    for loc in data["locations"]:
        an = analyze_location(loc)
        last, prev = an["last"], an["prev"]
        lid = f'loc{loc["provider_id"]}'
        notes = "".join(f"<li>{n}</li>" for n in an["notes"])
        arrow = {"up": "↑", "down": "↓", "stable": "→"}[an["trend"]]
        out += f"""
        <div class="loc">
          <div class="loc-r">
            <div class="loc-i">
              <h3>{arrow} {loc['short_name']}</h3>
              <div class="loc-meta">{loc['zone']} &nbsp;·&nbsp; ID {loc['provider_id']}
                &nbsp;·&nbsp; {_fmt(last['orders'], 'зам.')} &nbsp;·&nbsp;
                {_fmt(last['gross'], '₴')}
                {_delta(prev.get('orders', 0), last.get('orders', 0))}</div>
            </div>
            <button class="loc-btn" aria-expanded="false"
                    onclick="toggleLoc('{lid}', this)">Детальніше ▾</button>
          </div>
          <div class="loc-b" id="{lid}" hidden>
            {_charts_for(loc['months'], labels, palette)}
            <div class="loc-an {_sev_cls(an['severity'])}">
              <div style="display:flex;justify-content:space-between;align-items:center;gap:10px">
                <h4>Що відбувається на цій локації</h4>
                <span class="badge">{_sev_label(an['severity'])}</span>
              </div>
              <ul>{notes}</ul>
            </div>
          </div>
        </div>"""
    return out


# ─── PAGE ──────────────────────────────────────────────────────────────────────

PALETTE = ["#0b3b26", "#0d5c3c", "#0d8a52", "#12a862", "#1cba72",
           "#34D186", "#5ddba0", "#84e5b8", "#a8eecd", "#c8f5e0",
           "#0d8a52", "#12a862", "#34D186", "#5ddba0"]


def build_summary_html(dg: dict) -> str:
    last, peak = dg["last"], dg["peak"]
    lost_money = peak["gross"] - last["gross"]
    return (
        f'<div class="take bad" style="margin-bottom:16px;font-size:14px">'
        f'<b>Головне за {last["label"]}</b>'
        f'Замовлень — {_fmt(last["orders"], "шт.")} '
        f'({_signed(dg["orders_vs_peak"])} від вашого піку у {peak["label"]}). '
        f'Ринок доставки Харкова за той самий час '
        f'{"зріс" if (dg["city_chg"] or 0) >= 0 else "змінився"} на '
        f'{_signed(dg["city_chg"])}. '
        f'Причина падіння — не кухня і не меню, а видимість: '
        f'ви зупинили власні знижки, покази впали на {_signed(dg["sessions_chg"])}, '
        f'а CTR — з {peak["imp_menu"]:.1f}% до {last["imp_menu"]:.1f}%. '
        f'Недоотриманий оборот — близько {_fmt(lost_money, "₴")} на місяць.'
        f'</div>'
    )


def build_kpis_html(dg: dict) -> str:
    last, prev = dg["last"], dg["prev"]
    return (
        _kpi("Доставлені замовлення", _fmt(last["orders"], "шт."),
             _delta(prev["orders"], last["orders"]), "var(--green)") +
        _kpi("Gross Sales", _fmt(last["gross"], "₴"),
             _delta(prev["gross"], last["gross"]), "var(--green)") +
        _kpi("Net Sales", _fmt(last["net"], "₴"),
             _delta(prev["net"], last["net"]), "var(--green)") +
        _kpi("Середній чек", _fmt(last["aov"], "₴"),
             _delta(prev["aov"], last["aov"])) +
        _kpi("Активні гості", _fmt(last["active_users"], ""),
             _delta(prev["active_users"], last["active_users"])) +
        _kpi("Нові гості", _fmt(last["new_users"], ""),
             _delta(prev["new_users"], last["new_users"])) +
        _kpi("Покази у застосунку", _fmt(last["sessions"], ""),
             _delta(prev["sessions"], last["sessions"])) +
        _kpi("Показ → меню (CTR)", f'{last["imp_menu"]:.1f}%',
             _delta(prev["imp_menu"], last["imp_menu"]), "var(--tomato)") +
        _kpi("Ваші знижки", _fmt(last["camp_merch"], "₴"),
             _delta(prev["camp_merch"], last["camp_merch"]), "var(--tomato)") +
        _kpi("Знижки від Bolt", _fmt(last["camp_bolt"], "₴"),
             _delta(prev["camp_bolt"], last["camp_bolt"])) +
        _kpi("Замовлень з акцією", f'{last["camp_share"]:.0f}%',
             _delta(prev["camp_share"], last["camp_share"]), "var(--tomato)") +
        _kpi("Sponsored Listing", _fmt(last["sl_hours"], "год"),
             _delta(prev["sl_hours"], last["sl_hours"])) +
        _kpi("Час онлайн", f'{last["avail"]:.1f}%',
             _delta(prev["avail"], last["avail"])) +
        _kpi("Рейтинг", _fmt(last["rating"], "з 5"),
             _delta(prev["rating"], last["rating"]), "var(--warn)")
    )


def build_html(data: dict) -> str:
    dg = build_diagnosis(data)
    now = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
    nxt = next_update_date().strftime("%d.%m.%Y")
    last = dg["last"]
    n_loc = len(data["locations"])
    glovo = build_glovo_html(load_glovo())

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>MBR · {BRAND_TITLE} · {CITY_UK} · Bolt Food</title>
<meta name="report-period" content="{data['period_label']}"/>
<meta name="report-updated" content="{now}"/>
<style>{CSS}</style>
</head>
<body>

<header class="hdr">
  {PIZZA_TOP}
  <div class="hdr-in">
    <div class="hdr-l">
      <div class="mark">{BOLT_MARK}</div>
      <div>
        <h1>MBR · {BRAND_TITLE}</h1>
        <div class="sub">Bolt Food &nbsp;·&nbsp; Місячний звіт &nbsp;·&nbsp; {CITY_UK}</div>
      </div>
    </div>
    <div class="hdr-m">
      <div>Період: <strong>{data['period_label']}</strong></div>
      <div>Локацій: <strong>{n_loc}</strong> &nbsp;·&nbsp;
           Місяців: <strong>{len(data['month_keys'])}</strong></div>
      <div>Оновлено: <strong>{now}</strong></div>
    </div>
  </div>
</header>

<nav class="nav"><div class="nav-in">
  <a href="#ogljad">Огляд</a>
  <a href="#chomu">Чому падають замовлення</a>
  <a href="#grafiky">Показники по місяцях</a>
  <a href="#glovo">Порівняння з Glovo</a>
  <a href="#lokacii">Локації</a>
</div></nav>

<div class="wrap">

  <div class="period">
    <b>Дані помісячно</b>
    <span>{data['period_label']} &nbsp;·&nbsp; лише повні календарні місяці
      &nbsp;·&nbsp; валюта UAH (₴) &nbsp;·&nbsp; {CITY_UK}, {n_loc} локації</span>
    <span style="margin-left:auto;font-size:11.5px;color:var(--g400)">
      Наступне оновлення: {nxt}</span>
  </div>

  <h2 class="sec" id="ogljad">{PIZZA_SLICE} Огляд — {last['label']}</h2>
  {build_summary_html(dg)}
  <div class="kpis">{build_kpis_html(dg)}</div>

  <h2 class="sec" id="chomu">{PIZZA_SLICE} Чому падають ваші замовлення</h2>
  <p style="margin-bottom:16px;font-size:14px;color:var(--g700)">
    Розбір простими словами: сім кроків від «скільки втрачено» до «через що саме».
    Усі цифри — з даних Bolt Food за повні місяці.</p>
  {build_diagnosis_html(data, dg)}

  <h2 class="sec" id="grafiky">{PIZZA_SLICE} Показники по місяцях — бренд загалом</h2>
  {_charts_for(data['brand_months'], data['month_labels_s'], PALETTE)}

  <h2 class="sec" id="glovo">{PIZZA_SLICE} Marcello у Glovo — що бачить гість у конкурента</h2>
  {glovo}

  <h2 class="sec" id="lokacii">{PIZZA_SLICE} Локації</h2>
  {build_locations_html(data, PALETTE)}

</div>

<div class="foot">
  Звіт оновлюється автоматично <span>4-го числа кожного місяця</span> —
  у нього додається щойно завершений місяць. Посилання завжди веде на найсвіжішу версію.<br/>
  <span>Bolt Food</span> &nbsp;·&nbsp; {BRAND_TITLE} &nbsp;·&nbsp; {CITY_UK}
  &nbsp;·&nbsp; питання щодо показників — до вашого менеджера
</div>

<script>
function toggleLoc(id, btn) {{
  const body = document.getElementById(id);
  const open = btn.getAttribute('aria-expanded') === 'true';
  body.hidden = open;
  btn.setAttribute('aria-expanded', String(!open));
  btn.textContent = open ? 'Детальніше ▾' : 'Згорнути ▴';
}}
</script>
</body>
</html>"""


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"=== MBR {BRAND_TITLE} ({CITY_UK}) — {datetime.date.today().isoformat()} ===")
    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN не заданий")
        sys.exit(1)

    try:
        data = fetch_data_checked()
    except Exception as exc:
        print(f"\n❌ Дані з Databricks не отримано: {exc}")
        print(f"Звіт НЕ перезаписано, попередня версія збережена:\n   {OUTPUT_HTML}")
        sys.exit(1)

    print(f"  → місяців: {len(data['month_keys'])}, локацій: {len(data['locations'])}")
    OUTPUT_HTML.write_text(build_html(data), encoding="utf-8")
    print(f"\n✅ Готово → {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
