#!/usr/bin/env python3
"""Звіт по платному просуванню (sponsored listings) для Josper Svintus, Mavra Pizza, Mavra Azia.

Джерело: Databricks, mart_provider_sponsored_listing_attribution_hourly /
_performance_hourly (рекламні метрики) + ng_delivery.fact_provider_daily
(операційні метрики). Періоди беруться з самих даних — це фактичні дати кампаній.

Запуск:  python3 generate_ads_report.py
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

HOST = os.getenv("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
CLUSTER = os.getenv("DATABRICKS_CLUSTER_ID", "0505-112942-d3yviznw")
PROFILE = os.getenv("DATABRICKS_PROFILE", "bolt-incentives-temp")

SCRIPT_DIR = Path(__file__).parent
OUT_NAME = "platne-prosuvannia.html"
# Стара назва файлу — щоб посилання, яке вже передали партнеру, далі працювало.
LEGACY_NAME = "josper-svintus-2026-07.html"

STRIP_JOSPER = [r"Josper\s+Svintuz", r"JOSPER\s+SVINTUZ", r"Josper\s+Svintus"]
STRIP_PIZZA = [r"Mavra\s+[Pp]izza", r"MAVRA\s+PIZZA"]
STRIP_AZIA = [r"Mavra\s+Azia", r"MAVRA\s+AZIA"]

# Вкладки звіту. Кожна вкладка — бренд; усередині — групи по містах.
TABS = [
    {
        "key": "josper", "label": "Josper Svintus", "brand": "JOSPER SVINTUS",
        "groups": [
            {"key": "josper-zap", "city": "Запоріжжя", "strip": STRIP_JOSPER,
             "pids": [195007, 195012, 195019, 195024, 195025, 197892, 197909, 922610],
             "no_ads": []},
            {"key": "josper-vin", "city": "Вінниця", "strip": STRIP_JOSPER,
             "pids": [194993], "no_ads": []},
        ],
    },
    {
        "key": "pizza", "label": "Mavra Pizza", "brand": "MAVRA PIZZA",
        "groups": [
            {"key": "pizza-zap", "city": "Запоріжжя", "strip": STRIP_PIZZA,
             "pids": [194965, 194972, 194976, 194977, 194981, 194982, 194984, 194985],
             "no_ads": [138974]},
            {"key": "pizza-kr", "city": "Кривий Ріг", "strip": STRIP_PIZZA,
             "pids": [139521, 139522], "no_ads": [139520]},
        ],
    },
    {
        "key": "azia", "label": "Mavra Azia", "brand": "MAVRA AZIA",
        "groups": [
            {"key": "azia-zap", "city": "Запоріжжя", "strip": STRIP_AZIA,
             "pids": [195164, 195169, 195171, 195174], "no_ads": []},
        ],
    },
]


# ─── DATABRICKS ────────────────────────────────────────────────────────────────

def _token() -> str:
    env = os.getenv("DATABRICKS_TOKEN")
    if env:
        return env
    out = subprocess.check_output(["databricks", "auth", "token", "-p", PROFILE], text=True)
    return json.loads(out)["access_token"]


_H = {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}


def _post(path, payload):
    r = requests.post(HOST + path, headers=_H, json=payload, timeout=90)
    r.raise_for_status()
    return r.json()


def _get(path, params):
    r = requests.get(HOST + path, headers=_H, params=params, timeout=90)
    r.raise_for_status()
    return r.json()


_ctx = {"id": None}


def _wait_cluster(timeout=900):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = _get("/api/2.0/clusters/get", {"cluster_id": CLUSTER}).get("state")
        if st in ("RUNNING", "RESIZING"):
            return
        print(f"  кластер {st}, чекаю…", flush=True)
        time.sleep(15)
    raise TimeoutError("cluster not running")


def sql(query: str, max_s: int = 900):
    if _ctx["id"] is None:
        _wait_cluster()
        _ctx["id"] = _post("/api/1.2/contexts/create",
                           {"language": "sql", "clusterId": CLUSTER})["id"]
    cid = _post("/api/1.2/commands/execute",
                {"language": "sql", "clusterId": CLUSTER,
                 "contextId": _ctx["id"], "command": query})["id"]
    deadline = time.time() + max_s
    while time.time() < deadline:
        time.sleep(4)
        resp = _get("/api/1.2/commands/status",
                    {"clusterId": CLUSTER, "contextId": _ctx["id"], "commandId": cid})
        st = resp.get("status")
        if st == "Finished":
            res = resp.get("results", {})
            if res.get("resultType") == "error":
                raise RuntimeError(res.get("summary", "error")[:1500])
            return res.get("data", []), [c.get("name") for c in (res.get("schema") or [])]
        if st in ("Cancelled", "Error"):
            raise RuntimeError(f"{st}: {json.dumps(resp)[:600]}")
    raise TimeoutError("query timeout")


def fl(v) -> float:
    try:
        return float(v) if v not in (None, "", "null") else 0.0
    except (TypeError, ValueError):
        return 0.0


# ─── ВИТЯГ ДАНИХ ───────────────────────────────────────────────────────────────

def fetch_ads(pids):
    """Рекламні метрики за весь час існування кампаній цих локацій."""
    p = ",".join(map(str, pids))
    data, cols = sql(f"""
    WITH at AS (
      SELECT provider_id,
             SUM(hourly_price_local)         AS spend_local,
             SUM(hourly_price_eur)           AS spend_eur,
             SUM(provider_net_revenue_local) AS rev_local,
             SUM(provider_net_revenue_eur)   AS rev_eur,
             SUM(attributed_orders)          AS orders,
             SUM(attributed_users)           AS users,
             SUM(attributed_new_users)       AS new_users,
             COUNT(DISTINCT sponsored_listing_date_local) AS days,
             MIN(sponsored_listing_date_local) AS d_start,
             MAX(sponsored_listing_date_local) AS d_end
      FROM main.mart_models.mart_provider_sponsored_listing_attribution_hourly
      WHERE provider_id IN ({p})
      GROUP BY 1
    ), pf AS (
      SELECT provider_id, SUM(impressions) AS impr, SUM(clicks) AS clicks
      FROM main.mart_models.mart_provider_sponsored_listing_performance_hourly
      WHERE provider_id IN ({p})
      GROUP BY 1
    )
    SELECT at.*, pf.impr, pf.clicks
    FROM at LEFT JOIN pf ON at.provider_id = pf.provider_id
    """)
    i = {c: k for k, c in enumerate(cols)}
    out = {}
    for r in data:
        out[int(fl(r[i["provider_id"]]))] = {
            "spend_local": fl(r[i["spend_local"]]), "spend_eur": fl(r[i["spend_eur"]]),
            "rev_local": fl(r[i["rev_local"]]), "rev_eur": fl(r[i["rev_eur"]]),
            "orders": int(fl(r[i["orders"]])), "users": int(fl(r[i["users"]])),
            "new_users": int(fl(r[i["new_users"]])), "days": int(fl(r[i["days"]])),
            "d_start": str(r[i["d_start"]])[:10], "d_end": str(r[i["d_end"]])[:10],
            "impr": int(fl(r[i["impr"]])), "clicks": int(fl(r[i["clicks"]])),
        }
    return out


def fetch_ops(pids, a, b):
    """Операційні метрики за період кампанії."""
    p = ",".join(map(str, pids))
    data, cols = sql(f"""
    SELECT f.provider_id,
      MAX(d.provider_name)                       AS nm,
      MAX(d.city_name)                           AS city,
      SUM(f.sessions_available)                  AS s_avail,
      SUM(f.sessions_viewed)                     AS s_view,
      SUM(f.sessions_added)                      AS s_add,
      SUM(f.sessions_ordered)                    AS s_ord,
      SUM(f.delivered_orders)                    AS dlv,
      SUM(f.gmv_orders_eur)                      AS gmv_eur,
      SUM(f.provider_active_time_minutes)        AS act_min,
      SUM(f.provider_working_time_minutes)       AS work_min,
      SUM(f.provider_total_cooking_time_minutes) AS cook_min,
      SUM(f.delivered_orders_delivery_price_before_discount_eur) AS delfee_eur,
      SUM(f.provider_rating_per_order_value * f.provider_rating_per_order_weight) AS rt_v,
      SUM(f.provider_rating_per_order_weight)    AS rt_w
    FROM main.ng_delivery.fact_provider_daily f
    LEFT JOIN main.ng_delivery.dim_provider_v2 d ON f.provider_id = d.provider_id
    WHERE f.provider_id IN ({p})
      AND f.observation_date >= '{a}' AND f.observation_date <= '{b}'
    GROUP BY 1
    """)
    i = {c: k for k, c in enumerate(cols)}

    def pc(x, y):
        return (x / y * 100) if y else None

    out = {}
    for r in data:
        sa, sv = fl(r[i["s_avail"]]), fl(r[i["s_view"]])
        sad, so = fl(r[i["s_add"]]), fl(r[i["s_ord"]])
        dlv = fl(r[i["dlv"]])
        rt_w = fl(r[i["rt_w"]])
        out[int(fl(r[i["provider_id"]]))] = {
            "name": r[i["nm"]] or "?", "city": r[i["city"]] or "",
            "sessions": int(sa), "view_pct": pc(sv, sa), "add_pct": pc(sad, sv),
            "ord_pct": pc(so, sad), "uptime": pc(fl(r[i["act_min"]]), fl(r[i["work_min"]])),
            "delivered": int(dlv),
            "aov_eur": (fl(r[i["gmv_eur"]]) / dlv) if dlv else None,
            "delfee_eur": (fl(r[i["delfee_eur"]]) / dlv) if dlv else None,
            "cook_min": (fl(r[i["cook_min"]]) / dlv) if dlv else None,
            "rating": (fl(r[i["rt_v"]]) / rt_w) if rt_w else None,
        }
    return out


def build_dataset():
    fx_num = fx_den = 0.0
    for tab in TABS:
        for g in tab["groups"]:
            print(f"▶ {tab['brand']} · {g['city']}", flush=True)
            ads = fetch_ads(g["pids"])
            if not ads:
                raise RuntimeError(f"немає рекламних даних: {g['key']}")
            g["start"] = min(a["d_start"] for a in ads.values())
            g["end"] = max(a["d_end"] for a in ads.values())
            ops = fetch_ops(g["pids"] + g["no_ads"], g["start"], g["end"])

            def short(nm):
                s = str(nm)
                for pat in g["strip"]:
                    s = re.sub(pat, "", s)
                return re.sub(r"\s+", " ", s).strip(" ,–-") or str(nm)

            locs = []
            for pid in g["pids"]:
                if pid not in ads:
                    continue
                o = ops.get(pid, {})
                a = ads[pid]
                fx_num += a["spend_local"]; fx_den += a["spend_eur"]
                locs.append({"pid": pid, "name": short(o.get("name", pid)), "ads": a, "ops": o})
            locs.sort(key=lambda x: -(x["ads"]["rev_local"] / x["ads"]["spend_local"]
                                      if x["ads"]["spend_local"] else 0))
            g["locations"] = locs
            g["no_ads_locs"] = [{"pid": pid, "name": short(ops.get(pid, {}).get("name", pid)),
                                 "ops": ops.get(pid, {})} for pid in g["no_ads"]]
            print(f"   {len(locs)} з рекламою, {len(g['no_ads_locs'])} без · "
                  f"{g['start']} → {g['end']}", flush=True)
    return fx_num / fx_den if fx_den else 51.88


# ─── ФОРМАТУВАННЯ ──────────────────────────────────────────────────────────────

def nm(v, dec=2):
    """Число в українському форматі: 9 852,35"""
    if v is None:
        return "—"
    s = f"{abs(v):,.{dec}f}".replace(",", "\u00a0").replace(".", ",")
    return ("−" if v < 0 else "") + s


def uah(v, dec=2):
    return f"₴{nm(v, dec)}" if v is None or v >= 0 else f"−₴{nm(abs(v), dec)}"


def eur(v, dec=2):
    return f"€{nm(v, dec)}" if v is not None else "—"


def pct(v, dec=1):
    return f"{nm(v, dec)}%" if v is not None else "—"


def roas_s(v):
    return f"{nm(v, 2)}×" if v is not None else "—"


UK_MONTHS = ["", "січня", "лютого", "березня", "квітня", "травня", "червня",
             "липня", "серпня", "вересня", "жовтня", "листопада", "грудня"]


def date_uk(iso, with_year=True):
    y, m, d = iso.split("-")
    s = f"{int(d)} {UK_MONTHS[int(m)]}"
    return f"{s} {y}" if with_year else s


def period_uk(a, b):
    ya, ma, _ = a.split("-")
    yb, mb, _ = b.split("-")
    if ya == yb and ma == mb:
        return f"{int(a.split('-')[2])}–{date_uk(b)}"
    return f"{date_uk(a, ya != yb)} – {date_uk(b)}"


def med(vals):
    v = sorted(x for x in vals if x is not None)
    if not v:
        return None
    n = len(v)
    return v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2


def esc(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


# ─── ПОХІДНІ МЕТРИКИ ───────────────────────────────────────────────────────────

def derive(loc):
    a = loc["ads"]
    sp, rv = a["spend_local"], a["rev_local"]
    d = {
        "spend": sp, "rev": rv, "profit": rv - sp,
        "roas": (rv / sp) if sp else 0.0,
        "orders": a["orders"], "new_users": a["new_users"],
        "impr": a["impr"], "clicks": a["clicks"], "days": a["days"],
        "aov": (rv / a["orders"]) if a["orders"] else None,
        "ctr": (a["clicks"] / a["impr"] * 100) if a["impr"] else None,
        "cvr": (a["orders"] / a["clicks"] * 100) if a["clicks"] else None,
        "cpm": (a["spend_eur"] / a["impr"] * 1000) if a["impr"] else None,
        "cpc": (a["spend_eur"] / a["clicks"]) if a["clicks"] else None,
        "cpo": (a["spend_eur"] / a["orders"]) if a["orders"] else None,
        "new_share": (a["new_users"] / a["orders"] * 100) if a["orders"] else None,
    }
    loc["d"] = d
    return d


def group_totals(g):
    T = dict(spend=0.0, rev=0.0, orders=0, new_users=0, impr=0, clicks=0)
    for l in g["locations"]:
        d = l["d"]
        T["spend"] += d["spend"]; T["rev"] += d["rev"]
        T["orders"] += d["orders"]; T["new_users"] += d["new_users"]
        T["impr"] += d["impr"]; T["clicks"] += d["clicks"]
    T["days"] = max([l["ads"]["days"] for l in g["locations"]] or [0])
    T["profit"] = T["rev"] - T["spend"]
    T["roas"] = (T["rev"] / T["spend"]) if T["spend"] else 0.0
    T["aov"] = (T["rev"] / T["orders"]) if T["orders"] else None
    T["ctr"] = (T["clicks"] / T["impr"] * 100) if T["impr"] else None
    T["cvr"] = (T["orders"] / T["clicks"] * 100) if T["clicks"] else None
    T["locations"] = len(g["locations"])
    g["T"] = T
    return T


def group_medians(g):
    L = g["locations"]
    M = {
        "view_pct": med([l["ops"].get("view_pct") for l in L]),
        "add_pct": med([l["ops"].get("add_pct") for l in L]),
        "ord_pct": med([l["ops"].get("ord_pct") for l in L]),
        "uptime": med([l["ops"].get("uptime") for l in L]),
        "aov_eur": med([l["ops"].get("aov_eur") for l in L]),
        "cook_min": med([l["ops"].get("cook_min") for l in L]),
        "delfee_eur": med([l["ops"].get("delfee_eur") for l in L]),
        "ctr": med([l["d"]["ctr"] for l in L]),
        "cvr": med([l["d"]["cvr"] for l in L]),
        "roas": med([l["d"]["roas"] for l in L]),
    }
    g["M"] = M
    return M


def badge(roas):
    if roas >= 6:
        return "b-star", "Відмінно"
    if roas >= 3:
        return "b-good", "Добре"
    if roas >= 1:
        return "b-mid", "Слабко"
    return "b-bad", "Збиток"


def severity(roas):
    if roas >= 6:
        return "sev-ok"
    if roas >= 3:
        return "sev-good"
    if roas >= 1:
        return "sev-mid"
    return "sev-high"


UPTIME_FLAG = 85.0


# ─── ТЕКСТ ПО ЛОКАЦІЇ (лише факти, без рекомендацій) ───────────────────────────

def loc_bullets(l, g):
    d, o, M, T = l["d"], l["ops"], g["M"], g["T"]
    b = []

    # 1. Економіка
    if d["orders"] == 0:
        b.append(f"За кампанію оголошення отримало <b>{nm(d['impr'],0)}</b> показів і "
                 f"<b>{nm(d['clicks'],0)}</b> кліків, але <b>жодного замовлення</b>. "
                 f"Витрати {uah(d['spend'])} не повернулися — це єдина локація групи "
                 f"без атрибутованої виручки.")
    else:
        rel = d["roas"] / T["roas"] if T["roas"] else 1
        cmp_txt = ("вище за середній по місту" if rel >= 1.15 else
                   "нижче за середній по місту" if rel <= 0.85 else
                   "на рівні середнього по місту")
        b.append(f"Витрати {uah(d['spend'])} принесли {uah(d['rev'])} атрибутованої виручки — "
                 f"прибуток <b>{uah(d['profit'])}</b> при ROAS <b>{roas_s(d['roas'])}</b>, "
                 f"це {cmp_txt} ({roas_s(T['roas'])}). "
                 f"Замовлень {d['orders']}, середній чек {uah(d['aov'])}.")

    # 2. Воронка реклами
    parts = [f"покази <b>{nm(d['impr'],0)}</b>", f"кліки <b>{nm(d['clicks'],0)}</b>"]
    if d["ctr"] is not None:
        mark = ""
        if M["ctr"] is not None:
            if d["ctr"] >= M["ctr"] * 1.2:
                mark = " — вище за медіану групи"
            elif d["ctr"] <= M["ctr"] * 0.8:
                mark = " — нижче за медіану групи"
        parts.append(f"CTR <b>{pct(d['ctr'])}</b> (медіана {pct(M['ctr'])}){mark}")
    if d["cvr"] is not None and d["clicks"]:
        parts.append(f"конверсія з кліку в замовлення <b>{pct(d['cvr'],0)}</b>")
    b.append("Рекламна воронка: " + ", ".join(parts) + ".")

    # 3. Ціна результату
    if d["cpo"] is not None:
        b.append(f"Ціна результату: {eur(d['cpm'])} за 1000 показів, {eur(d['cpc'])} за клік, "
                 f"<b>{eur(d['cpo'])} за замовлення</b>. Нових клієнтів {d['new_users']} "
                 f"({pct(d['new_share'],0)} усіх атрибутованих замовлень).")

    # 4. Операційний контекст за той самий період
    ops_bits = []
    if o.get("uptime") is not None:
        flag = " — заклад був доступний менше ніж потрібно, тому частина показів припала на час, коли замовити було неможливо" if o["uptime"] < UPTIME_FLAG else ""
        ops_bits.append(f"аптайм <b>{pct(o['uptime'])}</b> (медіана {pct(M['uptime'])}){flag}")
    if o.get("aov_eur") is not None:
        rel = o["aov_eur"] / M["aov_eur"] if M["aov_eur"] else 1
        tail = (" — найдорожче меню в групі" if rel >= 1.15 else
                " — дешевше за медіану" if rel <= 0.85 else "")
        ops_bits.append(f"середній чек {eur(o['aov_eur'])} (медіана {eur(M['aov_eur'])}){tail}")
    if o.get("cook_min") is not None:
        tail = " — довше за медіану" if M["cook_min"] and o["cook_min"] >= M["cook_min"] * 1.25 else ""
        ops_bits.append(f"готує {nm(o['cook_min'],1)} хв (медіана {nm(M['cook_min'],1)} хв){tail}")
    if o.get("rating") is not None:
        ops_bits.append(f"рейтинг {nm(o['rating'],2)}")
    if ops_bits:
        se, dl = o.get("sessions", 0), o.get("delivered", 0)
        b.append(f"Операційні показники за той самий період "
                 f"({nm(se,0)} {plural(se,'сесія','сесії','сесій')}, "
                 f"{nm(dl,0)} {plural(dl,'доставлене замовлення','доставлені замовлення','доставлених замовлень')}): "
                 + ", ".join(ops_bits) + ".")

    # 5. Воронка закладу
    if o.get("view_pct") is not None:
        b.append(f"Воронка закладу: відкрили картку <b>{pct(o['view_pct'])}</b> "
                 f"(медіана {pct(M['view_pct'])}), додали в кошик <b>{pct(o['add_pct'])}</b> "
                 f"(медіана {pct(M['add_pct'])}), оформили замовлення <b>{pct(o['ord_pct'])}</b> "
                 f"(медіана {pct(M['ord_pct'])}).")
    return b


# ─── РЕНДЕР ────────────────────────────────────────────────────────────────────

CSS = """
  :root{
    --green:#34D186; --green-d:#0d8a52; --black:#0d0d0d;
    --gray-700:#4a4a4a; --gray-400:#9a9a9a; --gray-100:#f5f5f5;
    --positive:#1aad6a; --warning:#e67e22; --danger:#c0392b; --info:#2980b9;
  }
  *{margin:0;padding:0;box-sizing:border-box}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
    font-size:14px;line-height:1.55;color:#1a1a1a;background:var(--gray-100)}

  .header{background:var(--black);padding:20px 40px;display:flex;align-items:flex-start;
    justify-content:space-between;border-bottom:4px solid var(--green);flex-wrap:wrap;gap:16px}
  .header-left{display:flex;align-items:center;gap:14px;flex:1;min-width:240px}
  .bolt-logo{width:44px;height:44px;background:var(--green);border-radius:10px;
    display:flex;align-items:center;justify-content:center;font-size:22px}
  .header-title h1{font-size:20px;font-weight:700;color:#fff}
  .header-title p{font-size:11px;color:var(--green);text-transform:uppercase;letter-spacing:1.2px;font-weight:600;margin-top:4px}
  .header-meta{text-align:right;color:var(--gray-400);font-size:12px;line-height:1.9}
  .header-meta strong{color:var(--green)}

  .tabbar{background:var(--black);padding:0 40px;display:flex;gap:4px;flex-wrap:wrap;
    border-bottom:1px solid #262626;position:sticky;top:0;z-index:20}
  .tabbtn{appearance:none;border:0;background:transparent;color:var(--gray-400);cursor:pointer;
    font:inherit;font-size:13px;font-weight:700;padding:13px 18px;border-bottom:3px solid transparent;
    transition:color .15s,border-color .15s;white-space:nowrap}
  .tabbtn:hover{color:#fff}
  .tabbtn[aria-selected="true"]{color:#fff;border-bottom-color:var(--green)}
  .tabbtn .tcount{display:inline-block;font-size:10px;font-weight:700;color:var(--black);
    background:var(--green);border-radius:999px;padding:1px 7px;margin-left:7px;vertical-align:1px}

  .container{max-width:1320px;margin:0 auto;padding:28px 40px 48px}
  .tabpanel[hidden]{display:none}

  .city-bar{background:#fff;border-radius:12px;padding:15px 20px;margin:26px 0 4px;
    box-shadow:0 1px 4px rgba(0,0,0,.06);border-left:4px solid var(--green);
    display:flex;justify-content:space-between;align-items:center;gap:14px;flex-wrap:wrap}
  .city-bar .cname{font-size:17px;font-weight:700;color:var(--black)}
  .city-bar .cmeta{font-size:12px;color:var(--gray-400);margin-top:2px}
  .city-bar .cper{font-size:12px;color:var(--gray-700);text-align:right}
  .city-bar .cper b{display:block;font-size:15px;color:var(--black)}

  .section-title{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;
    color:var(--gray-700);padding-bottom:10px;border-bottom:2px solid var(--green);margin:30px 0 10px}
  .section-hint{font-size:12px;color:var(--gray-400);margin-bottom:14px;line-height:1.5}

  .kpi-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:12px;margin-bottom:8px}
  .kpi-card{background:#fff;border-radius:12px;padding:14px 16px;border-top:3px solid var(--green);
    box-shadow:0 1px 4px rgba(0,0,0,.06)}
  .kpi-card.hl{border-top-color:var(--positive)}
  .kpi-label{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--gray-400);margin-bottom:4px}
  .kpi-value{font-size:20px;font-weight:700}
  .kpi-sub{font-size:11px;color:var(--gray-400);margin-top:3px}
  .pos{color:var(--positive)} .neg{color:var(--danger)} .warnc{color:var(--warning)}

  .money-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}
  .money-card{background:#fff;border-radius:12px;padding:18px 20px;box-shadow:0 1px 4px rgba(0,0,0,.06)}
  .money-card.spend{border-left:4px solid var(--danger)}
  .money-card.rev{border-left:4px solid var(--info)}
  .money-card.profit{border-left:4px solid var(--positive)}
  .money-card h3{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;color:var(--gray-400);margin-bottom:8px}
  .money-big{font-size:26px;font-weight:700;line-height:1.2}
  .money-alt{font-size:13px;color:var(--gray-700);margin-top:2px}
  .money-desc{font-size:12px;color:var(--gray-700);margin-top:10px;padding-top:10px;border-top:1px solid #f0f0f0}

  table{width:100%;border-collapse:collapse;background:#fff;border-radius:12px;overflow:hidden;
    box-shadow:0 1px 4px rgba(0,0,0,.06);font-size:13px}
  thead th{background:var(--black);color:#fff;font-size:10px;text-transform:uppercase;letter-spacing:.5px;
    padding:11px 9px;text-align:right;font-weight:700;white-space:nowrap}
  thead th:first-child,thead th:nth-child(2){text-align:left}
  tbody td{padding:11px 9px;text-align:right;border-bottom:1px solid #f2f2f2;white-space:nowrap}
  tbody td:first-child,tbody td:nth-child(2){text-align:left;white-space:normal}
  tbody tr:hover{background:#f9fefb}
  tbody tr.total-row{background:var(--gray-100);font-weight:700}
  tbody tr.total-row td{border-bottom:none}
  tbody tr.bad td{background:#fff8f6}
  tbody tr.bad:hover td{background:#ffefe9}
  tbody tr.muted td{color:var(--gray-400);background:#fafafa}
  .table-wrap{overflow-x:auto;border-radius:12px}
  .rank{display:inline-flex;width:22px;height:22px;border-radius:50%;background:var(--gray-100);
    align-items:center;justify-content:center;font-size:11px;font-weight:700;color:var(--gray-700);margin-right:6px}
  .city{display:block;font-size:11px;color:var(--gray-400);font-weight:400}
  .flag{color:var(--danger);font-weight:700}
  .okv{color:var(--positive);font-weight:700}
  .table-legend{font-size:11px;color:var(--gray-400);margin-top:8px;line-height:1.5}

  .badge{display:inline-block;padding:3px 9px;border-radius:999px;font-size:10px;font-weight:700;
    text-transform:uppercase;letter-spacing:.4px;white-space:nowrap}
  .b-star{background:#e6faf2;color:var(--positive)}
  .b-good{background:#eaf4fb;color:var(--info)}
  .b-mid{background:#fff5e8;color:var(--warning)}
  .b-bad{background:#fdeceb;color:var(--danger)}
  .b-off{background:#f0f0f0;color:var(--gray-400)}

  .bars-card{background:#fff;border-radius:12px;padding:18px 20px;box-shadow:0 1px 4px rgba(0,0,0,.06)}
  .bar-row{display:grid;grid-template-columns:210px 1fr 84px;gap:12px;align-items:center;margin-bottom:9px;font-size:12px}
  .bar-row .bname{color:var(--gray-700);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .bar-track{background:var(--gray-100);border-radius:5px;height:20px;position:relative;overflow:hidden}
  .bar-fill{height:100%;border-radius:5px}
  .bar-num{font-weight:700;text-align:right}
  .bar-ref{position:absolute;top:0;bottom:0;width:2px;background:var(--danger);opacity:.55}
  .bars-legend{font-size:11px;color:var(--gray-400);margin-top:10px;padding-top:10px;border-top:1px solid #f0f0f0}

  .loc-card{background:#fff;border-radius:12px;margin-bottom:12px;box-shadow:0 1px 4px rgba(0,0,0,.06);
    border:1px solid #eee;overflow:hidden}
  .loc-head{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:15px 18px;flex-wrap:wrap}
  .loc-head-info{flex:1;min-width:200px}
  .loc-head-info h2{font-size:15px;font-weight:700;color:var(--black)}
  .loc-meta{font-size:11px;color:var(--gray-400);margin-top:2px}
  .loc-roas{font-size:22px;font-weight:700;white-space:nowrap}
  .loc-roas small{font-size:10px;color:var(--gray-400);display:block;text-transform:uppercase;font-weight:700;text-align:right}
  .loc-body{padding:0 18px 18px}
  .kpi-strip{font-size:11px;color:var(--gray-400);margin-bottom:6px;padding:9px 0;
    border-top:1px solid #f0f0f0;border-bottom:1px solid #f0f0f0;display:flex;flex-wrap:wrap;gap:12px}
  .kpi-strip b{color:var(--gray-700)}
  .kpi-strip .lbl{display:block;font-size:9px;text-transform:uppercase;letter-spacing:.4px;
    color:var(--green-d);font-weight:700;margin-bottom:6px;width:100%}
  .loc-analysis{background:var(--gray-100);border-radius:10px;padding:14px 16px;border-left:4px solid var(--gray-400);margin-top:14px}
  .loc-analysis.sev-ok{border-left-color:var(--positive);background:#f4fdf8}
  .loc-analysis.sev-good{border-left-color:var(--info);background:#f6fbff}
  .loc-analysis.sev-mid{border-left-color:var(--warning);background:#fffaf3}
  .loc-analysis.sev-high{border-left-color:var(--danger);background:#fff8f6}
  .loc-analysis h4{font-size:11px;text-transform:uppercase;letter-spacing:.5px;margin:0 0 4px;color:var(--gray-700)}
  .loc-analysis ul{margin-left:18px;font-size:13px}
  .loc-analysis ul li{margin-bottom:4px}

  .note-card{background:#fff;border-radius:12px;padding:18px 20px;box-shadow:0 1px 4px rgba(0,0,0,.06);
    font-size:12.5px;color:var(--gray-700)}
  .note-card h4{font-size:11px;text-transform:uppercase;letter-spacing:.5px;color:var(--gray-400);margin:14px 0 5px}
  .note-card h4:first-child{margin-top:0}
  .note-card ul{margin-left:18px}
  .note-card li{margin-bottom:4px}
  .note-card code{background:var(--gray-100);padding:1px 5px;border-radius:4px;font-size:11.5px}

  .footer{background:var(--black);color:var(--gray-400);font-size:11px;padding:22px 40px;text-align:center;margin-top:36px}
  .footer span{color:var(--green)}

  @media(max-width:760px){
    .header{padding:16px} .header-meta{text-align:left} .container{padding:16px 16px 32px}
    .tabbar{padding:0 8px} .tabbtn{padding:11px 12px;font-size:12px}
    .bar-row{grid-template-columns:120px 1fr 66px;font-size:11px}
    .loc-roas{font-size:18px}
  }
  @media print{
    body{background:#fff} .tabbar{display:none}
    .tabpanel[hidden]{display:block!important}
    .loc-card,.kpi-card,table,.bars-card,.money-card,.note-card{box-shadow:none;border:1px solid #e5e5e5}
  }
"""

TAB_JS = """
  (function(){
    var btns = Array.prototype.slice.call(document.querySelectorAll('.tabbtn'));
    var panels = Array.prototype.slice.call(document.querySelectorAll('.tabpanel'));
    function show(key, push){
      btns.forEach(function(b){ b.setAttribute('aria-selected', String(b.dataset.tab === key)); });
      panels.forEach(function(p){ p.hidden = (p.dataset.tab !== key); });
      if (push && history.replaceState) history.replaceState(null, '', '#' + key);
      window.scrollTo({top:0});
    }
    btns.forEach(function(b){ b.addEventListener('click', function(){ show(b.dataset.tab, true); }); });
    var hash = (location.hash || '').replace('#','');
    show(btns.some(function(b){return b.dataset.tab === hash;}) ? hash : btns[0].dataset.tab, false);
  })();
"""


def render_kpis(g):
    T, M = g["T"], g["M"]
    cards = [
        ("ROAS (загальний)", roas_s(T["roas"]),
         f"на 1 ₴ реклами — {nm(T['roas'],2)} ₴ виручки", "hl"),
        ("Витрати на просування", uah(T["spend"]), eur(T["spend"] / FX["v"]), ""),
        ("Атрибутована виручка", uah(T["rev"]), eur(T["rev"] / FX["v"]), ""),
        ("Прибуток", uah(T["profit"]), "виручка мінус витрати", "hl"),
        ("Замовлень з реклами", nm(T["orders"], 0), f"середній чек {uah(T['aov'])}", ""),
        ("Нових клієнтів", nm(T["new_users"], 0),
         f"{pct(T['new_users']/T['orders']*100,0) if T['orders'] else '—'} усіх замовлень", ""),
        ("Показів", nm(T["impr"], 0), f"кліків {nm(T['clicks'],0)} · CTR {pct(T['ctr'])}", ""),
        ("Локацій у кампанії", nm(T["locations"], 0),
         f"прибуткових {sum(1 for l in g['locations'] if l['d']['profit'] > 0)}", ""),
    ]
    out = ['<div class="kpi-grid">']
    for label, val, sub, cls in cards:
        vcls = " pos" if cls == "hl" else ""
        out.append(f'<div class="kpi-card {cls}"><div class="kpi-label">{label}</div>'
                   f'<div class="kpi-value{vcls}">{val}</div><div class="kpi-sub">{sub}</div></div>')
    out.append("</div>")
    return "\n".join(out)


def render_money(g):
    T = g["T"]
    n = T["locations"]
    spends = [l["d"]["spend"] for l in g["locations"]]
    top = max(g["locations"], key=lambda l: l["d"]["rev"])
    return f"""
  <div class="money-grid">
    <div class="money-card spend">
      <h3>Витрати партнера на просування</h3>
      <div class="money-big neg">{uah(T['spend'])}</div>
      <div class="money-alt">{eur(T['spend']/FX['v'])}</div>
      <div class="money-desc">
        Уся сума, яку партнер сплатив за просування {n} {plural(n,'локації','локацій','локацій')}
        за {T['days']} {plural(T['days'],'день','дні','днів')} кампанії.
        У середньому {uah(T['spend']/n)} на локацію (від {uah(min(spends))} до {uah(max(spends))}).
      </div>
    </div>
    <div class="money-card rev">
      <h3>Виручка, згенерована рекламою</h3>
      <div class="money-big">{uah(T['rev'])}</div>
      <div class="money-alt">{eur(T['rev']/FX['v'])} · Attributed NET Revenue</div>
      <div class="money-desc">
        {nm(T['orders'],0)} {plural(T['orders'],'замовлення','замовлення','замовлень')}
        із середнім чеком {uah(T['aov'])}. Найбільше принесла локація
        «{esc(top['name'])}» — {uah(top['d']['rev'])}.
      </div>
    </div>
    <div class="money-card profit">
      <h3>Прибуток (виручка − витрати)</h3>
      <div class="money-big pos">{uah(T['profit'])}</div>
      <div class="money-alt">{eur(T['profit']/FX['v'])}</div>
      <div class="money-desc">
        Кожна вкладена гривня повернулася як {nm(T['roas'],2)} ₴ виручки.
        Це приріст виручки, а не чистий прибуток: собівартість продуктів не врахована.
      </div>
    </div>
  </div>"""


def plural(n, one, few, many):
    n = abs(int(n))
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def render_main_table(g):
    rows = []
    for k, l in enumerate(g["locations"], 1):
        d, cls = l["d"], badge(l["d"]["roas"])
        tr = ' class="bad"' if d["profit"] <= 0 else ""
        rows.append(f"""        <tr{tr}>
          <td><span class="rank">{k}</span>{esc(l['name'])}<span class="city">{esc(g['city'])} · ID {l['pid']}</span></td>
          <td><span class="badge {cls[0]}">{cls[1]}</span></td>
          <td>{nm(d['spend'])}</td><td>{nm(d['rev'])}</td>
          <td class="{'pos' if d['profit']>0 else 'neg'}">{nm(d['profit'])}</td>
          <td><b>{roas_s(d['roas'])}</b></td><td>{nm(d['orders'],0)}</td>
          <td>{nm(d['aov']) if d['aov'] else '—'}</td><td>{nm(d['new_users'],0)}</td>
        </tr>""")
    for l in g["no_ads_locs"]:
        rows.append(f"""        <tr class="muted">
          <td><span class="rank">—</span>{esc(l['name'])}<span class="city">{esc(g['city'])} · ID {l['pid']}</span></td>
          <td><span class="badge b-off">Без просування</span></td>
          <td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td>
        </tr>""")
    T = g["T"]
    rows.append(f"""        <tr class="total-row">
          <td>РАЗОМ · {T['locations']} {plural(T['locations'],'локація','локації','локацій')}</td>
          <td>—</td>
          <td>{nm(T['spend'])}</td><td>{nm(T['rev'])}</td>
          <td class="pos">{nm(T['profit'])}</td><td>{roas_s(T['roas'])}</td>
          <td>{nm(T['orders'],0)}</td><td>{nm(T['aov'])}</td><td>{nm(T['new_users'],0)}</td>
        </tr>""")
    return f"""
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Локація</th><th>Оцінка</th><th>Витрати, ₴</th><th>Виручка, ₴</th>
        <th>Прибуток, ₴</th><th>ROAS</th><th>Зам.</th><th>Сер. чек, ₴</th><th>Нових</th>
      </tr></thead>
      <tbody>
{chr(10).join(rows)}
      </tbody>
    </table>
  </div>"""


def render_bars(g):
    L = g["locations"]
    mx = max([l["d"]["roas"] for l in L] + [3.5])
    ref = 3.0 / mx * 100
    rows = []
    for l in L:
        d = l["d"]
        w = max(d["roas"] / mx * 100, 0.6)
        color = ("var(--positive)" if d["roas"] >= 6 else "var(--info)" if d["roas"] >= 3
                 else "var(--warning)" if d["roas"] >= 1 else "var(--danger)")
        ncls = ("pos" if d["roas"] >= 3 else "warnc" if d["roas"] >= 1 else "neg")
        rows.append(f"""    <div class="bar-row">
      <div class="bname">{esc(l['name'])}</div>
      <div class="bar-track"><div class="bar-fill" style="width:{w:.1f}%;background:{color}"></div>
        <div class="bar-ref" style="left:{ref:.1f}%"></div></div>
      <div class="bar-num {ncls}">{roas_s(d['roas'])}</div>
    </div>""")
    return f"""
  <div class="bars-card">
{chr(10).join(rows)}
    <div class="bars-legend">
      Червона лінія — ROAS 3×, орієнтир окупності просування.
      Середній ROAS по місту — {roas_s(g['T']['roas'])}.
    </div>
  </div>"""


def render_ops_table(g):
    M = g["M"]
    rows = []
    for l in g["locations"]:
        o = l["ops"]
        if not o:
            continue

        def cell(v, mv, fmt, higher_better=True, flag_lo=0.8, flag_hi=1.25):
            if v is None:
                return "<td>—</td>"
            cls = ""
            if mv:
                rel = v / mv
                if higher_better:
                    cls = ' class="okv"' if rel >= flag_hi else ' class="flag"' if rel <= flag_lo else ""
                else:
                    cls = ' class="flag"' if rel >= flag_hi else ' class="okv"' if rel <= flag_lo else ""
            return f"<td{cls}>{fmt(v)}</td>"

        up = o.get("uptime")
        upcls = ' class="flag"' if up is not None and up < UPTIME_FLAG else ""
        tr = ' class="bad"' if l["d"]["profit"] <= 0 else ""
        rows.append(f"""        <tr{tr}>
          <td>{esc(l['name'])}</td>
          <td>{nm(o.get('delivered',0),0)}</td><td>{nm(o.get('sessions',0),0)}</td>
          {cell(o.get('view_pct'), M['view_pct'], lambda v: pct(v))}
          {cell(o.get('add_pct'), M['add_pct'], lambda v: pct(v))}
          {cell(o.get('ord_pct'), M['ord_pct'], lambda v: pct(v))}
          <td{upcls}>{pct(up)}</td>
          {cell(o.get('aov_eur'), M['aov_eur'], lambda v: nm(v,2), False)}
          <td>{nm(o.get('delfee_eur'),2) if o.get('delfee_eur') is not None else '—'}</td>
          {cell(o.get('cook_min'), M['cook_min'], lambda v: nm(v,1), False)}
          <td>{nm(o['rating'],2) if o.get('rating') else '—'}</td>
        </tr>""")
    rows.append(f"""        <tr class="total-row">
          <td>Медіана по місту</td><td>—</td><td>—</td>
          <td>{pct(M['view_pct'])}</td><td>{pct(M['add_pct'])}</td><td>{pct(M['ord_pct'])}</td>
          <td>{pct(M['uptime'])}</td><td>{nm(M['aov_eur'],2)}</td>
          <td>{nm(M['delfee_eur'],2)}</td><td>{nm(M['cook_min'],1)}</td><td>—</td>
        </tr>""")
    return f"""
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Локація</th><th>Замовлень</th><th>Сесій</th><th>Відкрили меню</th>
        <th>Додали в кошик</th><th>Оформили</th><th>Аптайм</th><th>Сер. чек, €</th>
        <th>Доставка, €</th><th>Готує, хв</th><th>Рейтинг</th>
      </tr></thead>
      <tbody>
{chr(10).join(rows)}
      </tbody>
    </table>
  </div>
  <div class="table-legend">
    <b>Відкрили меню</b> — частка людей, які побачили картку закладу й відкрили її.
    <b>Додали в кошик</b> — частка з тих, хто відкрив меню. <b>Оформили</b> — частка з тих, хто додав у кошик.
    <b>Аптайм</b> — частка робочого часу, коли заклад був онлайн і приймав замовлення.
    Це <b>всі</b> замовлення за період кампанії, не лише рекламні.
    Зеленим позначено показники помітно краще за медіану міста, червоним — помітно гірше.
  </div>"""


def render_loc_cards(g):
    out = []
    for k, l in enumerate(g["locations"], 1):
        d, o = l["d"], l["ops"]
        cls, txt = badge(d["roas"])
        rcls = "pos" if d["roas"] >= 3 else "warnc" if d["roas"] >= 1 else "neg"
        bullets = "\n".join(f"          <li>{b}</li>" for b in loc_bullets(l, g))
        ops_strip = ""
        if o:
            ops_strip = f"""      <div class="kpi-strip">
        <span class="lbl">Операційні дані за той самий період</span>
        <span>Замовлень усіх: <b>{nm(o.get('delivered',0),0)}</b></span>
        <span>Аптайм: <b>{pct(o.get('uptime'))}</b></span>
        <span>Відкрили меню: <b>{pct(o.get('view_pct'))}</b></span>
        <span>Додали в кошик: <b>{pct(o.get('add_pct'))}</b></span>
        <span>Оформили: <b>{pct(o.get('ord_pct'))}</b></span>
        <span>Сер. чек: <b>{eur(o.get('aov_eur'))}</b></span>
        <span>Готує: <b>{nm(o.get('cook_min'),1)} хв</b></span>
      </div>"""
        out.append(f"""
  <div class="loc-card">
    <div class="loc-head">
      <div class="loc-head-info">
        <h2>{k}. {esc(l['name'])} <span class="badge {cls}">{txt}</span></h2>
        <div class="loc-meta">{esc(g['city'])} · Provider ID {l['pid']} · {esc(o.get('city') or '')}</div>
      </div>
      <div class="loc-roas {rcls}"><small>ROAS</small>{roas_s(d['roas'])}</div>
    </div>
    <div class="loc-body">
      <div class="kpi-strip">
        <span class="lbl">Реклама</span>
        <span>Витрати: <b>{uah(d['spend'])}</b></span>
        <span>Виручка: <b>{uah(d['rev'])}</b></span>
        <span>{'Прибуток' if d['profit']>0 else 'Збиток'}: <b>{uah(d['profit'])}</b></span>
        <span>Замовлень: <b>{nm(d['orders'],0)}</b></span>
        <span>Показів: <b>{nm(d['impr'],0)}</b></span>
        <span>Кліків: <b>{nm(d['clicks'],0)}</b></span>
        <span>CPO: <b>{eur(d['cpo'])}</b></span>
      </div>
{ops_strip}
      <div class="loc-analysis {severity(d['roas'])}">
        <h4>Що показують дані</h4>
        <ul>
{bullets}
        </ul>
      </div>
    </div>
  </div>""")
    for l in g["no_ads_locs"]:
        o = l["ops"]
        out.append(f"""
  <div class="loc-card">
    <div class="loc-head">
      <div class="loc-head-info">
        <h2>{esc(l['name'])} <span class="badge b-off">Без просування</span></h2>
        <div class="loc-meta">{esc(g['city'])} · Provider ID {l['pid']}</div>
      </div>
      <div class="loc-roas" style="color:var(--gray-400)"><small>ROAS</small>—</div>
    </div>
    <div class="loc-body">
      <div class="loc-analysis">
        <h4>Що показують дані</h4>
        <ul>
          <li>Ця локація <b>не була включена в кампанію платного просування</b>, тому рекламних
            витрат і атрибутованої виручки в неї немає.</li>
          <li>{f"Для контексту за той самий період: {nm(o.get('delivered',0),0)} "
               f"{plural(o.get('delivered',0),'доставлене замовлення','доставлені замовлення','доставлених замовлень')}, "
               f"аптайм {pct(o['uptime'])}"
               + (f", рейтинг {nm(o['rating'],2)}" if o.get('rating') else '') + "."
               if o.get('uptime') is not None else
               "За цей період заклад не працював у Bolt Food: замовлень і робочого часу немає."}</li>
        </ul>
      </div>
    </div>
  </div>""")
    return "\n".join(out)


def render_conclusions(tab):
    groups = tab["groups"]
    sp = sum(g["T"]["spend"] for g in groups)
    rv = sum(g["T"]["rev"] for g in groups)
    od = sum(g["T"]["orders"] for g in groups)
    nu = sum(g["T"]["new_users"] for g in groups)
    nloc = sum(g["T"]["locations"] for g in groups)
    prof = [l for g in groups for l in g["locations"] if l["d"]["profit"] > 0]
    loss = [l for g in groups for l in g["locations"] if l["d"]["profit"] <= 0]
    alll = [l for g in groups for l in g["locations"]]
    roas = rv / sp if sp else 0

    bl = [f"Платне просування <b>{'прибуткове' if rv>sp else 'збиткове'} на рівні бренду</b>: "
          f"{uah(sp)} витрат принесли {uah(rv)} атрибутованої виручки, "
          f"прибуток <b>{uah(rv-sp)}</b>, ROAS {roas_s(roas)}.",
          f"<b>{len(prof)} із {nloc}</b> {plural(nloc,'локації','локацій','локацій')} прибуткові"
          + (f", {len(loss)} — у збитку ({', '.join('«'+esc(l['name'])+'»' for l in loss)}, "
             f"разом {uah(sum(l['d']['profit'] for l in loss))})." if loss else ".")]

    top = sorted(alll, key=lambda l: -l["d"]["rev"])[:3]
    share_rev = sum(l["d"]["rev"] for l in top) / rv * 100 if rv else 0
    share_sp = sum(l["d"]["spend"] for l in top) / sp * 100 if sp else 0
    bl.append(f"Результат сконцентрований: <b>{len(top)} локації дають {pct(share_rev,0)} виручки</b>, "
              f"витрачаючи {pct(share_sp,0)} бюджету.")
    if od:
        bl.append(f"<b>{nm(nu,0)} нових клієнтів</b> — {pct(nu/od*100,0)} усіх атрибутованих замовлень.")

    best = max(alll, key=lambda l: l["d"]["roas"])
    worst = min(alll, key=lambda l: l["d"]["roas"])
    bl.append(f"Розкид між локаціями: від {roas_s(best['d']['roas'])} "
              f"(«{esc(best['name'])}») до {roas_s(worst['d']['roas'])} («{esc(worst['name'])}»).")

    low_up = [l for l in alll if (l["ops"].get("uptime") or 100) < UPTIME_FLAG]
    obs = []
    if low_up:
        obs.append("<li><b>Доступність.</b> "
                   + ", ".join(f"«{esc(l['name'])}» — аптайм {pct(l['ops']['uptime'])}" for l in low_up)
                   + ". У цей час реклама показувалась, але замовити було неможливо.</li>")
    zero = [l for l in alll if l["d"]["orders"] == 0]
    if zero:
        obs.append("<li><b>Без замовлень.</b> "
                   + ", ".join(f"«{esc(l['name'])}» ({nm(l['d']['impr'],0)} показів, "
                               f"{nm(l['d']['clicks'],0)} кліків)" for l in zero)
                   + " — витрачений бюджет не дав атрибутованих замовлень.</li>")
    expensive = sorted([l for l in alll if l["d"]["cpo"]], key=lambda l: -l["d"]["cpo"])[:1]
    if expensive:
        e = expensive[0]
        obs.append(f"<li><b>Найдорожче замовлення.</b> «{esc(e['name'])}» — {eur(e['d']['cpo'])} "
                   f"за одне атрибутоване замовлення при ROAS {roas_s(e['d']['roas'])}.</li>")
    aovs = [l for l in alll if l["ops"].get("aov_eur")]
    if aovs:
        hi = max(aovs, key=lambda l: l["ops"]["aov_eur"])
        obs.append(f"<li><b>Найвищий середній чек.</b> «{esc(hi['name'])}» — "
                   f"{eur(hi['ops']['aov_eur'])} проти медіани "
                   f"{eur(med([l['ops']['aov_eur'] for l in aovs]))} по бренду.</li>")

    per_city = "".join(
        f"<li><b>{esc(g['city'])}</b> ({period_uk(g['start'], g['end'])}): "
        f"{uah(g['T']['spend'])} витрат → {uah(g['T']['rev'])} виручки, "
        f"ROAS {roas_s(g['T']['roas'])}, {nm(g['T']['orders'],0)} "
        f"{plural(g['T']['orders'],'замовлення','замовлення','замовлень')}.</li>"
        for g in groups)

    return f"""
  <div class="note-card">
    <h4>Загальна оцінка</h4>
    <ul>
{chr(10).join('      <li>'+b+'</li>' for b in bl)}
    </ul>

    <h4>По містах</h4>
    <ul>{per_city}</ul>

    {'<h4>Що ще видно в даних</h4><ul>' + ''.join(obs) + '</ul>' if obs else ''}
  </div>"""




FX = {"v": 51.88}


def render(fx):
    FX["v"] = fx
    tabs_html, btns = [], []
    for tab in TABS:
        nloc = sum(g["T"]["locations"] for g in tab["groups"])
        btns.append(f'<button class="tabbtn" role="tab" data-tab="{tab["key"]}" '
                    f'aria-selected="false">{esc(tab["label"])}'
                    f'<span class="tcount">{nloc}</span></button>')
        body = []
        for g in tab["groups"]:
            nl = g["T"]["locations"]
            extra = (f" · {len(g['no_ads_locs'])} без просування" if g["no_ads_locs"] else "")
            body.append(f"""
  <div class="city-bar">
    <div>
      <div class="cname">{esc(tab['label'])} · {esc(g['city'])}</div>
      <div class="cmeta">{nl} {plural(nl,'локація','локації','локацій')} у кампанії{extra}</div>
    </div>
    <div class="cper">Період кампанії<b>{period_uk(g['start'], g['end'])}</b></div>
  </div>

  <div class="section-title">Загальні результати · {esc(g['city'])}</div>
  <div class="section-hint">Сумарно по локаціях, які брали участь у платному просуванні.</div>
{render_kpis(g)}

  <div class="section-title">Витрати партнера та прибуток · {esc(g['city'])}</div>
  <div class="section-hint">Економіка просування за період {period_uk(g['start'], g['end'])}.</div>
{render_money(g)}

  <div class="section-title">Результат по локаціях · {esc(g['city'])}</div>
  <div class="section-hint">Локації відсортовані за ROAS — від найефективнішої до найслабшої.</div>
{render_main_table(g)}

  <div class="section-title">ROAS по локаціях · {esc(g['city'])}</div>
{render_bars(g)}

  <div class="section-title">Операційна діагностика · {esc(g['city'])}</div>
  <div class="section-hint">
    Реклама лише приводить людину на сторінку закладу — далі вирішує сам заклад.
    Тому нижче — воронка та операційні показники кожної локації за той самий період,
    що й кампанія.
  </div>
{render_ops_table(g)}

  <div class="section-title">Ефективність кожної локації · {esc(g['city'])}</div>
  <div class="section-hint">
    У кожному блоці перший рядок — рекламні показники, другий — операційні дані Bolt
    за той самий період.
  </div>
{render_loc_cards(g)}""")
        body.append(f"""
  <div class="section-title">Ключові висновки · {esc(tab['label'])}</div>
{render_conclusions(tab)}""")
        tabs_html.append(f'<section class="tabpanel" role="tabpanel" '
                         f'data-tab="{tab["key"]}" hidden>{"".join(body)}</section>')

    total_loc = sum(g["T"]["locations"] for t in TABS for g in t["groups"])
    total_sp = sum(g["T"]["spend"] for t in TABS for g in t["groups"])
    total_rv = sum(g["T"]["rev"] for t in TABS for g in t["groups"])

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Платне просування · Аналіз ефективності · Bolt Food</title>
<style>{CSS}</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <div class="bolt-logo">⚡</div>
    <div class="header-title">
      <h1>Платне просування — аналіз ефективності</h1>
      <p>Bolt Food · Ads Performance Report</p>
    </div>
  </div>
  <div class="header-meta">
    Бренди: <strong>Josper Svintus · Mavra Pizza · Mavra Azia</strong><br/>
    Локацій у звіті: <strong>{total_loc}</strong><br/>
    Витрати: <strong>{uah(total_sp)}</strong> · виручка: <strong>{uah(total_rv)}</strong>
  </div>
</div>

<div class="tabbar" role="tablist">
{chr(10).join('  ' + b for b in btns)}
</div>

<div class="container">
{''.join(tabs_html)}
</div>

<div class="footer">
  <span>Bolt Food</span> · Звіт по платному просуванню · Дані з внутрішніх систем Bolt<br/>
  Сформовано: <span id="gen"></span>
</div>

<script>
  document.getElementById('gen').textContent = new Date().toLocaleString('uk-UA');
{TAB_JS}
</script>
</body>
</html>
"""


def main():
    print("Витягую дані з Databricks…", flush=True)
    fx = build_dataset()
    for tab in TABS:
        for g in tab["groups"]:
            for l in g["locations"]:
                derive(l)
            group_totals(g)
            group_medians(g)
    html = render(fx)

    targets = [SCRIPT_DIR / OUT_NAME, SCRIPT_DIR / LEGACY_NAME]
    boltable = Path.home() / "Desktop" / "partner-ads-reports" / "public"
    if boltable.is_dir():
        targets += [boltable / OUT_NAME, boltable / LEGACY_NAME]
    for t in targets:
        t.write_text(html, encoding="utf-8")
        print(f"→ {t}")
    print(f"\nкурс: {fx:.4f} ₴/€ · {len(html)} символів")


if __name__ == "__main__":
    main()
