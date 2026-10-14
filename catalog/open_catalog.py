"""
open_catalog.py — הקטלוג של מאגר האופציות: בסיס נתונים אחד (DuckDB) מעל שני המאגרים — trading-options-data ו-trading-market-data.

נוצר אוטומטית על ידי engines/pull_options.py (מאגר הקוד trading-engines; כלי המשיכה v1.17 (14/10/2026)) — **לא לערוך ידנית**. אותן הגדרות של
views.sql ושל DATA_DICTIONARY.md (מקור אחד). עצמאי: תלוי רק ב-duckdb (ו-pytz — כדי להחזיר לפייתון עמודות זמן עם אזור זמן), בלי שום
ייבוא ממאגר הקוד.

    pip install duckdb pytz
    python3 catalog/open_catalog.py                      # רשימת התצוגות ומספר השורות בכל אחת
    python3 catalog/open_catalog.py "SELECT ..."         # שאילתה — התוצאה מודפסת (עמודות מופרדות בטאב)
    python3 catalog/open_catalog.py --build lab.duckdb   # קובץ DuckDB מקומי עם התצוגות (בנתיבים המוחלטים של המחשב הזה)
    --options-dir D · --market-dir D                     # במקום ברירות המחדל

    from open_catalog import connect                      # מפייתון (התיקייה catalog/ ב-sys.path)
    con = connect()                                       # options_dir = התיקייה שמעל הקובץ · market_dir = העותק האח ../trading-market-data
    con.execute("SELECT count(*) FROM options_eod").fetchall()

הנתיבים נפתרים ברגע הקריאה (מוחלטים) — עובד מכל תיקיית הרצה ומכל מחשב. אין עותק של מאגר נתוני השוק ⇒ תצוגות האופציות עובדות, תצוגות
נתוני השוק ריקות, והודעה אחת. החיבור בשעון ניו-יורק (SET TimeZone) — עמודות TIMESTAMPTZ מוצגות ומוחזרות בשעון ניו-יורק.
"""
from __future__ import annotations

import argparse
import glob
import os
import random
import subprocess
import sys
from typing import Any, List, Optional, Tuple

SCHEMA = "options-data/1"
MARKET_SIBLING = "trading-market-data"
TIMEZONE = "America/New_York"
PATH_MARK = "@@PATH@@"
CLOSED_TAG: Optional[str] = 'data-2026-10-14'               # התגית של המאגר הסגור (catalog --closed-tag); None — טרם נסגר
VIEWS: List[dict] = [
    {'name': 'options_eod', 'repo': 'od', 'sub': 'eod/*/*.parquet', 'sql': "CREATE OR REPLACE VIEW options_eod AS SELECT underlying AS symbol, * FROM read_parquet('@@PATH@@', union_by_name=true)", 'sql_empty': 'CREATE OR REPLACE VIEW options_eod AS SELECT NULL::VARCHAR AS "symbol", NULL::VARCHAR AS "option_symbol", NULL::VARCHAR AS "underlying", NULL::DATE AS "session", NULL::DATE AS "expiration", NULL::VARCHAR AS "side", NULL::DOUBLE AS "strike", NULL::DOUBLE AS "bid", NULL::DOUBLE AS "ask", NULL::DOUBLE AS "mid", NULL::BIGINT AS "bid_size", NULL::BIGINT AS "ask_size", NULL::DOUBLE AS "open", NULL::DOUBLE AS "high", NULL::DOUBLE AS "low", NULL::DOUBLE AS "close", NULL::BIGINT AS "volume", NULL::BIGINT AS "trades", NULL::DOUBLE AS "open_interest", NULL::BOOLEAN AS "oi_asof_prev_close", NULL::BOOLEAN AS "crossed", NULL::TIMESTAMP WITH TIME ZONE AS "quote_time", NULL::TIMESTAMP WITH TIME ZONE AS "last_trade", NULL::TIMESTAMP WITH TIME ZONE AS "created", NULL::BIGINT AS "dte", NULL::DOUBLE AS "T_years", NULL::DOUBLE AS "risk_free", NULL::DOUBLE AS "iv_calc", NULL::DOUBLE AS "delta_calc", NULL::VARCHAR AS "kept_by", NULL::BIGINT AS "filter_version", NULL::DOUBLE AS "spot_raw", NULL::VARCHAR AS "spot_source", NULL::VARCHAR AS "price_basis", NULL::VARCHAR AS "model_version" WHERE 1=0'},
    {'name': 'options_intraday', 'repo': 'od', 'sub': 'intraday/*/*.parquet', 'sql': "CREATE OR REPLACE VIEW options_intraday AS SELECT underlying AS symbol, * FROM read_parquet('@@PATH@@', union_by_name=true)", 'sql_empty': 'CREATE OR REPLACE VIEW options_intraday AS SELECT NULL::VARCHAR AS "symbol", NULL::VARCHAR AS "option_symbol", NULL::VARCHAR AS "underlying", NULL::DATE AS "session", NULL::DATE AS "expiration", NULL::VARCHAR AS "side", NULL::DOUBLE AS "strike", NULL::DOUBLE AS "bid", NULL::DOUBLE AS "ask", NULL::DOUBLE AS "mid", NULL::BIGINT AS "bid_size", NULL::BIGINT AS "ask_size", NULL::BOOLEAN AS "crossed", NULL::TIMESTAMP WITH TIME ZONE AS "snapshot_time", NULL::TIMESTAMP WITH TIME ZONE AS "quote_time", NULL::DOUBLE AS "lag_sec", NULL::DOUBLE AS "open_interest", NULL::BOOLEAN AS "oi_asof_prev_close", NULL::VARCHAR AS "point", NULL::BIGINT AS "dte", NULL::DOUBLE AS "T_years", NULL::DOUBLE AS "risk_free", NULL::DOUBLE AS "iv_calc", NULL::DOUBLE AS "delta_calc", NULL::VARCHAR AS "kept_by", NULL::BIGINT AS "filter_version", NULL::DOUBLE AS "spot_raw", NULL::VARCHAR AS "spot_source", NULL::VARCHAR AS "price_basis", NULL::VARCHAR AS "model_version" WHERE 1=0'},
    {'name': 'candles_daily', 'repo': 'md', 'sub': 'daily/*.csv', 'sql': "CREATE OR REPLACE VIEW candles_daily AS SELECT regexp_extract(filename, '([A-Za-z0-9._-]+)\\.(csv|parquet)$', 1) AS symbol, time_ny::DATE AS session, open, high, low, close, volume FROM read_csv('@@PATH@@', header=true, filename=true, types={'time_ny': 'VARCHAR', 'open': 'DOUBLE', 'high': 'DOUBLE', 'low': 'DOUBLE', 'close': 'DOUBLE', 'volume': 'DOUBLE'})", 'sql_empty': 'CREATE OR REPLACE VIEW candles_daily AS SELECT NULL::VARCHAR AS "symbol", NULL::DATE AS "session", NULL::DOUBLE AS "open", NULL::DOUBLE AS "high", NULL::DOUBLE AS "low", NULL::DOUBLE AS "close", NULL::DOUBLE AS "volume" WHERE 1=0'},
    {'name': 'candles_daily_raw', 'repo': 'md', 'sub': 'daily_raw/*.csv', 'sql': "CREATE OR REPLACE VIEW candles_daily_raw AS SELECT regexp_extract(filename, '([A-Za-z0-9._-]+)\\.(csv|parquet)$', 1) AS symbol, time_ny::DATE AS session, open, high, low, close, volume, adj_factor FROM read_csv('@@PATH@@', header=true, filename=true, types={'time_ny': 'VARCHAR', 'open': 'DOUBLE', 'high': 'DOUBLE', 'low': 'DOUBLE', 'close': 'DOUBLE', 'volume': 'DOUBLE', 'adj_factor': 'DOUBLE'})", 'sql_empty': 'CREATE OR REPLACE VIEW candles_daily_raw AS SELECT NULL::VARCHAR AS "symbol", NULL::DATE AS "session", NULL::DOUBLE AS "open", NULL::DOUBLE AS "high", NULL::DOUBLE AS "low", NULL::DOUBLE AS "close", NULL::DOUBLE AS "volume", NULL::DOUBLE AS "adj_factor" WHERE 1=0'},
    {'name': 'candles_30m', 'repo': 'md', 'sub': 'intraday30/*.csv', 'sql': "CREATE OR REPLACE VIEW candles_30m AS SELECT regexp_extract(filename, '([A-Za-z0-9._-]+)\\.(csv|parquet)$', 1) AS symbol, time_ny::TIMESTAMPTZ AS time_ny, open, high, low, close, volume FROM read_csv('@@PATH@@', header=true, filename=true, types={'time_ny': 'VARCHAR', 'open': 'DOUBLE', 'high': 'DOUBLE', 'low': 'DOUBLE', 'close': 'DOUBLE', 'volume': 'DOUBLE'})", 'sql_empty': 'CREATE OR REPLACE VIEW candles_30m AS SELECT NULL::VARCHAR AS "symbol", NULL::TIMESTAMP WITH TIME ZONE AS "time_ny", NULL::DOUBLE AS "open", NULL::DOUBLE AS "high", NULL::DOUBLE AS "low", NULL::DOUBLE AS "close", NULL::DOUBLE AS "volume" WHERE 1=0'},
    {'name': 'vix_family', 'repo': 'md', 'sub': 'cboe/*.csv', 'sql': "CREATE OR REPLACE VIEW vix_family AS SELECT regexp_extract(filename, '([A-Za-z0-9._-]+)\\.(csv|parquet)$', 1) AS symbol, time_ny::DATE AS session, open, high, low, close FROM read_csv('@@PATH@@', header=true, filename=true, types={'time_ny': 'VARCHAR', 'open': 'DOUBLE', 'high': 'DOUBLE', 'low': 'DOUBLE', 'close': 'DOUBLE'})", 'sql_empty': 'CREATE OR REPLACE VIEW vix_family AS SELECT NULL::VARCHAR AS "symbol", NULL::DATE AS "session", NULL::DOUBLE AS "open", NULL::DOUBLE AS "high", NULL::DOUBLE AS "low", NULL::DOUBLE AS "close" WHERE 1=0'},
    {'name': 'corporate_actions', 'repo': 'od', 'sub': 'corporate_actions.csv', 'sql': "CREATE OR REPLACE VIEW corporate_actions AS SELECT * FROM read_csv('@@PATH@@', header=true, types={'symbol': 'VARCHAR', 'date': 'DATE', 'ratio': 'DOUBLE', 'source': 'VARCHAR', 'verified': 'VARCHAR', 'verified_by': 'VARCHAR', 'note': 'VARCHAR'})", 'sql_empty': 'CREATE OR REPLACE VIEW corporate_actions AS SELECT NULL::VARCHAR AS "symbol", NULL::DATE AS "date", NULL::DOUBLE AS "ratio", NULL::VARCHAR AS "source", NULL::VARCHAR AS "verified", NULL::VARCHAR AS "verified_by", NULL::VARCHAR AS "note" WHERE 1=0'},
    {'name': 'contracts_watch', 'repo': 'od', 'sub': 'contracts_watch.csv', 'sql': "CREATE OR REPLACE VIEW contracts_watch AS SELECT * FROM read_csv('@@PATH@@', header=true, types={'option_symbol': 'VARCHAR', 'underlying': 'VARCHAR', 'expiration': 'DATE', 'side': 'VARCHAR', 'strike': 'DOUBLE', 'added_ny': 'VARCHAR', 'note': 'VARCHAR'})", 'sql_empty': 'CREATE OR REPLACE VIEW contracts_watch AS SELECT NULL::VARCHAR AS "option_symbol", NULL::VARCHAR AS "underlying", NULL::DATE AS "expiration", NULL::VARCHAR AS "side", NULL::DOUBLE AS "strike", NULL::VARCHAR AS "added_ny", NULL::VARCHAR AS "note" WHERE 1=0'},
    {'name': 'files', 'repo': 'od', 'sub': 'manifest.json', 'sql': 'CREATE OR REPLACE VIEW files AS WITH m AS (SELECT content::JSON AS j FROM read_text(\'@@PATH@@\')), f AS (SELECT unnest(map_entries(CAST(j -> \'$.files\' AS MAP(VARCHAR, JSON)))) AS e FROM m), g AS (SELECT e.key AS file, json_transform(e.value, \'{"symbol": "VARCHAR", "kind": "VARCHAR", "month": "VARCHAR", "sessions": "BIGINT", "rows": "BIGINT", "first_session": "DATE", "last_session": "DATE", "complete": "BOOLEAN", "oi_rows_pct": "DOUBLE", "spot_sources": {"stock_eod_raw": "BIGINT", "candles_30m": "BIGINT", "parity": "BIGINT", "none": "BIGINT"}, "contracts_per_day_median": "DOUBLE", "contracts_per_day_median_total": "DOUBLE", "alarm": {"count": "BIGINT", "max": "BIGINT"}, "filter_version": "BIGINT", "retention": "VARCHAR", "updated_ny": "VARCHAR"}\') AS s FROM f) SELECT CAST(file AS VARCHAR) AS "file", CAST(struct_extract(s, \'symbol\') AS VARCHAR) AS "symbol", CAST(struct_extract(s, \'kind\') AS VARCHAR) AS "data_kind", CAST(struct_extract(s, \'month\') AS VARCHAR) AS "month", CAST(struct_extract(s, \'sessions\') AS BIGINT) AS "sessions", CAST(struct_extract(s, \'rows\') AS BIGINT) AS "rows", CAST(struct_extract(s, \'first_session\') AS DATE) AS "first_session", CAST(struct_extract(s, \'last_session\') AS DATE) AS "last_session", CAST(struct_extract(s, \'complete\') AS BOOLEAN) AS "complete", CAST(struct_extract(s, \'oi_rows_pct\') AS DOUBLE) AS "oi_rows_pct", CAST(struct_extract(struct_extract(s, \'spot_sources\'), \'stock_eod_raw\') AS BIGINT) AS "spot_stock_eod_raw", CAST(struct_extract(struct_extract(s, \'spot_sources\'), \'candles_30m\') AS BIGINT) AS "spot_candles_30m", CAST(struct_extract(struct_extract(s, \'spot_sources\'), \'parity\') AS BIGINT) AS "spot_parity", CAST(struct_extract(struct_extract(s, \'spot_sources\'), \'none\') AS BIGINT) AS "spot_none", CAST(struct_extract(s, \'contracts_per_day_median\') AS DOUBLE) AS "contracts_median", CAST(struct_extract(s, \'contracts_per_day_median_total\') AS DOUBLE) AS "contracts_median_total", CAST(struct_extract(struct_extract(s, \'alarm\'), \'count\') AS BIGINT) AS "alarm_count", CAST(struct_extract(struct_extract(s, \'alarm\'), \'max\') AS BIGINT) AS "alarm_max", CAST(struct_extract(s, \'filter_version\') AS BIGINT) AS "filter_version", CAST(struct_extract(s, \'retention\') AS VARCHAR) AS "retention", CAST(struct_extract(s, \'updated_ny\') AS VARCHAR) AS "updated_ny" FROM g', 'sql_empty': 'CREATE OR REPLACE VIEW files AS SELECT NULL::VARCHAR AS "file", NULL::VARCHAR AS "symbol", NULL::VARCHAR AS "data_kind", NULL::VARCHAR AS "month", NULL::BIGINT AS "sessions", NULL::BIGINT AS "rows", NULL::DATE AS "first_session", NULL::DATE AS "last_session", NULL::BOOLEAN AS "complete", NULL::DOUBLE AS "oi_rows_pct", NULL::BIGINT AS "spot_stock_eod_raw", NULL::BIGINT AS "spot_candles_30m", NULL::BIGINT AS "spot_parity", NULL::BIGINT AS "spot_none", NULL::DOUBLE AS "contracts_median", NULL::DOUBLE AS "contracts_median_total", NULL::BIGINT AS "alarm_count", NULL::BIGINT AS "alarm_max", NULL::BIGINT AS "filter_version", NULL::VARCHAR AS "retention", NULL::VARCHAR AS "updated_ny" WHERE 1=0'},
    {'name': 'known_gaps', 'repo': 'od', 'sub': 'manifest.json', 'sql': "CREATE OR REPLACE VIEW known_gaps AS WITH m AS (SELECT content::JSON AS j FROM read_text('@@PATH@@')), s AS (SELECT unnest(map_entries(CAST(j -> '$.symbols' AS MAP(VARCHAR, JSON)))) AS e FROM m), k AS (SELECT e.key AS symbol, unnest(['eod', 'intraday']) AS data_kind, e.value AS sv FROM s), mo AS (SELECT symbol, data_kind, unnest(map_values(TRY_CAST(json_extract(sv, '$.' || data_kind || '.known_gaps') AS MAP(VARCHAR, JSON)))) AS mv FROM k), d AS (SELECT symbol, data_kind, unnest(map_entries(TRY_CAST(mv AS MAP(VARCHAR, VARCHAR)))) AS e FROM mo), f AS (SELECT unnest(map_values(CAST(j -> '$.files' AS MAP(VARCHAR, JSON)))) AS fv FROM m), pd AS (SELECT fv ->> 'symbol' AS symbol, fv ->> 'kind' AS data_kind, unnest(map_entries(TRY_CAST(fv -> 'point_gaps' AS MAP(VARCHAR, MAP(VARCHAR, VARCHAR))))) AS e FROM f), pp AS (SELECT symbol, data_kind, e.key AS day, unnest(map_entries(e.value)) AS p FROM pd) SELECT symbol::VARCHAR AS symbol, data_kind::VARCHAR AS data_kind, CAST(e.key AS DATE) AS gap_date, NULL::VARCHAR AS point, e.value::VARCHAR AS reason FROM d UNION ALL SELECT symbol::VARCHAR, data_kind::VARCHAR, CAST(day AS DATE), p.key::VARCHAR, p.value::VARCHAR FROM pp", 'sql_empty': 'CREATE OR REPLACE VIEW known_gaps AS SELECT NULL::VARCHAR AS "symbol", NULL::VARCHAR AS "data_kind", NULL::DATE AS "gap_date", NULL::VARCHAR AS "point", NULL::VARCHAR AS "reason" WHERE 1=0'},
    {'name': 'earnings', 'repo': 'md', 'sub': 'earnings/*.csv', 'sql': "CREATE OR REPLACE VIEW earnings AS SELECT *, coalesce(lower(trim(report_time)) IN ('before open', 'bmo', 'before-market', 'pre-market', 'after close', 'amc', 'after-market', 'post-market'), false) AS time_known FROM read_csv('@@PATH@@', header=true, columns={'symbol': 'VARCHAR', 'report_date': 'DATE', 'report_time': 'VARCHAR', 'fiscal_year': 'BIGINT', 'fiscal_quarter': 'BIGINT', 'updated': 'VARCHAR', 'kind': 'VARCHAR', 'fetched_ny': 'VARCHAR', 'source': 'VARCHAR'})", 'sql_empty': 'CREATE OR REPLACE VIEW earnings AS SELECT NULL::VARCHAR AS "symbol", NULL::DATE AS "report_date", NULL::VARCHAR AS "report_time", NULL::BIGINT AS "fiscal_year", NULL::BIGINT AS "fiscal_quarter", NULL::VARCHAR AS "updated", NULL::VARCHAR AS "kind", NULL::VARCHAR AS "fetched_ny", NULL::VARCHAR AS "source", NULL::BOOLEAN AS "time_known" WHERE 1=0'},
    {'name': 'known_exceptions', 'repo': 'od', 'sub': 'catalog/known_exceptions.csv', 'sql': "CREATE OR REPLACE VIEW known_exceptions AS SELECT * FROM read_csv('@@PATH@@', header=true, columns={'id': 'VARCHAR', 'scope': 'VARCHAR', 'symbols': 'VARCHAR', 'date_from': 'DATE', 'date_to': 'DATE', 'field': 'VARCHAR', 'stored': 'DOUBLE', 'correct': 'DOUBLE', 'correct_30m': 'DOUBLE', 'what': 'VARCHAR', 'effect': 'VARCHAR', 'guidance': 'VARCHAR'})", 'sql_empty': 'CREATE OR REPLACE VIEW known_exceptions AS SELECT NULL::VARCHAR AS "id", NULL::VARCHAR AS "scope", NULL::VARCHAR AS "symbols", NULL::DATE AS "date_from", NULL::DATE AS "date_to", NULL::VARCHAR AS "field", NULL::DOUBLE AS "stored", NULL::DOUBLE AS "correct", NULL::DOUBLE AS "correct_30m", NULL::VARCHAR AS "what", NULL::VARCHAR AS "effect", NULL::VARCHAR AS "guidance" WHERE 1=0'},
    {'name': 'earnings_corrections', 'repo': 'od', 'sub': 'catalog/earnings_corrections.csv', 'sql': "CREATE OR REPLACE VIEW earnings_corrections AS SELECT * FROM read_csv('@@PATH@@', header=true, columns={'symbol': 'VARCHAR', 'fiscal_year': 'BIGINT', 'fiscal_quarter': 'BIGINT', 'stored_date': 'DATE', 'stored_time': 'VARCHAR', 'verified_date': 'DATE', 'verified_time': 'VARCHAR', 'status': 'VARCHAR', 'confidence': 'VARCHAR', 'source': 'VARCHAR', 'note': 'VARCHAR'})", 'sql_empty': 'CREATE OR REPLACE VIEW earnings_corrections AS SELECT NULL::VARCHAR AS "symbol", NULL::BIGINT AS "fiscal_year", NULL::BIGINT AS "fiscal_quarter", NULL::DATE AS "stored_date", NULL::VARCHAR AS "stored_time", NULL::DATE AS "verified_date", NULL::VARCHAR AS "verified_time", NULL::VARCHAR AS "status", NULL::VARCHAR AS "confidence", NULL::VARCHAR AS "source", NULL::VARCHAR AS "note" WHERE 1=0'},
    {'name': 'options_eod_uid', 'repo': None, 'sub': '', 'sql_empty': '', 'sql': "CREATE OR REPLACE VIEW options_eod_uid AS SELECT e.*, e.symbol || '|' || strftime(e.expiration, '%Y%m%d') || '|' || e.side || '|' || printf('%.2f', round(e.strike / coalesce(f.factor, 1), 2)) AS contract_uid FROM options_eod e LEFT JOIN LATERAL (SELECT exp(sum(ln(a.ratio))) AS factor FROM corporate_actions a WHERE a.symbol = e.symbol AND a.date > e.session) f ON TRUE"},
    {'name': 'earnings_checked', 'repo': None, 'sub': '', 'sql_empty': '', 'sql': "CREATE OR REPLACE VIEW earnings_checked AS WITH cal AS (SELECT DISTINCT session AS day FROM candles_daily WHERE symbol = 'SPY'), caln AS (SELECT day, row_number() OVER (ORDER BY day) AS n FROM cal), b AS (SELECT min(day) AS d0, max(day) AS d1 FROM cal), j AS (SELECT e.*, c.status AS c_status, c.verified_date AS c_date, nullif(trim(c.verified_time), '') AS c_time FROM earnings e LEFT JOIN earnings_corrections c ON c.symbol = e.symbol AND c.stored_date = e.report_date AND lower(trim(c.stored_time)) = lower(trim(e.report_time)) AND ((c.fiscal_year IS NOT NULL AND c.fiscal_year = e.fiscal_year AND c.fiscal_quarter = e.fiscal_quarter) OR (c.fiscal_year IS NULL AND (e.fiscal_year IS NULL OR e.fiscal_quarter IS NULL)))), s AS (SELECT j.symbol, j.report_date, j.report_time, j.fiscal_year, j.fiscal_quarter, j.updated, j.kind, j.fetched_ny, j.source, j.time_known, coalesce(j.c_status, CASE WHEN j.time_known THEN 'כפי שהספק מסר' ELSE 'שעה לא ידועה' END) AS check_status, CASE WHEN lower(trim(j.report_time)) IN ('before open', 'bmo', 'before-market', 'pre-market') THEN 'before open' WHEN lower(trim(j.report_time)) IN ('after close', 'amc', 'after-market', 'post-market') THEN 'after close' END AS v_time, j.c_date, j.c_time FROM j), t AS (SELECT s.*, CASE WHEN check_status LIKE 'תוקן%' THEN c_date ELSE report_date END AS report_date_checked, CASE WHEN check_status LIKE 'תוקן%' THEN c_time ELSE v_time END AS report_time_checked FROM s), u AS (SELECT t.*, CASE WHEN t.report_date >= b.d0 AND t.report_date < b.d1 THEN (SELECT min(day) FROM cal WHERE day > t.report_date) ELSE t.report_date + CASE dayofweek(t.report_date) WHEN 5 THEN 3 WHEN 6 THEN 2 ELSE 1 END END AS nx_v, CASE WHEN t.report_date_checked >= b.d0 AND t.report_date_checked < b.d1 THEN (SELECT min(day) FROM cal WHERE day > t.report_date_checked) ELSE t.report_date_checked + CASE dayofweek(t.report_date_checked) WHEN 5 THEN 3 WHEN 6 THEN 2 ELSE 1 END END AS nx_c, CASE WHEN t.report_date >= b.d0 AND t.report_date <= b.d1 THEN (SELECT min(day) FROM cal WHERE day >= t.report_date) ELSE t.report_date + CASE dayofweek(t.report_date) WHEN 6 THEN 2 WHEN 0 THEN 1 ELSE 0 END END AS on_v, CASE WHEN t.report_date_checked >= b.d0 AND t.report_date_checked <= b.d1 THEN (SELECT min(day) FROM cal WHERE day >= t.report_date_checked) ELSE t.report_date_checked + CASE dayofweek(t.report_date_checked) WHEN 6 THEN 2 WHEN 0 THEN 1 ELSE 0 END END AS on_c, CASE WHEN t.report_date_checked >= b.d0 AND t.report_date_checked <= b.d1 THEN (SELECT min(n) FROM caln WHERE day >= t.report_date_checked) END AS n0, CASE WHEN t.report_date >= b.d0 AND t.report_date <= b.d1 THEN (SELECT min(n) FROM caln WHERE day >= t.report_date) END AS n0_v FROM t, b), w AS (SELECT u.*, CASE WHEN v_time = 'after close' THEN nx_v WHEN v_time = 'before open' THEN on_v ELSE report_date END AS lo_v, CASE WHEN v_time = 'before open' THEN on_v ELSE nx_v END AS hi_v, CASE WHEN report_time_checked = 'after close' THEN nx_c WHEN report_time_checked = 'before open' THEN on_c ELSE report_date_checked END AS lo_c, CASE WHEN report_time_checked = 'before open' THEN on_c ELSE nx_c END AS hi_c, CASE WHEN u.n0 > 15 THEN (SELECT day FROM caln WHERE n = u.n0 - 15) ELSE report_date_checked - 21 END AS back_c, CASE WHEN u.n0_v > 15 THEN (SELECT day FROM caln WHERE n = u.n0_v - 15) ELSE report_date - 21 END AS back_v FROM u) SELECT symbol, report_date, report_time, fiscal_year, fiscal_quarter, updated, kind, fetched_ny, source, time_known, check_status::VARCHAR AS check_status, report_date_checked::DATE AS report_date_checked, report_time_checked::VARCHAR AS report_time_checked, (check_status <> 'לא דוח') AS is_report, (check_status IN ('תוקן — אומת מול הודעת החברה', 'אומת — תואם לספק', 'כפי שהספק מסר') AND report_time_checked IS NOT NULL AND coalesce(kind = 'פורסם', false)) AS reliable, CAST(CASE WHEN check_status = 'לא דוח' THEN NULL WHEN check_status = 'תוקן — מוצע, טרם אומת חיצונית' THEN least(CASE WHEN v_time IS NULL THEN back_v ELSE lo_v END, lo_c) WHEN check_status = 'חשוד — טרם אומת' OR report_time_checked IS NULL THEN back_c ELSE lo_c END AS DATE) AS effect_day_from, CAST(CASE WHEN check_status = 'לא דוח' THEN NULL WHEN check_status = 'תוקן — מוצע, טרם אומת חיצונית' THEN greatest(hi_v, hi_c) WHEN check_status = 'חשוד — טרם אומת' OR report_time_checked IS NULL THEN nx_c ELSE hi_c END AS DATE) AS effect_day_to, coalesce(check_status IN ('תוקן — אומת מול הודעת החברה', 'אומת — תואם לספק'), false) AS externally_verified FROM w"},
    {'name': 'catalog_info', 'repo': None, 'sub': '', 'sql_empty': '', 'sql': "CREATE OR REPLACE VIEW catalog_info AS SELECT 'options-data/1' AS schema, current_timestamp AS built_at, 'data-2026-10-14'::VARCHAR AS closed_tag, DATE '2026-10-02' AS closed_through"},
]
NO_MARKET = ("הערה: אין עותק של מאגר נתוני השוק ({where}) — תצוגות האופציות עובדות; התצוגות של נתוני השוק ({views}) ריקות. "
             "לשכפל את trading-market-data ליד מאגר האופציות (אותה תיקיית אב), או connect(market_dir=<נתיב>) / --market-dir <נתיב>.")


def _q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def resolve_dirs(options_dir: Optional[str] = None, market_dir: Optional[str] = None) -> Tuple[str, Optional[str], str]:
    """(שורש מאגר האופציות, שורש מאגר נתוני השוק או None, איפה חיפשנו אותו) — נתיבים מוחלטים, נפתרים עכשיו."""
    od = os.path.abspath(os.path.expanduser(options_dir)) if options_dir else os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    md_try = os.path.abspath(os.path.expanduser(market_dir)) if market_dir else os.path.join(os.path.dirname(od), MARKET_SIBLING)
    return od, (md_try if os.path.isdir(md_try) else None), md_try


def connect(options_dir: Optional[str] = None, market_dir: Optional[str] = None, database: str = ":memory:", quiet: bool = False) -> Any:
    """חיבור DuckDB עם כל התצוגות, בנתיבים מוחלטים שנפתרים ברגע הקריאה. תצוגה שאין לה קבצים — ריקה, עם כל העמודות.
    options_dir — שורש מאגר האופציות (ברירת המחדל: התיקייה שמעל הקובץ הזה); market_dir — שורש מאגר נתוני השוק (ברירת המחדל: העותק האח
    ../trading-market-data, אם קיים); database — ":memory:" או נתיב לקובץ DuckDB מקומי; quiet — בלי ההודעה על עותק חסר."""
    import duckdb
    od, md, md_try = resolve_dirs(options_dir, market_dir)
    if md is None and not quiet:
        print(NO_MARKET.format(where=md_try, views=", ".join(v["name"] for v in VIEWS if v["repo"] == "md")), file=sys.stderr)
    con = duckdb.connect(database)
    try:
        con.execute("SET TimeZone=" + _q(TIMEZONE))
    except Exception as e:  # noqa: BLE001 — בלי הרחבת ICU אין אזורי זמן: ממשיכים, והזמנים ב-UTC
        print(f"הערה: אזור הזמן לא הוגדר ({type(e).__name__}) — עמודות הזמן יוצגו ב-UTC ולא בשעון ניו-יורק.", file=sys.stderr)
    for v in VIEWS:
        if v["repo"] is None:
            con.execute(v["sql"])
            continue
        base = od if v["repo"] == "od" else md
        path = (base.replace(os.sep, "/") + "/" + v["sub"]) if base else ""
        con.execute(v["sql"].replace(_q(PATH_MARK), _q(path)) if path and glob.glob(path) else v["sql_empty"])
    if md and CLOSED_TAG and not quiet:
        on = _on_tag(md, CLOSED_TAG)
        if on is not True:
            print(f"אזהרה: עותק מאגר נתוני השוק ({md}) אינו על התגית {CLOSED_TAG} של המאגר הסגור"
                  + (" (לא ניתן לבדוק — אינו מאגר git)" if on is None else "") + " — מה שאחרי התגית אינו חלק מהמאגר הסגור. "
                  f"git -C {md} checkout {CLOSED_TAG}", file=sys.stderr)
    if CLOSED_TAG and not quiet:                          # קת-2: גם עותק האופציות — התגית מצביעה על הקומיט שהפקודה catalog יצרה
        on_o = _on_tag(od, CLOSED_TAG)
        if on_o is not True:
            print(f"אזהרה: עותק מאגר האופציות ({od}) אינו על התגית {CLOSED_TAG} של המאגר הסגור"
                  + (" (לא ניתן לבדוק — אינו מאגר git)" if on_o is None else "") + " — מה שאחרי התגית אינו חלק מהמאגר הסגור. "
                  f"git -C {od} checkout {CLOSED_TAG}", file=sys.stderr)
    return con


def _on_tag(path: str, tag: str) -> Optional[bool]:
    """האם HEAD של העותק הוא התגית (True / False); None — לא ניתן לבדוק (אינו מאגר git, אין git)."""
    try:
        r = subprocess.run(["git", "-C", path, "tag", "--points-at", "HEAD"], capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.SubprocessError):
        return None
    return (tag in r.stdout.split()) if r.returncode == 0 else None


def view_names() -> List[str]:
    return [v["name"] for v in VIEWS]


def _print_rows(cur: Any) -> None:
    cols = [d[0] for d in cur.description] if cur.description else []
    if cols:
        print("\t".join(cols))
    for row in cur.fetchall():
        print("\t".join("" if x is None else str(x) for x in row))


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="הקטלוג של מאגר האופציות — DuckDB מעל שני המאגרים")
    ap.add_argument("sql", nargs="?", default="", help="שאילתה; בלי — רשימת התצוגות ומספר השורות")
    ap.add_argument("--options-dir", default=None, help="שורש מאגר האופציות (ברירת המחדל: התיקייה שמעל הקובץ)")
    ap.add_argument("--market-dir", default=None, help="שורש מאגר נתוני השוק (ברירת המחדל: העותק האח ../trading-market-data)")
    ap.add_argument("--build", default="", help="קובץ DuckDB מקומי לבנות (התצוגות בנתיבים המוחלטים של המחשב הזה)")
    a = ap.parse_args(argv)
    try:
        import duckdb
    except ImportError:
        print("עצירה: הספרייה duckdb אינה מותקנת — pip install duckdb pytz", file=sys.stderr)
        return 2
    try:
        import pytz  # noqa: F401 — DuckDB צריך אותה כדי להחזיר TIMESTAMPTZ לפייתון
    except ImportError:
        print("הערה: pytz אינה מותקנת — עמודות זמן עם אזור זמן לא יוחזרו לפייתון (pip install pytz).", file=sys.stderr)
    try:
        if a.build:
            target = os.path.abspath(os.path.expanduser(a.build))
            con = connect(a.options_dir, a.market_dir, database=target)
            con.close()
            print(f"נבנה: {target} · {len(VIEWS)} תצוגות, בנתיבים של המחשב הזה. בפתיחה: SET TimeZone='{TIMEZONE}'.")
            return 0
        con = connect(a.options_dir, a.market_dir)
        try:
            if a.sql:
                _print_rows(con.execute(a.sql))
            else:                                         # קת-6: בזיכרון מוגבל; התצוגה הנגזרת options_eod_uid (כבדה) — בלי ספירה
                con.execute("SET memory_limit='1GB'; SET threads=1")
                for v in VIEWS:
                    n = "— (תצוגה נגזרת מ-options_eod; לא נספרת)" if v["name"] == "options_eod_uid" else \
                        con.execute("SELECT count(*) FROM " + v["name"]).fetchone()[0]
                    print(f"{v['name']}\t{n}")
        finally:
            con.close()
    except duckdb.Error as e:
        print(f"שגיאה: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
