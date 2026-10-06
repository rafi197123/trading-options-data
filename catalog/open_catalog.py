"""
open_catalog.py — הקטלוג של מאגר האופציות: בסיס נתונים אחד (DuckDB) מעל שני המאגרים — trading-options-data ו-trading-market-data.

נוצר אוטומטית על ידי engines/pull_options.py (מאגר הקוד trading-engines; כלי המשיכה v1.8 (06/10/2026)) — **לא לערוך ידנית**. אותן הגדרות של
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
import sys
from typing import Any, List, Optional, Tuple

SCHEMA = "options-data/1"
MARKET_SIBLING = "trading-market-data"
TIMEZONE = "America/New_York"
PATH_MARK = "@@PATH@@"
VIEWS: List[dict] = [
    {'name': 'options_eod', 'repo': 'od', 'sub': 'eod/*/*.parquet', 'sql': "CREATE OR REPLACE VIEW options_eod AS SELECT underlying AS symbol, * FROM read_parquet('@@PATH@@', union_by_name=true)", 'sql_empty': 'CREATE OR REPLACE VIEW options_eod AS SELECT NULL::VARCHAR AS "symbol", NULL::VARCHAR AS "option_symbol", NULL::VARCHAR AS "underlying", NULL::DATE AS "session", NULL::DATE AS "expiration", NULL::VARCHAR AS "side", NULL::DOUBLE AS "strike", NULL::DOUBLE AS "bid", NULL::DOUBLE AS "ask", NULL::DOUBLE AS "mid", NULL::BIGINT AS "bid_size", NULL::BIGINT AS "ask_size", NULL::DOUBLE AS "open", NULL::DOUBLE AS "high", NULL::DOUBLE AS "low", NULL::DOUBLE AS "close", NULL::BIGINT AS "volume", NULL::BIGINT AS "trades", NULL::DOUBLE AS "open_interest", NULL::BOOLEAN AS "oi_asof_prev_close", NULL::BOOLEAN AS "crossed", NULL::TIMESTAMP WITH TIME ZONE AS "quote_time", NULL::TIMESTAMP WITH TIME ZONE AS "last_trade", NULL::TIMESTAMP WITH TIME ZONE AS "created", NULL::BIGINT AS "dte", NULL::DOUBLE AS "T_years", NULL::DOUBLE AS "risk_free", NULL::DOUBLE AS "iv_calc", NULL::DOUBLE AS "delta_calc", NULL::VARCHAR AS "kept_by", NULL::BIGINT AS "filter_version", NULL::DOUBLE AS "spot_raw", NULL::VARCHAR AS "spot_source", NULL::VARCHAR AS "price_basis", NULL::VARCHAR AS "model_version" WHERE 1=0'},
    {'name': 'options_intraday', 'repo': 'od', 'sub': 'intraday/*/*.parquet', 'sql': "CREATE OR REPLACE VIEW options_intraday AS SELECT underlying AS symbol, * FROM read_parquet('@@PATH@@', union_by_name=true)", 'sql_empty': 'CREATE OR REPLACE VIEW options_intraday AS SELECT NULL::VARCHAR AS "symbol", NULL::VARCHAR AS "option_symbol", NULL::VARCHAR AS "underlying", NULL::DATE AS "session", NULL::DATE AS "expiration", NULL::VARCHAR AS "side", NULL::DOUBLE AS "strike", NULL::DOUBLE AS "bid", NULL::DOUBLE AS "ask", NULL::DOUBLE AS "mid", NULL::BIGINT AS "bid_size", NULL::BIGINT AS "ask_size", NULL::BOOLEAN AS "crossed", NULL::TIMESTAMP WITH TIME ZONE AS "snapshot_time", NULL::TIMESTAMP WITH TIME ZONE AS "quote_time", NULL::DOUBLE AS "lag_sec", NULL::DOUBLE AS "open_interest", NULL::BOOLEAN AS "oi_asof_prev_close", NULL::VARCHAR AS "point", NULL::BIGINT AS "dte", NULL::DOUBLE AS "T_years", NULL::DOUBLE AS "risk_free", NULL::DOUBLE AS "iv_calc", NULL::DOUBLE AS "delta_calc", NULL::VARCHAR AS "kept_by", NULL::BIGINT AS "filter_version", NULL::DOUBLE AS "spot_raw", NULL::VARCHAR AS "spot_source", NULL::VARCHAR AS "price_basis", NULL::VARCHAR AS "model_version" WHERE 1=0'},
    {'name': 'candles_daily', 'repo': 'md', 'sub': 'daily/*.csv', 'sql': "CREATE OR REPLACE VIEW candles_daily AS SELECT regexp_extract(filename, '([A-Za-z0-9._-]+)\\.(csv|parquet)$', 1) AS symbol, time_ny::DATE AS session, open, high, low, close, volume FROM read_csv('@@PATH@@', header=true, filename=true, types={'time_ny': 'VARCHAR', 'open': 'DOUBLE', 'high': 'DOUBLE', 'low': 'DOUBLE', 'close': 'DOUBLE', 'volume': 'DOUBLE'})", 'sql_empty': 'CREATE OR REPLACE VIEW candles_daily AS SELECT NULL::VARCHAR AS "symbol", NULL::DATE AS "session", NULL::DOUBLE AS "open", NULL::DOUBLE AS "high", NULL::DOUBLE AS "low", NULL::DOUBLE AS "close", NULL::DOUBLE AS "volume" WHERE 1=0'},
    {'name': 'candles_daily_raw', 'repo': 'md', 'sub': 'daily_raw/*.csv', 'sql': "CREATE OR REPLACE VIEW candles_daily_raw AS SELECT regexp_extract(filename, '([A-Za-z0-9._-]+)\\.(csv|parquet)$', 1) AS symbol, time_ny::DATE AS session, open, high, low, close, volume, adj_factor FROM read_csv('@@PATH@@', header=true, filename=true, types={'time_ny': 'VARCHAR', 'open': 'DOUBLE', 'high': 'DOUBLE', 'low': 'DOUBLE', 'close': 'DOUBLE', 'volume': 'DOUBLE', 'adj_factor': 'DOUBLE'})", 'sql_empty': 'CREATE OR REPLACE VIEW candles_daily_raw AS SELECT NULL::VARCHAR AS "symbol", NULL::DATE AS "session", NULL::DOUBLE AS "open", NULL::DOUBLE AS "high", NULL::DOUBLE AS "low", NULL::DOUBLE AS "close", NULL::DOUBLE AS "volume", NULL::DOUBLE AS "adj_factor" WHERE 1=0'},
    {'name': 'candles_30m', 'repo': 'md', 'sub': 'intraday30/*.csv', 'sql': "CREATE OR REPLACE VIEW candles_30m AS SELECT regexp_extract(filename, '([A-Za-z0-9._-]+)\\.(csv|parquet)$', 1) AS symbol, time_ny::TIMESTAMPTZ AS time_ny, open, high, low, close, volume FROM read_csv('@@PATH@@', header=true, filename=true, types={'time_ny': 'VARCHAR', 'open': 'DOUBLE', 'high': 'DOUBLE', 'low': 'DOUBLE', 'close': 'DOUBLE', 'volume': 'DOUBLE'})", 'sql_empty': 'CREATE OR REPLACE VIEW candles_30m AS SELECT NULL::VARCHAR AS "symbol", NULL::TIMESTAMP WITH TIME ZONE AS "time_ny", NULL::DOUBLE AS "open", NULL::DOUBLE AS "high", NULL::DOUBLE AS "low", NULL::DOUBLE AS "close", NULL::DOUBLE AS "volume" WHERE 1=0'},
    {'name': 'vix_family', 'repo': 'md', 'sub': 'cboe/*.csv', 'sql': "CREATE OR REPLACE VIEW vix_family AS SELECT regexp_extract(filename, '([A-Za-z0-9._-]+)\\.(csv|parquet)$', 1) AS symbol, time_ny::DATE AS session, open, high, low, close FROM read_csv('@@PATH@@', header=true, filename=true, types={'time_ny': 'VARCHAR', 'open': 'DOUBLE', 'high': 'DOUBLE', 'low': 'DOUBLE', 'close': 'DOUBLE'})", 'sql_empty': 'CREATE OR REPLACE VIEW vix_family AS SELECT NULL::VARCHAR AS "symbol", NULL::DATE AS "session", NULL::DOUBLE AS "open", NULL::DOUBLE AS "high", NULL::DOUBLE AS "low", NULL::DOUBLE AS "close" WHERE 1=0'},
    {'name': 'corporate_actions', 'repo': 'od', 'sub': 'corporate_actions.csv', 'sql': "CREATE OR REPLACE VIEW corporate_actions AS SELECT * FROM read_csv('@@PATH@@', header=true, types={'symbol': 'VARCHAR', 'date': 'DATE', 'ratio': 'DOUBLE', 'source': 'VARCHAR', 'verified': 'VARCHAR', 'verified_by': 'VARCHAR', 'note': 'VARCHAR'})", 'sql_empty': 'CREATE OR REPLACE VIEW corporate_actions AS SELECT NULL::VARCHAR AS "symbol", NULL::DATE AS "date", NULL::DOUBLE AS "ratio", NULL::VARCHAR AS "source", NULL::VARCHAR AS "verified", NULL::VARCHAR AS "verified_by", NULL::VARCHAR AS "note" WHERE 1=0'},
    {'name': 'contracts_watch', 'repo': 'od', 'sub': 'contracts_watch.csv', 'sql': "CREATE OR REPLACE VIEW contracts_watch AS SELECT * FROM read_csv('@@PATH@@', header=true, types={'option_symbol': 'VARCHAR', 'underlying': 'VARCHAR', 'expiration': 'DATE', 'side': 'VARCHAR', 'strike': 'DOUBLE', 'added_ny': 'VARCHAR', 'note': 'VARCHAR'})", 'sql_empty': 'CREATE OR REPLACE VIEW contracts_watch AS SELECT NULL::VARCHAR AS "option_symbol", NULL::VARCHAR AS "underlying", NULL::DATE AS "expiration", NULL::VARCHAR AS "side", NULL::DOUBLE AS "strike", NULL::VARCHAR AS "added_ny", NULL::VARCHAR AS "note" WHERE 1=0'},
    {'name': 'files', 'repo': 'od', 'sub': 'manifest.json', 'sql': 'CREATE OR REPLACE VIEW files AS WITH m AS (SELECT content::JSON AS j FROM read_text(\'@@PATH@@\')), k AS (SELECT j, unnest(json_keys(j, \'$.files\')) AS file FROM m), f AS (SELECT file, json_extract(j, \'$.files."\' || file || \'"\') AS v FROM k) SELECT CAST(file AS VARCHAR) AS "file", CAST(v->>\'symbol\' AS VARCHAR) AS "symbol", CAST(v->>\'kind\' AS VARCHAR) AS "data_kind", CAST(v->>\'month\' AS VARCHAR) AS "month", CAST(v->>\'sessions\' AS BIGINT) AS "sessions", CAST(v->>\'rows\' AS BIGINT) AS "rows", CAST(v->>\'first_session\' AS DATE) AS "first_session", CAST(v->>\'last_session\' AS DATE) AS "last_session", CAST(v->>\'complete\' AS BOOLEAN) AS "complete", CAST(v->>\'oi_rows_pct\' AS DOUBLE) AS "oi_rows_pct", CAST(v->\'spot_sources\'->>\'stock_eod_raw\' AS BIGINT) AS "spot_stock_eod_raw", CAST(v->\'spot_sources\'->>\'candles_30m\' AS BIGINT) AS "spot_candles_30m", CAST(v->\'spot_sources\'->>\'parity\' AS BIGINT) AS "spot_parity", CAST(v->\'spot_sources\'->>\'none\' AS BIGINT) AS "spot_none", CAST(v->>\'contracts_per_day_median\' AS DOUBLE) AS "contracts_median", CAST(v->>\'contracts_per_day_median_total\' AS DOUBLE) AS "contracts_median_total", CAST(v->\'alarm\'->>\'count\' AS BIGINT) AS "alarm_count", CAST(v->\'alarm\'->>\'max\' AS BIGINT) AS "alarm_max", CAST(v->>\'filter_version\' AS BIGINT) AS "filter_version", CAST(v->>\'retention\' AS VARCHAR) AS "retention", CAST(v->>\'updated_ny\' AS VARCHAR) AS "updated_ny" FROM f', 'sql_empty': 'CREATE OR REPLACE VIEW files AS SELECT NULL::VARCHAR AS "file", NULL::VARCHAR AS "symbol", NULL::VARCHAR AS "data_kind", NULL::VARCHAR AS "month", NULL::BIGINT AS "sessions", NULL::BIGINT AS "rows", NULL::DATE AS "first_session", NULL::DATE AS "last_session", NULL::BOOLEAN AS "complete", NULL::DOUBLE AS "oi_rows_pct", NULL::BIGINT AS "spot_stock_eod_raw", NULL::BIGINT AS "spot_candles_30m", NULL::BIGINT AS "spot_parity", NULL::BIGINT AS "spot_none", NULL::DOUBLE AS "contracts_median", NULL::DOUBLE AS "contracts_median_total", NULL::BIGINT AS "alarm_count", NULL::BIGINT AS "alarm_max", NULL::BIGINT AS "filter_version", NULL::VARCHAR AS "retention", NULL::VARCHAR AS "updated_ny" WHERE 1=0'},
    {'name': 'known_gaps', 'repo': 'od', 'sub': 'manifest.json', 'sql': 'CREATE OR REPLACE VIEW known_gaps AS WITH m AS (SELECT content::JSON AS j FROM read_text(\'@@PATH@@\')), s AS (SELECT j, unnest(json_keys(j, \'$.symbols\')) AS symbol FROM m), k AS (SELECT j, symbol, unnest([\'eod\', \'intraday\']) AS data_kind FROM s), mo AS (SELECT j, symbol, data_kind, unnest(json_keys(j, \'$.symbols."\' || symbol || \'".\' || data_kind || \'.known_gaps\')) AS month FROM k), d AS (SELECT j, symbol, data_kind, month, unnest(json_keys(j, \'$.symbols."\' || symbol || \'".\' || data_kind || \'.known_gaps\' || \'."\' || month || \'"\')) AS day FROM mo), pf AS (SELECT j, unnest(json_keys(j, \'$.files\')) AS file FROM m), pd AS (SELECT j, file, unnest(json_keys(j, \'$.files."\' || file || \'"\' || \'.point_gaps\')) AS day FROM pf), pp AS (SELECT j, file, day, unnest(json_keys(j, \'$.files."\' || file || \'"\' || \'.point_gaps."\' || day || \'"\')) AS point FROM pd) SELECT symbol::VARCHAR AS symbol, data_kind::VARCHAR AS data_kind, CAST(day AS DATE) AS gap_date, NULL::VARCHAR AS point, json_extract_string(j, \'$.symbols."\' || symbol || \'".\' || data_kind || \'.known_gaps\' || \'."\' || month || \'"."\' || day || \'"\') AS reason FROM d UNION ALL SELECT json_extract_string(j, \'$.files."\' || file || \'"\' || \'.symbol\'), json_extract_string(j, \'$.files."\' || file || \'"\' || \'.kind\'), CAST(day AS DATE), point::VARCHAR, json_extract_string(j, \'$.files."\' || file || \'"\' || \'.point_gaps."\' || day || \'"."\' || point || \'"\') FROM pp', 'sql_empty': 'CREATE OR REPLACE VIEW known_gaps AS SELECT NULL::VARCHAR AS "symbol", NULL::VARCHAR AS "data_kind", NULL::DATE AS "gap_date", NULL::VARCHAR AS "point", NULL::VARCHAR AS "reason" WHERE 1=0'},
    {'name': 'catalog_info', 'repo': None, 'sub': '', 'sql_empty': '', 'sql': "CREATE OR REPLACE VIEW catalog_info AS SELECT 'options-data/1' AS schema, current_timestamp AS built_at"},
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
    return con


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
            else:
                for name in view_names():
                    print(f"{name}\t{con.execute('SELECT count(*) FROM ' + name).fetchone()[0]}")
        finally:
            con.close()
    except duckdb.Error as e:
        print(f"שגיאה: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
