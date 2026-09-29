# trading-options-data — מאגר האופציות של המעבדה

שרשראות אופציות היסטוריות (ThetaData) **אחרי הסינון** של תפיסת מאגר האופציות v1 (29/09/2026): לא כל השרשרת — הטווח הרלוונטי למסחר של רפי.
נכתב רק על ידי כלי המשיכה `engines/pull_options.py` שבמאגר הקוד (`trading-engines`) — **לא לערוך ידנית**. ספר ההפעלה, סעיף 33.

| תיקייה / קובץ | מה | הזמן |
|---|---|---|
| `eod/<סמל>/<שנה-חודש>.parquet` | שרשרת סוף-יום, שורה לחוזה-סשן: ציטוט הסגירה (bid/ask/mid, גדלים), OHLC, נפח, מספר עסקאות, עניין פתוח (של סוף D−1 — `oi_asof_prev_close`), `crossed` (bid > ask — מסומן, לא נמחק), הדלתא והגלומה שלנו (`delta_calc`, `iv_calc`, בלאק-שולס — אותה פונקציה של מנוע הפרמיה), `kept_by` (filter / atm / watch), `spot_raw` + `spot_source`, `price_basis=raw`, `filter_version`, `model_version` | `session` — יום המסחר · `quote_time` — סגירת הסשן (ניו-יורק) · `created`, `last_trade` — חותמות הספק |
| `intraday/<סמל>/<שנה-חודש>.parquet` | תמונות שרשרת בארבע נקודות ביום (10:00 · 11:30 · 13:30 · 15:30 ניו-יורק): אותן עמודות ציטוט וסינון + `point`, `snapshot_time`, `quote_time` (חותמת הציטוט האמיתית), `lag_sec`, מחיר הנכס בנקודה (`spot_raw`; `spot_source` = candles_30m מנר 30 הדק' של מאגר נתוני השוק, או parity — זוגיות קול-פוט) | ניו-יורק |
| `corporate_actions.csv` | פיצולים: `symbol, date, ratio, source (config / detected / both), verified (yes / no / no-data), verified_by, note` | — |
| `contracts_watch.csv` | חוזים במעקב — נשמרים עד פקיעתם גם מחוץ למסננים (`kept_by=watch`); ריק כרגע, עם כותרת | — |
| `catalog/lab.duckdb` | הקטלוג: תצוגות (`options_eod`, `options_intraday`, `candles_daily`, `candles_daily_raw`, `candles_30m`, `vix_family`, `corporate_actions`, `contracts_watch`) מעל הקבצים של שני המאגרים — **תוצר, לא מקור**; נבנה מחדש ב-`pull_options.py catalog` | — |
| `manifest.json` | לכל קובץ: חתימה (sha256), שורות, סשנים, ראשון/אחרון, `updated_ny`, ספק, גרסת הספרייה, גרסת הסינון והמודל, חציון החוזים ליום; לכל סמל: כיסוי חודשים, `known_gaps` עם סיבת הספק, חציון החוזים ליום | — |
| `dry_run_report.md` | דוח ההרצה היבשה (SPY, TSLA, יום אחד) — המדידה והאומדן | — |

**הכללים (הסינון — `config/options_store.json` במאגר הקוד):** פקיעות עם 3–60 ימים לפקיעה + החודשית הראשונה בכל טווח (60,120], (120,240], (240,400];
ב-SPY/QQQ/IWM — רק פקיעות שישי וחודשיות · |דלתא| 0.10–0.55, שני הצדדים (הדלתא שלנו, ממחיר האמצע, ממחיר הנכס הגולמי, מריבית לפי שנה) · חוזה בלי
ציטוט דו-צדדי — רק בתוך ±3 סטרייקים מהכסף · חוזים במעקב — עד הפקיעה. **המחירים והסטרייקים גולמיים "כפי שנסחרו"** (החוזה א.2) — הפיצולים
ב-`corporate_actions.csv`. מזהה חוזה: OCC בלי רווחים (`NVDA261120P00170000`).

**איך קוראים:** `SELECT * FROM options_eod WHERE symbol='NVDA' AND session='2025-04-04'` על `catalog/lab.duckdb` (מתיקיית המאגר; מאגר נתוני
השוק כעותק אחות `../trading-market-data`), או ישירות `read_parquet('eod/NVDA/*.parquet')`. **ThetaData: מפתח אחד = הפעלה אחת** — בזמן משיכה אף
שיחה אחרת מול הספק.
