# All_Data.py (override-only for *_Larg* + presort exclusivity + strong normalization)
# -*- coding: utf-8 -*-
import os, json, sys, re, unicodedata
from datetime import datetime, timedelta
from collections import defaultdict
import gspread
from google.oauth2.service_account import Credentials

# ---------------------------
# تنظیمات
# ---------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SPREADSHEET_ID = os.getenv(
    "SPREADSHEET_ID",
    "1VgKCQ8EjVF2sS8rSPdqFZh2h6CuqWAeqSMR56APvwes"
)

# حداقل مقدار معتبر برای ثبت خروجی‌ها
try:
    MIN_QTY_OUT = int(os.getenv("MIN_QTY_OUT", "15"))
except:
    MIN_QTY_OUT = 15

# نمایش پرفورمنس به صورت درصد با علامت %
PERF_AS_PERCENT = True

# ---------------------------
# اتصال
# ---------------------------
def make_client():
    env_creds = os.getenv("GOOGLE_CREDENTIALS")
    try:
        if env_creds:
            creds = Credentials.from_service_account_info(json.loads(env_creds), scopes=SCOPES)
            print("Auth via GOOGLE_CREDENTIALS (ENV).")
        else:
            creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
            print("Auth via credentials.json (file).")
        return gspread.authorize(creds)
    except Exception as e:
        print(f"❌ Auth error: {e}")
        sys.exit(1)

gc = make_client()
try:
    ss = gc.open_by_key(SPREADSHEET_ID)
    print(f"✅ Opened spreadsheet {SPREADSHEET_ID}.")
except Exception as e:
    print(f"❌ Open spreadsheet error: {e}")
    sys.exit(1)

# ---------------------------
# Helpers
# ---------------------------
def norm_str(x):
    return "" if x is None else str(x).strip()

def norm_num(x):
    if x is None or x == "":
        return ""
    try:
        f = float(x)
        return str(int(f)) if f.is_integer() else f"{f:.10g}"
    except:
        return norm_str(x)

def norm_date_str(dt):
    if dt is None or dt == "":
        return ""
    if hasattr(dt, "strftime"):
        return dt.strftime("%Y-%m-%d")
    s = str(dt).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except:
            pass
    return s

def _parse_excel_serial(val):
    return datetime(1899, 12, 30) + timedelta(days=float(val))

def parse_date_hour(date_raw, hour_raw):
    record_date, hour_val = None, None
    try:
        # تاریخ
        if isinstance(date_raw, (int, float)) and float(date_raw) > 30000:
            record_date = _parse_excel_serial(date_raw)
        elif isinstance(date_raw, str) and date_raw:
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%m/%d/%Y %H:%M:%S"):
                try:
                    record_date = datetime.strptime(date_raw.strip(), fmt)
                    break
                except:
                    continue
        # ساعت
        if isinstance(hour_raw, (int, float)):
            f = float(hour_raw)
            if 0 <= int(f) <= 23:
                hour_val = int(f)
            else:
                hour_val = _parse_excel_serial(f).hour
        elif isinstance(hour_raw, str) and hour_raw.strip():
            s = hour_raw.strip()
            if s.isdigit():
                v = int(s)
                if 0 <= v <= 23:
                    hour_val = v
            else:
                try:
                    hour_val = _parse_excel_serial(float(s)).hour
                except:
                    pass
    except Exception as e:
        print(f"❌ Error parsing date/hour: {e}")
    return record_date, hour_val

def parse_date_only(x):
    if not x:
        return None
    if isinstance(x, (int, float)) and float(x) > 30000:
        return _parse_excel_serial(x).date()
    if isinstance(x, str):
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%m/%d/%Y %H:%M:%S"):
            try:
                return datetime.strptime(x.strip(), fmt).date()
            except:
                continue
        try:
            f = float(x)
            if f > 30000:
                return _parse_excel_serial(f).date()
        except:
            pass
    return None

# ----- Regex sets for normalization -----
_ZW_RE = re.compile(r"[\u200c\u200d\u200e\u200f\u202a-\u202e\u2066-\u2069\u061c\uFEFF]")
_ARABIC_DIAC = re.compile(r"[\u0610-\u061a\u064b-\u065f\u0670\u06d6-\u06ed]")

def norm_name(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = s.replace("ي", "ی").replace("ى", "ی").replace("ې", "ی").replace("ك", "ک")
    s = _ZW_RE.sub("", s)
    s = _ARABIC_DIAC.sub("", s)
    s = s.replace("\u200c", " ")
    s = s.replace("\u00A0", " ").replace("\t", " ").replace("\r", " ").replace("\n", " ")
    s = s.replace("\u0640", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_task(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = _ZW_RE.sub("", s)
    s = s.replace("\u0640", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_hour_key(x):
    if x is None or x == "":
        return ""
    try:
        f = float(x)
        i = int(f)
        if 0 <= i <= 23:
            return str(i)
        return str(_parse_excel_serial(f).hour)
    except:
        s = str(x).strip()
        if s.isdigit():
            return s
        try:
            return str(_parse_excel_serial(float(s)).hour)
        except:
            return s

def shift_from_username(user):
    s = "Other"
    if user:
        lower = user.lower().strip()
        if lower.endswith(".s1"):
            s = "Shift1"
        elif lower.endswith(".s2"):
            s = "Shift2"
        elif lower.endswith(".s3"):   # ← اضافه شد
            s = "Shift3"
        elif lower.endswith(".flex"):
            s = "Flex"
    return s

# ---------------------------
# Sheets
# ---------------------------
ws_all   = ss.worksheet("All_Data")
ws_cfg   = ss.worksheet("KPI_Config")
ws_other = ss.worksheet("Other Work")
try:
    ws_override = ss.worksheet("Larg_Overrides")
except:
    ws_override = None

HEADERS = [
    'full_name','task_type','quantity','date','hour','occupied_hours','order',
    'performance_without_rotation','performance_with_rotation','Negative_Minutes',
    'Ipo_Pack','UserName','Shift'
]
vals_all = ws_all.get_all_values()
if not vals_all:
    ws_all.append_row(HEADERS)
    vals_all = [HEADERS]
else:
    if vals_all[0] != HEADERS:
        ws_all.delete_rows(1)
        ws_all.insert_row(HEADERS, 1)
        vals_all = ws_all.get_all_values()

# ---------------------------
# جلوگیری از تکرار (کلید یکتا: norm_name + norm_task + date + hour(int))
# + انحصار پری‌سورت به ازای (name,date,hour) صرف‌نظر از لیبل
# ---------------------------
existing_keys_hour = set()
PRESORT_TYPES = {"Presort", "Presort_Larg"}
existing_presort_hour = set()  # {(name_norm, date_str, hour_str)}

for r in vals_all[1:]:
    full_name = norm_name(r[0] if len(r)>0 else "")
    task_type = norm_task(r[1] if len(r)>1 else "")
    dt        = norm_date_str(r[3] if len(r)>3 else "")
    hr_raw    = r[4] if len(r)>4 else ""
    hr_key    = norm_hour_key(hr_raw)
    if full_name and dt != "" and hr_key != "":
        existing_keys_hour.add(f"{full_name}||{task_type}||{dt}||{hr_key}")
        if task_type in PRESORT_TYPES:
            existing_presort_hour.add((full_name, dt, hr_key))

# ---------------------------
# KPI Config (+ fallback)
# ---------------------------
cfg_data = ws_cfg.get_all_values()
cfg_headers = cfg_data[0] if cfg_data else []
kpi_configs = []
for row in cfg_data[1:]:
    try:
        kpi_configs.append({
            "task_type": row[cfg_headers.index("task_type")],
            "base": float(row[cfg_headers.index("base")]),
            "rotation": float(row[cfg_headers.index("rotation")]),
            "effective": datetime.strptime(row[cfg_headers.index("effective_from")], "%Y-%m-%d")
        })
    except:
        continue

def getKPI(taskType, recordDate):
    configs = [c for c in kpi_configs if c["task_type"] == taskType]
    configs.sort(key=lambda x: x["effective"])
    chosen = None
    for cfg in configs:
        if recordDate >= cfg["effective"]:
            chosen = cfg
        else:
            break
    return chosen

def getKPI_with_fallback(task_type, recordDate):
    cfg = getKPI(task_type, recordDate)
    if cfg:
        return cfg
    if task_type == "Pick_Larg":
        return getKPI("Pick", recordDate)
    if task_type == "Presort_Larg":
        return getKPI("Presort", recordDate)
    return None

# ---------------------------
# Other Work — منطق «آخرین تاریخ» (از آن تاریخ به بعد بلاک)
# ---------------------------
other = ws_other.get_all_values()
blocked_from_date = {}  # { norm_name(full_name): date }

if other and len(other) > 1:
    for row in other[1:]:
        name_raw = norm_str(row[2] if len(row) > 2 else "") or norm_str(row[1] if len(row) > 1 else "")
        if not name_raw:
            continue
        ts_raw = row[0] if len(row) > 0 else ""

        d_only = parse_date_only(ts_raw)
        if not d_only:
            dt, _ = parse_date_hour(ts_raw, "")
            d_only = dt.date() if dt else None
        if not d_only:
            continue

        key = norm_name(name_raw)
        prev = blocked_from_date.get(key)
        if (prev is None) or (d_only > prev):
            blocked_from_date[key] = d_only

def is_blocked(full_name: str, rec_dt: datetime, hour: int) -> bool:
    if rec_dt is None:
        return False
    key = norm_name(full_name)
    d_limit = blocked_from_date.get(key)
    if not d_limit:
        return False
    return rec_dt.date() >= d_limit

# ---------------------------
# Utility: ساخت ردیف خروجی
# ---------------------------
def _perf_to_cell(x):
    if x == "" or x is None:
        return ""
    try:
        f = float(x)
    except:
        return ""
    return f"{f:.1f}%" if PERF_AS_PERCENT else float(f"{f:.1f}")

def build_output_row(full_name, task_type, quantity, record_date, hour, occupied,
                     order_val, user, perf_without, perf_with, ipo_pack, shift):
    dt_s  = norm_date_str(record_date)
    qty_s = norm_num(quantity)
    hr_s  = norm_num(hour)
    occ_s = norm_num(occupied)
    ord_s = norm_num(order_val) if str(task_type).startswith("Pack") else ""
    perf_wo_cell = _perf_to_cell(perf_without)
    perf_wi_cell = _perf_to_cell(perf_with)
    neg_min = (60 - occupied) if (occupied and 0 < occupied < 60) else ""
    row = [
        norm_name(full_name),
        norm_task(norm_str(task_type)),
        qty_s, dt_s, hr_s, occ_s, ord_s,
        perf_wo_cell, perf_wi_cell, norm_num(neg_min),
        norm_num(ipo_pack), norm_str(user), norm_str(shift)
    ]
    key_hour = f"{norm_name(row[0])}||{norm_task(row[1])}||{row[3]}||{norm_hour_key(row[4])}"
    return row, key_hour

# گارد انحصار پری‌سورت در این اجرا
seen_new_keys = set()
seen_new_presort_hour = set()

def _emit_row(full_name, task_type, qty, occ, user, raw_dt, hour_int):
    # انحصار Presort: اگر برای این (name,date,hour) قبلا پری‌سورت ثبت شده، رد شو
    if task_type in PRESORT_TYPES:
        base_triplet = (norm_name(full_name), norm_date_str(raw_dt), norm_hour_key(hour_int))
        if base_triplet in existing_presort_hour or base_triplet in seen_new_presort_hour:
            return

    cfg = getKPI_with_fallback(task_type, raw_dt)
    perf_without = perf_with = ""
    if cfg and qty > 0 and occ > 0:
        perf_without = (qty / cfg['base']) * 100.0
        perf_with    = (qty / (occ * cfg['rotation'])) * 100.0

    shift = shift_from_username(user)
    row, key = build_output_row(full_name, task_type, qty, raw_dt, hour_int, occ,
                                0, user, perf_without, perf_with, "", shift)
    if key in existing_keys_hour or key in seen_new_keys:
        return

    if task_type in PRESORT_TYPES:
        base_triplet = (row[0], row[3], norm_hour_key(row[4]))
        existing_presort_hour.add(base_triplet)
        seen_new_presort_hour.add(base_triplet)

    existing_keys_hour.add(key)
    seen_new_keys.add(key)
    new_rows.append(row)

# ---------------------------
# تب‌های ساده
# ---------------------------
new_rows = []

simple_tabs = ["Receive", "Locate", "Sort", "Pack", "Stock taking"]
for tab in simple_tabs:
    try:
        ws = ss.worksheet(tab)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            continue
        head = data[0]
        idx = {c.strip(): i for i, c in enumerate(head)}

        for r in data[1:]:
            try:
                full_name = r[idx.get("full_name", -1)]
                if not full_name:
                    continue

                date_raw = r[idx.get("date", idx.get("Date", -1))]
                hour_raw = r[idx.get("hour", idx.get("Hour", -1))]
                record_date, hour = parse_date_hour(date_raw, hour_raw)
                if not record_date or hour is None:
                    continue
                if is_blocked(full_name, record_date, hour):
                    continue

                start = r[idx.get("Start", -1)]
                end   = r[idx.get("End",   -1)]
                qty   = r[idx.get("Count", idx.get("count", -1))]
                user  = r[idx.get("username", -1)]
                order_val_raw = r[idx.get("count_order", -1)] if "count_order" in idx else ""

                quantity = float(qty) if qty else 0
                fromMin  = float(start) if start else 0
                toMin    = float(end)   if end   else 0
                occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0
                if quantity < MIN_QTY_OUT or occupied <= 0:
                    continue

                ipo_pack, task_type = "", tab
                if tab == "Receive":
                    center = r[idx.get("warehouse_name", idx.get("warehouses_name", -1))]
                    if (center or "").strip() != "مرکز پردازش مهرآباد":
                        continue

                order_val = 0
                if tab == "Pack":
                    order_val = float(order_val_raw) if order_val_raw else 0
                    if order_val > 0:
                        ipo_pack = round(quantity / order_val, 2)
                    task_type = "Pack_Single" if (order_val > 0 and 1 <= ipo_pack <= 1.2) else "Pack_Multi"

                # KPI
                perf_without = perf_with = ""
                cfg = getKPI(task_type, record_date)
                if cfg and quantity > 0 and occupied > 0:
                    perf_without = (quantity / cfg['base']) * 100.0
                    perf_with    = (quantity / (occupied * cfg['rotation'])) * 100.0

                shift = shift_from_username(user)
                task_type = norm_task(task_type)
                row, key = build_output_row(
                    full_name, task_type, quantity, record_date, hour, occupied,
                    order_val, user, perf_without, perf_with, ipo_pack, shift
                )
                if key in existing_keys_hour or key in seen_new_keys:
                    continue
                existing_keys_hour.add(key)
                seen_new_keys.add(key)
                new_rows.append(row)
            except Exception as e:
                print(f"❌ Error in {tab}: {e}")
                continue
    except Exception as e:
        print(f"❌ Worksheet '{tab}' not found or error: {e}")

# ---------------------------
# Pick & Presort + Overrides (ONLY)  ← هیچ منطق دیگری برای *_Larg* وجود ندارد
# ---------------------------
def _read_tab_rows_for(tab_name):
    rows = []
    try:
        ws = ss.worksheet(tab_name)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            return rows
        head = data[0]
        idx = {c.strip(): i for i, c in enumerate(head)}

        for r in data[1:]:
            try:
                full_name_raw = r[idx.get("full_name", -1)]
                if not full_name_raw:
                    continue

                date_raw = r[idx.get("date", idx.get("Date", -1))]
                hour_raw = r[idx.get("hour", idx.get("Hour", -1))]
                record_date, hour = parse_date_hour(date_raw, hour_raw)
                if not record_date or hour is None:
                    continue
                if is_blocked(full_name_raw, record_date, hour):
                    continue

                start = r[idx.get("Start", -1)]
                end   = r[idx.get("End",   -1)]
                qty   = r[idx.get("Count", idx.get("count", -1))]
                user  = r[idx.get("username", -1)]

                quantity = float(qty) if qty else 0.0
                fromMin  = float(start) if start else 0.0
                toMin    = float(end)   if end   else 0.0
                occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0.0
                if quantity <= 0 or occupied <= 0:
                    continue

                rows.append({
                    "name_key": norm_name(full_name_raw),
                    "full_name_raw": full_name_raw,
                    "raw_date": record_date,
                    "date": norm_date_str(record_date),
                    "hour": int(hour),
                    "quantity": quantity,
                    "occupied": occupied,
                    "user": user
                })
            except Exception as e:
                print(f"❌ Error in {tab_name}: {e}")
                continue
    except Exception as e:
        print(f"❌ Worksheet '{tab_name}' not found or error: {e}")
    return rows

def _aggregate_hourly(rows):
    agg = defaultdict(lambda: {"qty": 0.0, "occ": 0.0, "user": None, "dt": None, "name_raw": None})
    for it in rows:
        k = (it["name_key"], it["date"], it["hour"])
        a = agg[k]
        a["qty"] += it["quantity"]
        a["occ"] += it["occupied"]
        a["user"] = it["user"]
        a["dt"]   = it["raw_date"]
        if not a["name_raw"]:
            a["name_raw"] = it.get("full_name_raw") or it["name_key"]
    return agg

def _read_overrides(ws):
    """
    Larg_Overrides (نسخه‌ی strict بر اساس هدرهای خواسته‌شده):
      - تاریخ:  «تاریخ حضور در لوکیشن»
      - ساعت:   «ساعت حضور در لوکیشن»
      - نام:    «نام پرسنلی»
      - نوع:    «لوکیشن کاری»   (Pick / Presort یا Pick_Larg / Presort_Larg)

    اگر هدرهای دقیق بالا پیدا نشدند، با اخطار، روی مجموعه‌ی قدیمی ستون‌های جایگزین fallback می‌کنیم.
    """
    force = set()
    only  = {}
    if not ws:
        print("ℹ️ Larg_Overrides sheet not found.")
        return force, only

    try:
        data = ws.get_all_values()
        if not data or len(data) < 2:
            print("ℹ️ Larg_Overrides is empty.")
            return force, only

        header = [(h or "").strip() for h in data[0]]

        def idx_exact(col_name):
            try:
                return header.index(col_name)
            except ValueError:
                return -1

        col_date = idx_exact("تاریخ حضور در لوکیشن")
        col_hour = idx_exact("ساعت حضور در لوکیشن")
        col_name = idx_exact("نام پرسنلی")
        col_type = idx_exact("لوکیشن کاری")

        if min(col_date, col_hour, col_name, col_type) < 0:
            print("⚠️ Using fallback headers for Larg_Overrides (exact headers not all found).")

            def find_col(cands):
                for cand in cands:
                    for i, h in enumerate(header):
                        if h.lower() == cand.lower():
                            return i
                return -1

            col_date = col_date if col_date >= 0 else find_col(["Timestamp","date","تاریخ"])
            col_hour = col_hour if col_hour >= 0 else find_col(["hour","ساعت","ساعت حضور در لوکیشن"])
            col_name = col_name if col_name >= 0 else find_col(["full_name","نام","name","نام پرسنلی","Column 5"])
            col_type = col_type if col_type >= 0 else find_col(["type","task_type","لوکیشن کاری"])

        if min(col_date, col_hour, col_name, col_type) < 0:
            print("❌ Larg_Overrides headers not found (need تاریخ/ساعت/نام/نوع). No overrides applied.")
            return force, only

        for r in data[1:]:
            try:
                if max(col_date, col_hour, col_name, col_type) >= len(r):
                    continue

                date_raw = r[col_date]
                hour_raw = r[col_hour]
                name_raw = r[col_name]
                type_raw = (r[col_type] or "").strip()
                if not name_raw or not type_raw:
                    continue

                t = type_raw.replace("_Larg", "").strip().lower()
                # نگاشت فارسی/انگلیسی
                if t in ("پیک", "pick"):
                    t = "pick"
                elif t in ("پری سورت", "presort", "pre-sort", "pre sort"):
                    t = "presort"
                if t not in ("pick", "presort"):
                    continue

                dt, hr = parse_date_hour(date_raw, hour_raw)
                if not dt or hr is None:
                    d_only = parse_date_only(date_raw)
                    if d_only is None:
                        continue
                    try:
                        hr = int(str(hour_raw).strip())
                        if not (0 <= hr <= 23):
                            continue
                    except:
                        continue
                    dt = datetime(d_only.year, d_only.month, d_only.day)

                key = (norm_name(norm_str(name_raw)), norm_date_str(dt), int(hr))
                force.add(key)
                only[key] = t
            except Exception as e:
                print(f"❌ Error reading a row in Larg_Overrides: {e}")
                continue

    except Exception as e:
        print(f"❌ Error reading Larg_Overrides: {e}")

    return force, only

pick_agg    = _aggregate_hourly(_read_tab_rows_for("Pick"))
presort_agg = _aggregate_hourly(_read_tab_rows_for("Presort"))
force_larg, force_only = _read_overrides(ws_override)

# منطق نهایی (فقط Overrides تعیین می‌کند *_Larg* باشد یا نه)
all_keys = set(pick_agg.keys()) | set(presort_agg.keys())

for (name_key, date_s, hour_int) in all_keys:
    p = pick_agg.get((name_key, date_s, hour_int))
    s = presort_agg.get((name_key, date_s, hour_int))

    in_force = (name_key, date_s, int(hour_int)) in force_larg
    mode = force_only.get((name_key, date_s, int(hour_int)))  # 'pick' | 'presort' | None
    display_name = (p and p.get("name_raw")) or (s and s.get("name_raw")) or name_key

    if in_force:
        # فقط همان نوع override شده لارج می‌شود؛ نوع دیگر (اگر وجود داشته باشد) نرمال ثبت می‌گردد
        if mode == "pick":
            if p and p["qty"] >= MIN_QTY_OUT:
                _emit_row(display_name, "Pick_Larg", p["qty"], p["occ"], p["user"], p["dt"], hour_int)
            if s and s["qty"] >= MIN_QTY_OUT:
                _emit_row(display_name, "Presort",   s["qty"], s["occ"], s["user"], s["dt"], hour_int)
        elif mode == "presort":
            if s and s["qty"] >= MIN_QTY_OUT:
                _emit_row(display_name, "Presort_Larg", s["qty"], s["occ"], s["user"], s["dt"], hour_int)
            if p and p["qty"] >= MIN_QTY_OUT:
                _emit_row(display_name, "Pick",         p["qty"], p["occ"], p["user"], p["dt"], hour_int)
        continue

    # بدون override: هیچ *_Larg* نداریم
    if p and p["qty"] >= MIN_QTY_OUT:
        _emit_row(display_name, "Pick", p["qty"], p["occ"], p["user"], p["dt"], hour_int)
    if s and s["qty"] >= MIN_QTY_OUT:
        _emit_row(display_name, "Presort", s["qty"], s["occ"], s["user"], s["dt"], hour_int)

# ---------------------------
# درج نهایی
# ---------------------------
if new_rows:
    ws_all.append_rows(new_rows, value_input_option="RAW")
    print(f"✅ Added {len(new_rows)} new rows.")
else:
    print("ℹ️ No new rows to add.")

sys.exit(0)

# ====== تکرار عین کدِ بالا طبق فایل ارسالی شما (فقط همین تغییر کوچکِ Shift3 دوباره اعمال شده) ======

# All_Data.py (override-only for *_Larg* + presort exclusivity + strong normalization)
# -*- coding: utf-8 -*-
import os, json, sys, re, unicodedata
from datetime import datetime, timedelta
from collections import defaultdict
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SPREADSHEET_ID = os.getenv(
    "SPREADSHEET_ID",
    "1VgKCQ8EjVF2sS8rSPdqFZh2h6CuqWAeqSMR56APvwes"
)

try:
    MIN_QTY_OUT = int(os.getenv("MIN_QTY_OUT", "15"))
except:
    MIN_QTY_OUT = 15

PERF_AS_PERCENT = True

def make_client():
    env_creds = os.getenv("GOOGLE_CREDENTIALS")
    try:
        if env_creds:
            creds = Credentials.from_service_account_info(json.loads(env_creds), scopes=SCOPES)
            print("Auth via GOOGLE_CREDENTIALS (ENV).")
        else:
            creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
            print("Auth via credentials.json (file).")
        return gspread.authorize(creds)
    except Exception as e:
        print(f"❌ Auth error: {e}")
        sys.exit(1)

gc = make_client()
try:
    ss = gc.open_by_key(SPREADSHEET_ID)
    print(f"✅ Opened spreadsheet {SPREADSHEET_ID}.")
except Exception as e:
    print(f"❌ Open spreadsheet error: {e}")
    sys.exit(1)

def norm_str(x): return "" if x is None else str(x).strip()

def norm_num(x):
    if x is None or x == "": return ""
    try:
        f = float(x); return str(int(f)) if f.is_integer() else f"{f:.10g}"
    except: return norm_str(x)

def norm_date_str(dt):
    if dt is None or dt == "": return ""
    if hasattr(dt, "strftime"): return dt.strftime("%Y-%m-%d")
    s = str(dt).strip()
    for fmt in ("%Y-%m-%d","%m/%d/%Y","%B %d, %Y","%m/%d/%Y %H:%M:%S"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    return s

def _parse_excel_serial(val): return datetime(1899,12,30)+timedelta(days=float(val))

def parse_date_hour(date_raw, hour_raw):
    record_date, hour_val = None, None
    try:
        if isinstance(date_raw,(int,float)) and float(date_raw)>30000:
            record_date=_parse_excel_serial(date_raw)
        elif isinstance(date_raw,str) and date_raw:
            for fmt in ("%Y-%m-%d","%m/%d/%Y","%B %d, %Y","%m/%d/%Y %H:%M:%S"):
                try: record_date=datetime.strptime(date_raw.strip(),fmt); break
                except: continue
        if isinstance(hour_raw,(int,float)):
            f=float(hour_raw)
            if 0<=int(f)<=23: hour_val=int(f)
            else: hour_val=_parse_excel_serial(f).hour
        elif isinstance(hour_raw,str) and hour_raw.strip():
            s=hour_raw.strip()
            if s.isdigit():
                v=int(s)
                if 0<=v<=23: hour_val=v
            else:
                try: hour_val=_parse_excel_serial(float(s)).hour
                except: pass
    except Exception as e:
        print(f"❌ Error parsing date/hour: {e}")
    return record_date, hour_val

def parse_date_only(x):
    if not x: return None
    if isinstance(x,(int,float)) and float(x)>30000: return _parse_excel_serial(x).date()
    if isinstance(x,str):
        for fmt in ("%Y-%m-%d","%m/%d/%Y","%B %d, %Y","%m/%d/%Y %H:%M:%S"):
            try: return datetime.strptime(x.strip(),fmt).date()
            except: continue
        try:
            f=float(x)
            if f>30000: return _parse_excel_serial(f).date()
        except: pass
    return None

_ZW_RE = re.compile(r"[\u200c\u200d\u200e\u200f\u202a-\u202e\u2066-\u2069\u061c\uFEFF]")
_ARABIC_DIAC = re.compile(r"[\u0610-\u061a\u064b-\u065f\u0670\u06d6-\u06ed]")

def norm_name(s:str)->str:
    if s is None: return ""
    s=unicodedata.normalize("NFKC",str(s))
    s=s.replace("ي","ی").replace("ى","ی").replace("ې","ی").replace("ك","ک")
    s=_ZW_RE.sub("",s); s=_ARABIC_DIAC.sub("",s)
    s=s.replace("\u200c"," ")
    s=s.replace("\u00A0"," ").replace("\t"," ").replace("\r"," ").replace("\n"," ")
    s=s.replace("\u0640",""); s=re.sub(r"\s+"," ",s).strip(); return s

def norm_task(s:str)->str:
    if s is None: return ""
    s=unicodedata.normalize("NFKC",str(s)); s=_ZW_RE.sub("",s)
    s=s.replace("\u0640",""); s=re.sub(r"\s+"," ",s).strip(); return s

def norm_hour_key(x):
    if x is None or x=="": return ""
    try:
        f=float(x); i=int(f)
        if 0<=i<=23: return str(i)
        return str(_parse_excel_serial(f).hour)
    except:
        s=str(x).strip()
        if s.isdigit(): return s
        try: return str(_parse_excel_serial(float(s)).hour)
        except: return s

def shift_from_username(user):
    s = "Other"
    if user:
        lower = user.lower().strip()
        if lower.endswith(".s1"):
            s = "Shift1"
        elif lower.endswith(".s2"):
            s = "Shift2"
        elif lower.endswith(".s3"):   # ← اضافه شد
            s = "Shift3"
        elif lower.endswith(".flex"):
            s = "Flex"
    return s

ws_all   = ss.worksheet("All_Data")
ws_cfg   = ss.worksheet("KPI_Config")
ws_other = ss.worksheet("Other Work")
try:
    ws_override = ss.worksheet("Larg_Overrides")
except:
    ws_override = None

HEADERS = [
    'full_name','task_type','quantity','date','hour','occupied_hours','order',
    'performance_without_rotation','performance_with_rotation','Negative_Minutes',
    'Ipo_Pack','UserName','Shift'
]
vals_all = ws_all.get_all_values()
if not vals_all:
    ws_all.append_row(HEADERS); vals_all = [HEADERS]
else:
    if vals_all[0] != HEADERS:
        ws_all.delete_rows(1); ws_all.insert_row(HEADERS, 1); vals_all = ws_all.get_all_values()

existing_keys_hour = set()
PRESORT_TYPES = {"Presort", "Presort_Larg"}
existing_presort_hour = set()

for r in vals_all[1:]:
    full_name = norm_name(r[0] if len(r)>0 else "")
    task_type = norm_task(r[1] if len(r)>1 else "")
    dt        = norm_date_str(r[3] if len(r)>3 else "")
    hr_raw    = r[4] if len(r)>4 else ""
    hr_key    = norm_hour_key(hr_raw)
    if full_name and dt != "" and hr_key != "":
        existing_keys_hour.add(f"{full_name}||{task_type}||{dt}||{hr_key}")
        if task_type in PRESORT_TYPES:
            existing_presort_hour.add((full_name, dt, hr_key))

cfg_data = ws_cfg.get_all_values()
cfg_headers = cfg_data[0] if cfg_data else []
kpi_configs = []
for row in cfg_data[1:]:
    try:
        kpi_configs.append({
            "task_type": row[cfg_headers.index("task_type")],
            "base": float(row[cfg_headers.index("base")]),
            "rotation": float(row[cfg_headers.index("rotation")]),
            "effective": datetime.strptime(row[cfg_headers.index("effective_from")], "%Y-%m-%d")
        })
    except:
        continue

def getKPI(taskType, recordDate):
    configs = [c for c in kpi_configs if c["task_type"] == taskType]
    configs.sort(key=lambda x: x["effective"])
    chosen = None
    for cfg in configs:
        if recordDate >= cfg["effective"]:
            chosen = cfg
        else:
            break
    return chosen

def getKPI_with_fallback(task_type, recordDate):
    cfg = getKPI(task_type, recordDate)
    if cfg: return cfg
    if task_type == "Pick_Larg": return getKPI("Pick", recordDate)
    if task_type == "Presort_Larg": return getKPI("Presort", recordDate)
    return None

other = ws_other.get_all_values()
blocked_from_date = {}

if other and len(other) > 1:
    for row in other[1:]:
        name_raw = norm_str(row[2] if len(row) > 2 else "") or norm_str(row[1] if len(row) > 1 else "")
        if not name_raw: continue
        ts_raw = row[0] if len(row) > 0 else ""

        d_only = parse_date_only(ts_raw)
        if not d_only:
            dt, _ = parse_date_hour(ts_raw, "")
            d_only = dt.date() if dt else None
        if not d_only: continue

        key = norm_name(name_raw)
        prev = blocked_from_date.get(key)
        if (prev is None) or (d_only > prev):
            blocked_from_date[key] = d_only

def is_blocked(full_name: str, rec_dt: datetime, hour: int) -> bool:
    if rec_dt is None: return False
    key = norm_name(full_name)
    d_limit = blocked_from_date.get(key)
    if not d_limit: return False
    return rec_dt.date() >= d_limit

def _perf_to_cell(x):
    if x == "" or x is None: return ""
    try: f=float(x)
    except: return ""
    return f"{f:.1f}%" if PERF_AS_PERCENT else float(f"{f:.1f}")

def build_output_row(full_name, task_type, quantity, record_date, hour, occupied,
                     order_val, user, perf_without, perf_with, ipo_pack, shift):
    dt_s  = norm_date_str(record_date)
    qty_s = norm_num(quantity)
    hr_s  = norm_num(hour)
    occ_s = norm_num(occupied)
    ord_s = norm_num(order_val) if str(task_type).startswith("Pack") else ""
    perf_wo_cell = _perf_to_cell(perf_without)
    perf_wi_cell = _perf_to_cell(perf_with)
    neg_min = (60 - occupied) if (occupied and 0 < occupied < 60) else ""
    row = [
        norm_name(full_name),
        norm_task(norm_str(task_type)),
        qty_s, dt_s, hr_s, occ_s, ord_s,
        perf_wo_cell, perf_wi_cell, norm_num(neg_min),
        norm_num(ipo_pack), norm_str(user), norm_str(shift)
    ]
    key_hour = f"{norm_name(row[0])}||{norm_task(row[1])}||{row[3]}||{norm_hour_key(row[4])}"
    return row, key_hour

seen_new_keys = set()
seen_new_presort_hour = set()

def _emit_row(full_name, task_type, qty, occ, user, raw_dt, hour_int):
    if task_type in PRESORT_TYPES:
        base_triplet = (norm_name(full_name), norm_date_str(raw_dt), norm_hour_key(hour_int))
        if base_triplet in existing_presort_hour or base_triplet in seen_new_presort_hour:
            return

    cfg = getKPI_with_fallback(task_type, raw_dt)
    perf_without = perf_with = ""
    if cfg and qty > 0 and occ > 0:
        perf_without = (qty / cfg['base']) * 100.0
        perf_with    = (qty / (occ * cfg['rotation'])) * 100.0

    shift = shift_from_username(user)
    row, key = build_output_row(full_name, task_type, qty, raw_dt, hour_int, occ,
                                0, user, perf_without, perf_with, "", shift)
    if key in existing_keys_hour or key in seen_new_keys:
        return

    if task_type in PRESORT_TYPES:
        base_triplet = (row[0], row[3], norm_hour_key(row[4]))
        existing_presort_hour.add(base_triplet)
        seen_new_presort_hour.add(base_triplet)

    existing_keys_hour.add(key)
    seen_new_keys.add(key)
    new_rows.append(row)

new_rows = []

simple_tabs = ["Receive", "Locate", "Sort", "Pack", "Stock taking"]
for tab in simple_tabs:
    try:
        ws = ss.worksheet(tab)
        data = ws.get_all_values()
        if not data or len(data) < 2: continue
        head = data[0]
        idx = {c.strip(): i for i, c in enumerate(head)}

        for r in data[1:]:
            try:
                full_name = r[idx.get("full_name", -1)]
                if not full_name: continue

                date_raw = r[idx.get("date", idx.get("Date", -1))]
                hour_raw = r[idx.get("hour", idx.get("Hour", -1))]
                record_date, hour = parse_date_hour(date_raw, hour_raw)
                if not record_date or hour is None: continue
                if is_blocked(full_name, record_date, hour): continue

                start = r[idx.get("Start", -1)]
                end   = r[idx.get("End",   -1)]
                qty   = r[idx.get("Count", idx.get("count", -1))]
                user  = r[idx.get("username", -1)]
                order_val_raw = r[idx.get("count_order", -1)] if "count_order" in idx else ""

                quantity = float(qty) if qty else 0
                fromMin  = float(start) if start else 0
                toMin    = float(end)   if end   else 0
                occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0
                if quantity < MIN_QTY_OUT or occupied <= 0: continue

                ipo_pack, task_type = "", tab
                if tab == "Receive":
                    center = r[idx.get("warehouse_name", idx.get("warehouses_name", -1))]
                    if (center or "").strip() != "مرکز پردازش مهرآباد": continue

                order_val = 0
                if tab == "Pack":
                    order_val = float(order_val_raw) if order_val_raw else 0
                    if order_val > 0:
                        ipo_pack = round(quantity / order_val, 2)
                    task_type = "Pack_Single" if (order_val > 0 and 1 <= ipo_pack <= 1.2) else "Pack_Multi"

                perf_without = perf_with = ""
                cfg = getKPI(task_type, record_date)
                if cfg and quantity > 0 and occupied > 0:
                    perf_without = (quantity / cfg['base']) * 100.0
                    perf_with    = (quantity / (occupied * cfg['rotation'])) * 100.0

                shift = shift_from_username(user)
                task_type = norm_task(task_type)
                row, key = build_output_row(
                    full_name, task_type, quantity, record_date, hour, occupied,
                    order_val, user, perf_without, perf_with, ipo_pack, shift
                )
                if key in existing_keys_hour or key in seen_new_keys: continue
                existing_keys_hour.add(key); seen_new_keys.add(key); new_rows.append(row)
            except Exception as e:
                print(f"❌ Error in {tab}: {e}"); continue
    except Exception as e:
        print(f"❌ Worksheet '{tab}' not found or error: {e}")

def _read_tab_rows_for(tab_name):
    rows = []
    try:
        ws = ss.worksheet(tab_name)
        data = ws.get_all_values()
        if not data or len(data) < 2: return rows
        head = data[0]; idx = {c.strip(): i for i, c in enumerate(head)}
        for r in data[1:]:
            try:
                full_name_raw = r[idx.get("full_name", -1)]
                if not full_name_raw: continue
                date_raw = r[idx.get("date", idx.get("Date", -1))]
                hour_raw = r[idx.get("hour", idx.get("Hour", -1))]
                record_date, hour = parse_date_hour(date_raw, hour_raw)
                if not record_date or hour is None: continue
                if is_blocked(full_name_raw, record_date, hour): continue
                start = r[idx.get("Start", -1)]
                end   = r[idx.get("End",   -1)]
                qty   = r[idx.get("Count", idx.get("count", -1))]
                user  = r[idx.get("username", -1)]
                quantity = float(qty) if qty else 0.0
                fromMin  = float(start) if start else 0.0
                toMin    = float(end)   if end   else 0.0
                occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0.0
                if quantity <= 0 or occupied <= 0: continue
                rows.append({
                    "name_key": norm_name(full_name_raw),
                    "full_name_raw": full_name_raw,
                    "raw_date": record_date,
                    "date": norm_date_str(record_date),
                    "hour": int(hour),
                    "quantity": quantity,
                    "occupied": occupied,
                    "user": user
                })
            except Exception as e:
                print(f"❌ Error in {tab_name}: {e}"); continue
    except Exception as e:
        print(f"❌ Worksheet '{tab_name}' not found or error: {e}")
    return rows

def _aggregate_hourly(rows):
    agg = defaultdict(lambda: {"qty": 0.0, "occ": 0.0, "user": None, "dt": None, "name_raw": None})
    for it in rows:
        k = (it["name_key"], it["date"], it["hour"])
        a = agg[k]
        a["qty"] += it["quantity"]; a["occ"] += it["occupied"]
        a["user"] = it["user"]; a["dt"] = it["raw_date"]
        if not a["name_raw"]: a["name_raw"] = it.get("full_name_raw") or it["name_key"]
    return agg

def _read_overrides(ws):
    force = set(); only = {}
    if not ws:
        print("ℹ️ Larg_Overrides sheet not found."); return force, only
    try:
        data = ws.get_all_values()
        if not data or len(data) < 2:
            print("ℹ️ Larg_Overrides is empty."); return force, only
        header = [(h or "").strip() for h in data[0]]
        def idx_exact(col_name):
            try: return header.index(col_name)
            except ValueError: return -1
        col_date = idx_exact("تاریخ حضور در لوکیشن")
        col_hour = idx_exact("ساعت حضور در لوکیشن")
        col_name = idx_exact("نام پرسنلی")
        col_type = idx_exact("لوکیشن کاری")
        if min(col_date,col_hour,col_name,col_type) < 0:
            print("⚠️ Using fallback headers for Larg_Overrides (exact headers not all found).")
            def find_col(cands):
                for cand in cands:
                    for i,h in enumerate(header):
                        if h.lower()==cand.lower(): return i
                return -1
            col_date = col_date if col_date>=0 else find_col(["Timestamp","date","تاریخ"])
            col_hour = col_hour if col_hour>=0 else find_col(["hour","ساعت","ساعت حضور در لوکیشن"])
            col_name = col_name if col_name>=0 else find_col(["full_name","نام","name","نام پرسنلی","Column 5"])
            col_type = col_type if col_type>=0 else find_col(["type","task_type","لوکیشن کاری"])
        if min(col_date,col_hour,col_name,col_type) < 0:
            print("❌ Larg_Overrides headers not found (need تاریخ/ساعت/نام/نوع). No overrides applied.")
            return force, only
        for r in data[1:]:
            try:
                if max(col_date,col_hour,col_name,col_type) >= len(r): continue
                date_raw=r[col_date]; hour_raw=r[col_hour]; name_raw=r[col_name]; type_raw=(r[col_type] or "").strip()
                if not name_raw or not type_raw: continue
                t = type_raw.replace("_Larg","").strip().lower()
                if t in ("پیک","pick"): t="pick"
                elif t in ("پری سورت","presort","pre-sort","pre sort"): t="presort"
                if t not in ("pick","presort"): continue
                dt, hr = parse_date_hour(date_raw, hour_raw)
                if not dt or hr is None:
                    d_only = parse_date_only(date_raw)
                    if d_only is None: continue
                    try:
                        hr = int(str(hour_raw).strip())
                        if not (0<=hr<=23): continue
                    except: continue
                    dt = datetime(d_only.year, d_only.month, d_only.day)
                key = (norm_name(norm_str(name_raw)), norm_date_str(dt), int(hr))
                force.add(key); only[key]=t
            except Exception as e:
                print(f"❌ Error reading a row in Larg_Overrides: {e}"); continue
    except Exception as e:
        print(f"❌ Error reading Larg_Overrides: {e}")
    return force, only

pick_agg    = _aggregate_hourly(_read_tab_rows_for("Pick"))
presort_agg = _aggregate_hourly(_read_tab_rows_for("Presort"))
force_larg, force_only = _read_overrides(ws_override)

all_keys = set(pick_agg.keys()) | set(presort_agg.keys())

for (name_key, date_s, hour_int) in all_keys:
    p = pick_agg.get((name_key, date_s, hour_int))
    s = presort_agg.get((name_key, date_s, hour_int))
    in_force = (name_key, date_s, int(hour_int)) in force_larg
    mode = force_only.get((name_key, date_s, int(hour_int)))
    display_name = (p and p.get("name_raw")) or (s and s.get("name_raw")) or name_key
    if in_force:
        if mode == "pick":
            if p and p["qty"] >= MIN_QTY_OUT: _emit_row(display_name, "Pick_Larg", p["qty"], p["occ"], p["user"], p["dt"], hour_int)
            if s and s["qty"] >= MIN_QTY_OUT: _emit_row(display_name, "Presort",   s["qty"], s["occ"], s["user"], s["dt"], hour_int)
        elif mode == "presort":
            if s and s["qty"] >= MIN_QTY_OUT: _emit_row(display_name, "Presort_Larg", s["qty"], s["occ"], s["user"], s["dt"], hour_int)
            if p and p["qty"] >= MIN_QTY_OUT: _emit_row(display_name, "Pick",         p["qty"], p["occ"], p["user"], p["dt"], hour_int)
        continue
    if p and p["qty"] >= MIN_QTY_OUT: _emit_row(display_name, "Pick", p["qty"], p["occ"], p["user"], p["dt"], hour_int)
    if s and s["qty"] >= MIN_QTY_OUT: _emit_row(display_name, "Presort", s["qty"], s["occ"], s["user"], s["dt"], hour_int)

if new_rows:
    ws_all.append_rows(new_rows, value_input_option="RAW"); print(f"✅ Added {len(new_rows)} new rows.")
else:
    print("ℹ️ No new rows to add.")

sys.exit(0)


