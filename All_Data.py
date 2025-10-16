# All_Data.py
import os, json, sys
from datetime import datetime, timedelta
from collections import defaultdict
import gspread
from google.oauth2.service_account import Credentials

# =========================
# تنظیمات
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1VgKCQ8EjVF2sS8rSPdqFZh2h6CuqWAeqSMR56APvwes")

# حداقل مقدار خروجی
try:
    MIN_QTY_OUT = int(os.getenv("MIN_QTY_OUT", "15"))
except:
    MIN_QTY_OUT = 15

# =========================
# اتصال
# =========================
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

# =========================
# Helper Functions
# =========================
def norm_str(x):
    return "" if x is None else str(x).strip()

def norm_num(x):
    if x is None or x == "":
        return ""
    try:
        f = float(x)
        if f.is_integer():
            return str(int(f))
        return f"{f:.10g}"
    except:
        return norm_str(x)

def norm_date_str(dt):
    if dt is None or dt == "":
        return ""
    if hasattr(dt, "strftime"):
        return dt.strftime("%Y-%m-%d")
    s = str(dt).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y"):
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
                v = int(s);  hour_val = v if 0 <= v <= 23 else None
            else:
                try:
                    hour_val = _parse_excel_serial(float(s)).hour
                except:
                    pass
    except:
        pass
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

def shift_from_username(user):
    s = "Other"
    if user:
        lower = user.lower().strip()
        if lower.endswith(".s1"):
            s = "Shift1"
        elif lower.endswith(".s2"):
            s = "Shift2"
        elif lower.endswith(".flex"):
            s = "Flex"
    return s

# =========================
# Sheets
# =========================
ws_all   = ss.worksheet("All_Data")
ws_cfg   = ss.worksheet("KPI_Config")
ws_other = ss.worksheet("Other Work")
# تب جدید برای اجبار Larg
try:
    ws_override = ss.worksheet("Larg_Overrides")
except:
    ws_override = None  # اگر وجود نداشت، خروجی‌ها عادی خواهد بود

HEADERS = [
    'full_name','task_type','quantity','date','hour','occupied_hours','order',
    'performance_without_rotation','performance_with_rotation','Negative_Minutes',
    'Ipo_Pack','UserName','Shift'
]

vals_all = ws_all.get_all_values()
if not vals_all:
    ws_all.append_row(HEADERS)
    vals_all = [HEADERS]
elif vals_all[0] != HEADERS:
    ws_all.delete_rows(1)
    ws_all.insert_row(HEADERS, 1)
    vals_all = ws_all.get_all_values()

# =========================
# جلوگیری از تکرار
# =========================
existing_keys_full = set()
for r in vals_all[1:]:
    full_name = norm_str(r[0] if len(r)>0 else "")
    task_type = norm_str(r[1] if len(r)>1 else "")
    qty       = norm_num(r[2] if len(r)>2 else "")
    dt        = norm_date_str(r[3] if len(r)>3 else "")
    hr        = norm_num(r[4] if len(r)>4 else "")
    occ       = norm_num(r[5] if len(r)>5 else "")
    ordv      = norm_num(r[6] if len(r)>6 else "")
    existing_keys_full.add(f"{full_name}||{task_type}||{dt}||{qty}||{hr}||{occ}||{ordv}")

# =========================
# KPI Config + fallback
# =========================
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

# =========================
# Other Work (Blocked Users)
# =========================
other = ws_other.get_all_values()
blocked_from = {}
if other and len(other) > 1:
    for row in other[1:]:
        name = (row[2] if len(row) > 2 else "").strip()
        start_raw = row[0] if len(row) > 0 else ""
        if not name:
            continue
        start_date = parse_date_only(start_raw)
        if not start_date:
            continue
        blocked_from[name] = min(blocked_from.get(name, start_date), start_date) if name in blocked_from else start_date

def is_blocked(full_name: str, rec_dt: datetime) -> bool:
    nm = (full_name or "").strip()
    if nm in blocked_from and rec_dt is not None:
        return rec_dt.date() >= blocked_from[nm]
    return False

# =========================
# Utility: ساخت ردیف خروجی + کلید
# =========================
def build_output_row(full_name, task_type, quantity, record_date, hour, occupied,
                     order_val, user, perf_without, perf_with, ipo_pack, shift):
    dt_s  = norm_date_str(record_date)
    qty_s = norm_num(quantity)
    hr_s  = norm_num(hour)
    occ_s = norm_num(occupied)
    ord_s = norm_num(order_val) if str(task_type).startswith("Pack") else ""
    perf_wo_s = f"{perf_without:.1f}%" if isinstance(perf_without, (int,float)) else ""
    perf_wi_s = f"{perf_with:.1f}%" if isinstance(perf_with, (int,float)) else ""
    neg_min = (60 - occupied) if (occupied and 0 < occupied < 60) else ""
    row = [
        norm_str(full_name), norm_str(task_type), qty_s, dt_s, hr_s, occ_s, ord_s,
        perf_wo_s, perf_wi_s, norm_num(neg_min), norm_num(ipo_pack), norm_str(user), norm_str(shift)
    ]
    key_full = f"{row[0]}||{row[1]}||{row[3]}||{row[2]}||{row[4]}||{row[5]}||{row[6]}"
    return row, key_full

def _emit_row(full_name, task_type, qty, occ, user, raw_dt, hour_int):
    cfg = getKPI_with_fallback(task_type, raw_dt)
    perf_without = perf_with = ""
    if cfg and qty > 0 and occ > 0:
        perf_without = (qty / cfg['base']) * 100.0
        perf_with    = (qty / (occ * cfg['rotation'])) * 100.0
    shift = shift_from_username(user)
    row, key = build_output_row(full_name, task_type, qty, raw_dt, hour_int, occ,
                                0, user, perf_without, perf_with, "", shift)
    if key not in existing_keys_full:
        existing_keys_full.add(key)
        new_rows.append(row)

# =========================
# خواندن تب‌های Pick/Presort
# =========================
def read_tab(tab_name):
    rows = []
    try:
        ws = ss.worksheet(tab_name)
        data = ws.get_all_values()
        if not data or len(data) < 2:
            return rows
        head = data[0]
        idx = {c.strip(): i for i,c in enumerate(head)}
        for r in data[1:]:
            try:
                full_name = r[idx.get("full_name", -1)]
                if not full_name:
                    continue
                date_raw = r[idx.get("date", idx.get("Date", -1))]
                hour_raw = r[idx.get("hour", idx.get("Hour", -1))]
                record_date, hour = parse_date_hour(date_raw, hour_raw)
                if not record_date or hour is None or is_blocked(full_name, record_date):
                    continue
                start = r[idx.get("Start", -1)]
                end   = r[idx.get("End", -1)]
                qty   = r[idx.get("Count", idx.get("count", -1))]
                user  = r[idx.get("username", -1)]
                try:
                    quantity = float(qty)
                    fromMin  = float(start)
                    toMin    = float(end)
                except:
                    continue
                occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0
                if quantity <= 0 or occupied <= 0:
                    continue
                rows.append({
                    "full_name": full_name,
                    "raw_date": record_date,
                    "date": norm_date_str(record_date),
                    "hour": int(hour),
                    "quantity": quantity,
                    "occupied": occupied,
                    "user": user
                })
            except:
                continue
    except Exception as e:
        print(f"❌ Error reading {tab_name}: {e}")
    return rows

def aggregate_hourly(rows):
    agg = defaultdict(lambda: {"qty":0.0,"occ":0.0,"user":None,"dt":None})
    for it in rows:
        k = (it["full_name"], it["date"], it["hour"])
        a = agg[k]
        a["qty"] += it["quantity"]
        a["occ"] += it["occupied"]
        a["user"] = it["user"]
        a["dt"]   = it["raw_date"]
    return agg

# =========================
# خواندن تب Larg_Overrides (A:date, B:hour, C:full_name)
# =========================
def read_overrides(ws):
    force_larg = set()
    if not ws:
        return force_larg
    try:
        data = ws.get_all_values()
        for i, r in enumerate(data):
            if len(r) < 3:
                continue
            date_raw, hour_raw, name_raw = r[0], r[1], r[2]
            if not name_raw:
                continue
            # تلاش برای پارس تاریخ/ساعت
            dt_obj, hr = parse_date_hour(date_raw, hour_raw)
            if not dt_obj or hr is None:
                # تلاش جایگزین: فقط تاریخ
                d_only = parse_date_only(date_raw)
                if d_only is None:
                    continue
                # اگر ساعت فقط عدد رشته‌ای بود
                try:
                    h_int = int(str(hour_raw).strip())
                    if 0 <= h_int <= 23:
                        hr = h_int
                    else:
                        continue
                    # تاریخ را به datetime برای نرمال‌سازی تبدیل می‌کنیم
                    dt_obj = datetime(d_only.year, d_only.month, d_only.day)
                except:
                    continue
            key = (norm_str(name_raw), norm_date_str(dt_obj), int(hr))
            force_larg.add(key)
    except Exception as e:
        print(f"❌ Error reading Larg_Overrides: {e}")
    return force_larg

# =========================
# پردازش
# =========================
new_rows = []

pick_agg    = aggregate_hourly(read_tab("Pick"))
presort_agg = aggregate_hourly(read_tab("Presort"))
force_larg  = read_overrides(ws_override)

all_keys = set(pick_agg.keys()) | set(presort_agg.keys())

for (full_name, date_s, hour_int) in all_keys:
    p = pick_agg.get((full_name, date_s, hour_int))
    s = presort_agg.get((full_name, date_s, hour_int))

    # آیا این شخص/تاریخ/ساعت در تب Larg_Overrides آمده؟
    in_force = (norm_str(full_name), date_s, int(hour_int)) in force_larg

    if p:
        task = "Pick_Larg" if in_force else "Pick"
        if p["qty"] >= MIN_QTY_OUT:
            _emit_row(full_name, task, p["qty"], p["occ"], p["user"], p["dt"], hour_int)

    if s:
        task = "Presort_Larg" if in_force else "Presort"
        if s["qty"] >= MIN_QTY_OUT:
            _emit_row(full_name, task, s["qty"], s["occ"], s["user"], s["dt"], hour_int)

# =========================
# درج نهایی
# =========================
if new_rows:
    ws_all.append_rows(new_rows, value_input_option="RAW")
    print(f"✅ Added {len(new_rows)} new rows.")
else:
    print("ℹ️ No new rows to add.")

sys.exit(0)

