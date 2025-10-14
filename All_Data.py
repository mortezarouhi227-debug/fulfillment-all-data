# All_Data.py
import os, json, sys
from datetime import datetime, timedelta
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
# Helper: نرمال‌سازی برای کلید یکتا
# ---------------------------
def norm_str(x):
    return "" if x is None else str(x).strip()

def norm_num(x):
    """ورودی عددی/رشته‌ای را به رشتهٔ عددی پایدار تبدیل می‌کند (بدون .0)"""
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
    """datetime/date/str -> 'YYYY-MM-DD'"""
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

# ---------------------------
# تاریخ/ساعت‌خوان
# ---------------------------
def parse_date_hour(date_raw, hour_raw):
    record_date, hour_val = None, None
    try:
        # تاریخ
        if isinstance(date_raw, (int, float)) and date_raw > 30000:
            base = datetime(1899, 12, 30)
            record_date = base + timedelta(days=int(date_raw))
        elif isinstance(date_raw, str) and date_raw:
            for fmt in ("%m/%d/%Y", "%B %d, %Y", "%Y-%m-%d"):
                try:
                    record_date = datetime.strptime(date_raw, fmt)
                    break
                except:
                    pass

        # ساعت
        if isinstance(hour_raw, (int, float)):
            if 0 <= int(hour_raw) <= 23:
                hour_val = int(hour_raw)
            else:
                base = datetime(1899, 12, 30)
                dt_val = base + timedelta(days=float(hour_raw))
                hour_val = dt_val.hour
        elif isinstance(hour_raw, str) and hour_raw.strip().isdigit():
            hour_val = int(hour_raw.strip())
    except Exception as e:
        print(f"❌ Error parsing date/hour: {e}")

    return record_date, hour_val

def parse_date_only(x):
    if not x:
        return None
    if isinstance(x, (int, float)) and x > 30000:
        base = datetime(1899, 12, 30)
        return (base + timedelta(days=int(x))).date()
    if isinstance(x, str):
        for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y"):
            try:
                return datetime.strptime(x, fmt).date()
            except:
                continue
    return None

# ---------------------------
# شیت‌ها
# ---------------------------
ws_all   = ss.worksheet("All_Data")
ws_cfg   = ss.worksheet("KPI_Config")
ws_other = ss.worksheet("Other Work")

# هدرها
HEADERS = [
    'full_name','task_type','quantity','date','hour','occupied_hours','order',
    'performance_without_rotation','performance_with_rotation','Negative_Minutes',
    'Ipo_Pack','UserName','Shift'
]
vals_all = ws_all.get_all_values()
if not vals_all:
    ws_all.append_row(HEADERS)
else:
    if vals_all[0] != HEADERS:
        ws_all.delete_rows(1)
        ws_all.insert_row(HEADERS, 1)

# ---------------------------
# کلیدهای موجود (با نرمال‌سازی پایدار)
# ---------------------------
existing = ws_all.get_all_values()[1:]
existing_keys_full = set()
for r in existing:
    full_name = norm_str(r[0] if len(r)>0 else "")
    task_type = norm_str(r[1] if len(r)>1 else "")
    qty       = norm_num(r[2] if len(r)>2 else "")
    dt        = norm_date_str(r[3] if len(r)>3 else "")
    hr        = norm_num(r[4] if len(r)>4 else "")
    occ       = norm_num(r[5] if len(r)>5 else "")
    ordv      = norm_num(r[6] if len(r)>6 else "")
    existing_keys_full.add(f"{full_name}||{task_type}||{dt}||{qty}||{hr}||{occ}||{ordv}")

# ---------------------------
# KPI config
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

# ---------------------------
# Other Work: بلاک از تاریخ A به بعد
# ---------------------------
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
        if name in blocked_from:
            if start_date < blocked_from[name]:
                blocked_from[name] = start_date
        else:
            blocked_from[name] = start_date

def is_blocked(full_name: str, rec_dt: datetime) -> bool:
    nm = (full_name or "").strip()
    if nm in blocked_from and rec_dt is not None:
        return rec_dt.date() >= blocked_from[nm]
    return False

# ---------------------------
# Utility: ساخت ردیف خروجی + کلید یکتا
# ---------------------------
def build_output_row(full_name, task_type, quantity, record_date, hour, occupied,
                     order_val, user, perf_without, perf_with, ipo_pack, shift):
    """مقادیر را به فرم نهایی (رشته/درصد/عدد) تبدیل می‌کند و کلید یکتا را برمی‌گرداند."""
    dt_s  = norm_date_str(record_date)
    qty_s = norm_num(quantity)
    hr_s  = norm_num(hour)
    occ_s = norm_num(occupied)

    # order فقط برای Pack_* پر می‌شود
    ord_s = norm_num(order_val) if str(task_type).startswith("Pack") else ""

    # درصدها را به صورت '95.5%' بنویسیم
    perf_wo_s = (f"{perf_without:.1f}%" if isinstance(perf_without, (int, float)) else norm_str(perf_without))
    perf_wi_s = (f"{perf_with:.1f}%"    if isinstance(perf_with,    (int, float)) else norm_str(perf_with))

    neg_min = (60 - occupied) if (occupied and occupied > 0) else ""
    if isinstance(neg_min, (int, float)) and neg_min <= 0:
        neg_min = ""

    row = [
        norm_str(full_name), norm_str(task_type), qty_s, dt_s, hr_s, occ_s, ord_s,
        perf_wo_s, perf_wi_s, norm_num(neg_min), norm_num(ipo_pack), norm_str(user), norm_str(shift)
    ]
    key_full = f"{row[0]}||{row[1]}||{row[3]}||{row[2]}||{row[4]}||{row[5]}||{row[6]}"
    return row, key_full

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

# ---------------------------
# پردازش تب‌های ساده
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
                if is_blocked(full_name, record_date):
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
                if quantity < 15 or occupied <= 0:
                    continue

                # فیلتر Receive (مرکز مهرآباد)
                if tab == "Receive":
                    center = r[idx.get("warehouse_name", idx.get("warehouses_name", -1))]
                    if (center or "").strip() != "مرکز پردازش مهرآباد":
                        continue

                # Pack: محاسبه ipo_pack + تشخیص Single/Multi
                ipo_pack, task_type = "", tab
                order_val = 0
                if tab == "Pack":
                    order_val = float(order_val_raw) if order_val_raw else 0
                    if order_val > 0:
                        ipo_pack = round(quantity / order_val, 2)
                    task_type = "Pack_Single" if (ipo_pack and 1 <= ipo_pack <= 1.2) else "Pack_Multi"

                # KPI و درصدها (به درصد متنی تبدیل می‌شود)
                perf_without = ""
                perf_with    = ""
                cfg = getKPI(task_type, record_date)
                if cfg and quantity > 0 and occupied > 0:
                    perf_without = (quantity / cfg['base']) * 100.0
                    perf_with    = (quantity / (occupied * cfg['rotation'])) * 100.0

                shift = shift_from_username(user)
                row, key = build_output_row(
                    full_name, task_type, quantity, record_date, hour, occupied,
                    order_val, user, perf_without, perf_with, ipo_pack, shift
                )
                if key in existing_keys_full:
                    continue
                existing_keys_full.add(key)
                new_rows.append(row)

            except Exception as e:
                print(f"❌ Error in {tab}: {e}")
                continue
    except Exception as e:
        print(f"❌ Worksheet '{tab}' not found or error: {e}")

# ---------------------------
# Pick & Presort: برچسب Large با full_name
# ---------------------------
pick_rows, presort_rows = [], []

for tab in ["Pick", "Presort"]:
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
                if is_blocked(full_name, record_date):
                    continue

                start = r[idx.get("Start", -1)]
                end   = r[idx.get("End",   -1)]
                qty   = r[idx.get("Count", idx.get("count", -1))]
                user  = r[idx.get("username", -1)]

                quantity = float(qty) if qty else 0
                fromMin  = float(start) if start else 0
                toMin    = float(end)   if end   else 0
                occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0
                if quantity < 15 or occupied <= 0:
                    continue

                base = {
                    "full_name": full_name,
                    "date": norm_date_str(record_date),
                    "hour": hour,  # عدد
                    "quantity": quantity,
                    "occupied": occupied,
                    "user": user,
                    "raw_date": record_date
                }
                if tab == "Pick":
                    pick_rows.append(base)
                else:
                    presort_rows.append(base)

            except Exception as e:
                print(f"❌ Error in {tab}: {e}")
                continue
    except Exception as e:
        print(f"❌ Worksheet '{tab}' not found or error: {e}")

# ساخت زوج بر اساس full_name + date + hour
pairs = {}
def kkey(r): return (r["full_name"], r["date"], norm_num(r["hour"]))
for r in pick_rows:
    pairs.setdefault(kkey(r), {})["pick"] = r
for r in presort_rows:
    pairs.setdefault(kkey(r), {})["presort"] = r

# دِداپ فشرده برای Pick/Presort
existing_keys_compact = set()
for (full_name, date_s, hour_s), sides in pairs.items():
    p = sides.get("pick")
    s = sides.get("presort")

    def add_one(base_row, task_type):
        compact_key = f"{base_row['full_name']}||{('Pick' if task_type.startswith('Pick') else 'Presort')}||{base_row['date']}||{norm_num(base_row['hour'])}"
        if compact_key in existing_keys_compact:
            return
        existing_keys_compact.add(compact_key)

        cfg = getKPI(task_type, base_row["raw_date"])
        perf_without = perf_with = ""
        if cfg and base_row["quantity"] > 0 and base_row["occupied"] > 0:
            perf_without = (base_row["quantity"] / cfg['base']) * 100.0
            perf_with    = (base_row["quantity"] / (base_row["occupied"] * cfg['rotation'])) * 100.0

        shift = shift_from_username(base_row["user"])
        row, key = build_output_row(
            base_row["full_name"], task_type, base_row["quantity"],
            base_row["raw_date"], base_row["hour"], base_row["occupied"],
            order_val=0, user=base_row["user"], perf_without=perf_without,
            perf_with=perf_with, ipo_pack="", shift=shift
        )
        if key in existing_keys_full:
            return
        existing_keys_full.add(key)
        new_rows.append(row)

    # قانون: اگر هر دو وجود دارند → هر دو Large؛ وگرنه هر کدام بود → همان Large
    if p and s:
        add_one(p, "Pick_Larg")
        add_one(s, "Presort_Larg")
    elif p:
        add_one(p, "Pick_Larg")
    elif s:
        add_one(s, "Presort_Larg")

# ---------------------------
# درج نهایی
# ---------------------------
if new_rows:
    # برای جلوگیری از مشکل json (date) همه چیز از قبل string/number شده است.
    ws_all.append_rows(new_rows, value_input_option="RAW")
    print(f"✅ Added {len(new_rows)} new rows.")
else:
    print("ℹ️ No new rows to add.")

sys.exit(0)

