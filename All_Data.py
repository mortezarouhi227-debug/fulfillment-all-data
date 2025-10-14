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

                # KPI و درصدها
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
# Pick & Presort: اگر هر دو وجود داشتند ⇒ هر دو *_Larg؛ وگرنه نوع پایه
# ---------------------------
from collections import defaultdict
MIN_QTY_OUT = 15  # حداقل مقدار برای ثبت خروجی بعد از تجمیع (در صورت نیاز 0 کنید)

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

                quantity = float(qty) if qty else 0.0
                fromMin  = float(start) if start else 0.0
                toMin    = float(end)   if end   else 0.0
                occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0.0

                # فیلتر کمینه را بعد از تجمیع اعمال می‌کنیم (اینجا فقط حذف رکوردهای معیوب)
                if quantity <= 0 or occupied <= 0:
                    continue

                rows.append({
                    "full_name": full_name,
                    "raw_date": record_date,              # datetime برای KPI
                    "date": norm_date_str(record_date),   # 'YYYY-MM-DD'
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
    """تجمیع ساعتی در سطح full_name+date+hour (جمع quantity و occupied)"""
    agg = defaultdict(lambda: {"qty": 0.0, "occ": 0.0, "user": None, "dt": None})
    for it in rows:
        k = (it["full_name"], it["date"], it["hour"])
        a = agg[k]
        a["qty"] += it["quantity"]
        a["occ"] += it["occupied"]
        a["user"] = it["user"]
        a["dt"]   = it["raw_date"]
    return agg

def _getKPI_with_fallback(task_type, dt):
    """اول خود task_type؛ اگر نبود و *_Larg بود، KPI نوع پایه‌اش را بگیر."""
    cfg = getKPI(task_type, dt)
    if cfg:
        return cfg
    if task_type == "Pick_Larg":
        return getKPI("Pick", dt)
    if task_type == "Presort_Larg":
        return getKPI("Presort", dt)
    return None

def _emit_row(full_name, task_type, qty, occ, user, raw_dt, hour_int):
    cfg = _getKPI_with_fallback(task_type, raw_dt)
    perf_without = ""
    perf_with    = ""
    if cfg and qty > 0 and occ > 0:
        perf_without = (qty / cfg['base']) * 100.0
        perf_with    = (qty / (occ * cfg['rotation'])) * 100.0

    shift = shift_from_username(user)
    row, key = build_output_row(
        full_name=full_name,
        task_type=task_type,
        quantity=qty,
        record_date=raw_dt,
        hour=hour_int,
        occupied=occ,
        order_val=0,
        user=user,
        perf_without=perf_without,
        perf_with=perf_with,
        ipo_pack="",
        shift=shift
    )
    if key not in existing_keys_full:
        existing_keys_full.add(key)
        new_rows.append(row)

# خواندن خام و تجمیع
pick_agg    = _aggregate_hourly(_read_tab_rows_for("Pick"))
presort_agg = _aggregate_hourly(_read_tab_rows_for("Presort"))

# اتحاد کلیدها و خروجی طبق منطق درخواستی
all_keys = set(pick_agg.keys()) | set(presort_agg.keys())

for (full_name, date_s, hour_int) in all_keys:
    p = pick_agg.get((full_name, date_s, hour_int))
    s = presort_agg.get((full_name, date_s, hour_int))

    if p and s:
        # هر دو وجود دارند ⇒ هر دو *_Larg (با مقدار خودشان)
        if p["qty"] >= MIN_QTY_OUT:
            _emit_row(full_name, "Pick_Larg",    p["qty"], p["occ"], p["user"], p["dt"], hour_int)
        if s["qty"] >= MIN_QTY_OUT:
            _emit_row(full_name, "Presort_Larg", s["qty"], s["occ"], s["user"], s["dt"], hour_int)
    elif p:
        # فقط Pick ⇒ Pick
        if p["qty"] >= MIN_QTY_OUT:
            _emit_row(full_name, "Pick", p["qty"], p["occ"], p["user"], p["dt"], hour_int)
    elif s:
        # فقط Presort ⇒ Presort
        if s["qty"] >= MIN_QTY_OUT:
            _emit_row(full_name, "Presort", s["qty"], s["occ"], s["user"], s["dt"], hour_int)

# ---------------------------
# درج نهایی
# ---------------------------
if new_rows:
    ws_all.append_rows(new_rows, value_input_option="RAW")
    print(f"✅ Added {len(new_rows)} new rows.")
else:
    print("ℹ️ No new rows to add.")

sys.exit(0)

