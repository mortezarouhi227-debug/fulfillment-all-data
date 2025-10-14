# All_Data.py
import os, sys, json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

# ---------------------------
# Config
# ---------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_ID = os.getenv(
    "SPREADSHEET_ID",
    "1VgKCQ8EjVF2sS8rSPdqFZh2h6CuqWAeqSMR56APvwes"  # ID پیش‌فرض شیت شما
)

ALL_SHEET_NAME = "All_Data"
KPI_SHEET_NAME = "KPI_Config"
OTHER_SHEET_NAME = "Other Work"

# ---------------------------
# Auth
# ---------------------------
def make_client():
    try:
        env_creds = os.getenv("GOOGLE_CREDENTIALS")
        if env_creds:
            creds = Credentials.from_service_account_info(json.loads(env_creds), scopes=SCOPES)
        else:
            creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
        return gspread.authorize(creds)
    except Exception as e:
        print(f"❌ Auth error: {e}")
        sys.exit(1)

client = make_client()
try:
    ss = client.open_by_key(SPREADSHEET_ID)
except Exception as e:
    print(f"❌ Unable to open spreadsheet by key: {e}")
    sys.exit(1)

# ---------------------------
# Helpers
# ---------------------------
def parse_date_hour(date_raw, hour_raw):
    """برگرداندن datetime و ساعت به‌صورت str (0..23)"""
    record_date, hour_val = None, None
    try:
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

        if isinstance(hour_raw, (int, float)):
            if 0 <= int(hour_raw) <= 23:
                hour_val = str(int(hour_raw))
            else:
                base = datetime(1899, 12, 30)
                dt_val = base + timedelta(days=float(hour_raw))
                hour_val = str(dt_val.hour)
        elif isinstance(hour_raw, str) and hour_raw.isdigit():
            hour_val = hour_raw
    except Exception as e:
        # اگر parsing خراب شد، خالی برمی‌گردیم
        hour_val = None
        record_date = None
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
                dt = datetime.strptime(x, fmt)
                return dt.date()
            except:
                continue
    return None

def percent_str(val):
    """یک عدد اعشاری 0..100 را به 'xx.x%' تبدیل می‌کند (اگر None بود خالی)."""
    if val is None:
        return ""
    try:
        return f"{float(val):.1f}%"
    except:
        return ""

def base_task_of(task_type_str: str) -> str:
    t = (task_type_str or "").strip()
    if t.startswith("Pick"):
        return "Pick"
    if t.startswith("Presort"):
        return "Presort"
    return t

# ---------------------------
# Read sheets
# ---------------------------
ws_all = ss.worksheet(ALL_SHEET_NAME)

# هدرهای استاندارد
headers = [
    "full_name", "task_type", "quantity", "date", "hour", "occupied_hours", "order",
    "performance_without_rotation", "performance_with_rotation", "Negative_Minutes",
    "Ipo_Pack", "UserName", "Shift"
]

current = ws_all.get_all_values()
if not current:
    ws_all.append_row(headers)
else:
    if current[0] != headers:
        ws_all.delete_rows(1)
        ws_all.insert_row(headers, 1)

existing = ws_all.get_all_values()[1:]

# کلید کامل برای تمام تسک‌ها
existing_keys_full = set(
    f"{r[0]}||{r[1]}||{r[2]}||{r[3]}||{r[4]}||{r[5]}||{r[6]}"
    for r in existing
)

# کلید فشرده مخصوص Pick/Presort
existing_keys_compact = set()
for r in existing:
    full_name = r[0]
    task_type = r[1]
    dt = r[3]
    hr = r[4]
    base_t = base_task_of(task_type)
    if base_t in ("Pick", "Presort"):
        existing_keys_compact.add(f"{full_name}||{base_t}||{dt}||{hr}")

# KPI config
ws_kpi = ss.worksheet(KPI_SHEET_NAME)
cfg_data = ws_kpi.get_all_values()
cfg_headers = cfg_data[0]
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

def get_kpi(taskType, recordDate):
    lst = [c for c in kpi_configs if c["task_type"] == taskType]
    lst.sort(key=lambda x: x["effective"])
    chosen = None
    for c in lst:
        if recordDate >= c["effective"]:
            chosen = c
        else:
            break
    return chosen

# Other Work → بلاک از تاریخ
blocked_from = {}
try:
    ws_other = ss.worksheet(OTHER_SHEET_NAME)
    other_vals = ws_other.get_all_values()
    if other_vals and len(other_vals) > 1:
        for row in other_vals[1:]:
            name = (row[2] if len(row) > 2 else "").strip()
            start_raw = row[0] if len(row) > 0 else ""
            if not name:
                continue
            sd = parse_date_only(start_raw)
            if not sd:
                continue
            if name in blocked_from:
                if sd < blocked_from[name]:
                    blocked_from[name] = sd
            else:
                blocked_from[name] = sd
except gspread.WorksheetNotFound:
    pass

def is_blocked(full_name: str, rec_dt: datetime) -> bool:
    nm = (full_name or "").strip()
    if nm in blocked_from and rec_dt is not None:
        return rec_dt.date() >= blocked_from[nm]
    return False

# ---------------------------
# جمع‌آوری
# ---------------------------
new_rows = []

# تب‌های ساده
simple_tabs = ["Receive", "Locate", "Sort", "Pack", "Stock taking"]
for tab in simple_tabs:
    try:
        ws = ss.worksheet(tab)
    except gspread.WorksheetNotFound:
        continue
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        continue

    hdr = vals[0]
    idx = {c.strip(): i for i, c in enumerate(hdr)}

    for r in vals[1:]:
        try:
            full_name = r[idx.get("full_name", -1)] if idx.get("full_name", -1) >= 0 else ""
            if not full_name:
                continue

            date_raw = r[idx.get("date", idx.get("Date", -1))]
            hour_raw = r[idx.get("hour", idx.get("Hour", -1))]
            rec_dt, hour = parse_date_hour(date_raw, hour_raw)
            if not rec_dt or hour is None:
                continue

            if is_blocked(full_name, rec_dt):
                continue

            start = r[idx.get("Start", -1)] if idx.get("Start", -1) >= 0 else ""
            end   = r[idx.get("End", -1)] if idx.get("End", -1) >= 0 else ""
            qty   = r[idx.get("Count", idx.get("count", -1))] if (idx.get("Count", -1) >= 0 or idx.get("count", -1) >= 0) else ""
            user  = r[idx.get("username", -1)] if idx.get("username", -1) >= 0 else ""
            order = r[idx.get("count_order", -1)] if "count_order" in idx else ""

            quantity = float(qty) if qty else 0
            fromMin = float(start) if start else 0
            toMin   = float(end) if end else 0
            occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0
            if quantity < 15 or occupied <= 0:
                continue

            if tab == "Receive":
                center = r[idx.get("warehouse_name", idx.get("warehouses_name", -1))] if (idx.get("warehouse_name", -1) >= 0 or idx.get("warehouses_name", -1) >= 0) else ""
                if (center or "").strip() != "مرکز پردازش مهرآباد":
                    continue

            ipo_pack, task_type = "", tab
            order_val = ""
            if tab == "Pack":
                order_val = float(order) if order else ""
                if order_val != "":
                    try:
                        op = float(order_val)
                        ipo_pack = round(quantity / op, 2) if op > 0 else ""
                    except:
                        ipo_pack = ""
                # نوع پَک: Single یا Multi
                if isinstance(ipo_pack, float) and 1.0 <= ipo_pack <= 1.2:
                    task_type = "Pack_Single"
                else:
                    task_type = "Pack_Multi"
            else:
                # برای غیر Pack، ستون order باید خالی بماند
                order_val = ""

            # KPI
            perf_wo, perf_w = "", ""
            cfg = get_kpi(task_type, rec_dt)
            if cfg and quantity > 0 and occupied > 0:
                perf_wo = percent_str((quantity / cfg["base"]) * 100.0)
                perf_w  = percent_str((quantity / (occupied * cfg["rotation"])) * 100.0)

            neg_min = (60 - occupied) if occupied > 0 else ""
            if isinstance(neg_min, (int, float)) and neg_min <= 0:
                neg_min = ""

            # شیفت از username
            shift = "Other"
            if user:
                lw = user.lower()
                if lw.endswith(".s1"):
                    shift = "Shift1"
                elif lw.endswith(".s2"):
                    shift = "Shift2"
                elif lw.endswith(".flex"):
                    shift = "Flex"

            key_full = f"{full_name}||{task_type}||{rec_dt.date()}||{quantity}||{hour}||{occupied}||{order_val}"
            if key_full in existing_keys_full:
                continue
            existing_keys_full.add(key_full)

            new_rows.append([
                full_name, task_type, quantity, rec_dt.strftime("%Y-%m-%d"),  # تاریخ به صورت رشته
                int(hour), occupied, order_val,
                perf_wo, perf_w, neg_min,
                ipo_pack, user, shift
            ])
        except Exception as e:
            # swallow row error
            continue

# Pick & Presort برای تشخیص Large فقط با full_name
pick_rows, presort_rows = [], []
for tab in ["Pick", "Presort"]:
    try:
        ws = ss.worksheet(tab)
    except gspread.WorksheetNotFound:
        continue
    vals = ws.get_all_values()
    if not vals or len(vals) < 2:
        continue

    hdr = vals[0]
    idx = {c.strip(): i for i, c in enumerate(hdr)}

    for r in vals[1:]:
        try:
            full_name = r[idx.get("full_name", -1)] if idx.get("full_name", -1) >= 0 else ""
            if not full_name:
                continue

            date_raw = r[idx.get("date", idx.get("Date", -1))]
            hour_raw = r[idx.get("hour", idx.get("Hour", -1))]
            rec_dt, hour = parse_date_hour(date_raw, hour_raw)
            if not rec_dt or hour is None:
                continue

            if is_blocked(full_name, rec_dt):
                continue

            start = r[idx.get("Start", -1)] if idx.get("Start", -1) >= 0 else ""
            end   = r[idx.get("End", -1)] if idx.get("End", -1) >= 0 else ""
            qty   = r[idx.get("Count", idx.get("count", -1))] if (idx.get("Count", -1) >= 0 or idx.get("count", -1) >= 0) else ""
            user  = r[idx.get("username", -1)] if idx.get("username", -1) >= 0 else ""

            quantity = float(qty) if qty else 0
            fromMin = float(start) if start else 0
            toMin   = float(end) if end else 0
            occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0
            if quantity < 15 or occupied <= 0:
                continue

            base_data = {
                "full_name": full_name,
                "date": rec_dt.strftime("%Y-%m-%d"),
                "hour": hour,
                "quantity": quantity,
                "occupied": occupied,
                "user": user,
                "raw_date": rec_dt
            }
            if tab == "Pick":
                pick_rows.append(base_data)
            else:
                presort_rows.append(base_data)
        except:
            continue

# زوج‌سازی فقط با full_name + date + hour
by_key = {}  # (full_name, date, hour) -> {"pick":..., "presort":...}
def kkey(r): return (r["full_name"], r["date"], r["hour"])
for r in pick_rows:
    by_key.setdefault(kkey(r), {})["pick"] = r
for r in presort_rows:
    by_key.setdefault(kkey(r), {})["presort"] = r

def append_output_line(base_row, task_type):
    # Dedup فشرده برای Pick/Presort
    base_t = "Pick" if task_type.startswith("Pick") else "Presort"
    compact_key = f"{base_row['full_name']}||{base_t}||{base_row['date']}||{base_row['hour']}"
    if compact_key in existing_keys_compact:
        return
    existing_keys_compact.add(compact_key)

    cfg = get_kpi(task_type, base_row["raw_date"])
    perf_wo = perf_w = ""
    if cfg and base_row["quantity"] > 0 and base_row["occupied"] > 0:
        perf_wo = percent_str((base_row["quantity"] / cfg["base"]) * 100.0)
        perf_w  = percent_str((base_row["quantity"] / (base_row["occupied"] * cfg["rotation"])) * 100.0)

    neg_min = (60 - base_row["occupied"]) if base_row["occupied"] > 0 else ""
    if isinstance(neg_min, (int, float)) and neg_min <= 0:
        neg_min = ""

    # شیفت
    shift = "Other"
    if base_row["user"]:
        lw = base_row["user"].lower()
        if lw.endswith(".s1"):
            shift = "Shift1"
        elif lw.endswith(".s2"):
            shift = "Shift2"
        elif lw.endswith(".flex"):
            shift = "Flex"

    new_rows.append([
        base_row["full_name"], task_type, base_row["quantity"], base_row["date"],
        int(base_row["hour"]), base_row["occupied"], "",    # order خالی
        perf_wo, perf_w, neg_min,
        "", base_row["user"], shift
    ])

# اعمال قانون Large
for key, sides in by_key.items():
    p = sides.get("pick")
    s = sides.get("presort")
    if p and s:
        append_output_line(p, "Pick_Larg")
        append_output_line(s, "Presort_Larg")
    elif p:
        append_output_line(p, "Pick")          # تنها Pick
    elif s:
        append_output_line(s, "Presort")       # تنها Presort

# ---------------------------
# write back
# ---------------------------
msg = ""
if new_rows:
    # حتماً تاریخ را به رشته تبدیل کرده‌ایم (JSON-safe)
    ws_all.append_rows(new_rows, value_input_option="RAW")
    msg = f"✅ Added {len(new_rows)} new rows."
else:
    msg = "ℹ️ No new rows to add."

print(msg)
sys.stdout.flush()
sys.exit(0)

