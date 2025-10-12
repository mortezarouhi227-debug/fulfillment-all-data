import os, json, sys
from datetime import datetime, timedelta, date

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
# اتصال به گوگل شیت
# ---------------------------
def make_client():
    try:
        env_creds = os.getenv("GOOGLE_CREDENTIALS")
        if env_creds:
            creds = Credentials.from_service_account_info(json.loads(env_creds), scopes=SCOPES)
            print("Auth via GOOGLE_CREDENTIALS (ENV).")
        else:
            creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
            print("Auth via credentials.json (FILE).")
        return gspread.authorize(creds)
    except Exception as e:
        print(f"❌ Auth error: {e}")
        sys.exit(1)

client = make_client()
try:
    ss = client.open_by_key(SPREADSHEET_ID)
    print(f"✅ Opened spreadsheet {SPREADSHEET_ID}.")
except Exception as e:
    print(f"❌ Unable to open spreadsheet by key: {e}")
    sys.exit(1)

# ---------------------------
# تب مقصد و هدرها
# ---------------------------
ws = ss.worksheet("All_Data")
HEADERS = [
    'full_name', 'task_type', 'quantity', 'date', 'hour', 'occupied_hours', 'order',
    'performance_without_rotation', 'performance_with_rotation', 'Negative_Minutes',
    'Ipo_Pack', 'UserName', 'Shift'
]

vals = ws.get_all_values()
if not vals:
    ws.append_row(HEADERS)
elif vals[0] != HEADERS:
    ws.delete_rows(1)
    ws.insert_row(HEADERS, 1)

# --- کلیدهای موجود برای جلوگیری از تکرار (نسخه کامل) ---
existing = ws.get_all_values()[1:]
existing_keys_full = set(
    f"{r[0]}||{r[1]}||{r[2]}||{r[3]}||{r[4]}||{r[5]}||{r[6]}"
    for r in existing
)

# --- کلیدهای فشرده برای Pick/Presort: (full_name, base_task, date, hour) ---
def base_task_of(task_type_str: str) -> str:
    t = (task_type_str or "").strip()
    if t.startswith("Pick"):
        return "Pick"
    if t.startswith("Presort"):
        return "Presort"
    return t

existing_keys_compact = set()
for r in existing:
    full_name = r[0]
    task_type = r[1]
    dt = r[3]
    hr = r[4]
    base_t = base_task_of(task_type)
    if base_t in ("Pick", "Presort"):
        existing_keys_compact.add(f"{full_name}||{base_t}||{dt}||{hr}")

# ---------------------------
# KPI_Config
# ---------------------------
cfg_ws = ss.worksheet("KPI_Config")
cfg_data = cfg_ws.get_all_values()
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
    except Exception:
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
# پارس تاریخ/ساعت
# ---------------------------
def parse_date_hour(date_raw, hour_raw):
    record_date, hour_val = None, None
    try:
        # تاریخ
        if isinstance(date_raw, (int, float)) and date_raw > 30000:
            base = datetime(1899, 12, 30)
            record_date = (base + timedelta(days=int(date_raw)))
        elif isinstance(date_raw, str) and date_raw:
            for fmt in ("%m/%d/%Y", "%B %d, %Y", "%Y-%m-%d"):
                try:
                    record_date = datetime.strptime(date_raw, fmt)
                    break
                except Exception:
                    pass

        # ساعت
        if isinstance(hour_raw, (int, float)):
            if 0 <= int(hour_raw) <= 23:
                hour_val = int(hour_raw)
            else:
                base = datetime(1899, 12, 30)
                dt_val = base + timedelta(days=float(hour_raw))
                hour_val = dt_val.hour
        elif isinstance(hour_raw, str) and hour_raw.isdigit():
            hour_val = int(hour_raw)
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
                dt = datetime.strptime(x, fmt)
                return dt.date()
            except Exception:
                continue
    return None

# ---------------------------
# Other Work: بلاک از تاریخ A به بعد
# ---------------------------
other_ws = ss.worksheet("Other Work")
other_vals = other_ws.get_all_values()
blocked_from = {}  # name -> date

if other_vals and len(other_vals) > 1:
    for row in other_vals[1:]:
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
# پردازش تب‌های ساده
# ---------------------------
new_rows = []

simple_sheets = ["Receive", "Locate", "Sort", "Pack", "Stock taking"]
for sheet_name in simple_sheets:
    s = ss.worksheet(sheet_name)
    v = s.get_all_values()
    if not v or len(v) < 2:
        continue

    hdr = v[0]
    idx = {col.strip(): i for i, col in enumerate(hdr)}

    for row in v[1:]:
        try:
            full_name = row[idx.get("full_name", -1)]
            if not full_name:
                continue

            date_raw = row[idx.get("date", idx.get("Date", -1))]
            hour_raw = row[idx.get("hour", idx.get("Hour", -1))]
            record_dt, hour = parse_date_hour(date_raw, hour_raw)
            if not record_dt or hour is None:
                continue

            if is_blocked(full_name, record_dt):
                continue

            start = row[idx.get("Start", -1)]
            end = row[idx.get("End", -1)]
            qty = row[idx.get("Count", idx.get("count", -1))]
            user = row[idx.get("username", -1)]
            order = row[idx.get("count_order", -1)] if "count_order" in idx else ""

            quantity = float(qty) if qty else 0.0
            fromMin = float(start) if start else 0.0
            toMin = float(end) if end else 0.0
            occupied = int(toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0
            if quantity < 15 or occupied <= 0:
                continue

            if sheet_name == "Receive":
                center = row[idx.get("warehouse_name", idx.get("warehouses_name", -1))]
                if (center or "").strip() != "مرکز پردازش مهرآباد":
                    continue

            ipo_pack, task_type = 0.0, sheet_name
            if sheet_name == "Pack":
                order_val = float(order) if order else 0.0
                if order_val > 0:
                    ipo_pack = round(quantity / order_val, 2)
                task_type = "Pack_Single" if (1.0 <= ipo_pack <= 1.2) else "Pack_Multi"

            # دی‌داپ کامل
            k_full = f"{full_name}||{task_type}||{record_dt.date()}||{quantity}||{hour}||{occupied}||{order}"
            if k_full in existing_keys_full:
                continue
            existing_keys_full.add(k_full)

            # KPI
            perf_wo, perf_w = None, None
            cfg = getKPI(task_type, record_dt)
            if cfg and quantity > 0 and occupied > 0:
                # برای نمایش درصد، مقدار نسبت را می‌نویسیم (نه ضربدر 100)
                perf_wo = (quantity / cfg["base"])
                perf_w = (quantity / (occupied * cfg["rotation"])) if cfg["rotation"] > 0 else None

            neg_min = max(0, 60 - occupied)

            shift = "Other"
            if user:
                lower = user.lower()
                if lower.endswith(".s1"):
                    shift = "Shift1"
                elif lower.endswith(".s2"):
                    shift = "Shift2"
                elif lower.endswith(".flex"):
                    shift = "Flex"

            new_rows.append([
                full_name, task_type, quantity, record_dt.date(), hour, occupied,
                float(order) if str(order).strip() != "" else 0.0,
                perf_wo if perf_wo is not None else "",  # درصد به صورت نسبت
                perf_w if perf_w is not None else "",     # درصد به صورت نسبت
                neg_min, ipo_pack, user, shift
            ])
        except Exception as e:
            print(f"❌ Error in {sheet_name}: {e}")
            continue

# ---------------------------
# Pick & Presort: جفت‌سازی فقط با full_name + date + hour
# ---------------------------
pick_rows, presort_rows = [], []

for sheet_name in ["Pick", "Presort"]:
    s = ss.worksheet(sheet_name)
    v = s.get_all_values()
    if not v or len(v) < 2:
        continue

    hdr = v[0]
    idx = {col.strip(): i for i, col in enumerate(hdr)}

    for row in v[1:]:
        try:
            full_name = row[idx.get("full_name", -1)]
            if not full_name:
                continue

            date_raw = row[idx.get("date", idx.get("Date", -1))]
            hour_raw = row[idx.get("hour", idx.get("Hour", -1))]
            record_dt, hour = parse_date_hour(date_raw, hour_raw)
            if not record_dt or hour is None:
                continue

            if is_blocked(full_name, record_dt):
                continue

            start = row[idx.get("Start", -1)]
            end = row[idx.get("End", -1)]
            qty = row[idx.get("Count", idx.get("count", -1))]
            user = row[idx.get("username", -1)]

            quantity = float(qty) if qty else 0.0
            fromMin = float(start) if start else 0.0
            toMin = float(end) if end else 0.0
            occupied = int(toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0
            if quantity < 15 or occupied <= 0:
                continue

            base_data = {
                "full_name": full_name,
                "date": record_dt.date(),
                "hour": hour,
                "quantity": quantity,
                "occupied": occupied,
                "user": user,
                "raw_date": record_dt
            }
            if sheet_name == "Pick":
                pick_rows.append(base_data)
            else:
                presort_rows.append(base_data)
        except Exception as e:
            print(f"❌ Error in {sheet_name}: {e}")
            continue

def compact_key(full_name, base_task, d, h):
    return f"{full_name}||{base_task}||{d}||{h}"

def append_output_line(base_row, task_type):
    # دی‌داپ فشرده برای Pick/Presort (بر اساس full_name + base_task + date + hour)
    base_t = "Pick" if task_type.startswith("Pick") else "Presort"
    ckey = compact_key(base_row["full_name"], base_t, base_row["date"], base_row["hour"])
    if ckey in existing_keys_compact:
        return
    existing_keys_compact.add(ckey)

    perf_wo, perf_w = None, None
    cfg = getKPI(task_type, base_row["raw_date"])
    if cfg and base_row["quantity"] > 0 and base_row["occupied"] > 0:
        perf_wo = (base_row["quantity"] / cfg["base"])
        denom = base_row["occupied"] * cfg["rotation"]
        perf_w = (base_row["quantity"] / denom) if denom > 0 else None

    neg_min = max(0, 60 - base_row["occupied"])

    shift = "Other"
    if base_row["user"]:
        lower = base_row["user"].lower()
        if lower.endswith(".s1"):
            shift = "Shift1"
        elif lower.endswith(".s2"):
            shift = "Shift2"
        elif lower.endswith(".flex"):
            shift = "Flex"

    new_rows.append([
        base_row["full_name"], task_type, base_row["quantity"], base_row["date"],
        base_row["hour"], base_row["occupied"], 0.0,
        perf_wo if perf_wo is not None else "",
        perf_w if perf_w is not None else "",
        neg_min, 0.0, base_row["user"], shift
    ])

# جفت‌سازی با full_name + date + hour
pairs = {}
def kkey(r): return (r["full_name"], r["date"], r["hour"])
for r in pick_rows:
    pairs.setdefault(kkey(r), {})["pick"] = r
for r in presort_rows:
    pairs.setdefault(kkey(r), {})["presort"] = r

# فقط اگر هر دو باشند Large؛ وگرنه همان عنوان عادی
for key, sides in pairs.items():
    p = sides.get("pick")
    s = sides.get("presort")
    if p and s:
        append_output_line(p, "Pick_Larg")
        append_output_line(s, "Presort_Larg")
    elif p:
        append_output_line(p, "Pick")
    elif s:
        append_output_line(s, "Presort")

# ---------------------------
# درج ردیف‌های جدید
# ---------------------------
if new_rows:
    ws.append_rows(new_rows, value_input_option="RAW")
    print(f"✅ Added {len(new_rows)} new rows.")
else:
    print("ℹ️ No new rows to add.")

# ---------------------------
# فرمت‌دهی ستون‌ها (تاریخ/عدد/درصد)
# ---------------------------
def format_all_data(worksheet: gspread.Worksheet):
    # آخرین ردیف واقعی
    all_vals = worksheet.get_all_values()
    last_row = max(2, len(all_vals))  # حداقل تا ردیف 2

    def rng(col):
        return f"{col}2:{col}{last_row}"

    body = {
        "requests": [
            # تاریخ
            {
                "repeatCell": {
                    "range": {"sheetId": worksheet.id, "startRowIndex": 1, "endRowIndex": last_row, "startColumnIndex": 3, "endColumnIndex": 4},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            },
            # اعداد ساده: C, E, F, G, J
            *[
                {
                    "repeatCell": {
                        "range": {"sheetId": worksheet.id,
                                  "startRowIndex": 1, "endRowIndex": last_row,
                                  "startColumnIndex": c, "endColumnIndex": c+1},
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                }
                for c in [2, 4, 5, 6, 9]  # 0-based indices: C=2, E=4, F=5, G=6, J=9
            ],
            # درصد با یک رقم اعشار: H, I
            *[
                {
                    "repeatCell": {
                        "range": {"sheetId": worksheet.id,
                                  "startRowIndex": 1, "endRowIndex": last_row,
                                  "startColumnIndex": c, "endColumnIndex": c+1},
                        "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                        "fields": "userEnteredFormat.numberFormat"
                    }
                }
                for c in [7, 8]  # H=7, I=8
            ],
            # Ipo_Pack عدد با دو رقم اعشار: K
            {
                "repeatCell": {
                    "range": {"sheetId": worksheet.id, "startRowIndex": 1, "endRowIndex": last_row, "startColumnIndex": 10, "endColumnIndex": 11},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            }
        ]
    }
    worksheet.spreadsheet.batch_update(body)

format_all_data(ws)
print("✅ Formatting applied.")

sys.exit(0)
