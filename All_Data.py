# All_Data.py
import os, json, sys
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

# ---------------------------
# تنظیمات پایه
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
# اتصال به Google Sheets
# ---------------------------
def make_client():
    try:
        env_creds = os.getenv("GOOGLE_CREDENTIALS")
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

client = make_client()

try:
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    print(f"✅ Opened spreadsheet {SPREADSHEET_ID}.")
except Exception as e:
    print(f"❌ Unable to open spreadsheet by key: {e}")
    sys.exit(1)

# ---------------------------
# تب مقصد (All_Data) و هدرها
# ---------------------------
target_sheet = spreadsheet.worksheet("All_Data")
headers = [
    "full_name", "task_type", "quantity", "date", "hour", "occupied_hours",
    "order", "performance_without_rotation", "performance_with_rotation",
    "Negative_Minutes", "Ipo_Pack", "UserName", "Shift"
]

current_values = target_sheet.get_all_values()
if not current_values:
    target_sheet.append_row(headers)
else:
    if current_values[0] != headers:
        # اگر هدر فرق دارد، ردیف اول را هدر استاندارد می‌کنیم
        target_sheet.delete_rows(1)
        target_sheet.insert_row(headers, 1)

# کلیدهای موجود برای جلوگیری از تکرار (نسخه کامل)
existing = target_sheet.get_all_values()[1:]
existing_keys_full = set(
    f"{r[0]}||{r[1]}||{r[2]}||{r[3]}||{r[4]}||{r[5]}||{r[6]}"
    for r in existing
)

# کلیدهای فشرده مخصوص Pick/Presort: (full_name, base_task, date, hour)
def base_task_of(task_type_str: str) -> str:
    t = (task_type_str or "").strip()
    if t.startswith("Pick"):
        return "Pick"
    if t.startswith("Presort"):
        return "Presort"
    return t

existing_keys_compact = set()
for r in existing:
    full_name = r[0] if len(r) > 0 else ""
    task_type = r[1] if len(r) > 1 else ""
    dt        = r[3] if len(r) > 3 else ""
    hr        = r[4] if len(r) > 4 else ""
    base_t    = base_task_of(task_type)
    if base_t in ("Pick", "Presort"):
        existing_keys_compact.add(f"{full_name}||{base_t}||{dt}||{hr}")

# ---------------------------
# KPI Config
# ---------------------------
cfg_sheet = spreadsheet.worksheet("KPI_Config")
cfg_data = cfg_sheet.get_all_values()
cfg_headers = cfg_data[0] if cfg_data else []

kpi_configs = []
if cfg_data and len(cfg_data) > 1:
    for row in cfg_data[1:]:
        try:
            kpi_configs.append({
                "task_type": row[cfg_headers.index("task_type")],
                "base": float(row[cfg_headers.index("base")]),
                "rotation": float(row[cfg_headers.index("rotation")]),
                "effective": datetime.strptime(row[cfg_headers.index("effective_from")], "%Y-%m-%d"),
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
# توابع تبدیل تاریخ/ساعت
# ---------------------------
def parse_date_hour(date_raw, hour_raw):
    """برگشت: (datetime یا None, hour(str یا None))"""
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
                except Exception:
                    pass

        # ساعت
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
# Other Work (حذف از تاریخ مشخص به بعد)
# ---------------------------
other_sheet = spreadsheet.worksheet("Other Work")
other_values = other_sheet.get_all_values()

blocked_from = {}  # name -> date (datetime.date)
if other_values and len(other_values) > 1:
    for row in other_values[1:]:
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
# منطق پردازش تب‌های ساده
# ---------------------------
new_rows = []
source_sheets = ["Receive", "Locate", "Sort", "Pack", "Stock taking"]

for sheet_name in source_sheets:
    sheet = spreadsheet.worksheet(sheet_name)
    values = sheet.get_all_values()
    if not values or len(values) < 2:
        continue

    headers_map = values[0]
    header_index = {col.strip(): idx for idx, col in enumerate(headers_map)}

    for row in values[1:]:
        try:
            full_name = (row[header_index.get("full_name", -1)] or "").strip()
            if not full_name:
                continue

            date_raw = row[header_index.get("date", header_index.get("Date", -1))]
            hour_raw = row[header_index.get("hour", header_index.get("Hour", -1))]
            record_date, hour = parse_date_hour(date_raw, hour_raw)
            if not record_date or hour is None:
                continue

            if is_blocked(full_name, record_date):
                continue

            start = row[header_index.get("Start", -1)]
            end   = row[header_index.get("End", -1)]
            qty   = row[header_index.get("Count", header_index.get("count", -1))]
            user  = row[header_index.get("username", -1)]
            order = row[header_index.get("count_order", -1)] if "count_order" in header_index else ""

            quantity = float(qty) if qty else 0.0
            fromMin  = float(start) if start else 0.0
            toMin    = float(end)   if end   else 0.0
            occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0.0
            if quantity < 15 or occupied <= 0:
                continue

            # فیلتر مرکز در Receive
            if sheet_name == "Receive":
                center = row[header_index.get("warehouse_name", header_index.get("warehouses_name", -1))]
                if (center or "").strip() != "مرکز پردازش مهرآباد":
                    continue

            ipo_pack, task_type = "", sheet_name
            if sheet_name == "Pack":
                try:
                    order_val = float(order) if order else 0.0
                except Exception:
                    order_val = 0.0
                if order_val > 0:
                    ipo_pack = round(quantity / order_val, 2)
                task_type = "Pack_Single" if (ipo_pack and 1 <= ipo_pack <= 1.2) else "Pack_Multi"

            # کلید دی‌داپ کامل
            key_full = f"{full_name}||{task_type}||{record_date.date()}||{quantity}||{int(hour)}||{int(occupied)}||{order}"
            if key_full in existing_keys_full:
                continue
            existing_keys_full.add(key_full)

            # Performance به‌صورت درصد متن
            perf_without, perf_with = "", ""
            cfg = getKPI(task_type, record_date)
            if cfg and quantity > 0 and occupied > 0:
                perf_without = f"{(quantity / cfg['base']) * 100:.1f}%"
                perf_with    = f"{(quantity / (occupied * cfg['rotation'])) * 100:.1f}%"

            neg_min = int(60 - occupied) if occupied > 0 else ""
            if isinstance(neg_min, int) and neg_min <= 0:
                neg_min = ""

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
                full_name, task_type, quantity, record_date.strftime("%Y-%m-%d"),
                int(hour), int(occupied), order, perf_without, perf_with, neg_min,
                ipo_pack, user, shift
            ])

        except Exception as e:
            print(f"❌ Error in {sheet_name}: {e}")
            continue

# ---------------------------
# پردازش Pick و Presort → فقط بر اساس full_name جفت‌سازی
# ---------------------------
pick_rows, presort_rows = [], []

for sheet_name in ["Pick", "Presort"]:
    sheet = spreadsheet.worksheet(sheet_name)
    values = sheet.get_all_values()
    if not values or len(values) < 2:
        continue

    headers_map = values[0]
    header_index = {col.strip(): idx for idx, col in enumerate(headers_map)}

    for row in values[1:]:
        try:
            full_name = (row[header_index.get("full_name", -1)] or "").strip()
            if not full_name:
                continue

            date_raw = row[header_index.get("date", header_index.get("Date", -1))]
            hour_raw = row[header_index.get("hour", header_index.get("Hour", -1))]
            record_date, hour = parse_date_hour(date_raw, hour_raw)
            if not record_date or hour is None:
                continue

            if is_blocked(full_name, record_date):
                continue

            start = row[header_index.get("Start", -1)]
            end   = row[header_index.get("End", -1)]
            qty   = row[header_index.get("Count", header_index.get("count", -1))]
            user  = row[header_index.get("username", -1)]

            quantity = float(qty) if qty else 0.0
            fromMin  = float(start) if start else 0.0
            toMin    = float(end)   if end   else 0.0
            occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0.0
            if quantity < 15 or occupied <= 0:
                continue

            base_data = {
                "full_name": full_name,
                "date": record_date.strftime("%Y-%m-%d"),
                "hour": int(hour),
                "quantity": quantity,
                "occupied": int(occupied),
                "user": user,
                "raw_date": record_date
            }

            if sheet_name == "Pick":
                pick_rows.append(base_data)
            else:
                presort_rows.append(base_data)

        except Exception as e:
            print(f"❌ Error in {sheet_name}: {e}")
            continue

# جفت‌سازی بر اساس full_name + date + hour
def kkey(r):
    return (r["full_name"], r["date"], r["hour"])

pairs = {}
for r in pick_rows:
    pairs.setdefault(kkey(r), {})["pick"] = r
for r in presort_rows:
    pairs.setdefault(kkey(r), {})["presort"] = r

def append_output_line(base_row, task_type):
    base_t = "Pick" if task_type.startswith("Pick") else "Presort"
    compact_key = f"{base_row['full_name']}||{base_t}||{base_row['date']}||{base_row['hour']}"
    if compact_key in existing_keys_compact:
        return
    existing_keys_compact.add(compact_key)

    perf_without, perf_with = "", ""
    cfg = getKPI(task_type, base_row["raw_date"])
    if cfg and base_row["quantity"] > 0 and base_row["occupied"] > 0:
        perf_without = f"{(base_row['quantity'] / cfg['base']) * 100:.1f}%"
        perf_with    = f"{(base_row['quantity'] / (base_row['occupied'] * cfg['rotation'])) * 100:.1f}%"

    neg_min = int(60 - base_row["occupied"]) if base_row["occupied"] > 0 else ""
    if isinstance(neg_min, int) and neg_min <= 0:
        neg_min = ""

    shift = "Other"
    if base_row.get("user"):
        lower = base_row["user"].lower()
        if lower.endswith(".s1"):
            shift = "Shift1"
        elif lower.endswith(".s2"):
            shift = "Shift2"
        elif lower.endswith(".flex"):
            shift = "Flex"

    new_rows.append([
        base_row["full_name"], task_type, base_row["quantity"], base_row["date"],
        base_row["hour"], base_row["occupied"], "", perf_without, perf_with, neg_min,
        "", base_row.get("user", ""), shift
    ])

# اگر هر دو وجود دارند ⇒ هر دو Large؛ وگرنه همان Large
for key, sides in pairs.items():
    p = sides.get("pick")
    s = sides.get("presort")
    if p and s:
        append_output_line(p, "Pick_Larg")
        append_output_line(s, "Presort_Larg")
    elif p:
        append_output_line(p, "Pick_Larg")
    elif s:
        append_output_line(s, "Presort_Larg")

# ---------------------------
# اضافه کردن به All_Data (فقط موارد جدید)
# ---------------------------
if new_rows:
    target_sheet.append_rows(new_rows)
    print(f"✅ Added {len(new_rows)} new rows.")
else:
    print("ℹ️ No new rows to add.")

sys.exit(0)

