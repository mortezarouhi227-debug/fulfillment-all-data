# All_Data.py
import os, sys, json
from datetime import datetime, timedelta, date
import gspread
from google.oauth2.service_account import Credentials

# =========================
# تنظیمات
# =========================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SPREADSHEET_ID = os.getenv(
    "SPREADSHEET_ID",
    "1VgKCQ8EjVF2sS8rSPdqFZh2h6CuqWAeqSMR56APvwes"
)

# =========================
# اتصال
# =========================
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
    print(f"❌ Unable to open spreadsheet: {e}")
    sys.exit(1)

# =========================
# ابزارها
# =========================
def iso(d):
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    return d

def parse_date_hour(date_raw, hour_raw):
    """خروجی: (record_dt(datetime) یا None, hour_str یا None)"""
    record_dt, hour_val = None, None
    try:
        # تاریخ
        if isinstance(date_raw, (int, float)) and date_raw > 30000:
            base = datetime(1899, 12, 30)
            record_dt = base + timedelta(days=int(date_raw))
        elif isinstance(date_raw, str) and date_raw.strip():
            for fmt in ("%m/%d/%Y", "%B %d, %Y", "%Y-%m-%d"):
                try:
                    record_dt = datetime.strptime(date_raw.strip(), fmt)
                    break
                except:
                    pass

        # ساعت
        if isinstance(hour_raw, (int, float)):
            if 0 <= int(hour_raw) <= 23:
                hour_val = str(int(hour_raw))
            else:
                base = datetime(1899, 12, 30)
                dt_val = base + timedelta(days=float(hour_raw))
                hour_val = str(dt_val.hour)
        elif isinstance(hour_raw, str) and hour_raw.strip().isdigit():
            hour_val = hour_raw.strip()
    except Exception as e:
        print(f"❌ Error parse_date_hour: {e}")
    return record_dt, hour_val

def parse_date_only(x):
    if not x:
        return None
    if isinstance(x, (int, float)) and x > 30000:
        base = datetime(1899, 12, 30)
        return (base + timedelta(days=int(x))).date()
    if isinstance(x, str) and x.strip():
        for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y"):
            try:
                return datetime.strptime(x.strip(), fmt).date()
            except:
                continue
    return None

# درصد به صورت «%»
def pct(v):
    return f"{v:.1f}%" if v is not None else ""

# =========================
# شیت مقصد و Header
# =========================
ws = ss.worksheet("All_Data")
HEADERS = [
    'full_name','task_type','quantity','date','hour','occupied_hours','order',
    'performance_without_rotation','performance_with_rotation',
    'Negative_Minutes','Ipo_Pack','UserName','Shift'
]

curr = ws.get_all_values()
if not curr:
    ws.append_row(HEADERS)
else:
    if curr[0] != HEADERS:
        ws.delete_rows(1)
        ws.insert_row(HEADERS, 1)

existing = ws.get_all_values()[1:]
# کلید کامل برای دی‌داپ کلی
existing_keys_full = set(
    f"{r[0]}||{r[1]}||{r[2]}||{r[3]}||{r[4]}||{r[5]}||{r[6]}"
    for r in existing
)

# کلید فشرده‌ی Pick/Presort بر اساس full_name
def base_task_of(t):
    t = (t or "").strip()
    if t.startswith("Pick"):
        return "Pick"
    if t.startswith("Presort"):
        return "Presort"
    return t

existing_keys_compact = set()
for r in existing:
    fn = r[0] if len(r)>0 else ""
    tt = r[1] if len(r)>1 else ""
    dt = r[3] if len(r)>3 else ""
    hr = r[4] if len(r)>4 else ""
    base_t = base_task_of(tt)
    if base_t in ("Pick","Presort"):
        existing_keys_compact.add(f"{fn}||{base_t}||{dt}||{hr}")

# =========================
# KPI_Config
# =========================
cfg_ws = ss.worksheet("KPI_Config")
cfg_vals = cfg_ws.get_all_values()
cfg_hdr = cfg_vals[0]
kpi_configs = []
for row in cfg_vals[1:]:
    try:
        kpi_configs.append({
            "task_type": row[cfg_hdr.index("task_type")],
            "base": float(row[cfg_hdr.index("base")]),
            "rotation": float(row[cfg_hdr.index("rotation")]),
            "effective": datetime.strptime(row[cfg_hdr.index("effective_from")], "%Y-%m-%d")
        })
    except:
        pass

def getKPI(taskType, record_dt):
    items = [c for c in kpi_configs if c["task_type"] == taskType]
    items.sort(key=lambda x: x["effective"])
    chosen = None
    for c in items:
        if record_dt >= c["effective"]:
            chosen = c
        else:
            break
    return chosen

# =========================
# Other Work → بلوکه از تاریخ A به بعد
# =========================
other = ss.worksheet("Other Work").get_all_values()
blocked_from = {}
if other and len(other) > 1:
    for r in other[1:]:
        name = (r[2] if len(r) > 2 else "").strip()
        start_raw = r[0] if len(r) > 0 else ""
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

def is_blocked(full_name, record_dt):
    nm = (full_name or "").strip()
    if nm in blocked_from and record_dt is not None:
        return record_dt.date() >= blocked_from[nm]
    return False

# =========================
# جمع‌آوری
# =========================
new_rows = []

# تب‌های ساده
simple_tabs = ["Receive","Locate","Sort","Pack","Stock taking"]
for tab in simple_tabs:
    vals = ss.worksheet(tab).get_all_values()
    if not vals or len(vals) < 2:
        continue

    hdr = vals[0]
    idx = {c.strip(): i for i, c in enumerate(hdr)}

    for r in vals[1:]:
        try:
            full_name = r[idx.get("full_name",-1)]
            if not full_name:
                continue

            date_raw = r[idx.get("date", idx.get("Date",-1))]
            hour_raw = r[idx.get("hour", idx.get("Hour",-1))]
            record_dt, hour = parse_date_hour(date_raw, hour_raw)
            if not record_dt or not hour:
                continue

            if is_blocked(full_name, record_dt):
                continue

            start = r[idx.get("Start",-1)]
            end   = r[idx.get("End",-1)]
            qty   = r[idx.get("Count", idx.get("count",-1))]
            user  = r[idx.get("username",-1)]
            order = r[idx.get("count_order",-1)] if "count_order" in idx else ""

            quantity = float(qty) if qty else 0.0
            fromMin  = float(start) if start else 0.0
            toMin    = float(end) if end else 0.0
            occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0.0
            if quantity < 15 or occupied <= 0:
                continue

            if tab == "Receive":
                center = r[idx.get("warehouse_name", idx.get("warehouses_name",-1))]
                if (center or "").strip() != "مرکز پردازش مهرآباد":
                    continue

            ipo_pack, task_type = "", tab
            if tab == "Pack":
                ord_val = float(order) if order else 0.0
                if ord_val > 0:
                    ipo_pack = round(quantity / ord_val, 2)
                task_type = "Pack_Single" if (ipo_pack and 1 <= ipo_pack <= 1.2) else "Pack_Multi"

            # دی‌داپ کلّی
            key_full = f"{full_name}||{task_type}||{quantity}||{record_dt.date()}||{hour}||{int(occupied)}||{order or 0}"
            if key_full in existing_keys_full:
                continue
            existing_keys_full.add(key_full)

            perf_wo = perf_w = None
            cfg = getKPI(task_type, record_dt)
            if cfg and quantity > 0 and occupied > 0:
                perf_wo = (quantity / cfg["base"]) * 100.0
                perf_w  = (quantity / (occupied * cfg["rotation"])) * 100.0

            neg_min = (60 - occupied) if occupied > 0 else ""
            if isinstance(neg_min, (int, float)) and neg_min <= 0:
                neg_min = ""

            # شیفت
            shift = "Other"
            if user:
                low = user.lower()
                if low.endswith(".s1"):
                    shift = "Shift1"
                elif low.endswith(".s2"):
                    shift = "Shift2"
                elif low.endswith(".flex"):
                    shift = "Flex"

            new_rows.append([
                full_name, task_type, quantity, iso(record_dt), hour, int(occupied),
                float(order) if str(order).strip() != "" else 0.0,
                pct(perf_wo), pct(perf_w),
                neg_min, ipo_pack, user, shift
            ])
        except Exception as e:
            print(f"❌ Error in {tab}: {e}")
            continue

# Pick & Presort
def collect_pp(tab):
    out = []
    vals = ss.worksheet(tab).get_all_values()
    if not vals or len(vals) < 2:
        return out
    hdr = vals[0]
    idx = {c.strip(): i for i, c in enumerate(hdr)}
    for r in vals[1:]:
        try:
            full_name = r[idx.get("full_name",-1)]
            if not full_name:
                continue
            date_raw = r[idx.get("date", idx.get("Date",-1))]
            hour_raw = r[idx.get("hour", idx.get("Hour",-1))]
            record_dt, hour = parse_date_hour(date_raw, hour_raw)
            if not record_dt or not hour:
                continue
            if is_blocked(full_name, record_dt):
                continue
            start = r[idx.get("Start",-1)]
            end   = r[idx.get("End",-1)]
            qty   = r[idx.get("Count", idx.get("count",-1))]
            user  = r[idx.get("username",-1)]

            quantity = float(qty) if qty else 0.0
            fromMin  = float(start) if start else 0.0
            toMin    = float(end) if end else 0.0
            occupied = (toMin - fromMin + 1) if (toMin - fromMin) > 0 else 0.0
            if quantity < 15 or occupied <= 0:
                continue

            out.append({
                "full_name": full_name,
                "date_dt": record_dt,          # datetime
                "date": iso(record_dt),        # 'YYYY-MM-DD'
                "hour": hour,
                "quantity": quantity,
                "occupied": int(occupied),
                "user": user
            })
        except Exception as e:
            print(f"❌ Error in {tab}: {e}")
            continue
    return out

pick_rows    = collect_pp("Pick")
presort_rows = collect_pp("Presort")

# زوج‌ها فقط بر اساس full_name + date + hour
pairs = {}
def key_tuple(x): return (x["full_name"], x["date"], x["hour"])

for r in pick_rows:
    pairs.setdefault(key_tuple(r), {})["pick"] = r
for r in presort_rows:
    pairs.setdefault(key_tuple(r), {})["presort"] = r

def append_output_line(base_row, task_type):
    base_t = "Pick" if task_type.startswith("Pick") else "Presort"
    compact_key = f"{base_row['full_name']}||{base_t}||{base_row['date']}||{base_row['hour']}"
    if compact_key in existing_keys_compact:
        return
    existing_keys_compact.add(compact_key)

    cfg = getKPI(task_type, base_row["date_dt"])
    perf_wo = perf_w = None
    if cfg and base_row["quantity"] > 0 and base_row["occupied"] > 0:
        perf_wo = (base_row["quantity"] / cfg["base"]) * 100.0
        perf_w  = (base_row["quantity"] / (base_row["occupied"] * cfg["rotation"])) * 100.0

    neg_min = (60 - base_row["occupied"]) if base_row["occupied"] > 0 else ""
    if isinstance(neg_min, (int, float)) and neg_min <= 0:
        neg_min = ""

    # شیفت از username
    shift = "Other"
    if base_row["user"]:
        low = base_row["user"].lower()
        if low.endswith(".s1"):   shift = "Shift1"
        elif low.endswith(".s2"): shift = "Shift2"
        elif low.endswith(".flex"): shift = "Flex"

    new_rows.append([
        base_row["full_name"], task_type, base_row["quantity"], base_row["date"],
        base_row["hour"], base_row["occupied"], 0.0,
        pct(perf_wo), pct(perf_w),
        neg_min, 0.0, base_row["user"], shift
    ])

# تصمیم نهایی:
for (_, _, _), sides in pairs.items():
    p = sides.get("pick")
    s = sides.get("presort")
    if p and s:
        append_output_line(p, "Pick_Larg")
        append_output_line(s, "Presort_Larg")
    elif p:
        append_output_line(p, "Pick")
    elif s:
        append_output_line(s, "Presort")

# =========================
# درج در شیت
# =========================
if new_rows:
    ws.append_rows(new_rows, value_input_option="USER_ENTERED")
    print(f"✅ Added {len(new_rows)} new rows.")
else:
    print("ℹ️ No new rows to add.")

sys.exit(0)

