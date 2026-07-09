# -*- coding: utf-8 -*-
import os, json, math
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials

# =========================
# تنظیمات
# =========================
SPREADSHEET_ID = "1VgKCQ8EjVF2sS8rSPdqFZh2h6CuqWAeqSMR56APvwes"
SOURCE_SHEET = "All_Data"
TARGET_SHEET = "Hourly_Performance"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ====== تغییر: حذف Pick_Larg و Presort_Larg، اضافه کردن FBM ======
TASK_TYPES = [
    "Receive","Locate","Sort","Pack_Multi","Pack_Single",
    "Pick","Presort","Stock taking","FBM",
]

# =========================
# اتصال
# =========================
def _client():
    if "GOOGLE_CREDENTIALS" in os.environ:
        creds = Credentials.from_service_account_info(
            json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES
        )
    else:
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    return gspread.authorize(creds)

# =========================
# تاریخ
# =========================
def serial_to_datetime(n):
    base = datetime(1899, 12, 30)
    return base + timedelta(days=float(n))

def parse_date_floor_ms(v):
    if v in (None, ""): return float("nan")
    # Excel serial
    try:
        f = float(v)
        dt = serial_to_datetime(f)
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt.timestamp()*1000
    except:
        pass
    # ← اصلاح: اول ISO، بعد m/d/Y، بعد d/m/Y
    s = str(v).strip()
    for fmt in ("%Y-%m-%d","%Y/%m/%d","%m/%d/%Y","%d/%m/%Y","%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            return dt.timestamp()*1000
        except:
            continue
    try:
        dt = datetime.fromisoformat(s)
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt.timestamp()*1000
    except:
        return float("nan")

def day_start_ms(v): return parse_date_floor_ms(v)
def day_end_ms(v):
    ms = parse_date_floor_ms(v)
    return ms if math.isnan(ms) else ms + (24*60*60*1000 - 1)

# =========================
# تبدیل A1 / ستون
# =========================
def a1(col_idx, row_idx):
    s, c = "", col_idx
    while c:
        c, r = divmod(c - 1, 26)
        s = chr(65 + r) + s
    return f"{s}{row_idx}"

def col_to_a(col_idx):
    s, c = "", col_idx
    while c:
        c, r = divmod(c - 1, 26)
        s = chr(65 + r) + s
    return s

# =========================
# نرمال‌سازی اعداد/درصد با ارقام فارسی/عربی
# =========================
PERSIAN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
ARABIC_INDIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

def normalize_digits(s: str) -> str:
    return s.translate(PERSIAN_DIGITS).translate(ARABIC_INDIC_DIGITS)

def to_number_locale(x, default=0.0):
    if x in (None, ""): return default
    s = normalize_digits(str(x)).strip().replace("\u00a0", " ")
    s = s.replace("%", "").replace("٪", "").strip()
    s = s.replace(" ", "")
    s = s.replace("٫", ".")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        if "," in s:
            parts = s.split(",")
            if len(parts[-1]) in (1,2):
                s = s.replace(",", ".")
            else:
                s = s.replace(",", "")
    try:
        return float(s)
    except:
        return default

def to_percent_locale(x, default=0.0):
    val = to_number_locale(x, default=None)
    if val is None:
        return default
    if 0 <= val <= 1:
        return val
    if 1 < val <= 1000:
        return val / 100.0
    return default

def _to_int_hour(x):
    if x in (None, ""): return None
    try:
        return int(float(normalize_digits(str(x)).strip()))
    except:
        return None

# =========================
# بدنه اصلی
# =========================
def build_hourly_performance():
    gc = _client()
    ss = gc.open_by_key(SPREADSHEET_ID)
    source_ws = ss.worksheet(SOURCE_SHEET)
    target_ws = ss.worksheet(TARGET_SHEET)

    # فیلترها
    b1 = target_ws.acell("B1").value
    f1 = target_ws.acell("F1").value
    j1 = target_ws.acell("J1").value
    selected_hour = _to_int_hour(f1)
    selected_shift = (str(j1).strip() if j1 not in (None, "") else None)

    # داده‌ها
    values = source_ws.get_all_values()
    if len(values) < 2:
        target_ws.update(range_name="A4", values=[["⚠️ All_Data خالی است."]])
        return
    headers, rows = values[0], values[1:]

    def idx(names):
        for n in names:
            if n in headers: return headers.index(n)
        return -1

    colFullName   = idx(["full_name","Full_Name","FULL_NAME"])
    colHour       = idx(["hour","Hour","HOUR"])
    colQuantity   = idx(["quantity","Quantity","QUANTITY"])
    colOccupied   = idx(["occupied_hours","Occupied_Hours","OCCUPIED_HOURS"])
    colPerfNoRot  = idx(["performance_without_rotation"])
    colPerfWith   = idx(["performance_with_rotation"])
    colTaskType   = idx(["task_type","Task_Type","TASK_TYPE"])
    colDate       = idx(["date","Date","DATE"])
    colShift      = idx(["Shift","shift","SHIFT"])

    need = [colFullName,colHour,colQuantity,colOccupied,colPerfNoRot,colPerfWith,colTaskType,colDate,colShift]
    if any(i<0 for i in need):
        target_ws.update(range_name="A4", values=[["⚠️ ستون‌های لازم در All_Data یافت نشد."]])
        return

    # تاریخ هدف
    start_ms = day_start_ms(b1); end_ms = day_end_ms(b1)
    if math.isnan(start_ms):
        dms = [day_start_ms(r[colDate]) for r in rows if not math.isnan(day_start_ms(r[colDate]))]
        if not dms:
            target_ws.update(range_name="A4", values=[["⚠️ تاریخ معتبر در All_Data نیست."]])
            return
        latest = max(dms)
        start_ms = latest
        end_ms = latest + (24*60*60*1000 - 1)

    # ← نرمال‌سازی همیشگی B1 به ISO
    target_ws.update(range_name="B1", values=[[datetime.utcfromtimestamp(start_ms/1000).date().isoformat()]])

    # پاکسازی خروجی از ردیف 3+
    vals = target_ws.get_all_values()
    if len(vals) >= 3:
        last_col = max(1, target_ws.col_count)
        rng = f"A3:{col_to_a(last_col)}{max(3, len(vals))}"
        target_ws.batch_clear([rng])

    # هدر بلوکی
    def build_header_row(task_types):
        hdr = []
        for i,t in enumerate(task_types):
            hdr += [f"{t}_full_name", f"{t}_hour", f"{t}_quantity",
                    f"{t}_occupied_hours", f"{t}_Negative_Minutes",
                    f"{t}_performance_without_rotation", f"{t}_performance_with_rotation"]
            if i < len(task_types)-1: hdr.append("")
        return hdr

    header_row = build_header_row(TASK_TYPES)
    target_ws.update(values=[header_row], range_name=f"A3:{a1(len(header_row),3)}")

    # DataValidation و برچسب‌ها
    ss.batch_update({
        "requests": [
            {
                "setDataValidation": {
                    "range": {"sheetId": target_ws.id, "startRowIndex":0,"endRowIndex":1,"startColumnIndex":5,"endColumnIndex":6},
                    "rule": {
                        "condition":{"type":"ONE_OF_LIST","values":[{"userEnteredValue":str(i)} for i in range(24)]},
                        "strict": False, "showCustomUi": True
                    }
                }
            },
            {
                "setDataValidation": {
                    "range": {"sheetId": target_ws.id, "startRowIndex":0,"endRowIndex":1,"startColumnIndex":9,"endColumnIndex":10},
                    "rule": {
                        # ====== تغییر: اضافه کردن Shift3 ======
                        "condition":{"type":"ONE_OF_LIST","values":[{"userEnteredValue":v} for v in ["Shift1","Shift2","Shift3","Flex","Other"]]},
                        "strict": False, "showCustomUi": True
                    }
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": target_ws.id, "startRowIndex":2, "endRowIndex":3, "startColumnIndex":0, "endColumnIndex":len(header_row)},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red":1.0,"green":0.902,"blue":0.412},  # #FFE699
                            "horizontalAlignment":"CENTER",
                            "textFormat":{"bold": True}
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                }
            }
        ]
    })
    target_ws.update(range_name="E1", values=[["Hour"]])
    target_ws.update(range_name="I1", values=[["Shift"]])

    # فیلتر
    filtered = []
    for r in rows:
        dms = day_start_ms(r[colDate])
        if math.isnan(dms) or not (start_ms <= dms <= end_ms): continue
        if selected_hour is not None and _to_int_hour(r[colHour]) != selected_hour: continue
        if selected_shift is not None and str(r[colShift]).strip() != selected_shift: continue
        filtered.append(r)

    if not filtered:
        target_ws.update(range_name="A4", values=[["ℹ️ نتیجه فیلتر خالی است (تاریخ/ساعت/شیفت را چک کنید)."]])
        return

    # گروه‌بندی و تبدیل انواع
    rows_by_task = {t: [] for t in TASK_TYPES}
    for r in filtered:
        t = str(r[colTaskType]).strip()
        if t not in rows_by_task: continue
        occ = to_number_locale(r[colOccupied])
        neg = max(0.0, 60.0 - occ)
        perf_no  = to_percent_locale(r[colPerfNoRot])
        perf_yes = to_percent_locale(r[colPerfWith])
        rows_by_task[t].append([
            r[colFullName],
            _to_int_hour(r[colHour]),
            to_number_locale(r[colQuantity]),
            occ,
            int(neg),
            perf_no,
            perf_yes
        ])

    for t in TASK_TYPES:
        rows_by_task[t].sort(key=lambda x: (x[1] if x[1] is not None else -9999), reverse=True)

    max_len = max((len(v) for v in rows_by_task.values()), default=0)
    if max_len == 0:
        target_ws.update(range_name="A4", values=[["ℹ️ بعد از گروه‌بندی چیزی نماند."]])
        return

    # خروجی
    output = []
    for i in range(max_len):
        row_out = []
        for j,t in enumerate(TASK_TYPES):
            if i < len(rows_by_task[t]): row_out += rows_by_task[t][i]
            else: row_out += ["","","","","","",""]
            if j < len(TASK_TYPES)-1: row_out.append("")
        output.append(row_out)

    end_col = len(header_row); end_row = 3 + len(output)
    target_ws.update(values=output, range_name=f"A4:{a1(end_col,end_row)}")

    # فرمت عددی ستون‌ها
    requests = []
    for b in range(len(TASK_TYPES)):
        start_col = b*8
        neg_col   = start_col + 4
        pct_no    = start_col + 5
        pct_with  = start_col + 6
        if b > 0:
            sep = start_col - 1
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": target_ws.id, "startRowIndex":2, "endRowIndex":end_row, "startColumnIndex":sep, "endColumnIndex":sep+1},
                    "cell": {"userEnteredFormat": {"backgroundColor": {"red":0.94,"green":0.94,"blue":0.94}}},
                    "fields": "userEnteredFormat.backgroundColor"
                }
            })
        for col_idx, numfmt in [
            (neg_col, {"type":"NUMBER","pattern":"0"}),
            (pct_no,  {"type":"PERCENT","pattern":"0.00%"}),
            (pct_with,{"type":"PERCENT","pattern":"0.00%"})
        ]:
            requests.append({
                "repeatCell": {
                    "range": {"sheetId": target_ws.id, "startRowIndex":3, "endRowIndex":end_row, "startColumnIndex":col_idx, "endColumnIndex":col_idx+1},
                    "cell": {"userEnteredFormat": {"numberFormat": numfmt}},
                    "fields": "userEnteredFormat.numberFormat"
                }
            })
    if requests:
        ss.batch_update({"requests": requests})

    print("✅ Done.")

# اجرای مستقیم
if __name__ == "__main__":
    build_hourly_performance()
