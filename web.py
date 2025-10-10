# web.py
import os
import threading
import subprocess
import datetime as dt
from flask import Flask, jsonify, request

app = Flask(__name__)

# وضعیت اجرای Job
STATE_LOCK = threading.Lock()
STATE = {
    "status": "idle",          # idle | busy
    "started_at": None,
    "finished_at": None,
    "exit_code": None,
    "output_tail": "",         # دم آخر لاگ برای نمایش سریع
}

LOG_PATH = "latest_run.log"
RUN_KEY = os.getenv("RUN_KEY", "MyStrongKey123")  # اگر خواستی در Render ست کن

def _update_state(**kwargs):
    with STATE_LOCK:
        STATE.update(kwargs)

def _now():
    return dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _start_worker():
    """
    اجرای All_Data.py در یک ترد جدا با ساب‌پروسس،
    و استریم کردن خروجی به فایل لاگ و output_tail.
    """
    def _worker():
        _update_state(status="busy", started_at=_now(), finished_at=None, exit_code=None, output_tail="")

        # لاگ را از نو بساز
        with open(LOG_PATH, "w", encoding="utf-8") as f:
            f.write(f"[{_now()}] ▶️ Job started\n")

        # اجرای اسکریپت اصلی
        proc = subprocess.Popen(
            ["python", "All_Data.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        tail_buf = []
        try:
            for line in proc.stdout:
                # نوشتن کامل لاگ
                with open(LOG_PATH, "a", encoding="utf-8") as f:
                    f.write(line)

                # به‌روز کردن دم خروجی (آخرین ~4000 کاراکتر)
                tail_buf.append(line)
                joined = "".join(tail_buf)[-4000:]
                tail_buf = [joined]

                _update_state(output_tail=joined)
        except Exception as e:
            with open(LOG_PATH, "a", encoding="utf-8") as f:
                f.write(f"\n[ERROR] {e}\n")

        proc.wait()
        exit_code = proc.returncode

        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{_now()}] ⏹ Job finished (exit_code={exit_code})\n")

        _update_state(status="idle", finished_at=_now(), exit_code=exit_code)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()

@app.route("/", methods=["GET"])
def root():
    return "OK", 200

@app.route("/status", methods=["GET"])
def status():
    with STATE_LOCK:
        return jsonify(STATE), 200

@app.route("/logs", methods=["GET"])
def logs():
    if not os.path.exists(LOG_PATH):
        return jsonify({"message": "No logs found"}), 200
    with open(LOG_PATH, "r", encoding="utf-8") as f:
        content = f.read()[-5000:]  # فقط آخر ۵۰۰۰ کاراکتر
    return f"<pre>{content}</pre>", 200

@app.route("/run", methods=["GET", "POST"])
def run():
    # (اختیاری) کلید ساده برای جلوگیری از استفاده‌ی ناخواسته
    key = request.args.get("key") or request.headers.get("X-Run-Key")
    if RUN_KEY and key and key != RUN_KEY:
        return jsonify({"error": "invalid key"}), 401

    with STATE_LOCK:
        if STATE["status"] == "busy":
            return jsonify({"status": "busy"}), 200

    _start_worker()
    return jsonify({"status": "started"}), 202

if __name__ == "__main__":
    # Render پورت را از ENV می‌دهد
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
