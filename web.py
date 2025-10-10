from flask import Flask, request, jsonify
import threading, subprocess, sys, os, time

app = Flask(__name__)

RUN_KEY = os.environ.get("RUN_KEY")

last_run = {
    "status": "idle",     # idle | running | done | error
    "started_at": None,
    "finished_at": None,
    "exit_code": None,
    "output_tail": "",
}

def run_all_data():
    """runs All_Data.py in background and updates last_run dict"""
    try:
        last_run.update({
            "status": "running",
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": None,
            "exit_code": None,
            "output_tail": "",
        })
        # NOTE: capture_output to log tail; do not block request thread
        p = subprocess.Popen(
            [sys.executable, "All_Data.py"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        tail_buf = []
        for line in p.stdout:
            tail_buf.append(line.rstrip())
            # فقط 400 خط آخر را نگه می‌داریم
            if len(tail_buf) > 400:
                tail_buf.pop(0)
        p.wait()
        last_run.update({
            "status": "done" if p.returncode == 0 else "error",
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_code": p.returncode,
            "output_tail": "\n".join(tail_buf)[-8000:],  # ~8KB آخر
        })
    except Exception as e:
        last_run.update({
            "status": "error",
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "exit_code": -1,
            "output_tail": f"Exception: {e}",
        })

@app.get("/")
def health():
    return "OK"

@app.get("/run")
def run_job():
    if RUN_KEY and request.args.get("key") != RUN_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    if last_run["status"] == "running":
        return jsonify({"status": "running"}), 409  # already running
    threading.Thread(target=run_all_data, daemon=True).start()
    return jsonify({"status": "started"}), 202   # پاسخ سریع؛ بدون تایم‌اوت

@app.get("/status")
def status():
    # برای دیدن خروجی و وضعیت آخرین اجرا
    return jsonify(last_run), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
