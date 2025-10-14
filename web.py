# web.py
from flask import Flask, jsonify
import subprocess, textwrap, sys, os

app = Flask(__name__)

@app.route("/")
def home():
    return "OK"

@app.route("/run")
def run():
    try:
        # اجرای اسکریپت جمع‌آوری
        p = subprocess.run(
            ["python", "All_Data.py"],
            capture_output=True, text=True, timeout=1200  # تا 20 دقیقه
        )
        out = (p.stdout or "").strip()
        err = (p.stderr or "").strip()
        ok = (p.returncode == 0)
        # پیامی که All_Data.py چاپ کرده را می‌فرستیم
        msg = out.splitlines()[-1] if out else ("✅ Done" if ok else "❌ Failed")
        return jsonify({
            "status": "ok" if ok else "error",
            "message": msg,
            "log_tail": out[-2000:] if out else "",
            "error_tail": err[-2000:] if err else ""
        }), (200 if ok else 500)
    except subprocess.TimeoutExpired:
        return jsonify({"status":"error","message":"⏱ Timeout"}), 500
    except Exception as e:
        return jsonify({"status":"error","message":f"❌ {e}"}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
