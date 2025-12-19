# web.py
import os, time, subprocess
from flask import Flask, jsonify, request

app = Flask(__name__)

RUN_TOKEN = os.getenv("RUN_TOKEN", "")  # در Render ست کن
LOCK_PATH = "/tmp/all_data.lock"
MAX_RUN_SECONDS = int(os.getenv("MAX_RUN_SECONDS", "1200"))  # 20 min
LOCK_STALE_SECONDS = int(os.getenv("LOCK_STALE_SECONDS", "7200"))  # 2h

def authorized(req) -> bool:
    # Authorization: Bearer <token>
    if not RUN_TOKEN:
        return False
    auth = (req.headers.get("Authorization") or "").strip()
    return auth == f"Bearer {RUN_TOKEN}"

def lock_active() -> bool:
    if not os.path.exists(LOCK_PATH):
        return False
    try:
        age = time.time() - os.path.getmtime(LOCK_PATH)
        if age > LOCK_STALE_SECONDS:
            os.remove(LOCK_PATH)
            return False
        return True
    except:
        return True

def acquire_lock() -> None:
    with open(LOCK_PATH, "w", encoding="utf-8") as f:
        f.write(str(time.time()))

def release_lock() -> None:
    try:
        if os.path.exists(LOCK_PATH):
            os.remove(LOCK_PATH)
    except:
        pass

@app.get("/")
def home():
    return "OK"

@app.get("/health")
def health():
    return jsonify({"status": "ok", "lock": lock_active()}), 200

@app.post("/run")
def run():
    if not authorized(request):
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    if lock_active():
        return jsonify({"status": "error", "message": "Already running"}), 409

    acquire_lock()
    try:
        p = subprocess.run(
            ["python", "All_Data.py"],
            capture_output=True,
            text=True,
            timeout=MAX_RUN_SECONDS,
            env=os.environ.copy(),
        )
        out = (p.stdout or "").strip()
        err = (p.stderr or "").strip()
        ok = (p.returncode == 0)
        msg = out.splitlines()[-1] if out else ("✅ Done" if ok else "❌ Failed")

        return jsonify({
            "status": "ok" if ok else "error",
            "message": msg,
            "returncode": p.returncode,
            "stdout_tail": out[-2000:] if out else "",
            "stderr_tail": err[-2000:] if err else ""
        }), (200 if ok else 500)

    except subprocess.TimeoutExpired:
        return jsonify({"status": "error", "message": "⏱ Timeout"}), 504
    except Exception as e:
        return jsonify({"status": "error", "message": f"❌ {e}"}), 500
    finally:
        release_lock()
