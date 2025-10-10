from flask import Flask, request
import subprocess, sys, os

app = Flask(__name__)

RUN_KEY = os.environ.get("RUN_KEY")  # اختیاری: کلید ساده جهت امنیت

@app.get("/")
def health():
    return "OK"

@app.get("/run")
def run_job():
    if RUN_KEY and request.args.get("key") != RUN_KEY:
        return "Unauthorized", 401
    p = subprocess.run([sys.executable, "All_Data.py"], capture_output=True, text=True)
    body = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    code = 200 if p.returncode == 0 else 500
    return f"Exit: {p.returncode}\n\n{body}", code

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
