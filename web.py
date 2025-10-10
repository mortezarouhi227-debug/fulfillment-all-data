from flask import Flask, request, jsonify
import subprocess
import os

app = Flask(__name__)

@app.route('/')
def home():
    return "OK"

@app.route('/run', methods=['GET'])
def run_script():
    key = request.args.get("key", "")
    run_key = os.getenv("RUN_KEY")

    # اگر کلید تنظیم شده بود، بررسی امنیتی انجام بده
    if run_key and key != run_key:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        result = subprocess.run(["python", "All_Data.py"], capture_output=True, text=True)
        return jsonify({"output": result.stdout.strip() or "✅ Done!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
