from flask import Flask, request, jsonify
import subprocess
import threading

app = Flask(__name__)
running = False

@app.route('/')
def home():
    return "✅ Fulfillment All_Data service is live!"

@app.route('/run', methods=['GET'])
def run_script():
    global running
    if running:
        return jsonify({"status": "busy"})
    running = True

    def worker():
        global running
        try:
            subprocess.run(["python", "All_Data.py"], check=True)
        except Exception as e:
            print("Error:", e)
        running = False

    threading.Thread(target=worker).start()
    return jsonify({"status": "started"})

@app.route('/status')
def status():
    global running
    return jsonify({
        "status": "busy" if running else "idle"
    })

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)
