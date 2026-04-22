import os
import hashlib
import secrets
import json
import threading
import subprocess
from http.server import HTTPServer, SimpleHTTPRequestHandler
import base64

PASSWORD = os.environ.get("APP_PASSWORD", "MediaGroup2026!")
PORT = int(os.environ.get("PORT", 8080))
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Track running profiler jobs
_profiler_jobs = {}


class AuthHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=os.path.join(os.path.dirname(__file__), "static"), **kwargs)

    def do_GET(self):
        auth = self.headers.get("Authorization")
        if not auth or not self._check_auth(auth):
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="Stock Dashboard"')
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h1>Authentication required</h1>")
            return
        # API endpoints
        if self.path == "/api/profiler/status":
            return self._api_profiler_status()
        super().do_GET()

    def do_POST(self):
        auth = self.headers.get("Authorization")
        if not auth or not self._check_auth(auth):
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="Stock Dashboard"')
            self.end_headers()
            return

        if self.path == "/api/profiler/run":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            return self._api_profiler_run(body)

        self.send_response(404)
        self.end_headers()

    def _api_profiler_status(self):
        """Return status of profiler jobs."""
        status = {}
        for key, info in _profiler_jobs.items():
            status[key] = {
                "running": info["thread"].is_alive() if info.get("thread") else False,
                "started": info.get("started", ""),
                "result": info.get("result", "pending"),
            }
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(status).encode())

    def _api_profiler_run(self, body):
        """Trigger LinkedIn profiler in background thread."""
        pool = body.get("pool", "agency")  # "agency", "client", or "relationships"
        max_profiles = body.get("max_profiles", 15)

        job_key = f"{pool}_{int(__import__('time').time())}"

        def run_profiler():
            try:
                if pool == "relationships":
                    cmd = ["python", os.path.join(ROOT, "scripts", "run_linkedin_profiler.py"),
                           "--relationships-only"]
                else:
                    cmd = ["python", os.path.join(ROOT, "scripts", "run_linkedin_profiler.py"),
                           "--pool", pool, "--max-profiles", str(max_profiles)]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
                _profiler_jobs[job_key]["result"] = "done" if result.returncode == 0 else f"error: {result.stderr[-200:]}"
            except Exception as e:
                _profiler_jobs[job_key]["result"] = f"error: {str(e)}"

        t = threading.Thread(target=run_profiler, daemon=True)
        _profiler_jobs[job_key] = {
            "thread": t,
            "started": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
            "pool": pool,
            "result": "running",
        }
        t.start()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "started", "job_id": job_key, "pool": pool}).encode())

    def _check_auth(self, auth):
        try:
            scheme, creds = auth.split(" ", 1)
            if scheme.lower() != "basic":
                return False
            decoded = base64.b64decode(creds).decode("utf-8")
            username, password = decoded.split(":", 1)
            return username == "admin" and secrets.compare_digest(password, PASSWORD)
        except Exception:
            return False


if __name__ == "__main__":
    # Start background data updater (fetches prices daily after market close)
    try:
        from updater import start_background_updater
        start_background_updater()
    except Exception as e:
        print(f"WARNING: updater failed to start: {e}")

    server = HTTPServer(("0.0.0.0", PORT), AuthHandler)
    print(f"Server running on port {PORT}")
    server.serve_forever()
