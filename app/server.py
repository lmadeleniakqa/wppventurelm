import os
import hashlib
import secrets
from http.server import HTTPServer, SimpleHTTPRequestHandler
import base64

PASSWORD = os.environ.get("APP_PASSWORD", "MediaGroup2026!")
PORT = int(os.environ.get("PORT", 8080))


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
        super().do_GET()

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
