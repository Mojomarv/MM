"""Tiny static-file server for the OndoPerps MM dashboard.

Why: opening index.html via file:// can hit browser CORS guards when it
tries to fetch http://127.0.0.1:8140/status (mixed-origin / file-scheme
restrictions). Serving from a localhost http:// origin sidesteps that.

Usage (from anywhere in OndoPerpsMM):
    python dashboard/serve.py
Then open  http://127.0.0.1:8141  in your browser.

Override the port:
    python dashboard/serve.py 9000
Override bot control-server host/port via URL query:
    http://127.0.0.1:8141?host=192.168.1.10&port=8140
"""
from __future__ import annotations
import http.server
import socketserver
import sys
from pathlib import Path

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8141
ROOT = Path(__file__).resolve().parent


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self):
        # Tell the browser to never cache. Without this, an old index.html
        # in cache will hide any new dashboard features (Tune panel, etc.)
        # until the user hard-reloads. The bot's not perf-critical so the
        # bandwidth cost is fine.
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def log_message(self, fmt, *args):  # quiet down access log
        pass


if __name__ == "__main__":
    with socketserver.TCPServer(("127.0.0.1", PORT), Handler) as httpd:
        print(f"Dashboard serving at http://127.0.0.1:{PORT}")
        print(f"   (control server expected at http://127.0.0.1:8140; "
              f"override via ?host=...&port=...)")
        print("Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nBye.")
