"""One-command launcher for OndoPerpsMM.

Starts:
  1. The bot         (bots/mm/ondo_mm/multi_grid_bot.py, cwd=accounts/ondo1)
  2. The dashboard   (dashboard/serve.py, cwd=project root)
  3. The browser     (http://127.0.0.1:8141)

Both subprocesses run with their own process group so this launcher can
send them a clean shutdown signal on Ctrl+C without the bot eating the
same Ctrl+C from the parent console and skipping its cleanup.

Output from both is prefixed and color-coded so you can read them side
by side in one terminal:

    [   bot    ] 2026-06-02 19:30:01,234 INFO config: pairs=...
    [ dashboard ] Dashboard serving at http://127.0.0.1:8141

Usage:
    python start.py
    python start.py --account ondo2          # use accounts/ondo2 instead
    python start.py --dashboard-port 9000    # override dashboard port
    python start.py --no-browser             # don't auto-open browser
    python start.py --flatten-on-stop        # market-close positions on shutdown

Exit:
    Ctrl+C → graceful shutdown (cancels orders if TRADING=true).
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Prefer the project's venv python if present; falls back to the launcher's
# own python otherwise.
def _resolve_python() -> str:
    venv = ROOT / "venv" / ("Scripts" if os.name == "nt" else "bin")
    candidate = venv / ("python.exe" if os.name == "nt" else "python")
    if candidate.exists():
        return str(candidate)
    return sys.executable


# ── ANSI colors (Windows 10+ terminal supports them since 1607) ──────────
CYAN  = "\033[36m"
PINK  = "\033[35m"
GREY  = "\033[90m"
DIM   = "\033[2m"
RESET = "\033[0m"


def _stream(proc: subprocess.Popen, label: str, color: str) -> None:
    """Pump one subprocess's stdout line-by-line to our stdout with a tag."""
    tag = f"{color}[{label:^10}]{RESET}"
    for line in iter(proc.stdout.readline, ""):
        if not line:
            break
        sys.stdout.write(f"{tag} {line}")
        sys.stdout.flush()


def _spawn(cmd: list[str], cwd: Path, label: str, color: str,
             extra_env: dict | None = None) -> subprocess.Popen:
    """Launch a subprocess in its own process group and start a stdout pump."""
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    kwargs: dict = dict(
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True

    proc = subprocess.Popen(cmd, **kwargs)
    threading.Thread(
        target=_stream, args=(proc, label, color), daemon=True,
    ).start()
    return proc


def _shutdown(proc: subprocess.Popen, label: str,
                timeout: float = 15.0) -> None:
    """Send a clean stop signal; force-kill after `timeout` if still alive."""
    if proc.poll() is not None:
        return
    print(f"{GREY}[  start   ]{RESET} stopping {label}…", flush=True)
    try:
        if os.name == "nt":
            # CTRL_BREAK_EVENT raises KeyboardInterrupt in the child Python
            # process; asyncio.run's catcher then unwinds main()'s finally
            # block, which runs cancel_all_orders_safe before exit.
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
    except (ProcessLookupError, OSError) as exc:
        print(f"{GREY}[  start   ]{RESET} {label} already gone ({exc})",
              flush=True)
        return
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"{GREY}[  start   ]{RESET} {label} didn't exit in {timeout}s, "
              f"force-killing", flush=True)
        proc.kill()


def _request_close_flatten(port: int, log) -> None:
    """POST /close with flatten=true so the bot market-closes everything
    before its main loop exits. We then still send the shutdown signal."""
    url = f"http://127.0.0.1:{port}/close"
    body = json.dumps({"flatten": True}).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5.0) as r:
            log(f"close+flatten requested via /close ({r.status})")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
        log(f"close+flatten request failed: {exc}")


def _ensure_env(account_dir: Path) -> None:
    env_file = account_dir / ".env"
    if env_file.exists():
        return
    tmpl = account_dir / ".env.template"
    print(f"{GREY}[  start   ]{RESET} no .env in {account_dir} — "
          f"copy {tmpl.name} -> .env first.", flush=True)
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch OndoPerps MM bot + dashboard.")
    parser.add_argument("--account", default="ondo1",
                          help="Subfolder under accounts/ (default: ondo1)")
    parser.add_argument("--control-port", type=int, default=8140,
                          help="Bot's control_server port (default 8140)")
    parser.add_argument("--dashboard-port", type=int, default=8141,
                          help="Dashboard static server port (default 8141)")
    parser.add_argument("--no-browser", action="store_true",
                          help="Don't auto-open the browser")
    parser.add_argument("--flatten-on-stop", action="store_true",
                          help="On Ctrl+C, POST /close?flatten=true before "
                               "terminating (market-closes all positions)")
    parser.add_argument("--paused", action="store_true",
                          help="Boot the bot in paused state — WS feeds + "
                               "dashboard come up but the quote loop won't "
                               "place any orders until you click Start on "
                               "the dashboard.")
    args = parser.parse_args()

    account_dir = ROOT / "accounts" / args.account
    if not account_dir.is_dir():
        print(f"{GREY}[  start   ]{RESET} account dir not found: {account_dir}",
              flush=True)
        sys.exit(1)
    _ensure_env(account_dir)

    python = _resolve_python()
    bot_script  = ROOT / "bots" / "mm" / "ondo_mm" / "multi_grid_bot.py"
    dash_script = ROOT / "dashboard" / "serve.py"

    def log(msg: str) -> None:
        print(f"{GREY}[  start   ]{RESET} {msg}", flush=True)

    log(f"python      = {python}")
    log(f"bot         = {bot_script}  (cwd={account_dir})")
    log(f"dashboard   = {dash_script}  port={args.dashboard_port}")

    bot_env = {}
    if args.paused:
        bot_env["START_PAUSED"] = "true"
        log("--paused: bot will boot idle; press Start on the dashboard to begin quoting")

    bot = _spawn(
        [python, "-u", str(bot_script)],
        cwd=account_dir, label="bot", color=CYAN,
        extra_env=bot_env,
    )

    # Tiny delay so the bot's banner lands first in the log.
    time.sleep(0.5)

    dash = _spawn(
        [python, "-u", str(dash_script), str(args.dashboard_port)],
        cwd=ROOT, label="dashboard", color=PINK,
    )

    # Append a timestamp so the URL is unique per launch — bypasses any
    # cached copy of index.html the browser may be holding from before
    # we added the no-cache headers / Tune panel / etc.
    url = (f"http://127.0.0.1:{args.dashboard_port}/?"
           f"port={args.control_port}&v={int(time.time())}")
    if not args.no_browser:
        # Give the http.server a moment to bind before opening the page.
        time.sleep(2.0)
        try:
            webbrowser.open(url)
            log(f"opened {url}")
        except Exception as exc:  # noqa: BLE001
            log(f"open browser failed: {exc}")
    else:
        log(f"dashboard ready at {url}")

    # Watch both subprocesses; exit when either dies or Ctrl+C arrives.
    try:
        while True:
            time.sleep(1)
            for name, p in (("bot", bot), ("dashboard", dash)):
                if p.poll() is not None:
                    log(f"{name} exited (code {p.returncode}) — shutting down")
                    raise KeyboardInterrupt
    except KeyboardInterrupt:
        print()  # newline after ^C
        log("shutdown requested")
    finally:
        # Optional graceful flatten via the bot's /close endpoint before
        # we send the stop signal. Best-effort; we still terminate either way.
        if args.flatten_on_stop and bot.poll() is None:
            _request_close_flatten(args.control_port, log)
            time.sleep(3.0)  # let the bot's close routine start

        _shutdown(bot,  "bot")
        _shutdown(dash, "dashboard")
        log("done.")


if __name__ == "__main__":
    main()
