"""One-command launcher for the multi-venue MM bot.

Starts:
  1. One or more bot processes (Ondo, Rise, or both) — each runs in its
     own process group so this launcher can clean-shutdown via signal.
  2. The dashboard (dashboard/serve.py, cwd=project root, default port 8141)
  3. The browser (one tab per running bot)

Output from every subprocess is prefixed and color-coded so you can read
them side by side in one terminal:

    [   ondo   ] 2026-06-02 19:30:01,234 INFO config: pairs=...
    [   rise   ] 2026-06-02 19:30:01,567 INFO config: pairs=...
    [ dashboard ] Dashboard serving at http://127.0.0.1:8141

Usage:
    python start.py                          # default: ondo only
    python start.py --venue rise             # rise only
    python start.py --venue both             # both bots, one dashboard
    python start.py --account ondo2          # override account subfolder
    python start.py --dashboard-port 9000    # override dashboard port
    python start.py --no-browser             # don't auto-open browser
    python start.py --paused                 # boot bot(s) paused
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


# Per-venue configuration: (default account dir, bot module path,
# default control port, log-prefix color).
VENUES = {
    "ondo": {
        "account":  "ondo1",
        "bot_path": ROOT / "bots" / "mm" / "ondo_mm" / "multi_grid_bot.py",
        "port":     8140,
        "color":    CYAN,
        "label":    "ondo",
    },
    "rise": {
        "account":  "risex1",
        "bot_path": ROOT / "bots" / "mm" / "risex_mm" / "multi_grid_bot.py",
        "port":     8150,
        "color":    "\033[33m",   # amber, distinct from ondo cyan
        "label":    "rise",
    },
}


def _launch_venue(venue: str, args, log) -> tuple[subprocess.Popen, str, int]:
    """Launch one venue's bot subprocess. Returns (popen, dashboard URL, control_port)."""
    cfg = VENUES[venue]
    account = args.account or cfg["account"]
    account_dir = ROOT / "accounts" / account
    if not account_dir.is_dir():
        log(f"account dir not found for venue={venue}: {account_dir}")
        sys.exit(1)
    _ensure_env(account_dir)

    # Use the venue's default port unless --control-port overrides it.
    port = args.control_port if args.control_port is not None else cfg["port"]

    bot_env = {"CONTROL_PORT": str(port)}
    if args.paused:
        bot_env["START_PAUSED"] = "true"

    log(f"{venue}: bot={cfg['bot_path'].name}  cwd={account_dir.name}  port={port}")
    proc = _spawn(
        [_resolve_python(), "-u", str(cfg["bot_path"])],
        cwd=account_dir, label=cfg["label"], color=cfg["color"],
        extra_env=bot_env,
    )
    url = (f"http://127.0.0.1:{args.dashboard_port}/?"
           f"port={port}&v={int(time.time())}")
    return proc, url, port


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the MM bot(s) + dashboard.")
    parser.add_argument("--venue", default="ondo",
                          choices=list(VENUES.keys()) + ["both"],
                          help="Which venue to run (default: ondo). "
                               "'both' launches Ondo + Rise on distinct ports.")
    parser.add_argument("--account", default=None,
                          help="Override the venue's default account subfolder. "
                               "Only honoured for single-venue launches.")
    parser.add_argument("--control-port", type=int, default=None,
                          help="Override the venue's default control port. "
                               "Only honoured for single-venue launches.")
    parser.add_argument("--dashboard-port", type=int, default=8141,
                          help="Dashboard static server port (default 8141)")
    parser.add_argument("--no-browser", action="store_true",
                          help="Don't auto-open the browser")
    parser.add_argument("--flatten-on-stop", action="store_true",
                          help="On Ctrl+C, POST /close?flatten=true before "
                               "terminating (market-closes all positions)")
    parser.add_argument("--paused", action="store_true",
                          help="Boot bots in paused state. WS feeds + "
                               "dashboard wire up but no orders are placed "
                               "until you click Start.")
    args = parser.parse_args()

    venues_to_launch = ["ondo", "rise"] if args.venue == "both" else [args.venue]
    if args.venue == "both" and (args.account or args.control_port is not None):
        print("note: --account and --control-port are ignored when --venue=both. "
              "Override per-venue via accounts/<dir>/.env CONTROL_PORT instead.")
        args.account = None
        args.control_port = None

    def log(msg: str) -> None:
        print(f"{GREY}[  start   ]{RESET} {msg}", flush=True)

    log(f"python      = {_resolve_python()}")
    log(f"venue       = {args.venue}  ({', '.join(venues_to_launch)})")
    log(f"dashboard   = port {args.dashboard_port}")
    if args.paused:
        log("--paused: bots will boot idle; press Start on the dashboard "
            "to begin quoting")

    # 1. Spawn each venue's bot
    bots: list[tuple[str, subprocess.Popen, str, int]] = []
    for venue in venues_to_launch:
        proc, url, port = _launch_venue(venue, args, log)
        bots.append((venue, proc, url, port))
        time.sleep(0.3)   # stagger so startup banners don't interleave

    # 2. Spawn the single dashboard server
    time.sleep(0.5)
    dash = _spawn(
        [_resolve_python(), "-u",
         str(ROOT / "dashboard" / "serve.py"),
         str(args.dashboard_port)],
        cwd=ROOT, label="dashboard", color=PINK,
    )

    # 3. Open one browser tab per running bot
    if not args.no_browser:
        time.sleep(2.0)
        for venue, _proc, url, _port in bots:
            try:
                webbrowser.open(url)
                log(f"opened {venue}: {url}")
            except Exception as exc:  # noqa: BLE001
                log(f"open browser for {venue} failed: {exc}")
    else:
        for venue, _proc, url, _port in bots:
            log(f"{venue} dashboard ready at {url}")

    # 4. Watch every subprocess. A single bot dying (e.g. config error)
    # no longer kills the others — we only shut down when EVERY bot is
    # gone, or the dashboard dies, or the user hits Ctrl+C.
    bot_exit_logged: set[int] = set()
    try:
        while True:
            time.sleep(1)
            for venue, p, _url, _port in bots:
                if p.poll() is not None and id(p) not in bot_exit_logged:
                    log(f"{venue} bot exited (code {p.returncode}) — "
                        f"others continue. Re-run `python start.py "
                        f"--venue {venue}` after fixing to bring it back.")
                    bot_exit_logged.add(id(p))
            if all(p.poll() is not None for _v, p, _u, _po in bots):
                log("all bots have exited — shutting down")
                raise KeyboardInterrupt
            if dash.poll() is not None:
                log(f"dashboard exited (code {dash.returncode}) — shutting down")
                raise KeyboardInterrupt
    except KeyboardInterrupt:
        print()
        log("shutdown requested")
    finally:
        # Optional graceful flatten via each bot's /close endpoint before
        # we send the stop signal. Best-effort; we still terminate either way.
        if args.flatten_on_stop:
            for venue, p, _url, port in bots:
                if p.poll() is None:
                    _request_close_flatten(port, log)
            time.sleep(3.0)

        for venue, p, _url, _port in bots:
            _shutdown(p, f"{venue} bot")
        _shutdown(dash, "dashboard")
        log("done.")


if __name__ == "__main__":
    main()
