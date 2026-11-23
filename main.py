import asyncio
import base64
import json
import os
import urllib.error
import urllib.request
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="Mora AI")

API_KEY = os.getenv("MORA_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", API_KEY).strip()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
STATE_REPO = os.getenv("STATE_REPO", "stephenbeck5-art/MORA-AI").strip()
STATE_FILE_PATH = os.getenv("STATE_FILE_PATH", "state.json").strip()
LOCAL_STATE_FILE = Path("/tmp/mora_state.json")

DEFAULT_STATE: Dict[str, Any] = {
    "gold_15m_signal": {
        "ticker": "XAUUSD",
        "timeframe": "15m",
        "phase": "HEADS_UP",
        "action": "STANDBY",
        "entry": "--",
        "sl": "--",
        "tp1": "--",
        "tp2": "--",
        "note": "Waiting for first 15m Gold webhook.",
        "updated_at": None,
    },
    "weinstein_signal": {
        "ticker": "WATCH",
        "signal_type": "WATCH_EARLY",
        "action": "STANDBY",
        "stage": "Stage 1 Wait",
        "score": 0,
        "price": "--",
        "stop": "--",
        "shares": 0,
        "mrs": "0.00",
        "updated_at": None,
    },
    "todos": [],
    "shopping": [],
}

STATE: Dict[str, Any] = deepcopy(DEFAULT_STATE)
_github_sha: Optional[str] = None
_persist_lock = asyncio.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _github_api_request(method: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"https://api.github.com/repos/{STATE_REPO}/contents/{STATE_FILE_PATH}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "Mora-AI-Backend",
    }
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=8) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _load_initial_state() -> None:
    global STATE, _github_sha
    if GITHUB_TOKEN and STATE_REPO:
        try:
            res = _github_api_request("GET")
            _github_sha = res.get("sha")
            raw = base64.b64decode(res["content"]).decode("utf-8")
            loaded = json.loads(raw)
            if isinstance(loaded, dict):
                STATE.update(loaded)
                return
        except Exception:
            pass

    if LOCAL_STATE_FILE.exists():
        try:
            loaded = json.loads(LOCAL_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                STATE.update(loaded)
        except Exception:
            pass


async def persist_state() -> None:
    global _github_sha
    async with _persist_lock:
        serialized = json.dumps(STATE, indent=2)
        try:
            LOCAL_STATE_FILE.write_text(serialized, encoding="utf-8")
        except Exception:
            pass

        if not (GITHUB_TOKEN and STATE_REPO):
            return

        def _push_to_github() -> None:
            global _github_sha
            body: Dict[str, Any] = {
                "message": f"chore(state): sync mora state {_now_iso()}",
                "content": base64.b64encode(serialized.encode("utf-8")).decode("ascii"),
            }
            if _github_sha:
                body["sha"] = _github_sha
            try:
                res = _github_api_request("PUT", body)
                _github_sha = res.get("content", {}).get("sha", _github_sha)
            except urllib.error.HTTPError as e:
                if e.code == 409:
                    latest = _github_api_request("GET")
                    _github_sha = latest.get("sha")
                    body["sha"] = _github_sha
                    res = _github_api_request("PUT", body)
                    _github_sha = res.get("content", {}).get("sha", _github_sha)

        try:
            await asyncio.to_thread(_push_to_github)
        except Exception:
            pass


@app.on_event("startup")
async def startup_event() -> None:
    await asyncio.to_thread(_load_initial_state)


def check_auth(authorization: Optional[str]) ->
