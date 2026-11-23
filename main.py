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


def check_auth(authorization: Optional[str]) -> None:
    if not API_KEY:
        return
    if not authorization or authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Unauthorized")


async def parse_json_dict(request: Request) -> Dict[str, Any]:
    raw = await request.body()
    if not raw or not raw.strip():
        raise HTTPException(status_code=400, detail="Empty request body; expected JSON object")
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail=f"Malformed JSON payload: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="JSON payload must be an object ({...})")
    return parsed


@app.get("/")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "Mora AI",
        "persistence": "github" if (GITHUB_TOKEN and STATE_REPO) else "ephemeral_tmp",
    }


@app.get("/mora/briefing")
def get_briefing(authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    check_auth(authorization)
    return STATE


@app.post("/webhook")
async def tradingview_webhook(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> JSONResponse:
    data = await parse_json_dict(request)

    if WEBHOOK_SECRET:
        body_secret = str(data.pop("secret", "")).strip()
        bearer_ok = authorization == f"Bearer {WEBHOOK_SECRET}"
        secret_ok = body_secret == WEBHOOK_SECRET
        if not (bearer_ok or secret_ok):
            raise HTTPException(status_code=401, detail="Invalid or missing webhook secret")
    else:
        data.pop("secret", None)

    data["updated_at"] = _now_iso()
    engine = str(data.get("engine", "")).lower()
    tf = str(data.get("timeframe", "")).lower()

    if engine == "weinstein" or tf in ("1w", "w1"):
        STATE["weinstein_signal"].update(data)
        target = "weinstein_signal"
    else:
        STATE["gold_15m_signal"].update(data)
        target = "gold_15m_signal"

    await persist_state()
    return JSONResponse(status_code=200, content={"ok": True, "stream": target, "data": STATE[target]})


def _next_id(items: list) -> int:
    return max((int(item.get("id", 0)) for item in items if isinstance(item, dict)), default=0) + 1


def _toggle_or_remove_item(items: list, query: str, remove: bool = False) -> Optional[Dict[str, Any]]:
    q = query.strip().lower()
    for idx, item in enumerate(items):
        if str(item.get("id")) == q or q in str(item.get("text", "")).lower():
            if remove:
                return items.pop(idx)
            item["done"] = not bool(item.get("done", False))
            return item
    return None


@app.post("/mora/command")
async def post_command(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    check_auth(authorization)
    body = await parse_json_dict(request)

    action = str(body.get("action", "")).strip().lower()
    text = str(body.get("text") or body.get("item") or body.get("command") or "").strip()
    message = "No state change"

    if not action and text:
        low = text.lower()
        if low.startswith(("add todo ", "todo ")):
            action = "add_todo"
            text = text.split(" ", 2)[-1] if low.startswith("add todo ") else text[5:].strip()
        elif low.startswith(("complete todo ", "done todo ", "toggle todo ")):
            action = "toggle_todo"
            text = text.split(" ", 2)[-1]
        elif low.startswith(("remove todo ", "delete todo ")):
            action = "remove_todo"
            text = text.split(" ", 2)[-1]
        elif low.startswith(("add shopping ", "buy ")):
            action = "add_shopping"
            text = text[13:].strip() if low.startswith("add shopping ") else text[4:].strip()
        elif low.startswith(("bought ", "complete shopping ", "toggle shopping ")):
            action = "toggle_shopping"
            text = text.split(" ", 1)[-1] if low.startswith("bought ") else text.split(" ", 2)[-1]
        elif low.startswith(("remove shopping ", "delete shopping ")):
            action = "remove_shopping"
            text = text.split(" ", 2)[-1]
        elif low in ("clear completed todos", "clear done todos"):
            action = "clear_done_todos"
        elif low in ("clear bought", "clear completed shopping", "clear shopping"):
            action = "clear_bought_shopping"

    if action == "add_todo" and text:
        item = {"id": _next_id(STATE["todos"]), "text": text, "done": False, "created_at": _now_iso()}
        STATE["todos"].append(item)
        message = f"Added todo: {text}"
    elif action == "toggle_todo" and text:
        updated = _toggle_or_remove_item(STATE["todos"], text, remove=False)
        message = f"Toggled todo: {updated['text']}" if updated else f"Todo not found: {text}"
    elif action == "remove_todo" and text:
        removed = _toggle_or_remove_item(STATE["todos"], text, remove=True)
        message = f"Removed todo: {removed['text']}" if removed else f"Todo not found: {text}"
    elif action == "clear_done_todos":
        STATE["todos"] = [t for t in STATE["todos"] if not t.get("done")]
        message = "Cleared completed todos"
    elif action == "add_shopping" and text:
        item = {"id": _next_id(STATE["shopping"]), "text": text, "done": False, "created_at": _now_iso()}
        STATE["shopping"].append(item)
        message = f"Added shopping item: {text}"
    elif action == "toggle_shopping" and text:
        updated = _toggle_or_remove_item(STATE["shopping"], text, remove=False)
        message = f"Toggled shopping item: {updated['text']}" if updated else f"Item not found: {text}"
    elif action == "remove_shopping" and text:
        removed = _toggle_or_remove_item(STATE["shopping"], text, remove=True)
        message = f"Removed shopping item: {removed['text']}" if removed else f"Item not found: {text}"
    elif action == "clear_bought_shopping":
        STATE["shopping"] = [s for s in STATE["shopping"] if not s.get("done")]
        message = "Cleared bought shopping items"

    await persist_state()
    return {"ok": True, "message": message, "state": STATE}
 
