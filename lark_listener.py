"""Lark WS listener — agent-bridge adapter (Agent SDK execution, streaming card replies).

Architecture:
  Lark WS  →  this listener  →  claude-agent-sdk (headless, resume per chat)
                                      │ streaming messages
                                      ▼
                              Lark interactive card（原地更新進度）
                              → 最終結果 + cost footer

特性：
  - 即時進度：tool 事件 / 文字增量邊跑邊更新卡片，不用乾等
  - 回覆不漏：輸出由 bridge 程式抓取，不依賴 claude 自覺呼叫 lark-cli
  - session 連續：chat_id ↔ session_id，同一對話 resume 同一 context
  - 成本錶：每個任務記錄 usage/cost 到 usage.jsonl（6/15 起 SDK 走獨立 credit）

Note: 本 adapter 早於 core.py，仍是獨立實作（owner only）；後續收斂到 core
（吃到 async sink / 全域並發上限 / role override）是既定路線。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import lark_oapi as lark
from lark_oapi.api.im.v1.model.p2_im_message_receive_v1 import P2ImMessageReceiveV1

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ResultMessage,
    SystemMessage,
    TextBlock,
    ToolUseBlock,
    query,
)

log = logging.getLogger("lark-listener")

# ── Config ──────────────────────────────────────────────────────────────

APP_ID = os.environ["LARK_APP_ID"]
APP_SECRET = os.environ["LARK_APP_SECRET"]
DOMAIN = os.environ.get("LARK_DOMAIN", "https://open.larksuite.com")
OWNER_OPEN_IDS = set(
    x.strip() for x in os.environ.get("LARK_OWNER_OPEN_IDS", "").split(",") if x.strip()
)
# MVP：只回應白名單 chat（test 群）。空 = 不限制。
CHAT_WHITELIST = set(
    x.strip() for x in os.environ.get("LARK_CHAT_WHITELIST", "").split(",") if x.strip()
)

STATE_DIR = Path(os.environ.get(
    "LARK_LISTENER_STATE_DIR",
    os.path.expanduser("~/.cache/lark-listener"),
))
SESSIONS_FILE = STATE_DIR / "sessions.json"
USAGE_FILE = STATE_DIR / "usage.jsonl"
HEARTBEAT_FILE = STATE_DIR / "heartbeat"

WORK_DIR = os.path.expanduser(os.environ.get("BRIDGE_WORK_DIR", "~"))
CLAUDE_CONFIG_DIR = os.path.expanduser(
    os.environ.get("BRIDGE_CLAUDE_CONFIG_DIR", "~/.claude"))
CLAUDE_BIN = os.path.expanduser(
    os.environ.get("BRIDGE_CLAUDE_BIN", "~/.local/bin/claude"))

TASK_TIMEOUT_S = int(os.environ.get("LARK_TASK_TIMEOUT_S", "900"))
CARD_UPDATE_MIN_INTERVAL = 1.5   # 秒；卡片 PATCH 節流
CARD_TEXT_LIMIT = 3500           # 卡片 markdown 長度上限（超過截斷 + 補發全文）

SYSTEM_APPEND = """
你正透過 Lark bridge 與使用者對話（streaming 卡片回覆）。
- 你的文字輸出會被 bridge 即時轉貼到 Lark，**不要**呼叫 lark-cli 回覆本對話（其他用途的 lark-cli 操作不受限）。
- 回覆用 Lark 可渲染的 markdown：不用表格內 **bold**、不用 ~ 緊鄰 $。群組回覆精簡。
- 失敗別呆等別重試太多次，說明卡點即可。
"""

# ── Shared state ────────────────────────────────────────────────────────

_state_lock = threading.Lock()
BOT_OPEN_ID: str | None = None

# 每 chat 串行：chat_id → asyncio.Lock（在 SDK loop 內使用）
_chat_locks: dict[str, asyncio.Lock] = {}

# SDK 任務跑在獨立 asyncio loop thread（lark ws handler 是 sync thread）
_loop: asyncio.AbstractEventLoop | None = None


def _load_sessions() -> dict:
    if not SESSIONS_FILE.exists():
        return {}
    try:
        return json.loads(SESSIONS_FILE.read_text())
    except Exception:  # noqa: BLE001
        return {}


def _save_session(chat_id: str, session_id: str) -> None:
    with _state_lock:
        st = _load_sessions()
        st[chat_id] = {"session_id": session_id, "last_activity": int(time.time())}
        tmp = SESSIONS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, ensure_ascii=False, indent=1))
        tmp.replace(SESSIONS_FILE)


def _record_usage(chat_id: str, session_id: str, elapsed: float,
                  cost_usd: float | None, usage: dict | None) -> None:
    entry = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "chat_id": chat_id,
        "session_id": session_id,
        "elapsed_s": round(elapsed, 1),
        "cost_usd": cost_usd,
        "usage": usage,
    }
    with _state_lock:
        with USAGE_FILE.open("a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _today_cost() -> float:
    """今日累計 cost（給卡片 footer）。"""
    today = datetime.now().strftime("%Y-%m-%d")
    total = 0.0
    try:
        for line in USAGE_FILE.read_text().splitlines():
            e = json.loads(line)
            if e["ts"].startswith(today) and e.get("cost_usd"):
                total += e["cost_usd"]
    except FileNotFoundError:
        pass
    except Exception:  # noqa: BLE001
        pass
    return total


# ── Lark REST（卡片建立 / 更新）────────────────────────────────────────

class LarkCards:
    def __init__(self) -> None:
        self._token: str = ""
        self._token_exp: float = 0
        self._http = httpx.Client(timeout=15)
        self._lock = threading.Lock()

    def _auth(self) -> str:
        with self._lock:
            if time.time() < self._token_exp - 60:
                return self._token
            r = self._http.post(
                f"{DOMAIN}/open-apis/auth/v3/tenant_access_token/internal",
                json={"app_id": APP_ID, "app_secret": APP_SECRET},
            )
            r.raise_for_status()
            j = r.json()
            self._token = j["tenant_access_token"]
            self._token_exp = time.time() + j.get("expire", 3600)
            return self._token

    @staticmethod
    def _card(body_md: str, footer: str) -> str:
        card = {
            "config": {"wide_screen_mode": True},
            "elements": [
                {"tag": "markdown", "content": body_md[:CARD_TEXT_LIMIT]},
                {"tag": "hr"},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": footer}]},
            ],
        }
        return json.dumps(card, ensure_ascii=False)

    def send_card(self, chat_id: str, body_md: str, footer: str,
                  reply_to: str | None = None) -> str:
        """發卡片。reply_to 給定時用 reply-in-thread → 卡片進同一話題串。"""
        if reply_to:
            r = self._http.post(
                f"{DOMAIN}/open-apis/im/v1/messages/{reply_to}/reply",
                headers={"Authorization": f"Bearer {self._auth()}"},
                json={
                    "msg_type": "interactive",
                    "content": self._card(body_md, footer),
                    "reply_in_thread": True,
                },
            )
        else:
            r = self._http.post(
                f"{DOMAIN}/open-apis/im/v1/messages",
                params={"receive_id_type": "chat_id"},
                headers={"Authorization": f"Bearer {self._auth()}"},
                json={
                    "receive_id": chat_id,
                    "msg_type": "interactive",
                    "content": self._card(body_md, footer),
                },
            )
        r.raise_for_status()
        j = r.json()
        if j.get("code") != 0:
            raise RuntimeError(f"send_card failed: {j}")
        return j["data"]["message_id"]

    def patch_card(self, message_id: str, body_md: str, footer: str) -> None:
        r = self._http.patch(
            f"{DOMAIN}/open-apis/im/v1/messages/{message_id}",
            headers={"Authorization": f"Bearer {self._auth()}"},
            json={"content": self._card(body_md, footer)},
        )
        r.raise_for_status()
        j = r.json()
        if j.get("code") != 0:
            raise RuntimeError(f"patch_card failed: {j}")

    def send_text(self, chat_id: str, text: str,
                  reply_to: str | None = None) -> None:
        if reply_to:
            r = self._http.post(
                f"{DOMAIN}/open-apis/im/v1/messages/{reply_to}/reply",
                headers={"Authorization": f"Bearer {self._auth()}"},
                json={
                    "msg_type": "text",
                    "content": json.dumps({"text": text}, ensure_ascii=False),
                    "reply_in_thread": True,
                },
            )
        else:
            r = self._http.post(
                f"{DOMAIN}/open-apis/im/v1/messages",
                params={"receive_id_type": "chat_id"},
                headers={"Authorization": f"Bearer {self._auth()}"},
                json={
                    "receive_id": chat_id,
                    "msg_type": "text",
                    "content": json.dumps({"text": text}, ensure_ascii=False),
                },
            )
        r.raise_for_status()


_cards = LarkCards()


# ── 進度卡片 renderer ──────────────────────────────────────────────────

TOOL_ICONS = {
    "Bash": "🖥️", "Read": "📖", "Write": "✏️", "Edit": "✏️", "Grep": "🔍",
    "Glob": "🔍", "WebFetch": "🌐", "WebSearch": "🌐", "Task": "🤖", "Agent": "🤖",
}


def _tool_line(block: ToolUseBlock) -> str:
    icon = TOOL_ICONS.get(block.name, "🔧")
    hint = ""
    inp = block.input or {}
    for key in ("command", "description", "file_path", "pattern", "query", "prompt"):
        if inp.get(key):
            hint = str(inp[key]).replace("\n", " ")[:60]
            break
    name = block.name.replace("mcp__", "").replace("__", ":")
    return f"{icon} `{name}` {hint}"


class ProgressCard:
    """聚合執行狀態 → 節流更新同一張卡片。"""

    def __init__(self, chat_id: str, reply_to: str | None = None) -> None:
        self.chat_id = chat_id
        self.reply_to = reply_to          # 來源訊息 id → 卡片進同一話題串
        self.message_id: str | None = None
        self.t0 = time.time()
        self.tools: list[str] = []
        self.text = ""
        self._last_patch = 0.0
        self._dirty = False

    def _footer(self, status: str) -> str:
        return f"{status} · ⏱ {int(time.time() - self.t0)}s"

    def _body(self) -> str:
        parts = []
        if self.tools:
            parts.append("\n".join(self.tools[-6:]))
        if self.text:
            tail = self.text[-1200:]
            parts.append(tail if len(self.text) <= 1200 else "…" + tail)
        return "\n\n---\n\n".join(parts) or "_思考中…_"

    def start(self) -> None:
        self.message_id = _cards.send_card(
            self.chat_id, "_收到，開始處理…_", self._footer("🔄 啟動"),
            reply_to=self.reply_to)

    def on_tool(self, block: ToolUseBlock) -> None:
        self.tools.append(_tool_line(block))
        self._dirty = True
        self._maybe_patch()

    def on_text(self, text: str) -> None:
        self.text += text
        self._dirty = True
        self._maybe_patch()

    def _maybe_patch(self) -> None:
        if not self.message_id or not self._dirty:
            return
        if time.time() - self._last_patch < CARD_UPDATE_MIN_INTERVAL:
            return
        try:
            _cards.patch_card(self.message_id, self._body(), self._footer("🔄 執行中"))
            self._last_patch = time.time()
            self._dirty = False
        except Exception as e:  # noqa: BLE001
            log.warning("card patch failed: %s", e)

    def finish(self, final_text: str, cost_usd: float | None,
               session_id: str, error: str | None = None) -> None:
        elapsed = int(time.time() - self.t0)
        if error:
            body = f"❌ {error}"
            status = "❌ 失敗"
        else:
            body = final_text or "(無文字輸出)"
            status = "✅ 完成"
        cost_part = f" · 💰 ${cost_usd:.3f}（今日 ${_today_cost():.2f}）" if cost_usd is not None else ""
        footer = f"{status} · ⏱ {elapsed}s{cost_part} · 🧵 {session_id[:8]}"
        if not self.message_id:
            return
        try:
            _cards.patch_card(self.message_id, body, footer)
        except Exception as e:  # noqa: BLE001
            log.warning("final card patch failed: %s", e)
        # 超長回覆：卡片被截斷，補發全文（同話題串）
        if len(body) > CARD_TEXT_LIMIT:
            try:
                _cards.send_text(self.chat_id, body, reply_to=self.reply_to)
            except Exception:  # noqa: BLE001
                log.exception("overflow text send failed")


# ── Agent SDK execution ─────────────────────────────────────────────────

def _sdk_env() -> dict[str, str]:
    # 訂閱 OAuth 計費：絕不能讓 ANTHROPIC_API_KEY / CLAUDE_CODE_OAUTH_TOKEN 進到子行程
    return {
        "CLAUDE_CODE_OAUTH_TOKEN": "",
        "ANTHROPIC_API_KEY": "",
        "CLAUDE_CONFIG_DIR": CLAUDE_CONFIG_DIR,
    }


async def _run_task(chat_id: str, session_key: str, prompt: str,
                    reply_to: str | None) -> None:
    """session_key：話題群 = thread_id（一話題一 session）；其他 = chat_id。"""
    lock = _chat_locks.setdefault(session_key, asyncio.Lock())
    async with lock:  # 同話題串行 → resume 順序正確
        card = ProgressCard(chat_id, reply_to=reply_to)
        try:
            card.start()
        except Exception:  # noqa: BLE001
            log.exception("progress card create failed — continuing without card")

        prior = _load_sessions().get(session_key, {}).get("session_id")
        options = ClaudeAgentOptions(
            cwd=WORK_DIR,
            cli_path=CLAUDE_BIN,
            resume=prior,
            permission_mode="bypassPermissions",
            setting_sources=["user", "project", "local"],
            system_prompt={"type": "preset", "preset": "claude_code",
                           "append": SYSTEM_APPEND},
            env=_sdk_env(),
        )

        session_id = prior or "?"
        final_text = ""
        cost: float | None = None
        usage: dict | None = None
        t0 = time.time()
        try:
            async with asyncio.timeout(TASK_TIMEOUT_S):
                async for message in query(prompt=prompt, options=options):
                    if isinstance(message, SystemMessage):
                        sid = message.data.get("session_id")
                        if sid:
                            session_id = sid
                    elif isinstance(message, AssistantMessage):
                        for block in message.content:
                            if isinstance(block, TextBlock):
                                card.on_text(block.text)
                            elif isinstance(block, ToolUseBlock):
                                card.on_tool(block)
                    elif isinstance(message, ResultMessage):
                        final_text = message.result or card.text
                        cost = message.total_cost_usd
                        usage = message.usage
                        if message.session_id:
                            session_id = message.session_id
        except TimeoutError:
            card.finish("", cost, session_id,
                        error=f"任務超過 {TASK_TIMEOUT_S}s 上限，已中止。")
            return
        except Exception as e:  # noqa: BLE001
            log.exception("sdk task failed chat=%s", chat_id)
            card.finish("", cost, session_id, error=f"執行失敗：{e}")
            return
        finally:
            if session_id != "?":
                _save_session(session_key, session_id)
                _record_usage(session_key, session_id, time.time() - t0, cost, usage)

        card.finish(final_text, cost, session_id)
        log.info("task done chat=%s session=%s cost=%s", chat_id, session_id, cost)


# ── WS event handling（沿用 v2 模式）──────────────────────────────────

def _heartbeat_touch() -> None:
    try:
        HEARTBEAT_FILE.write_text(str(int(time.time())))
    except Exception:  # noqa: BLE001
        pass


def _extract_text(msg: Any) -> str:
    mt = getattr(msg, "message_type", None)
    content = getattr(msg, "content", None) or "{}"
    try:
        j = json.loads(content)
    except Exception:  # noqa: BLE001
        return ""
    if mt == "text":
        return j.get("text", "") or ""
    if mt == "post":
        title = j.get("title", "") or ""
        lines: list[str] = [title] if title else []
        for para in j.get("content") or []:
            segs = []
            for seg in para:
                tag = seg.get("tag")
                if tag in ("text", "md", "code_inline"):
                    segs.append(seg.get("text") or "")
                elif tag == "a":
                    segs.append(seg.get("text") or seg.get("href") or "")
                elif tag == "at":
                    # @bot 本身不進 prompt；@別人保留（對話語意需要）
                    if seg.get("user_id") != BOT_OPEN_ID:
                        segs.append(f"@{seg.get('user_name') or ''}")
            lines.append("".join(segs))
        return "\n".join(lines)
    return ""


def _strip_mentions(text: str, msg: Any) -> str:
    for m in (getattr(msg, "mentions", None) or []):
        if m.key:
            text = text.replace(m.key, "")
    return text.strip()


def _on_message(event: P2ImMessageReceiveV1) -> None:
    _heartbeat_touch()
    try:
        msg = event.event.message
        sender = event.event.sender
        sender_id = sender.sender_id.open_id if sender and sender.sender_id else ""
        chat_id = msg.chat_id
        chat_type = getattr(msg, "chat_type", None) or "p2p"

        if sender_id == BOT_OPEN_ID:
            return
        if CHAT_WHITELIST and chat_id not in CHAT_WHITELIST:
            return
        if sender_id not in OWNER_OPEN_IDS:
            return  # owner only — Lark 側訪客模式待收斂進 core 後實作

        text = _extract_text(msg)
        if not text.strip():
            return
        # 群組一律需 @bot 才觸發；DM 直接收
        if chat_type != "p2p":
            if not any(m.id and m.id.open_id == BOT_OPEN_ID
                       for m in (msg.mentions or [])):
                return
            text = _strip_mentions(text, msg)
        if not text.strip():
            return

        # Session key = 話題根訊息維度（一話題一 session）：
        #   thread 內回覆 → root_id（= 話題根訊息）
        #   話題群根訊息 → thread_id
        #   一般模式群新訊息 → 自身 message_id（回覆 reply_in_thread 會以它為根拉出話題）
        # DM → chat_id（單一長 session）
        root_id = getattr(msg, "root_id", None) or ""
        thread_id = getattr(msg, "thread_id", None) or ""
        if chat_type == "p2p":
            session_key = chat_id
            reply_to = None
        else:
            session_key = root_id or thread_id or msg.message_id
            reply_to = msg.message_id

        log.info("recv chat=%s key=%s text=%r", chat_id, session_key, text[:120])
        assert _loop is not None
        asyncio.run_coroutine_threadsafe(
            _run_task(chat_id, session_key, text, reply_to), _loop)

    except Exception:  # noqa: BLE001
        log.exception("on_message error")


def _ignore(event: Any) -> None:  # noqa: ANN401
    _heartbeat_touch()


# ── Bootstrap ──────────────────────────────────────────────────────────

def _fetch_bot_open_id() -> str:
    with httpx.Client(timeout=10) as c:
        r = c.post(
            f"{DOMAIN}/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": APP_ID, "app_secret": APP_SECRET},
        )
        r.raise_for_status()
        token = r.json()["tenant_access_token"]
        r2 = c.get(f"{DOMAIN}/open-apis/bot/v3/info",
                   headers={"Authorization": f"Bearer {token}"})
        r2.raise_for_status()
        return r2.json()["bot"]["open_id"]


def _start_sdk_loop() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    threading.Thread(target=loop.run_forever, name="sdk-loop", daemon=True).start()
    return loop


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )
    if not Path(CLAUDE_BIN).exists():
        log.error("claude binary not found: %s", CLAUDE_BIN)
        sys.exit(1)

    STATE_DIR.mkdir(parents=True, exist_ok=True)

    global BOT_OPEN_ID, _loop
    BOT_OPEN_ID = _fetch_bot_open_id()
    _loop = _start_sdk_loop()
    log.info("bot open_id: %s", BOT_OPEN_ID)
    log.info("chat whitelist: %s", sorted(CHAT_WHITELIST) or "(none — all chats)")
    log.info("owner allowlist: %s", sorted(OWNER_OPEN_IDS))

    handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(_on_message)
        .register_p2_im_message_message_read_v1(_ignore)
        .register_p2_im_message_reaction_created_v1(_ignore)
        .register_p2_im_message_reaction_deleted_v1(_ignore)
        .build()
    )
    client = lark.ws.Client(
        APP_ID, APP_SECRET, event_handler=handler, domain=DOMAIN,
        log_level=lark.LogLevel.INFO,
    )
    _heartbeat_touch()
    log.info("Starting Lark WS listener (agent-bridge)")
    client.start()


if __name__ == "__main__":
    main()
