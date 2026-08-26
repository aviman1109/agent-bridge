"""Telegram listener — agent-bridge adapter（getUpdates long-poll + 訊息原地編輯串流）.

架構（對應 lark-listener-v3）：
  TG getUpdates long-poll → 過濾（owner user_id 白名單；群組需 @bot 或回覆 bot）
    → core.Bridge.run_task()（claude-agent-sdk headless，session_key 維度 resume）
    → 進度：sendMessage 一則「處理中」訊息，之後 editMessageText 原地更新
      （2.5s 節流 + trailing flush，安靜期也會把最後狀態刷上去）
    → 最終：markdown→HTML 渲染 + cost footer；>4096 自動分段補發

輸入支援：文字、圖片（photo）、檔案（document ≤20MB，Bot API getFile 上限）。
圖檔下載到 state dir 後把路徑塞進 prompt，由 agent 用 Read 工具檢視。

Session 維度：DM = chat；一般群 = 整群一條；forum 群 = 一個 topic 一條。

指令（群組可帶 @bot 後綴）：
  /start 說明   /new 重置 context   /stop 中止進行中任務   /id 查 user/chat id

"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

import httpx

from core import Bridge
from guest import guest_available, guest_role

log = logging.getLogger("telegram-listener")

# ── Config ──────────────────────────────────────────────────────────────

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
OWNER_USER_IDS = set(
    int(x) for x in os.environ.get("TG_OWNER_USER_IDS", "").split(",") if x.strip()
)
API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
FILE_BASE = f"https://api.telegram.org/file/bot{BOT_TOKEN}"
ALLOW_GROUPS = os.environ.get("TG_ALLOW_GROUPS", "1") == "1"
GUEST_DAILY_CAP_USD = float(os.environ.get("TG_GUEST_DAILY_CAP_USD", "2.0"))
BOT_TITLE = os.environ.get("BRIDGE_BOT_TITLE", "agent bridge")

STATE_DIR = Path(os.environ.get(
    "TG_LISTENER_STATE_DIR", os.path.expanduser("~/.cache/telegram-listener")))
FILES_DIR = STATE_DIR / "files"
OFFSET_FILE = STATE_DIR / "offset"
HEARTBEAT_FILE = STATE_DIR / "heartbeat"
# 群組白名單：Casper 用 /allow 加進來的 chat_id → 該群所有成員可用（訪客模式）
ALLOWED_CHATS_FILE = STATE_DIR / "allowed-chats.json"

EDIT_MIN_INTERVAL = 2.5      # 秒；editMessageText 節流（TG 對 edit 有 rate limit）
TG_TEXT_LIMIT = 4096         # Telegram 單則訊息硬上限
PROGRESS_TEXT_TAIL = 1200
FILE_SIZE_LIMIT = 20 * 1024 * 1024  # Bot API getFile 下載上限

BOT_USERNAME = ""   # main() 由 getMe 填入
BOT_ID = 0

SYSTEM_APPEND = """
你正透過 Telegram bridge 與使用者對話（訊息原地編輯串流）。
- 你的文字輸出由 bridge 即時轉貼到 Telegram，**不要**呼叫 telegram MCP / tg.py 回覆本對話（其他用途不受限）。
- Telegram 不渲染 markdown 表格與標題階層：用 bullet + **bold**，程式碼用 ``` 區塊。
- 回覆精簡。群組回覆更精簡。失敗別呆等別重試太多次，說明卡點即可。
"""


# ── markdown → Telegram HTML（沿用 telegram_mcp 實測過的轉換）──────────

def md_to_html(text: str) -> str:
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = re.sub(r"```(\w*)\n(.*?)```", r"<pre>\2</pre>", text, flags=re.DOTALL)
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"^#{1,6}\s+(.+)$", r"<b>\1</b>", text, flags=re.MULTILINE)
    text = re.sub(r"^[-=]{3,}\s*$", "─────────────", text, flags=re.MULTILINE)

    def _table_row(m: re.Match) -> str:
        row = m.group(0)
        if re.match(r"^\|[\s\-:|]+\|$", row.strip()):
            return ""
        return "  ".join(c.strip() for c in row.strip().strip("|").split("|"))

    text = re.sub(r"^\|.+\|$", _table_row, text, flags=re.MULTILINE)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"(?<!</b>)\*(.+?)\*", r"<i>\1</i>", text)
    text = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"<i>\1</i>", text)
    text = re.sub(r"~(.+?)~", r"<s>\1</s>", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="\2">\1</a>', text)
    return text


# ── Telegram Bot API（async thin wrapper）──────────────────────────────

class TelegramError(RuntimeError):
    def __init__(self, code: int, description: str) -> None:
        super().__init__(f"[{code}] {description}")
        self.code = code
        self.description = description


class TelegramAPI:
    def __init__(self) -> None:
        self._http = httpx.AsyncClient(timeout=70)

    async def call(self, method: str, **params) -> dict:
        r = await self._http.post(f"{API_BASE}/{method}", json=params)
        data = r.json()
        if not data.get("ok"):
            raise TelegramError(data.get("error_code", r.status_code),
                                data.get("description", r.text))
        return data["result"]

    async def send(self, chat_id: int, text: str, *, html: bool = False,
                   reply_to: int | None = None) -> int:
        params: dict = {"chat_id": chat_id, "text": text[:TG_TEXT_LIMIT],
                        "disable_web_page_preview": True}
        if html:
            params["parse_mode"] = "HTML"
        if reply_to:
            params["reply_parameters"] = {"message_id": reply_to,
                                          "allow_sending_without_reply": True}
        msg = await self.call("sendMessage", **params)
        return msg["message_id"]

    async def edit(self, chat_id: int, message_id: int, text: str, *,
                   html: bool = False) -> None:
        params: dict = {"chat_id": chat_id, "message_id": message_id,
                        "text": text[:TG_TEXT_LIMIT],
                        "disable_web_page_preview": True}
        if html:
            params["parse_mode"] = "HTML"
        await self.call("editMessageText", **params)

    async def download(self, file_id: str, suggested_name: str) -> Path:
        info = await self.call("getFile", file_id=file_id)
        remote = info["file_path"]
        FILES_DIR.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r"[^\w.\-]", "_", suggested_name or Path(remote).name)
        dest = FILES_DIR / f"{time.strftime('%Y%m%d-%H%M%S')}-{safe}"
        r = await self._http.get(f"{FILE_BASE}/{remote}")
        r.raise_for_status()
        dest.write_bytes(r.content)
        return dest


api = TelegramAPI()


# ── 進度訊息 sink（對應 v3 ProgressCard + trailing flush）──────────────

class TgProgress:
    def __init__(self, chat_id: int, reply_to: int | None, bridge: Bridge) -> None:
        self.chat_id = chat_id
        self.reply_to = reply_to
        self.bridge = bridge
        self.message_id: int | None = None
        self.t0 = time.time()
        self.tools: list[str] = []
        self.text = ""
        self._last_edit = 0.0
        self._dirty = False
        self._flusher: asyncio.Task | None = None
        self._done = False

    def _body(self) -> str:
        parts = []
        if self.tools:
            parts.append("\n".join(self.tools[-6:]))
        if self.text:
            tail = self.text[-PROGRESS_TEXT_TAIL:]
            parts.append(tail if len(self.text) <= PROGRESS_TEXT_TAIL else "…" + tail)
        body = "\n────────\n".join(parts) or "思考中…"
        return f"{body}\n\n🔄 執行中 · ⏱ {int(time.time() - self.t0)}s"

    async def start(self) -> None:
        self.message_id = await api.send(
            self.chat_id, "⏳ 收到，開始處理…", reply_to=self.reply_to)

    async def on_tool(self, line: str) -> None:
        self.tools.append(line)
        self._dirty = True
        await self._maybe_edit()

    async def on_text(self, text: str) -> None:
        self.text += text
        self._dirty = True
        await self._maybe_edit()

    async def _maybe_edit(self) -> None:
        if not self.message_id or not self._dirty or self._done:
            return
        wait = EDIT_MIN_INTERVAL - (time.time() - self._last_edit)
        if wait > 0:
            # trailing flush：節流期內不丟更新，安靜期一到就刷（v3 沒有這個，
            # 最後一批 tool 事件會卡到下一個事件才顯示）
            if not self._flusher or self._flusher.done():
                self._flusher = asyncio.create_task(self._delayed_flush(wait))
            return
        await self._flush()

    async def _delayed_flush(self, wait: float) -> None:
        await asyncio.sleep(wait)
        await self._flush()

    async def _flush(self) -> None:
        if not self.message_id or not self._dirty or self._done:
            return
        try:
            # 進度階段用純文字（增量 markdown 不完整，HTML parse 會炸）
            await api.edit(self.chat_id, self.message_id, self._body())
            self._last_edit = time.time()
            self._dirty = False
        except TelegramError as e:
            if "not modified" not in e.description:
                log.warning("progress edit failed: %s", e)
        except Exception as e:  # noqa: BLE001
            log.warning("progress edit failed: %s", e)

    async def finish(self, final_text: str, cost_usd: float | None,
                     session_id: str, error: str | None = None) -> None:
        self._done = True          # 先封進度更新，避免 flusher 蓋掉最終訊息
        if self._flusher and not self._flusher.done():
            self._flusher.cancel()
        elapsed = int(time.time() - self.t0)
        if error:
            body = f"❌ {error}" if not error.startswith(("已由", "任務超過")) else f"⏹ {error}"
            status = "⏹ 中止" if body.startswith("⏹") else "❌ 失敗"
        else:
            body = final_text or "(無文字輸出)"
            status = "✅ 完成"
        cost_part = (f" · 💰 ${cost_usd:.3f}（今日 ${self.bridge.today_cost():.2f}）"
                     if cost_usd is not None else "")
        footer = f"\n\n{status} · ⏱ {elapsed}s{cost_part} · 🧵 {session_id[:8]}"

        chunks = _chunk(body, TG_TEXT_LIMIT - len(footer) - 16)
        try:
            if self.message_id:
                await self._edit_fallback(md_to_html(chunks[0]) + footer,
                                          chunks[0] + footer)
            else:
                await self._send_fallback(md_to_html(chunks[0]) + footer,
                                          chunks[0] + footer)
            for extra in chunks[1:]:
                await self._send_fallback(md_to_html(extra), extra)
        except Exception:  # noqa: BLE001
            log.exception("final reply failed chat=%s", self.chat_id)

    async def _edit_fallback(self, html_text: str, plain_text: str) -> None:
        assert self.message_id
        try:
            await api.edit(self.chat_id, self.message_id, html_text, html=True)
        except TelegramError as e:
            if "not modified" in e.description:
                return
            log.warning("HTML edit failed (%s) — plain fallback", e)
            await api.edit(self.chat_id, self.message_id, plain_text)

    async def _send_fallback(self, html_text: str, plain_text: str) -> None:
        try:
            await api.send(self.chat_id, html_text, html=True)
        except TelegramError as e:
            log.warning("HTML send failed (%s) — plain fallback", e)
            await api.send(self.chat_id, plain_text)


def _chunk(text: str, size: int) -> list[str]:
    if len(text) <= size:
        return [text]
    out = []
    while text:
        cut = text.rfind("\n", 0, size) if len(text) > size else len(text)
        if cut <= 0:
            cut = min(size, len(text))
        out.append(text[:cut])
        text = text[cut:].lstrip("\n")
    return out


# ── chat allowlist（訪客模式群組）─────────────────────────────────────

def _load_allowed_chats() -> dict[str, dict]:
    """{ str(chat_id): {"title": str, "added_by": int, "added_at": int} }"""
    try:
        return json.loads(ALLOWED_CHATS_FILE.read_text())
    except Exception:  # noqa: BLE001
        return {}


def _save_allowed_chats(data: dict) -> None:
    tmp = ALLOWED_CHATS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1))
    tmp.replace(ALLOWED_CHATS_FILE)


def _chat_is_allowed(chat_id: int) -> bool:
    return str(chat_id) in _load_allowed_chats()


# ── update handling ─────────────────────────────────────────────────────

bridge = Bridge(STATE_DIR, SYSTEM_APPEND,
                task_timeout_s=int(os.environ.get("TG_TASK_TIMEOUT_S", "900")),
                max_concurrency=int(os.environ.get("TG_MAX_CONCURRENCY", "2")))


def _session_key(msg: dict, *, role: str, sender_id: int) -> str:
    """Owner 走原本規則；guest 每個訪客獨立 session（隱私 + resume 個別 context）。"""
    chat_id = msg["chat"]["id"]
    if role == "guest":
        return f"tg:{chat_id}:guest:{sender_id}"
    if msg["chat"].get("type") == "private":
        return f"tg:{chat_id}"
    if msg.get("is_topic_message") and msg.get("message_thread_id"):
        return f"tg:{chat_id}:{msg['message_thread_id']}"  # forum 群一 topic 一 session
    return f"tg:{chat_id}"                                 # 一般群整群一條


async def _build_prompt(msg: dict, text: str) -> str | None:
    """組 prompt；含圖片/檔案下載。不支援的媒體型別回 None（呼叫端提示）。"""
    parts: list[str] = []
    if msg.get("photo"):
        largest = msg["photo"][-1]  # PhotoSize 陣列由小到大
        path = await api.download(largest["file_id"],
                                  f"photo_{largest['file_unique_id']}.jpg")
        parts.append(f"[使用者透過 Telegram 傳來一張圖片，已存到 {path} — 用 Read 工具檢視]")
    if msg.get("document"):
        doc = msg["document"]
        if (doc.get("file_size") or 0) > FILE_SIZE_LIMIT:
            await api.send(msg["chat"]["id"],
                           "⚠️ 檔案超過 20MB（Bot API 下載上限），收不進來。",
                           reply_to=msg.get("message_id"))
            return None
        path = await api.download(doc["file_id"], doc.get("file_name") or "file.bin")
        parts.append(f"[使用者透過 Telegram 傳來檔案「{doc.get('file_name')}」，已存到 {path}]")
    if text:
        parts.append(text)
    if not parts:
        return None
    return "\n".join(parts)


async def _handle_new_chat_members(msg: dict) -> None:
    """Bot 被加入群組時，DM Casper 通知並提示怎麼開放。"""
    chat = msg.get("chat", {})
    new_members = msg.get("new_chat_members") or []
    if not any(m.get("id") == BOT_ID for m in new_members):
        return
    added_by = msg.get("from", {})
    chat_id = chat.get("id")
    title = chat.get("title", "(no title)")
    log.info("bot added to group chat=%s title=%r by user=%s",
             chat_id, title, added_by.get("id"))
    if _chat_is_allowed(chat_id):
        return
    text = (
        f"👥 我被加入群組「{title}」\n"
        f"chat_id: `{chat_id}`\n"
        f"加入人: {added_by.get('first_name', '?')} (id {added_by.get('id')})\n\n"
        f"要開放給群組成員使用（訪客模式）→ 回覆:\n"
        f"`/allow {chat_id}`"
    )
    for owner_id in OWNER_USER_IDS:
        try:
            await api.send(owner_id, text)
        except Exception:  # noqa: BLE001
            log.exception("notify owner failed uid=%s", owner_id)


async def handle_message(msg: dict) -> None:
    if msg.get("new_chat_members"):
        await _handle_new_chat_members(msg)
        return

    chat = msg.get("chat", {})
    sender = msg.get("from", {})
    chat_id = chat.get("id")
    user_id = sender.get("id")
    is_private = chat.get("type") == "private"
    raw_text = (msg.get("text") or msg.get("caption") or "").strip()

    if sender.get("is_bot"):
        return

    # ── 身份判定 ──
    is_owner = user_id in OWNER_USER_IDS
    chat_allowed = _chat_is_allowed(chat_id)
    if is_owner:
        role_name = "owner"
    elif not is_private and chat_allowed:
        role_name = "guest"
    else:
        if is_private and raw_text:
            log.info("drop non-owner DM user=%s (%s)", user_id,
                     sender.get("username", "?"))
        elif not is_private and raw_text:
            log.debug("drop msg in non-allowed group chat=%s user=%s", chat_id, user_id)
        return

    # 群組：需 @bot 或回覆 bot 的訊息才觸發
    if not is_private:
        if not ALLOW_GROUPS:
            return
        mentioned = bool(BOT_USERNAME) and f"@{BOT_USERNAME}" in raw_text
        reply_to_bot = (msg.get("reply_to_message", {})
                        .get("from", {}).get("id") == BOT_ID)
        if not (mentioned or reply_to_bot):
            return

    text = raw_text.replace(f"@{BOT_USERNAME}", "").strip() if BOT_USERNAME else raw_text
    session_key = _session_key(msg, role=role_name, sender_id=user_id)
    reply_to = msg.get("message_id")
    reply_kw = {"reply_to": None if is_private else reply_to}

    # ── 指令 ──（/cmd 或群組裡的 /cmd@bot — 上面已把 @bot 剝掉）
    cmd = text.split()[0] if text.startswith("/") else ""
    args = text.split()[1:] if text.startswith("/") else []

    if cmd == "/start":
        greet = (f"👋 我是 {BOT_TITLE}（訪客模式）。\n"
                 "直接 @我 提問即可，支援文字 / 圖片 / 檔案(≤20MB)。\n"
                 "我不會透露 owner 個人 / 客戶 / 內部資料 — 相關問題請直接找他。\n"
                 "/new 重置對話 · /stop 中止任務 · /id 查 id"
                 ) if role_name == "guest" else (
                 f"👋 我是 {BOT_TITLE}。\n"
                 "直接輸入訊息即可，支援文字 / 圖片 / 檔案(≤20MB)。\n"
                 "/new · /stop · /id · /allow <chat_id> · /deny <chat_id> · /allowed")
        await api.send(chat_id, greet, **reply_kw)
        return
    if cmd == "/new":
        had = bridge.reset_session(session_key)
        await api.send(chat_id,
                       "🧵 已重置對話 context。" if had else "🧵 目前沒有進行中的 context。",
                       **reply_kw)
        return
    if cmd == "/stop":
        n = bridge.cancel(session_key)
        await api.send(chat_id,
                       f"⏹ 已中止 {n} 個任務。" if n else "沒有進行中的任務。",
                       **reply_kw)
        return
    if cmd == "/id":
        info = f"user_id: {user_id}\nchat_id: {chat_id}"
        if msg.get("is_topic_message"):
            info += f"\ntopic_id: {msg.get('message_thread_id')}"
        info += f"\nrole: {role_name}"
        await api.send(chat_id, info, **reply_kw)
        return
    # 非 owner 打 owner 指令 → 直接擋（別花 SDK token）
    if cmd in {"/allow", "/deny", "/allowed"} and not is_owner:
        await api.send(chat_id, "🔒 這是 owner 專用指令。", **reply_kw)
        return
    # owner-only 管理指令
    if cmd == "/allow" and is_owner:
        target = int(args[0]) if args else chat_id
        allowed = _load_allowed_chats()
        if target == chat_id:
            title = chat.get("title", "(?)")
        else:
            # DM 打 /allow <id> — 反查 title
            try:
                info = await api.call("getChat", chat_id=target)
                title = info.get("title") or info.get("username") or "(?)"
            except Exception:  # noqa: BLE001
                title = "(unreachable)"
        allowed[str(target)] = {"title": title, "added_by": user_id,
                                "added_at": int(time.time())}
        _save_allowed_chats(allowed)
        await api.send(chat_id, f"✅ 已開放群組 {target}（{title}）給訪客使用。",
                       **reply_kw)
        return
    if cmd == "/deny" and is_owner:
        target = int(args[0]) if args else chat_id
        allowed = _load_allowed_chats()
        removed = allowed.pop(str(target), None)
        _save_allowed_chats(allowed)
        await api.send(chat_id,
                       f"🚫 已關閉群組 {target}" + (f"（{removed['title']}）" if removed else ""),
                       **reply_kw)
        return
    if cmd == "/allowed" and is_owner:
        allowed = _load_allowed_chats()
        if not allowed:
            await api.send(chat_id, "(沒有已開放的群組)", **reply_kw)
        else:
            lines = [f"• `{cid}` — {info.get('title', '?')}"
                     for cid, info in allowed.items()]
            await api.send(chat_id, "已開放的訪客群組:\n" + "\n".join(lines), **reply_kw)
        return

    # ── 訪客模式需要 policy 檔（fail-closed）──
    if role_name == "guest" and not guest_available():
        await api.send(chat_id, "🚧 訪客模式未配置（缺 guest-policy.json），暫停服務。", **reply_kw)
        return

    # ── 訪客成本上限 ──
    if role_name == "guest":
        spent = bridge.today_cost(sender_id=str(user_id), role="guest")
        if spent >= GUEST_DAILY_CAP_USD:
            await api.send(chat_id,
                           f"⛔ 你今天已用完訪客額度（${spent:.2f} / ${GUEST_DAILY_CAP_USD:.2f}）。"
                           "明天 UTC+8 00:00 重置。",
                           **reply_kw)
            return

    prompt = await _build_prompt(msg, text)
    if prompt is None:
        if msg.get("voice") or msg.get("video") or msg.get("sticker") or msg.get("audio"):
            await api.send(chat_id, "目前支援：文字、圖片、檔案(≤20MB)。語音/影片還收不進來。",
                           reply_to=reply_to)
        return

    role_ctx = guest_role() if role_name == "guest" else None
    log.info("recv chat=%s user=%s role=%s key=%s text=%r",
             chat_id, user_id, role_name, session_key, text[:120])
    asyncio.create_task(bridge.run_task(
        session_key, prompt, TgProgress(chat_id, reply_to, bridge),
        platform="telegram", sender_id=str(user_id), role=role_ctx))


# ── main loop ───────────────────────────────────────────────────────────

def _load_offset() -> int | None:
    try:
        return int(OFFSET_FILE.read_text().strip())
    except Exception:  # noqa: BLE001
        return None


def _save_offset(offset: int) -> None:
    OFFSET_FILE.write_text(str(offset))


async def main() -> None:
    global BOT_USERNAME, BOT_ID
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    me = await api.call("getMe")
    BOT_USERNAME = me.get("username") or ""
    BOT_ID = me.get("id") or 0
    log.info("bot: @%s (id=%s)", BOT_USERNAME, BOT_ID)
    log.info("owner allowlist: %s | groups: %s", sorted(OWNER_USER_IDS), ALLOW_GROUPS)

    offset = _load_offset()
    if offset is None:
        # 首次啟動：跳過歷史 backlog（舊訊息不該觸發昂貴的 SDK 任務）
        latest = await api.call("getUpdates", offset=-1, timeout=0,
                                allowed_updates=["message"])
        offset = (latest[-1]["update_id"] + 1) if latest else 0
        _save_offset(offset)
        log.info("first run — backlog skipped, offset=%s", offset)

    while True:
        HEARTBEAT_FILE.write_text(str(int(time.time())))
        try:
            updates = await api.call("getUpdates", offset=offset, timeout=50,
                                     allowed_updates=["message", "my_chat_member"])
        except TelegramError as e:
            if e.code == 409:
                log.error("getUpdates 409 — webhook 被搶回（n8n 重新註冊？）。30s 後重試")
                await asyncio.sleep(30)
                continue
            log.warning("getUpdates error: %s", e)
            await asyncio.sleep(5)
            continue
        except Exception as e:  # noqa: BLE001
            log.warning("getUpdates network error: %s", e)
            await asyncio.sleep(5)
            continue

        for u in updates:
            offset = u["update_id"] + 1
            _save_offset(offset)
            msg = u.get("message")
            if msg:
                try:
                    await handle_message(msg)
                except Exception:  # noqa: BLE001
                    log.exception("handle_message error")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )
    # httpx INFO 會把含 bot token 的 URL 印進 journal — 必須壓掉
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(main())
