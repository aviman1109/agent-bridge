"""agent-bridge core — platform-agnostic Claude Agent SDK execution layer.

從 lark-listener-v3 抽出的共用層：任何 IM 平台 adapter 提供一個 ProgressSink
（async callbacks），core 負責：

  - session 連續性：session_key ↔ session_id（resume 同一 context）
  - 同 key 串行（asyncio.Lock）+ 全域並發上限（Semaphore，防多聊天室同時燒錢）
  - usage 記帳：usage.jsonl 帶 platform + sender_id（多人使用時可分帳）
  - 任務 timeout
  - Role override（訪客模式）：不同 cwd / config_dir / allowed_tools / hooks，
    強制沙盒不靠 prompt

對 v3 的改良（v3 遷移到這裡時一併吃到）：
  - sink callbacks 是 async — 不再於 event loop 內跑同步 httpx 卡整個 loop
  - usage 記 sender_id / platform
  - 全域 Semaphore 並發上限
  - Role override — 支援多身份共用同一 bridge
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ResultMessage,
    SystemMessage,
    TextBlock,
    ToolUseBlock,
    query,
)

log = logging.getLogger("agent-bridge")

# 部署環境差異全走 env（見 .env.example）；預設值為通用值，不綁特定機器
WORK_DIR = os.path.expanduser(os.environ.get("BRIDGE_WORK_DIR", "~"))
CLAUDE_CONFIG_DIR = os.path.expanduser(
    os.environ.get("BRIDGE_CLAUDE_CONFIG_DIR", "~/.claude"))
CLAUDE_BIN = os.path.expanduser(
    os.environ.get("BRIDGE_CLAUDE_BIN", "~/.local/bin/claude"))

TOOL_ICONS = {
    "Bash": "🖥️", "Read": "📖", "Write": "✏️", "Edit": "✏️", "Grep": "🔍",
    "Glob": "🔍", "WebFetch": "🌐", "WebSearch": "🌐", "Task": "🤖", "Agent": "🤖",
}


def tool_line(block: ToolUseBlock) -> str:
    icon = TOOL_ICONS.get(block.name, "🔧")
    hint = ""
    inp = block.input or {}
    for key in ("command", "description", "file_path", "pattern", "query", "prompt"):
        if inp.get(key):
            hint = str(inp[key]).replace("\n", " ")[:60]
            break
    name = block.name.replace("mcp__", "").replace("__", ":")
    return f"{icon} {name} {hint}".rstrip()


class ProgressSink(Protocol):
    async def start(self) -> None: ...
    async def on_tool(self, line: str) -> None: ...
    async def on_text(self, text: str) -> None: ...
    async def finish(self, final_text: str, cost_usd: float | None,
                     session_id: str, error: str | None = None) -> None: ...


@dataclass
class RoleContext:
    """身份 profile — 給 run_task 用來覆蓋預設（owner）設定。

    owner 傳 None、guest 傳一個實例：不同 cwd / config_dir / system prompt / 白名單 / hook。
    """
    name: str                                       # "owner" / "guest" / ...
    cwd: str = WORK_DIR
    config_dir: str = CLAUDE_CONFIG_DIR
    # ── system prompt 兩選一 ──
    # override=str → 整段自訂（不套 claude_code preset、不載 CLAUDE.md）；guest 用這個
    # append=str → 套 preset 再 append（owner / bridge 預設）
    system_prompt_override: str | None = None
    system_prompt_append: str | None = None
    # ── setting_sources: None=default，[]=跳過 project/user/local settings（不載 CLAUDE.md）──
    setting_sources: list[str] | None = None
    allowed_tools: list[str] = field(default_factory=list)   # [] = 不設限
    disallowed_tools: list[str] = field(default_factory=list)
    hooks: dict[str, Any] | None = None             # SDK hooks 字典


class Bridge:
    def __init__(self, state_dir: Path, system_append: str, *,
                 task_timeout_s: int = 900, max_concurrency: int = 2) -> None:
        self.state_dir = state_dir
        state_dir.mkdir(parents=True, exist_ok=True)
        self.sessions_file = state_dir / "sessions.json"
        self.usage_file = state_dir / "usage.jsonl"
        self.system_append = system_append
        self.task_timeout_s = task_timeout_s
        self._sem = asyncio.Semaphore(max_concurrency)
        self._locks: dict[str, asyncio.Lock] = {}
        self._running: dict[str, set[asyncio.Task]] = {}

    # ── sessions / usage ────────────────────────────────────────────────

    def _load_sessions(self) -> dict:
        if not self.sessions_file.exists():
            return {}
        try:
            return json.loads(self.sessions_file.read_text())
        except Exception:  # noqa: BLE001
            return {}

    def _save_session(self, key: str, session_id: str) -> None:
        st = self._load_sessions()
        st[key] = {"session_id": session_id, "last_activity": int(time.time())}
        tmp = self.sessions_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, ensure_ascii=False, indent=1))
        tmp.replace(self.sessions_file)

    def reset_session(self, key: str) -> bool:
        """/new 指令：砍掉 key 的 session 映射，下一則訊息全新 context。"""
        st = self._load_sessions()
        if key not in st:
            return False
        del st[key]
        tmp = self.sessions_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, ensure_ascii=False, indent=1))
        tmp.replace(self.sessions_file)
        return True

    def _record_usage(self, key: str, session_id: str, elapsed: float,
                      cost_usd: float | None, usage: dict | None,
                      platform: str, sender_id: str, role: str) -> None:
        entry = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "platform": platform,
            "role": role,
            "sender_id": sender_id,
            "session_key": key,
            "session_id": session_id,
            "elapsed_s": round(elapsed, 1),
            "cost_usd": cost_usd,
            "usage": usage,
        }
        with self.usage_file.open("a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def today_cost(self, *, sender_id: str | None = None,
                   role: str | None = None) -> float:
        """今日累計 cost。給 sender_id 或 role → 只算符合條件的。"""
        today = datetime.now().strftime("%Y-%m-%d")
        total = 0.0
        try:
            for line in self.usage_file.read_text().splitlines():
                e = json.loads(line)
                if not e["ts"].startswith(today):
                    continue
                if sender_id is not None and e.get("sender_id") != sender_id:
                    continue
                if role is not None and e.get("role") != role:
                    continue
                if e.get("cost_usd"):
                    total += e["cost_usd"]
        except FileNotFoundError:
            pass
        except Exception:  # noqa: BLE001
            pass
        return total

    # ── task execution ──────────────────────────────────────────────────

    @staticmethod
    def _sdk_env(config_dir: str) -> dict[str, str]:
        # 訂閱 OAuth 計費：絕不能讓 ANTHROPIC_API_KEY / CLAUDE_CODE_OAUTH_TOKEN 進子行程
        return {
            "CLAUDE_CODE_OAUTH_TOKEN": "",
            "ANTHROPIC_API_KEY": "",
            "CLAUDE_CONFIG_DIR": config_dir,
        }

    def cancel(self, session_key: str) -> int:
        """/stop 指令：中止該 key 進行中 + 排隊中的任務。回傳中止數。"""
        tasks = self._running.get(session_key, set())
        n = 0
        for t in list(tasks):
            if not t.done():
                t.cancel()
                n += 1
        return n

    async def run_task(self, session_key: str, prompt: str, sink: ProgressSink,
                       *, platform: str = "", sender_id: str = "",
                       role: RoleContext | None = None) -> None:
        task = asyncio.current_task()
        assert task is not None
        self._running.setdefault(session_key, set()).add(task)
        try:
            lock = self._locks.setdefault(session_key, asyncio.Lock())
            async with lock:          # 同 key 串行 → resume 順序正確
                async with self._sem:  # 全域並發上限
                    await self._run_locked(session_key, prompt, sink,
                                           platform=platform, sender_id=sender_id,
                                           role=role)
        finally:
            self._running.get(session_key, set()).discard(task)

    async def _run_locked(self, session_key: str, prompt: str, sink: ProgressSink,
                          *, platform: str, sender_id: str,
                          role: RoleContext | None) -> None:
        try:
            await sink.start()
        except Exception:  # noqa: BLE001
            log.exception("progress sink start failed — continuing")

        prior = self._load_sessions().get(session_key, {}).get("session_id")
        cwd = role.cwd if role else WORK_DIR
        config_dir = role.config_dir if role else CLAUDE_CONFIG_DIR
        # setting_sources：owner 預設 [user, project, local]；guest role 可指定 []（不載 CLAUDE.md）
        setting_sources = (role.setting_sources if role and role.setting_sources is not None
                           else ["user", "project", "local"])
        # system_prompt：owner 走 preset + bridge append；guest 走 override（純字串，
        # 不套 preset → 不會拉入 work workspace CLAUDE.md 的 @import 內容）
        if role and role.system_prompt_override is not None:
            sys_prompt: Any = role.system_prompt_override
        else:
            append = (role.system_prompt_append if role and role.system_prompt_append
                      else self.system_append)
            sys_prompt = {"type": "preset", "preset": "claude_code", "append": append}

        opt_kwargs: dict[str, Any] = dict(
            cwd=cwd,
            cli_path=CLAUDE_BIN,
            resume=prior,
            permission_mode="bypassPermissions",
            setting_sources=setting_sources,
            system_prompt=sys_prompt,
            env=self._sdk_env(config_dir),
        )
        if role:
            if role.allowed_tools:
                opt_kwargs["allowed_tools"] = role.allowed_tools
            if role.disallowed_tools:
                opt_kwargs["disallowed_tools"] = role.disallowed_tools
            if role.hooks:
                opt_kwargs["hooks"] = role.hooks
        options = ClaudeAgentOptions(**opt_kwargs)

        role_name = role.name if role else "owner"
        session_id = prior or "?"
        final_text = ""
        cost: float | None = None
        usage: dict | None = None
        ok = False       # 只有拿到 ResultMessage 才視為成功；否則不 persist 新 session（否則 resume 會爛掉）
        t0 = time.time()
        try:
            async with asyncio.timeout(self.task_timeout_s):
                async for message in query(prompt=prompt, options=options):
                    if isinstance(message, SystemMessage):
                        sid = message.data.get("session_id")
                        if sid:
                            session_id = sid
                    elif isinstance(message, AssistantMessage):
                        for block in message.content:
                            if isinstance(block, TextBlock):
                                await sink.on_text(block.text)
                            elif isinstance(block, ToolUseBlock):
                                await sink.on_tool(tool_line(block))
                    elif isinstance(message, ResultMessage):
                        final_text = message.result or ""
                        cost = message.total_cost_usd
                        usage = message.usage
                        if message.session_id:
                            session_id = message.session_id
                        ok = True
        except (TimeoutError, asyncio.CancelledError, Exception) as e:  # noqa: BLE001
            # 統一錯誤處理：只記 usage（cost 已花），**絕不 persist session_id** —
            # SDK 有時會在拋錯前先把 SystemMessage/ResultMessage 送出，帶著看似有效的
            # session_id，但 CLI 端其實沒把 conversation 完整落地，下輪 resume 會拿
            # "No conversation found"。所以 error paths 全都當 no-op session。
            if isinstance(e, TimeoutError):
                msg = f"任務超過 {self.task_timeout_s}s 上限，已中止。"
            elif isinstance(e, asyncio.CancelledError):
                msg = "已由 /stop 中止。"
            else:
                log.exception("sdk task failed key=%s", session_key)
                msg = f"執行失敗：{e}"
            self._record_usage(session_key, session_id, time.time() - t0,
                               cost, usage, platform, sender_id, role_name)
            await sink.finish("", cost, session_id, error=msg)
            return

        if ok and session_id != "?":
            self._save_session(session_key, session_id)
            self._record_usage(session_key, session_id, time.time() - t0,
                               cost, usage, platform, sender_id, role_name)

        await sink.finish(final_text, cost, session_id)
        log.info("task done key=%s role=%s session=%s cost=%s",
                 session_key, role_name, session_id, cost)
