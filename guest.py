"""Guest role — Telegram 群組訪客的 read-only 沙盒.

設計原則：訪客的核心用途 = 查 owner workspace 內的公開脈絡
  → **共用 owner workspace 做 read-only，不另建 workspace**。

安全模型（不靠 prompt，靠 SDK 機制）：

  1. `setting_sources=[]` — 不載 CLAUDE.md，避開 CLAUDE.md 的 @import（業務檔案含
     機密數字），也避開 auto-memory
  2. 自帶 system prompt — 告知訪客 role 與可讀範圍，不用 Claude Code preset（不會被
     work workspace 的 project settings 灌爆）
  3. `allowed_tools` 白名單 — 只放 Read / Grep / Glob / WebSearch / WebFetch；Bash /
     Edit / Write / Task / Agent / 所有 MCP 因不在白名單 → 完全不存在
  4. PreToolUse hook 對 Read / Grep / Glob 三個檔案類工具：
     - 絕對路徑必須在 ALLOW_ROOTS 內（work workspace / uploads / /tmp）
     - 且**路徑不匹配任何 DENY_PATTERN**（照 .gitignore + CLAUDE.md「絕對不能 commit」
       + 商業敏感）
  5. PreToolUse hook 對 WebFetch：SSRF 阻擋（localhost/RFC1918/link-local/loopback）

每位訪客 session_key = `tg:{chat}:guest:{sender_id}` 獨立、每日 cost cap（可調）。
"""
from __future__ import annotations

import fnmatch
import ipaddress
import json
import logging
import os
import socket
from pathlib import Path
from urllib.parse import urlparse

from claude_agent_sdk import HookMatcher

from core import CLAUDE_CONFIG_DIR, RoleContext, WORK_DIR

log = logging.getLogger("agent-bridge.guest")

# 訪客直接站在 owner workspace（read-only）
GUEST_CWD = WORK_DIR
# 共用 owner 的 CLAUDE_CONFIG_DIR 讓 OAuth 憑證繼承（同一 Anthropic 帳號計費）
GUEST_CONFIG_DIR = CLAUDE_CONFIG_DIR

# ── Policy 外部化（deny 清單本身就是敏感資訊：它列出你有哪些機密檔）──
# 格式見 guest-policy.example.json。缺檔 = 訪客模式停用（fail-closed）。
_POLICY_FILE = Path(os.environ.get(
    "BRIDGE_GUEST_POLICY",
    str(Path(__file__).resolve().parent / "guest-policy.json")))


def _load_policy() -> dict | None:
    try:
        raw = json.loads(_POLICY_FILE.read_text())
        roots = tuple(Path(os.path.expanduser(p)).resolve()
                      for p in raw["read_roots"])
        return {
            "read_roots": roots,
            "deny_patterns": tuple(raw["deny_patterns"]),
            "system_prompt": raw["system_prompt"],
        }
    except FileNotFoundError:
        log.error("guest policy missing: %s — guest role DISABLED (fail-closed)",
                  _POLICY_FILE)
        return None
    except Exception:  # noqa: BLE001
        log.exception("guest policy invalid: %s — guest role DISABLED", _POLICY_FILE)
        return None


_POLICY = _load_policy()


def guest_available() -> bool:
    return _POLICY is not None


# 載入後的 policy 內容（policy 缺失時給空值 — guest_role() 會先擋）
GUEST_READ_ROOTS: tuple[Path, ...] = _POLICY["read_roots"] if _POLICY else ()
DENY_PATTERNS: tuple[str, ...] = _POLICY["deny_patterns"] if _POLICY else ("*",)

# 白名單：只有這些工具會被 SDK 放行使用
GUEST_ALLOWED_TOOLS: list[str] = [
    "Read", "Grep", "Glob", "WebSearch", "WebFetch",
]

# 訪客 system prompt 由 guest-policy.json 提供（policy["system_prompt"]）——
# 它描述可讀/不可讀範圍，本身即敏感資訊，不進 repo。


def _is_public_ip(host: str) -> bool:
    """SSRF 防護：判斷 host 是否是公網 IP。localhost / RFC1918 / link-local / 迴環都算內部。"""
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        try:
            infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
        except socket.gaierror:
            return False
        for _f, _t, _p, _c, sockaddr in infos:
            try:
                ip = ipaddress.ip_address(sockaddr[0])
            except ValueError:
                continue
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                return False
        return True
    return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved)


def _deny(reason: str) -> dict:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def _matches_deny(rel_path: str) -> str | None:
    """回傳匹配到的 pattern，或 None。fnmatch 語意（* 不跨 /）+ ** 自行處理。"""
    for pat in DENY_PATTERNS:
        if "**" in pat:
            # 拆掉 ** 對每個位置比對
            head, _, tail = pat.partition("**")
            head = head.rstrip("/")
            tail = tail.lstrip("/")
            # rel_path 若以 head 開頭，剩下部分 fnmatch tail（tail 允許 * 跨層）
            if head and not (rel_path == head or rel_path.startswith(head + "/")):
                continue
            remainder = rel_path[len(head):].lstrip("/") if head else rel_path
            if not tail or fnmatch.fnmatch(remainder, "*/" + tail) or fnmatch.fnmatch(
                    remainder, tail) or any(fnmatch.fnmatch(seg, tail) for seg in
                                            remainder.split("/")):
                return pat
        else:
            if fnmatch.fnmatch(rel_path, pat):
                return pat
            # 也試整段路徑的每個尾段（例如 `*.pdf` 對 `customer/foo.pdf` 也要中）
            if "/" in pat:
                continue
            if fnmatch.fnmatch(Path(rel_path).name, pat):
                return pat
    return None


def _abs_resolve(path_str: str) -> Path | None:
    try:
        return Path(path_str).expanduser().resolve()
    except Exception:  # noqa: BLE001
        return None


def _check_fs_path(path_str: str) -> dict:
    """共用檔案路徑檢查邏輯 — Read / Grep / Glob 都用它。allow 回 {}, deny 回 _deny()。"""
    abs_path = _abs_resolve(path_str)
    if abs_path is None:
        return _deny(f"Invalid path: {path_str}")
    # 1. 必須在 ALLOW_ROOTS 內
    root_match = None
    for root in GUEST_READ_ROOTS:
        try:
            abs_path.relative_to(root)
            root_match = root
            break
        except ValueError:
            continue
    if root_match is None:
        return _deny(
            f"路徑 {abs_path} 不在訪客可存取範圍。訪客只能存取授權目錄 / 自己上傳的檔案。"
        )
    # 2. 若在 work workspace 內，還要過 DENY_PATTERN
    try:
        rel = str(abs_path.relative_to(Path(WORK_DIR).resolve()))
        matched = _matches_deny(rel)
        if matched:
            return _deny(
                f"這個檔案（{rel}）是機密不能讀（match pattern: {matched}）。"
                "建議直接詢問 owner。"
            )
    except ValueError:
        pass
    return {}


async def _pre_tool_use_fs_guard(
    input_data: dict, tool_use_id: str | None, context
) -> dict:
    """Read / Grep / Glob 共用守衛。"""
    tool = input_data.get("tool_name")
    if tool not in ("Read", "Grep", "Glob"):
        return {}
    inp = input_data.get("tool_input") or {}
    if tool == "Read":
        p = inp.get("file_path", "")
    elif tool == "Grep":
        # Grep 有 path 參數（可選；預設 cwd）；也可能有多個 include 檔案
        p = inp.get("path") or WORK_DIR
    else:  # Glob
        p = inp.get("path") or WORK_DIR
    if not p:
        return {}
    return _check_fs_path(p)


async def _pre_tool_use_web_guard(
    input_data: dict, tool_use_id: str | None, context
) -> dict:
    if input_data.get("tool_name") != "WebFetch":
        return {}
    url = input_data.get("tool_input", {}).get("url", "") or ""
    if not url:
        return {}
    try:
        u = urlparse(url)
    except Exception:  # noqa: BLE001
        return _deny(f"Invalid URL: {url}")
    if u.scheme not in ("http", "https"):
        return _deny(f"WebFetch scheme 拒絕：{u.scheme} — 只允許 http/https。")
    if not u.hostname or not _is_public_ip(u.hostname):
        return _deny(f"WebFetch 拒絕：{u.hostname} 解析到內部/迴環位址（SSRF 防護）。")
    return {}


def guest_role() -> RoleContext:
    if _POLICY is None:
        raise RuntimeError(f"guest policy missing/invalid: {_POLICY_FILE}")
    return RoleContext(
        name="guest",
        cwd=GUEST_CWD,
        config_dir=GUEST_CONFIG_DIR,
        system_prompt_override=_POLICY["system_prompt"],  # 純字串取代整個 preset → 不載 CLAUDE.md
        setting_sources=[],                               # 跳過 CLAUDE.md / project settings
        allowed_tools=GUEST_ALLOWED_TOOLS,
        hooks={
            "PreToolUse": [
                HookMatcher(matcher="Read", hooks=[_pre_tool_use_fs_guard]),
                HookMatcher(matcher="Grep", hooks=[_pre_tool_use_fs_guard]),
                HookMatcher(matcher="Glob", hooks=[_pre_tool_use_fs_guard]),
                HookMatcher(matcher="WebFetch", hooks=[_pre_tool_use_web_guard]),
            ],
        },
    )
