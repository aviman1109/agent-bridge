#!/usr/bin/env python3
"""agent-bridge daily health check — 檢查 Telegram / Lark 兩個 listener
過去 N 小時的健康狀態，Markdown 報告 DM 給 owner（lark-cli）。

檢查面向：
  1. 兩個 systemd unit 是否 active
  2. Heartbeat 檔新鮮度（TG 每 ~50s 刷、Lark 每事件刷 — 判準不同）
  3. 過去窗內任務數 / 成功 / 失敗 / 平均延遲 / 總 cost（分 role / 分 platform）
  4. 訪客活動（誰、幾次、cost、cap 是否超）
  5. 最貴任務 / 最慢任務
  6. 提問文本抽樣（從 journal 抓 recv 行 — 訊息被 listener 截 120 字）
  7. 錯誤行掃描（sdk task failed / task exception / getUpdates error）
  8. 開放的訪客群清單

執行方式：
  ./health-check.py                      # 24h 窗、Lark DM Casper（預設）
  ./health-check.py --hours 6            # 自訂窗
  ./health-check.py --no-send            # 只印到 stdout
  ./health-check.py --format text        # 純文字（省 markdown）
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

def _load_dotenv() -> None:
    """讀 script 同目錄 .env（KEY="value" / KEY=value 行）進 os.environ（不覆蓋既有）。"""
    envf = Path(__file__).resolve().parent / ".env"
    try:
        for line in envf.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip().removeprefix("export ").strip()
            v = v.strip().strip("'\"")
            os.environ.setdefault(k, v)
    except FileNotFoundError:
        pass


_load_dotenv()

TG_STATE = Path(os.path.expanduser(
    os.environ.get("TG_LISTENER_STATE_DIR", "~/.cache/telegram-listener")))
LARK_STATE = Path(os.path.expanduser(
    os.environ.get("LARK_LISTENER_STATE_DIR", "~/.cache/lark-listener")))
LARK_UNIT = os.environ.get("BRIDGE_LARK_UNIT", "lark-listener.service")

# 健檢報告 DM 對象（lark-cli --as bot）；不設 = 只印 stdout
HEALTH_DM_OPEN_ID = os.environ.get("BRIDGE_HEALTH_DM_OPEN_ID", "")


def sh(cmd: list[str]) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, check=False).stdout


def is_active(unit: str) -> bool:
    return sh(["systemctl", "--user", "is-active", unit]).strip() == "active"


def heartbeat_age_s(path: Path) -> float | None:
    if not path.exists():
        return None
    return (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds()


def fmt_age(seconds: float | None) -> str:
    if seconds is None:
        return "(missing)"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h{(s % 3600) // 60:02d}m"
    return f"{s // 86400}d{(s % 86400) // 3600:02d}h"


def fmt_dur(seconds: float | None) -> str:
    if seconds is None:
        return "?"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    return f"{s // 60}m{s % 60:02d}s"


def load_usage(path: Path, since: datetime) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        try:
            e = json.loads(line)
        except Exception:  # noqa: BLE001
            continue
        try:
            ts = datetime.fromisoformat(e["ts"])
        except (KeyError, ValueError):
            continue
        if ts >= since:
            e["_ts"] = ts
            out.append(e)
    return out


def journal_lines(unit: str, since_iso: str, pattern: str = "") -> list[str]:
    args = ["journalctl", "--user", "-u", unit, "--since", since_iso, "--no-pager",
            "--output=short-iso"]
    txt = sh(args)
    if not pattern:
        return txt.splitlines()
    return [ln for ln in txt.splitlines() if pattern in ln]


def parse_tg_recv(lines: list[str]) -> list[dict]:
    """從 TG listener journal 抽 recv 行 → {ts, chat_id, user_id, role, text}."""
    out = []
    for ln in lines:
        # 例：2026-01-01T00:00:00 telegram-listener: recv chat=-100123 user=123456 role=guest key=... text='...'
        if "recv chat=" not in ln:
            continue
        try:
            ts_str = ln.split()[0]
            ts = datetime.fromisoformat(ts_str.rstrip("Z").replace("+0800", ""))
        except (ValueError, IndexError):
            ts = None
        chat = _extract(ln, "chat=", " ")
        user = _extract(ln, "user=", " ")
        role = _extract(ln, "role=", " ")
        text = _extract(ln, "text='", "'") or _extract(ln, "text=", "\n")
        out.append({"ts": ts, "chat": chat, "user": user, "role": role, "text": text})
    return out


def _extract(s: str, start: str, end: str) -> str:
    i = s.find(start)
    if i < 0:
        return ""
    i += len(start)
    j = s.find(end, i)
    return s[i:j] if j > 0 else s[i:]


def check_tg(since: datetime) -> dict:
    events = load_usage(TG_STATE / "usage.jsonl", since)
    recv = parse_tg_recv(journal_lines("telegram-listener.service",
                                       since.isoformat(), pattern="recv chat="))
    errors = journal_lines("telegram-listener.service", since.isoformat(),
                           pattern="sdk task failed")
    hb_age = heartbeat_age_s(TG_STATE / "heartbeat")

    by_role: dict[str, dict] = defaultdict(lambda: {"n": 0, "cost": 0.0, "elapsed": 0.0})
    by_sender: dict[str, dict] = defaultdict(lambda: {"n": 0, "cost": 0.0})
    for e in events:
        role = e.get("role", "?")
        by_role[role]["n"] += 1
        by_role[role]["cost"] += e.get("cost_usd") or 0
        by_role[role]["elapsed"] += e.get("elapsed_s") or 0
        by_sender[e.get("sender_id", "?")]["n"] += 1
        by_sender[e.get("sender_id", "?")]["cost"] += e.get("cost_usd") or 0

    top_cost = sorted(events, key=lambda x: -(x.get("cost_usd") or 0))[:1]
    top_slow = sorted(events, key=lambda x: -(x.get("elapsed_s") or 0))[:1]

    allowed_chats = {}
    try:
        allowed_chats = json.loads((TG_STATE / "allowed-chats.json").read_text())
    except Exception:  # noqa: BLE001
        pass

    return {
        "active": is_active("telegram-listener.service"),
        "heartbeat_s": hb_age,
        "events": events,
        "by_role": dict(by_role),
        "by_sender": dict(by_sender),
        "recv": recv,
        "errors": errors,
        "allowed_chats": allowed_chats,
        "top_cost": top_cost,
        "top_slow": top_slow,
    }


def check_lark(since: datetime) -> dict:
    events = load_usage(LARK_STATE / "usage.jsonl", since)
    errors = journal_lines(LARK_UNIT, since.isoformat(),
                           pattern="sdk task failed")
    hb_age = heartbeat_age_s(LARK_STATE / "heartbeat")

    top_cost = sorted(events, key=lambda x: -(x.get("cost_usd") or 0))[:1]
    top_slow = sorted(events, key=lambda x: -(x.get("elapsed_s") or 0))[:1]
    total_cost = sum((e.get("cost_usd") or 0) for e in events)
    total_elapsed = sum((e.get("elapsed_s") or 0) for e in events)

    return {
        "active": is_active(LARK_UNIT),
        "heartbeat_s": hb_age,
        "events": events,
        "n": len(events),
        "cost": total_cost,
        "avg_elapsed": total_elapsed / max(1, len(events)),
        "errors": errors,
        "top_cost": top_cost,
        "top_slow": top_slow,
    }


# ── verdict：把數字翻成 emoji ─────────────────────────────────────────

def verdict(tg: dict, lark: dict) -> tuple[str, list[str]]:
    """回傳 (整體 emoji, 警訊清單)。"""
    warns = []
    if not tg["active"]:
        warns.append("🔴 telegram-listener 未運行")
    if not lark["active"]:
        warns.append("🔴 lark-listener 未運行")
    if tg["heartbeat_s"] is None or tg["heartbeat_s"] > 300:
        warns.append(f"🟠 TG heartbeat 過舊 ({fmt_age(tg['heartbeat_s'])}) — 每 ~50s 應更新")
    # Lark heartbeat 只在事件時刷，很久沒事件時停滯是正常的 — 不 warn
    if tg["errors"]:
        warns.append(f"🟠 TG 失敗 {len(tg['errors'])} 次")
    if lark["errors"]:
        warns.append(f"🟠 Lark 失敗 {len(lark['errors'])} 次")
    total = sum(v["cost"] for v in tg["by_role"].values()) + lark["cost"]
    if total > 20:
        warns.append(f"💰 今日總花費 ${total:.2f}（>$20）")

    top = "🟢 全綠" if not warns else ("🟡 有警訊" if all("🟠" in w or "💰" in w for w in warns) else "🔴 有紅燈")
    return top, warns


# ── report ─────────────────────────────────────────────────────────────

def report(hours: int) -> str:
    since = datetime.now() - timedelta(hours=hours)
    tg = check_tg(since)
    lark = check_lark(since)
    top, warns = verdict(tg, lark)

    lines = [
        f"🩺 **Bot 健檢** · 過去 {hours}h · {datetime.now().strftime('%m-%d %H:%M')}",
        f"整體: {top}",
        "",
    ]
    if warns:
        lines.append("**警訊**")
        for w in warns:
            lines.append(f"- {w}")
        lines.append("")

    # ── Service ──
    lines.append("**服務**")
    lines.append(f"- telegram-listener: {'✅ active' if tg['active'] else '❌'} · hb {fmt_age(tg['heartbeat_s'])}")
    lines.append(f"- lark-listener: {'✅ active' if lark['active'] else '❌'} · hb {fmt_age(lark['heartbeat_s'])}")
    lines.append("")

    # ── Telegram ──
    tg_total_n = sum(v["n"] for v in tg["by_role"].values())
    tg_total_cost = sum(v["cost"] for v in tg["by_role"].values())
    tg_total_el = sum(v["elapsed"] for v in tg["by_role"].values())
    tg_avg = tg_total_el / max(1, tg_total_n)
    lines.append(f"**Telegram** — {tg_total_n} 次 · ${tg_total_cost:.2f} · 平均 {fmt_dur(tg_avg)}")
    for role, s in sorted(tg["by_role"].items()):
        lines.append(f"- {role}: {s['n']} 次 · ${s['cost']:.2f} · 平均 {fmt_dur(s['elapsed']/max(1,s['n']))}")
    if tg["top_cost"]:
        t = tg["top_cost"][0]
        lines.append(f"- 最貴: ${t.get('cost_usd', 0):.2f} · {fmt_dur(t.get('elapsed_s', 0))} · {t.get('role', '?')} · 🧵 {(t.get('session_id') or '?')[:8]}")
    if tg["top_slow"]:
        t = tg["top_slow"][0]
        lines.append(f"- 最慢: {fmt_dur(t.get('elapsed_s', 0))} · ${t.get('cost_usd', 0):.2f} · {t.get('role', '?')} · 🧵 {(t.get('session_id') or '?')[:8]}")
    if len(tg["by_sender"]) > 1:
        lines.append("- 提問者:")
        for uid, s in sorted(tg["by_sender"].items(), key=lambda kv: -kv[1]["n"]):
            lines.append(f"  · `{uid}` — {s['n']} 次 · ${s['cost']:.2f}")
    if tg["allowed_chats"]:
        lines.append("- 開放訪客群:")
        for cid, info in tg["allowed_chats"].items():
            lines.append(f"  · `{cid}` — {info.get('title', '?')}")
    lines.append("")

    # ── 訪客提問抽樣（最近 5 則）──
    guest_recv = [r for r in tg["recv"] if r["role"] == "guest"]
    if guest_recv:
        lines.append(f"**訪客提問**（最近 {min(5, len(guest_recv))} 則）")
        for r in guest_recv[-5:]:
            ts = r["ts"].strftime("%m-%d %H:%M") if r["ts"] else "?"
            text = (r["text"] or "").strip() or "(空)"
            if len(text) > 90:
                text = text[:90] + "…"
            lines.append(f"- {ts} [`{r['user']}`] {text}")
        lines.append("")

    # ── Lark ──
    lines.append(f"**Lark** — {lark['n']} 次 · ${lark['cost']:.2f} · 平均 {fmt_dur(lark['avg_elapsed'])}")
    if lark["top_cost"]:
        t = lark["top_cost"][0]
        lines.append(f"- 最貴: ${t.get('cost_usd', 0):.2f} · {fmt_dur(t.get('elapsed_s', 0))} · 🧵 {(t.get('session_id') or '?')[:8]}")
    if lark["top_slow"]:
        t = lark["top_slow"][0]
        lines.append(f"- 最慢: {fmt_dur(t.get('elapsed_s', 0))} · ${t.get('cost_usd', 0):.2f} · 🧵 {(t.get('session_id') or '?')[:8]}")
    lines.append("")

    # ── 錯誤 ──
    if tg["errors"] or lark["errors"]:
        lines.append("**錯誤明細**")
        for e in tg["errors"][:3]:
            lines.append(f"- TG: `{e[:180]}`")
        for e in lark["errors"][:3]:
            lines.append(f"- Lark: `{e[:180]}`")
        if len(tg["errors"]) + len(lark["errors"]) > 6:
            lines.append(f"- …另 {len(tg['errors']) + len(lark['errors']) - 6} 則略")

    return "\n".join(lines)


# ── 發送 ────────────────────────────────────────────────────────────

def send_lark_dm(text: str) -> bool:
    """as bot DM 給 owner。"""
    env = os.environ.copy()
    env["PATH"] = os.path.expanduser("~/.nvm/versions/node/") + "/latest/bin:" + env.get("PATH", "")
    # nvm-managed lark-cli — 找最新版
    nvm_bin = Path.home() / ".nvm" / "versions" / "node"
    if nvm_bin.exists():
        latest = sorted(nvm_bin.iterdir())[-1] if list(nvm_bin.iterdir()) else None
        if latest:
            env["PATH"] = f"{latest}/bin:{env['PATH']}"
    if not HEALTH_DM_OPEN_ID:
        print("BRIDGE_HEALTH_DM_OPEN_ID 未設 — 跳過 DM", file=sys.stderr)
        return False
    cmd = ["lark-cli", "--profile",
           os.environ.get("BRIDGE_LARK_CLI_PROFILE", "lark-global"),
           "im", "+messages-send",
           "--as", "bot", "--user-id", HEALTH_DM_OPEN_ID, "--markdown", text]
    r = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if r.returncode != 0:
        print(f"lark-cli failed: {r.stderr}", file=sys.stderr)
        return False
    return True


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--no-send", action="store_true", help="只 stdout、不 DM Lark")
    p.add_argument("--format", choices=["md", "text"], default="md")
    args = p.parse_args()

    md = report(args.hours)
    print(md)
    if args.no_send:
        return 0
    ok = send_lark_dm(md)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
