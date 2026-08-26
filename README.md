# agent-bridge

**IM bot → Claude Agent SDK execution layer.** Talk to a full [Claude Code](https://code.claude.com) agent from Telegram or Lark — with per-chat session resume, live streaming progress, usage accounting, and a hard-sandboxed **guest mode** so you can safely open the bot to group members who are not you.

```
Telegram getUpdates ──┐
                      ├─→ adapter ─→ core.Bridge ─→ claude-agent-sdk (headless)
Lark WebSocket ───────┘                 │                 │ streaming events
                                        │                 ▼
                                        │          progress rendering
                                        │   (TG editMessageText / Lark card patch)
                                        ▼
                          sessions.json / usage.jsonl (per state dir)
```

## Why not just use the Claude app?

The native Claude Code apps are for *you*. agent-bridge solves the adjacent problems:

- **Guests**: group members can @ the bot and query your workspace **read-only**, inside an SDK-enforced sandbox — you'd never hand them a real `bypassPermissions` session.
- **In-IM workflow**: answers arrive where the conversation already is, with live tool-by-tool progress (message edits / card patches), not in another app.
- **Accounting**: every task is logged to `usage.jsonl` with platform, sender, role, cost — multi-user cost attribution and per-guest daily caps.

## Components

| File | Role |
|---|---|
| `core.py` | Platform-agnostic SDK execution: session_key ↔ session_id resume, per-key serialization + global concurrency semaphore, timeouts, usage log, role abstraction |
| `guest.py` | Guest sandbox role: tool allowlist (Read/Grep/Glob/WebSearch/WebFetch only), PreToolUse path guard driven by `guest-policy.json`, WebFetch SSRF guard, fail-closed when policy is missing |
| `telegram_listener.py` | Telegram adapter: getUpdates long-poll, owner/guest role gating, `/allow`-managed group allowlist, message-edit streaming, photo/document input, per-guest daily cost cap |
| `lark_listener.py` | Lark adapter: WS events, interactive-card streaming (in-place patch), thread-scoped sessions. Owner-only; predates `core.py` and will converge onto it |
| `health-check.py` | Daily health report: unit/heartbeat status, task counts, per-role cost, guest activity, error scan — DM'd via `lark-cli` |
| `systemd/*.service` | User-unit templates |

## Security model (guest mode)

Enforced by SDK mechanisms, **not** by prompt:

1. `setting_sources=[]` + plain-string system prompt — the guest agent never loads your `CLAUDE.md`, its `@import`s, or auto-memory.
2. `allowed_tools` whitelist — Bash / Edit / Write / Task / all MCP tools simply don't exist for guests.
3. `PreToolUse` hook on Read/Grep/Glob — absolute path must be inside `read_roots`, and must not match any `deny_patterns` glob (both from `guest-policy.json`, which is **gitignored because the deny list itself reveals what's sensitive**).
4. `PreToolUse` hook on WebFetch — SSRF guard (blocks localhost / RFC1918 / link-local / loopback).
5. Per-guest isolated sessions and a daily cost cap.
6. **Fail-closed**: no policy file → guest mode refuses to run.

## Setup

```bash
cp .env.example .env                                # fill in tokens/ids
cp guest-policy.example.json guest-policy.json      # tailor roots/deny/prompt
cp systemd/*.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now telegram-listener lark-listener
```

Secrets are fetched at start time via a pluggable `SECRET_READ_CMD` (defaults to a [1Password Connect](https://developer.1password.com/docs/connect/) wrapper); raw env vars work too. Billing note: the start scripts and units deliberately unset `ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN` so SDK subprocesses bill to the subscription OAuth of `BRIDGE_CLAUDE_CONFIG_DIR`.

## State

Per-adapter state dir (`~/.cache/telegram-listener`, `~/.cache/lark-listener`):
`sessions.json` (session map — only persisted on success, so a failed run never poisons resume), `usage.jsonl`, `allowed-chats.json` (TG guest groups), `files/` (uploads), `offset`, `heartbeat`.

## License

MIT
