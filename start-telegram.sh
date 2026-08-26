#!/usr/bin/env bash
# Start telegram-listener (agent-bridge adapter).
# All deployment-specific config lives in .env (gitignored, see .env.example).
# Secrets are pulled at runtime (e.g. via 1Password Connect) — never written to disk.
set -euo pipefail

SVC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$SVC_DIR/.venv"

export PATH="$HOME/.local/bin:$HOME/bin:/usr/local/bin:/usr/bin:/bin"
if [[ -d "$HOME/.nvm/versions/node" ]]; then
  NVM_LATEST=$(ls -1 "$HOME/.nvm/versions/node" 2>/dev/null | sort -V | tail -1 || true)
  [[ -n "${NVM_LATEST:-}" ]] && export PATH="$HOME/.nvm/versions/node/$NVM_LATEST/bin:$PATH"
fi

if [[ ! -d "$VENV" ]]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --upgrade pip
  "$VENV/bin/pip" install --quiet -r "$SVC_DIR/requirements.txt"
fi

# ── Load deployment config ──
if [[ ! -f "$SVC_DIR/.env" ]]; then
  echo "FATAL: $SVC_DIR/.env missing — copy .env.example and fill it in" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1091
source "$SVC_DIR/.env"
set +a

# Secret-manager read command（預設 1Password Connect wrapper；可在 .env 覆蓋）
SECRET_READ_CMD="${SECRET_READ_CMD:-$HOME/bin/op-connect-read}"

# ── Fetch secrets at runtime（ref 指到哪個 secret manager 由 .env 決定）──
if [[ -n "${OP_TELEGRAM_TOKEN_REF:-}" ]]; then
  TELEGRAM_BOT_TOKEN=$("$SECRET_READ_CMD" "$OP_TELEGRAM_TOKEN_REF")
  export TELEGRAM_BOT_TOKEN
fi
: "${TELEGRAM_BOT_TOKEN:?TELEGRAM_BOT_TOKEN not set (set OP_TELEGRAM_TOKEN_REF or TELEGRAM_BOT_TOKEN in .env)}"
: "${TG_OWNER_USER_IDS:?TG_OWNER_USER_IDS not set in .env}"

# 計費安全：訂閱 OAuth 模式 — 兩個變數都不能存在
unset CLAUDE_CODE_OAUTH_TOKEN ANTHROPIC_API_KEY

exec "$VENV/bin/python" "$SVC_DIR/telegram_listener.py"
