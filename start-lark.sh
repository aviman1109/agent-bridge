#!/usr/bin/env bash
# Start lark-listener (agent-bridge adapter).
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

# ── Fetch secrets at runtime ──
if [[ -n "${OP_LARK_APP_ID_REF:-}" ]]; then
  LARK_APP_ID=$("$SECRET_READ_CMD" "$OP_LARK_APP_ID_REF")
  export LARK_APP_ID
fi
if [[ -n "${OP_LARK_APP_SECRET_REF:-}" ]]; then
  LARK_APP_SECRET=$("$SECRET_READ_CMD" "$OP_LARK_APP_SECRET_REF")
  export LARK_APP_SECRET
fi
: "${LARK_APP_ID:?LARK_APP_ID not set (set OP_LARK_APP_ID_REF or LARK_APP_ID in .env)}"
: "${LARK_APP_SECRET:?LARK_APP_SECRET not set}"
: "${LARK_OWNER_OPEN_IDS:?LARK_OWNER_OPEN_IDS not set in .env}"

# 計費安全：訂閱 OAuth 模式 — 兩個變數都不能存在
unset CLAUDE_CODE_OAUTH_TOKEN ANTHROPIC_API_KEY

exec "$VENV/bin/python" "$SVC_DIR/lark_listener.py"
