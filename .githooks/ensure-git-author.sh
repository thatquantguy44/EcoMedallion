#!/usr/bin/env bash
set -euo pipefail
shopt -s nocasematch

expected_name="${GIT_HOOK_EXPECTED_AUTHOR_NAME:-Joshua Lutkemuller}"
expected_email="${GIT_HOOK_EXPECTED_AUTHOR_EMAIL:-110635594+joshualutkemuller@users.noreply.github.com}"

agent_pattern='(claude|codex|openai|anthropic|chatgpt|copilot|assistant|agent)'

read_ident() {
  local kind="$1"
  git var "GIT_${kind}_IDENT" 2>/dev/null \
    | sed -E 's/^(.*) <([^>]*)> [0-9]+ [-+][0-9]{4}$/\1|\2/'
}

fail_identity() {
  local label="$1"
  local actual_name="$2"
  local actual_email="$3"

  cat >&2 <<EOF
Commit blocked: ${label} identity must be Joshua Lutkemuller.

Expected:
  ${expected_name} <${expected_email}>

Actual:
  ${actual_name} <${actual_email}>

Fix this checkout with:
  git config user.name "${expected_name}"
  git config user.email "${expected_email}"

If an agent set GIT_AUTHOR_NAME/GIT_AUTHOR_EMAIL or GIT_COMMITTER_NAME/GIT_COMMITTER_EMAIL,
unset those environment variables before committing.
EOF
  exit 1
}

check_ident() {
  local label="$1"
  local ident actual_name actual_email label_lower

  ident="$(read_ident "$label")"
  actual_name="${ident%%|*}"
  actual_email="${ident#*|}"
  label_lower="$(printf '%s' "$label" | tr '[:upper:]' '[:lower:]')"

  if [[ "${actual_name}" != "${expected_name}" || "${actual_email}" != "${expected_email}" ]]; then
    fail_identity "$label_lower" "$actual_name" "$actual_email"
  fi

  if [[ "${actual_name}" =~ $agent_pattern || "${actual_email}" =~ $agent_pattern ]]; then
    fail_identity "$label_lower" "$actual_name" "$actual_email"
  fi
}

check_ident AUTHOR
check_ident COMMITTER
