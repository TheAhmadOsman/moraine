#!/usr/bin/env bash
# Point this clone's git at the repo-managed hooks in .githooks/.
# Idempotent — safe to re-run.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [[ ! -d .githooks ]]; then
  echo "error: .githooks/ not found at $repo_root" >&2
  exit 1
fi

git config core.hooksPath .githooks
echo "[install-hooks] core.hooksPath = .githooks"
echo "[install-hooks] hooks now active:"
ls -1 .githooks | sed 's/^/  /'
