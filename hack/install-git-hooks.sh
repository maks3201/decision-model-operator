#!/usr/bin/env bash
# Install versioned git hooks from .githooks/ into .git/hooks/ (no git config changes).
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
hooks_dir="$(git rev-parse --git-path hooks)"
mkdir -p "${hooks_dir}"

for hook in "${root}"/.githooks/*; do
  name="$(basename "${hook}")"
  install -m 0755 "${hook}" "${hooks_dir}/${name}"
  echo "installed ${name} -> ${hooks_dir}/${name}"
done
