#!/usr/bin/env bash
# Alinha o repo local com origin/teste (Cursor PC ↔ nuvem).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== Agro Consulta — alinhar branch teste =="
git fetch origin teste

if ! git rev-parse --verify teste >/dev/null 2>&1; then
  git checkout -b teste origin/teste
else
  git checkout teste
fi

if ! git diff-index --quiet HEAD -- 2>/dev/null; then
  echo ""
  echo "AVISO: há alterações locais não commitadas."
  echo "  Commit, stash ou descarte antes do pull se quiser evitar conflito."
  echo ""
fi

git pull --ff-only origin teste || {
  echo ""
  echo "Pull falhou (provável divergência). Opções:"
  echo "  git status"
  echo "  git stash push -m wip && git pull --ff-only origin teste && git stash pop"
  exit 1
}

echo ""
echo "Branch: $(git branch --show-current) @ $(git rev-parse --short HEAD)"
echo "Últimos commits:"
git log -3 --oneline
echo ""
echo "Dica: grep CHECKPOINT no banana.md ou abra a seção ## CHECKPOINT DE ATUALIZAÇÃO."
