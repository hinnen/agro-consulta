#!/usr/bin/env bash
# Alinha branch teste local com origin/teste (Linux / Cloud Agent).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

read_version() {
  if [[ -f VERSION ]]; then
    tr -d ' \n\r' < VERSION
  else
    echo "(sem VERSION)"
  fi
}

echo "== Agro Consulta — alinhar branch teste =="

branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$branch" != "teste" ]]; then
  echo "Branch atual: $branch (esperado: teste)"
  read -r -p "Trocar para teste? (s/N) " ok
  if [[ "$ok" =~ ^[sS]$ ]]; then
    git checkout teste
  else
    echo "Abortado."
    exit 1
  fi
fi

echo "Fetching origin/teste..."
git fetch origin teste

if [[ -n "$(git status --porcelain)" ]]; then
  echo ""
  echo "Há alterações locais. Commit ou git stash antes do pull."
  git status -sb
  exit 2
fi

behind="$(git rev-list --count HEAD..origin/teste 2>/dev/null || echo 0)"
ahead="$(git rev-list --count origin/teste..HEAD 2>/dev/null || echo 0)"

echo ""
echo "VERSION local:  $(read_version)"
echo "Commits: atrás de origin/teste = $behind | à frente = $ahead"

if [[ "$ahead" -gt 0 && "$behind" -gt 0 ]]; then
  echo ""
  echo "Divergiu. Resolva manualmente — não auto-pull."
  exit 3
fi

if [[ "$behind" -eq 0 ]]; then
  echo ""
  echo "Já alinhado com origin/teste."
  exit 0
fi

echo ""
echo "Puxando origin/teste (rebase)..."
git pull --rebase origin teste

echo ""
echo "OK. VERSION agora: $(read_version)"
