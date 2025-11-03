#!/usr/bin/env zsh
set -euo pipefail

# ----------------------------
# Config / helpers
# ----------------------------
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCS_JSON="${ROOT_DIR}/docs/docfx.json"
SITE_DIR="${ROOT_DIR}/docs/_site"

# args
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <version-folder> \"<commit message>\"" >&2
  exit 2
fi

VERSION="$1"           # e.g. 5.4, 6.0, latest
COMMIT_MSG="$2"

if [[ -z "${COMMIT_MSG// }" ]]; then
  echo "Error: commit message is required." >&2
  exit 2
fi

# Ensure we're at repo root (docs/docfx.json should exist)
if [[ ! -f "$DOCS_JSON" ]]; then
  echo "Error: ${DOCS_JSON} not found. Run this from the repo root, with docs/docfx.json present." >&2
  exit 1
fi

# Ensure working tree is clean before we start switching branches
if ! git diff-index --quiet HEAD --; then
  echo "Error: working tree has uncommitted changes. Commit or stash before publishing." >&2
  exit 1
fi

# ----------------------------
# Build docs (DocFX)
# ----------------------------
echo "🔧 Ensuring DocFX is installed/updated…"
dotnet tool update -g docfx >/dev/null

# Ensure the dotnet tools path is in PATH for this shell
export PATH="$HOME/.dotnet/tools:$PATH"

echo "🧱 Building solution (Release) to produce XML docs…"
dotnet build -c Release

echo "🧾 Generating DocFX metadata…"
docfx metadata "$DOCS_JSON"

echo "🌐 Building DocFX site…"
docfx build "$DOCS_JSON"

if [[ ! -d "$SITE_DIR" ]]; then
  echo "Error: ${SITE_DIR} was not generated." >&2
  exit 1
fi

# Keep a temp copy because switching branches would hide _site
TMP_SITE="$(mktemp -d -t docsite-XXXXXXXX)"
trap 'rm -rf "$TMP_SITE"' EXIT
rsync -a "$SITE_DIR"/ "$TMP_SITE"/

# ----------------------------
# Publish to gh-pages
# ----------------------------
echo "📦 Preparing to publish to gh-pages/${VERSION}…"

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD || echo HEAD)"
echo "Current branch: ${CURRENT_BRANCH}"

# Fetch gh-pages (create if missing)
if git show-ref --quiet refs/heads/gh-pages; then
  :
else
  git fetch origin gh-pages:gh-pages || true
fi

if git show-ref --quiet refs/heads/gh-pages; then
  git checkout gh-pages
else
  echo "🔀 Creating gh-pages branch…"
  git checkout --orphan gh-pages
  # wipe working tree
  git rm -rf . >/dev/null 2>&1 || true
  # add a .nojekyll to disable Jekyll on Pages (keeps _ folders)
  echo > .nojekyll
  git add .nojekyll
  git commit -m "chore: initialize gh-pages"
fi

# Ensure we’re on gh-pages now
if [[ "$(git rev-parse --abbrev-ref HEAD)" != "gh-pages" ]]; then
  echo "Error: failed to switch to gh-pages." >&2
  exit 1
fi

# Replace the version folder contents
echo "🧹 Cleaning gh-pages/${VERSION}…"
rm -rf "${VERSION}"
mkdir -p "${VERSION}"

echo "📤 Copying site to gh-pages/${VERSION}…"
rsync -a --delete "$TMP_SITE"/ "${VERSION}"/

# Optional: keep a lightweight homepage that links to versions (only if missing)
if [[ ! -f index.html ]]; then
  cat > index.html <<'HTML'
<!DOCTYPE html><html><head><meta charset="utf-8"/><title>Documentation</title>
<style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,Noto Sans,sans-serif;max-width:720px;margin:3rem auto;padding:0 1rem;line-height:1.6}</style>
</head><body><h1>Documentation</h1><p>Select a version folder.</p></body></html>
HTML
  git add index.html
fi

echo "✅ Committing changes…"
git add "${VERSION}"
git commit -m "${COMMIT_MSG}"

echo "⬆️ Pushing gh-pages…"
git push -u origin gh-pages

echo "↩️ Switching back to ${CURRENT_BRANCH}…"
git checkout "${CURRENT_BRANCH}"

echo "🎉 Done. Published docs are under gh-pages/${VERSION}"
