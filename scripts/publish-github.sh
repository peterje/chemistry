#!/usr/bin/env bash
set -euo pipefail

# Publish this directory as a GitHub template repository.
# Requires a personal GitHub token with repo create permission.

OWNER="${GITHUB_OWNER:-peterje}"
NAME="${GITHUB_REPO:-alchemy-starter}"

gh repo create "${OWNER}/${NAME}" \
  --public \
  --source=. \
  --remote=origin \
  --push \
  --description "Alchemy + React starter with CI, oxlint anti-slop, oxfmt, and React Doctor"

gh api -X PATCH "repos/${OWNER}/${NAME}" -f is_template=true
