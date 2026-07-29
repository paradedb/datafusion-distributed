#!/bin/bash
# scripts/sync-upstream.sh
#
# Wrapper script to execute the centralized upstream sync logic.

set -euo pipefail

# 1. Define repository-specific configuration
export UPSTREAM_REPO="datafusion-contrib/datafusion-distributed"
export UPSTREAM_REPO_URL="https://github.com/datafusion-contrib/datafusion-distributed.git"
export TARGET_REPO="paradedb/datafusion-distributed"
export TARGET_BRANCH="main"
export UPSTREAM_BRANCH="main"

# 2. Define the URL to the centralized script
# Using the raw content URL from the central repository
CORE_SCRIPT_URL="https://raw.githubusercontent.com/paradedb/actions/v8/scripts/sync-core.sh"

# 3. Download and source the core logic as an API
TMP_SCRIPT=$(mktemp)
curl -fsSL "$CORE_SCRIPT_URL" -o "$TMP_SCRIPT"
# shellcheck source=/dev/null
source "$TMP_SCRIPT"
rm -f "$TMP_SCRIPT"

# The shared rebase workflow falls back to <!here> when a GitHub user cannot be
# mapped to Slack. Route this repo's upstream rebase alerts to the owning group.
if [[ "${1:-}" == "rebase" || "${GITHUB_WORKFLOW:-}" == "Upstream Rebase" || "${WORKFLOW:-}" == "Upstream Rebase" ]]; then
    slack_mention_for_user() {
        echo "<!subteam^S0BLH15TWRK|@datafusion-maintainers>"
    }
fi

# 4. Only execute the command router if run directly (not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    sync_core_main "$@"
fi
