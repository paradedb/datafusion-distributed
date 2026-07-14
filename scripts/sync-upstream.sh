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
CORE_SCRIPT_URL="https://raw.githubusercontent.com/paradedb/actions/v2/scripts/sync-core.sh"

# 3. Download and execute the core logic, passing through any arguments
echo "Fetching latest sync logic from central repository..."
curl -fsSL "$CORE_SCRIPT_URL" | bash -s -- "$@"
