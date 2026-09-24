#!/usr/bin/env bash
# One-time setup: create the dev and prod catalogs. Everything inside them is created by the bundle.
# Safe to re-run.
set -euo pipefail

databricks current-user me >/dev/null || { echo "Not authenticated. Run: databricks auth login --host <url> --profile pitlake"; exit 1; }

for catalog in pitlake_dev pitlake_prod; do
  if databricks catalogs get "$catalog" >/dev/null 2>&1; then
    echo "exists:  $catalog"
  else
    databricks catalogs create "$catalog" --comment "PITLake ${catalog#pitlake_} environment"
    echo "created: $catalog"
  fi
done
