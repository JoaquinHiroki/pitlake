#!/usr/bin/env bash
set -euo pipefail

databricks current-user me >/dev/null || { echo "Not authenticated. Run: databricks auth login --host <url> --profile pitlake"; exit 1; }

catalogs=("$@")
[ ${#catalogs[@]} -gt 0 ] || catalogs=(pitlake_dev pitlake_prod)

for catalog in "${catalogs[@]}"; do
  if databricks catalogs get "$catalog" >/dev/null 2>&1; then
    echo "exists:  $catalog"
  else
    databricks catalogs create "$catalog" --comment "PITLake ${catalog#pitlake_} environment"
    echo "created: $catalog"
  fi
done
