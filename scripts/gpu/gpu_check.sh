#!/bin/bash
set -euo pipefail
exec python "$(dirname "$(readlink -f "$0")")/gpu_check.py"
