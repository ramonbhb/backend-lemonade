#!/usr/bin/env bash
# Regenerate model.yaml from Lemonade Tahiti API using lemonade-llm gera_doc.py
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <LEMONADE_API_TOKEN>"
  exit 1
fi

TOKEN="$1"
OUTPUT="${2:-model.yaml}"

curl -sL "https://gitlab.com/waltersf/lemonade-llm/-/raw/main/gera_doc.py" -o /tmp/gera_doc.py
python /tmp/gera_doc.py "$TOKEN" > "$OUTPUT"
echo "Generated $OUTPUT"
