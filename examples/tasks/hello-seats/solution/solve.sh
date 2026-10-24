#!/bin/bash
set -eu

other=$([ "$SEAT" = p1 ] && echo p2 || echo p1)
echo "$SEAT posts its note"
curl -fsS http://world:8080/note -H 'Content-Type: application/json' \
  -d "{\"author\": \"$SEAT\", \"text\": \"hello from $SEAT\"}"
echo
until curl -fsS http://world:8080/notes | grep -q "\"author\": \"$other\""; do
  sleep 2
done
echo "$SEAT sees a note from $other"
