#!/usr/bin/env bash
# One scheduled cycle: collect -> build feed -> publish -> alerts. Always publishes status, even on failure.
set +e
python collect.py; C=$?
python build_live.py --out live_out; B=$?
if [ -f live_out/status.json ]; then
  ( cd live_out && git init -q && git checkout -q -b data-live && git add -A &&
    git -c user.name=ops-bot -c user.email=ops-bot@users.noreply.github.com commit -qm "live" &&
    git push -q -f "https://x-access-token:${GH_TOKEN}@github.com/${GITHUB_REPOSITORY}.git" data-live ); P=$?
else P=1; fi
if [ -f alert.txt ] && [ -n "$NTFY_TOPIC" ]; then curl -fsS -m 15 -d @alert.txt "https://ntfy.sh/${NTFY_TOPIC}" >/dev/null || true; fi
if [ $C -eq 0 ] && [ $B -eq 0 ] && [ $P -eq 0 ] && [ -n "$HC_PING_URL" ]; then curl -fsS -m 10 "$HC_PING_URL" >/dev/null || true; fi
[ $C -eq 0 ] && [ $B -eq 0 ] && [ $P -eq 0 ]
