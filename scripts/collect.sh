#!/usr/bin/env bash
# One cycle (delayed mode): daily catch-up -> build site+data -> publish to data-live only if changed -> alerts.
set +e
export GATE_PROFILE=delayed
rm -f publish.flag alert.txt
python catchup.py; C=$?
python build_live.py --out live_out; B=$?
P=0
if [ -f publish.flag ] && [ -f live_out/crypto/data/status.json ]; then
  ( cd live_out && git init -q && git checkout -q -b data-live && git add -A &&
    git -c user.name=ops-bot -c user.email=ops-bot@users.noreply.github.com commit -qm "site" &&
    git push -q -f "https://x-access-token:${GH_TOKEN}@github.com/${GITHUB_REPOSITORY}.git" data-live ); P=$?
  gh api -X POST "repos/${GITHUB_REPOSITORY}/pages/builds" >/dev/null 2>&1 || true
fi
if [ -f alert.txt ] && [ -n "$NTFY_TOPIC" ]; then curl -fsS -m 15 -d @alert.txt "https://ntfy.sh/${NTFY_TOPIC}" >/dev/null || true; fi
if [ $C -eq 0 ] && [ $B -eq 0 ] && [ $P -eq 0 ] && [ -n "$HC_PING_URL" ]; then curl -fsS -m 10 "$HC_PING_URL" >/dev/null || true; fi
[ $C -eq 0 ] && [ $B -eq 0 ] && [ $P -eq 0 ]
