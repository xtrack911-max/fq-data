def evaluate(status, cfg, now_ms):
    """Fail-closed freshness gate. Mirrored exactly by dashboard/gate.js (parity-tested)."""
    why = []
    if not status or status.get("schema") != 1:
        return {"signals_allowed": False, "reasons": ["status missing or invalid"]}
    ga = status.get("generated_at")
    if not ga or now_ms - ga > cfg["max_status_age_min"] * 60000:
        why.append("status stale")
    col = status.get("collector") or {}
    if col.get("consecutive_failures", 0) >= cfg["max_consecutive_failures"]:
        why.append(f"collector failures: {col['consecutive_failures']}")
    series = status.get("series") or {}
    for sym in cfg["symbols"]:
        for ds in ("klines_1m", "mark_1m", "funding", "metrics"):
            key = f"{ds}:{sym}"
            s = series.get(key)
            if not s:
                why.append(f"{key} missing")
                continue
            ts = s.get("last_close") if s.get("last_close") is not None else s.get("last_ts")
            if ts is None or now_ms - ts > cfg["max_age_min"][ds] * 60000:
                why.append(f"{key} stale")
            if s.get("recent_missing", 0) > cfg["recent_missing_allowed"]:
                why.append(f"{key} gaps: {s['recent_missing']}")
    return {"signals_allowed": len(why) == 0, "reasons": why}
