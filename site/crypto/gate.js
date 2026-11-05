// Mirror of mxq/gate.py. Keep in sync; tests/test_gate.py checks parity on shared vectors.
function evaluateGate(status, cfg, now) {
  const why = [];
  if (!status || status.schema !== 1) return { signals_allowed: false, reasons: ["status missing or invalid"] };
  const ga = status.generated_at;
  if (!ga || now - ga > cfg.max_status_age_min * 60000) why.push("status stale");
  const col = status.collector || {};
  const cf = col.consecutive_failures || 0;
  if (cf >= cfg.max_consecutive_failures) why.push(`collector failures: ${cf}`);
  const series = status.series || {};
  for (const sym of cfg.symbols) {
    for (const ds of ["klines_1m", "mark_1m", "funding", "metrics"]) {
      const key = `${ds}:${sym}`;
      const s = series[key];
      if (!s) { why.push(`${key} missing`); continue; }
      const ts = (s.last_close !== undefined && s.last_close !== null) ? s.last_close : s.last_ts;
      if (ts === undefined || ts === null || now - ts > cfg.max_age_min[ds] * 60000) why.push(`${key} stale`);
      const rm = s.recent_missing || 0;
      if (rm > cfg.recent_missing_allowed) why.push(`${key} gaps: ${rm}`);
    }
  }
  return { signals_allowed: why.length === 0, reasons: why };
}
if (typeof module !== "undefined") module.exports = { evaluateGate };
