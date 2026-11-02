const { evaluateGate } = require("../dashboard/gate.js");
const vec = JSON.parse(require("fs").readFileSync(process.argv[2], "utf8"));
let bad = 0;
for (const c of vec) {
  const got = evaluateGate(c.status, c.cfg, c.now);
  if (JSON.stringify(got) !== JSON.stringify(c.expected)) {
    bad++;
    console.error("MISMATCH", c.name, JSON.stringify(got), JSON.stringify(c.expected));
  }
}
process.exit(bad ? 1 : 0);
