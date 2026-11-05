const { JSDOM } = require("jsdom");
const fs = require("fs"), path = require("path");
const site = path.join(__dirname, "..", "site", "crypto");
const html = fs.readFileSync(path.join(site, "index.html"), "utf8");
const gateSrc = fs.readFileSync(path.join(site, "gate.js"), "utf8");
const inline = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m => m[1]).join("\n");
const body = html.replace(/<script[\s\S]*?<\/script>/g, "");
async function run(sc) {
  const dom = new JSDOM(body, { runScripts: "outside-only", pretendToBeVisual: true, url: "https://x.github.io/fq-data/crypto/" });
  const w = dom.window, calls = [];
  w.fetch = async u => { const k = u.split("?")[0]; return k in sc.files
    ? { ok: true, status: 200, json: async () => sc.files[k] } : { ok: false, status: 404, json: async () => ({}) }; };
  const series = { setData: d => calls.push(d.length) };
  w.LightweightCharts = { createChart: () => ({ addCandlestickSeries: () => series, timeScale: () => ({ fitContent() {} }) }) };
  w.Date.now = () => sc.now;
  w.eval(gateSrc); w.eval(inline);
  await new Promise(r => setTimeout(r, 60));
  const t = id => w.document.getElementById(id);
  const out = { name: sc.name, badge: t("b").textContent, cls: t("b").className, why: t("r").textContent,
    age: t("age").textContent, mark: t("m").textContent, fund: t("f").textContent, oi: t("o").textContent,
    note: t("n").textContent, drawn: calls };
  w.close();
  return out;
}
(async () => {
  const sc = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
  const res = []; for (const s of sc) res.push(await run(s));
  console.log(JSON.stringify(res));
})();
