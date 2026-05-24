import { chromium } from "playwright";
import { existsSync, mkdirSync, readdirSync, renameSync } from "node:fs";
import { join } from "node:path";

const OUT_DIR = "recordings";
if (!existsSync(OUT_DIR)) mkdirSync(OUT_DIR);

const VIEWPORT = { width: 1340, height: 1000 };

console.log("→ launching chromium");
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: VIEWPORT,
  recordVideo: { dir: OUT_DIR, size: VIEWPORT },
});
const page = await context.newPage();
page.on("pageerror", (e) => console.log("PAGE ERROR:", e.message));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function typeInto(sel, text) {
  await page.click(sel);
  await page.fill(sel, "");
  await page.type(sel, text, { delay: 35 });
}

console.log("→ opening http://localhost:3300");
await page.goto("http://localhost:3300", { waitUntil: "domcontentloaded" });
await page.waitForFunction(
  () => document.querySelector("#ws-state")?.textContent === "OPEN" &&
        document.querySelector("#sse-state")?.textContent === "OPEN",
  { timeout: 10_000 },
);
await sleep(2200);

console.log("→ WS panel chat");
await typeInto("#ws-text", "hello from the WebSocket panel");
await sleep(400);
await page.click("#ws-send");
await sleep(2000);

console.log("→ SSE panel chat (POST /send)");
await typeInto("#sse-text", "hello via plain HTTP POST");
await sleep(400);
await page.click("#sse-send");
await sleep(2200);

console.log("→ Ask LLM via WS");
await typeInto("#prompt", "name three TCP flags in one short line");
await sleep(400);
await page.click("#ask-ws");
// wait for first chunk to land
await page.waitForFunction(
  () => (document.querySelector("#ws-log .llm-msg .body")?.textContent ?? "").length > 0,
  { timeout: 20_000 },
).catch(() => console.log("  (no LLM chunk yet — continuing)"));
await sleep(9000);

console.log("→ Ask LLM via POST /ask");
await typeInto("#prompt", "what is HTTP/2 multiplexing in one short sentence");
await sleep(400);
await page.click("#ask-http");
await sleep(10000);

console.log("→ Kill all connections");
await page.click("#kill");
// SSE should go yellow then reconnect; WS should go red
await sleep(5000);

console.log("→ Reconnect WS");
await page.click("#reconnect-ws");
await page.waitForFunction(
  () => document.querySelector("#ws-state")?.textContent === "OPEN",
  { timeout: 5000 },
).catch(() => {});
await sleep(2500);

console.log("→ Follow-up chat after recovery");
await typeInto("#ws-text", "back online");
await sleep(400);
await page.click("#ws-send");
await sleep(2500);

console.log("→ closing context to flush video");
await context.close();
await browser.close();

// Pick up the produced webm and rename it
const files = readdirSync(OUT_DIR)
  .filter((f) => f.endsWith(".webm"))
  .map((f) => ({ f, m: 0 }));
const latest = files.sort().pop();
if (latest) {
  const src = join(OUT_DIR, latest.f);
  const dst = join(OUT_DIR, "ws-vs-sse-demo.webm");
  renameSync(src, dst);
  console.log(`\n✔ video saved → ${dst}`);
}
