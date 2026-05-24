import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3300;
const LLM_BASE_URL = process.env.LLM_BASE_URL ?? "http://localhost:11434/v1";
const LLM_API_KEY = process.env.LLM_API_KEY ?? process.env.OPENROUTER_API_KEY ?? "";
const MODEL = process.env.LLM_MODEL ?? process.env.OPENROUTER_MODEL ?? "gemma4:e2b";
const VISITOR_ASK_LIMIT = Number(process.env.VISITOR_ASK_LIMIT) || 4;
const VISITOR_ASK_WINDOW_MS = 24 * 60 * 60 * 1000;
const MAX_BODY_BYTES = 64 * 1024;
const MAX_WS_PAYLOAD = 64 * 1024;
const MAX_USER_LEN = 40;
const MAX_TEXT_LEN = 1000;
const MAX_PROMPT_LEN = 4000;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS ?? "")
  .split(",").map((s) => s.trim()).filter(Boolean);

type WireMsg =
  | { kind: "chat"; id: string; user: string; text: string; via: "ws" | "sse"; at: string }
  | { kind: "llm-start"; id: string; prompt: string; model: string; at: string }
  | { kind: "llm-chunk"; id: string; delta: string }
  | { kind: "llm-end"; id: string; at: string }
  | { kind: "llm-error"; id: string; error: string };

const sseClients = new Map<ServerResponse, string>();
const visitorAsks = new Map<string, { count: number; resetAt: number }>();

function originAllowed(req: IncomingMessage): boolean {
  const origin = req.headers.origin;
  if (!origin) return false;
  if (ALLOWED_ORIGINS.includes(origin)) return true;
  try {
    const u = new URL(origin);
    if (u.host === req.headers.host) return true;
  } catch {
    /* malformed origin */
  }
  return false;
}

function readBody(req: IncomingMessage): Promise<string | null> {
  return new Promise((resolve) => {
    let body = "";
    let aborted = false;
    req.on("data", (chunk: Buffer) => {
      if (aborted) return;
      body += chunk;
      if (body.length > MAX_BODY_BYTES) {
        aborted = true;
        body = "";
        resolve(null);
      }
    });
    req.on("end", () => { if (!aborted) resolve(body); });
    req.on("error", () => resolve(null));
  });
}

function clampStr(v: unknown, max: number): string {
  return String(v ?? "").slice(0, max);
}

function getVisitorIp(req: IncomingMessage): string {
  const xff = req.headers["x-forwarded-for"];
  if (typeof xff === "string" && xff.length) return xff.split(",")[0].trim();
  if (Array.isArray(xff) && xff[0]) return String(xff[0]).split(",")[0].trim();
  const fly = req.headers["fly-client-ip"];
  if (typeof fly === "string" && fly.length) return fly;
  return req.socket.remoteAddress ?? "unknown";
}

function consumeAskQuota(ip: string): { ok: true; remaining: number } | { ok: false; resetIn: number } {
  const now = Date.now();
  let entry = visitorAsks.get(ip);
  if (!entry || now >= entry.resetAt) {
    entry = { count: 0, resetAt: now + VISITOR_ASK_WINDOW_MS };
    visitorAsks.set(ip, entry);
  }
  if (entry.count >= VISITOR_ASK_LIMIT) {
    return { ok: false, resetIn: entry.resetAt - now };
  }
  entry.count++;
  return { ok: true, remaining: VISITOR_ASK_LIMIT - entry.count };
}

function fmtResetIn(ms: number): string {
  const h = Math.floor(ms / 3_600_000);
  const m = Math.floor((ms % 3_600_000) / 60_000);
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

function askOrReject(ip: string, prompt: string) {
  const q = consumeAskQuota(ip);
  if (!q.ok) {
    const id = randomUUID();
    broadcast({ kind: "llm-start", id, prompt, model: MODEL, at: new Date().toISOString() });
    broadcast({
      kind: "llm-error",
      id,
      error: `Per-visitor cap of ${VISITOR_ASK_LIMIT} LLM asks reached. Resets in ${fmtResetIn(q.resetIn)}.`,
    });
    return;
  }
  streamLLM(prompt);
}

function broadcast(msg: WireMsg) {
  const json = JSON.stringify(msg);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(json);
  }
  for (const res of sseClients.keys()) {
    res.write(`data: ${json}\n\n`);
  }
}

async function streamLLM(prompt: string) {
  const id = randomUUID();
  broadcast({ kind: "llm-start", id, prompt, model: MODEL, at: new Date().toISOString() });

  try {
    const res = await fetch(`${LLM_BASE_URL}/chat/completions`, {
      method: "POST",
      headers: {
        ...(LLM_API_KEY ? { Authorization: `Bearer ${LLM_API_KEY}` } : {}),
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: MODEL,
        stream: true,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    if (!res.ok || !res.body) {
      const errText = await res.text().catch(() => res.statusText);
      broadcast({ kind: "llm-error", id, error: `${res.status}: ${errText}` });
      return;
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split("\n");
      buf = lines.pop() ?? "";
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith("data:")) continue;
        const payload = trimmed.slice(5).trim();
        if (payload === "[DONE]") continue;
        try {
          const parsed = JSON.parse(payload);
          const delta: string = parsed.choices?.[0]?.delta?.content ?? "";
          if (delta) broadcast({ kind: "llm-chunk", id, delta });
        } catch {
          /* skip keep-alive comments etc. */
        }
      }
    }
    broadcast({ kind: "llm-end", id, at: new Date().toISOString() });
  } catch (err) {
    broadcast({ kind: "llm-error", id, error: err instanceof Error ? err.message : String(err) });
  }
}

const http = createServer(async (req, res) => {
  if (req.method === "GET" && (req.url === "/" || req.url === "/index.html")) {
    const html = await readFile(join(__dirname, "..", "public", "index.html"));
    res.writeHead(200, { "Content-Type": "text/html" });
    res.end(html);
    return;
  }

  if (req.method === "GET" && req.url === "/events") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
    res.write(": connected\n\n");
    sseClients.set(res, getVisitorIp(req));
    req.on("close", () => sseClients.delete(res));
    return;
  }

  if (req.method === "POST" && req.url === "/send") {
    if (!originAllowed(req)) { res.writeHead(403); res.end("forbidden origin"); return; }
    const body = await readBody(req);
    if (body === null) { res.writeHead(413); res.end("body too large"); return; }
    try {
      const raw = JSON.parse(body) as { user?: unknown; text?: unknown };
      const user = clampStr(raw.user, MAX_USER_LEN);
      const text = clampStr(raw.text, MAX_TEXT_LEN);
      if (!text) { res.writeHead(400); res.end("empty text"); return; }
      broadcast({
        kind: "chat", id: randomUUID(), user, text, via: "sse",
        at: new Date().toISOString(),
      });
      res.writeHead(204); res.end();
    } catch {
      res.writeHead(400); res.end("bad json");
    }
    return;
  }

  if (req.method === "POST" && req.url === "/ask") {
    if (!originAllowed(req)) { res.writeHead(403); res.end("forbidden origin"); return; }
    const body = await readBody(req);
    if (body === null) { res.writeHead(413); res.end("body too large"); return; }
    try {
      const raw = JSON.parse(body) as { prompt?: unknown };
      const prompt = clampStr(raw.prompt, MAX_PROMPT_LEN);
      if (!prompt) { res.writeHead(400); res.end("empty prompt"); return; }
      res.writeHead(202); res.end();
      askOrReject(getVisitorIp(req), prompt);
    } catch {
      res.writeHead(400); res.end("bad json");
    }
    return;
  }

  if (req.method === "POST" && req.url === "/kill") {
    if (!originAllowed(req)) { res.writeHead(403); res.end("forbidden origin"); return; }
    const callerIp = getVisitorIp(req);
    let killedWs = 0, killedSse = 0;
    for (const ws of wss.clients) {
      if ((ws as WebSocket & { _ip?: string })._ip === callerIp) {
        ws.terminate();
        killedWs++;
      }
    }
    for (const [sseRes, sseIp] of sseClients) {
      if (sseIp === callerIp) {
        sseRes.end();
        sseClients.delete(sseRes);
        killedSse++;
      }
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ killedWs, killedSse, scope: "caller" }));
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ server: http, maxPayload: MAX_WS_PAYLOAD });

wss.on("connection", (ws, req) => {
  const visitorIp = getVisitorIp(req);
  (ws as WebSocket & { _ip?: string })._ip = visitorIp;
  console.log("ws client connected from", visitorIp);
  ws.on("error", console.error);
  ws.on("message", (raw) => {
    try {
      const parsed = JSON.parse(raw.toString());
      if (parsed.kind === "ask") {
        askOrReject(visitorIp, clampStr(parsed.prompt, MAX_PROMPT_LEN));
        return;
      }
      const user = clampStr((parsed as { user?: unknown }).user, MAX_USER_LEN);
      const text = clampStr((parsed as { text?: unknown }).text, MAX_TEXT_LEN);
      if (!text) return;
      broadcast({
        kind: "chat",
        id: randomUUID(),
        user,
        text,
        via: "ws",
        at: new Date().toISOString(),
      });
    } catch {
      /* ignore */
    }
  });
  ws.on("close", () => console.log("ws client disconnected"));
});

http.listen(PORT, () => {
  console.log(`open http://localhost:${PORT}`);
  console.log(`llm: ${MODEL} @ ${LLM_BASE_URL}`);
  console.log(`auth: ${LLM_API_KEY ? "bearer-token" : "none"}`);
});
