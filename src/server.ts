import { createServer, ServerResponse } from "node:http";
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

type WireMsg =
  | { kind: "chat"; id: string; user: string; text: string; via: "ws" | "sse"; at: string }
  | { kind: "llm-start"; id: string; prompt: string; model: string; at: string }
  | { kind: "llm-chunk"; id: string; delta: string }
  | { kind: "llm-end"; id: string; at: string }
  | { kind: "llm-error"; id: string; error: string };

const sseClients = new Set<ServerResponse>();

function broadcast(msg: WireMsg) {
  const json = JSON.stringify(msg);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(json);
  }
  for (const res of sseClients) {
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
    sseClients.add(res);
    req.on("close", () => sseClients.delete(res));
    return;
  }

  if (req.method === "POST" && req.url === "/send") {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      try {
        const { user, text } = JSON.parse(body) as { user: string; text: string };
        broadcast({
          kind: "chat",
          id: randomUUID(),
          user,
          text,
          via: "sse",
          at: new Date().toISOString(),
        });
        res.writeHead(204);
        res.end();
      } catch {
        res.writeHead(400);
        res.end("bad json");
      }
    });
    return;
  }

  if (req.method === "POST" && req.url === "/ask") {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      try {
        const { prompt } = JSON.parse(body) as { prompt: string };
        res.writeHead(202);
        res.end();
        streamLLM(prompt);
      } catch {
        res.writeHead(400);
        res.end("bad json");
      }
    });
    return;
  }

  if (req.method === "POST" && req.url === "/kill") {
    let killedWs = 0,
      killedSse = 0;
    for (const ws of wss.clients) {
      ws.terminate();
      killedWs++;
    }
    for (const sseRes of sseClients) {
      sseRes.end();
      killedSse++;
    }
    sseClients.clear();
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ killedWs, killedSse }));
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ server: http });

wss.on("connection", (ws) => {
  console.log("ws client connected");
  ws.on("error", console.error);
  ws.on("message", (raw) => {
    try {
      const parsed = JSON.parse(raw.toString());
      if (parsed.kind === "ask") {
        streamLLM(parsed.prompt);
        return;
      }
      const { user, text } = parsed as { user: string; text: string };
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
