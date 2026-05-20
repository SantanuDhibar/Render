// server.ts
// SSH over WebSocket TLS (root path), with /sub returning raw payload

const SUB_PATH: string = Deno.env.get("SUB_PATH") || "sub";
const PORT: number = parseInt(Deno.env.get("PORT") || "8080");

// Fixed auth
const AUTH_USER = "sd";
const AUTH_PASS = "12345@1";

// Random WS key (as requested)
const WS_KEY = "KQ9zN7pVwX3mL2sA";

// SSH target (TLS)
const SSH_HOST = "render.santanudhibar.deno.net";
const SSH_PORT = 443;
const SSH_SNI = "render.santanudhibar.deno.net";

// Optional idle timeout
const IDLE_TIMEOUT_MS: number = parseInt(Deno.env.get("IDLE_TIMEOUT_MS") || "0");

function generatePadding(min: number, max: number): string {
  const length = min + Math.floor(Math.random() * (max - min));
  return btoa(Array(length).fill("X").join(""));
}

function wsReadableStream(ws: WebSocket, onActivity?: () => void): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      const onMessage = (evt: MessageEvent) => {
        if (!(evt.data instanceof ArrayBuffer)) return;
        onActivity?.();
        controller.enqueue(new Uint8Array(evt.data));
      };
      const onClose = () => controller.close();
      const onError = (e: Event | ErrorEvent) => {
        controller.error(e instanceof ErrorEvent ? e.error : e);
      };

      ws.addEventListener("message", onMessage);
      ws.addEventListener("close", onClose);
      ws.addEventListener("error", onError);

      this.cancel = () => {
        ws.removeEventListener("message", onMessage);
        ws.removeEventListener("close", onClose);
        ws.removeEventListener("error", onError);
      };
    },
  });
}

function wsWritableStream(ws: WebSocket, onActivity?: () => void): WritableStream<Uint8Array> {
  return new WritableStream<Uint8Array>({
    write(chunk) {
      if (ws.readyState !== WebSocket.OPEN) throw new Error("websocket not open");
      onActivity?.();
      ws.send(chunk);
    },
    close() {
      try { ws.close(); } catch (_err) {}
    },
    abort() {
      try { ws.close(); } catch (_err) {}
    },
  });
}

async function relay_ssh_ws(ws: WebSocket, reqUrl: URL): Promise<void> {
  ws.binaryType = "arraybuffer";

  // Auth check (query params)
  const key = reqUrl.searchParams.get("key") || "";
  const user = reqUrl.searchParams.get("user") || "";
  const pass = reqUrl.searchParams.get("pass") || "";

  if (key !== WS_KEY || user !== AUTH_USER || pass !== AUTH_PASS) {
    try { ws.close(); } catch (_err) {}
    return;
  }

  let idleTimer: number | undefined;
  const touch = () => {
    if (IDLE_TIMEOUT_MS <= 0) return;
    if (idleTimer) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      try { ws.close(); } catch (_err) {}
    }, IDLE_TIMEOUT_MS) as unknown as number;
  };

  // SSH over TLS
  const remote = await Deno.connectTls({
    hostname: SSH_HOST,
    port: SSH_PORT,
    serverName: SSH_SNI,
  });

  const aborter = new AbortController();
  const abort = () => {
    try { aborter.abort(); } catch (_err) {}
  };

  ws.addEventListener("close", abort);
  ws.addEventListener("error", abort);

  const wsStream = wsReadableStream(ws, touch);

  const wsToRemote = wsStream.pipeTo(remote.writable, {
    signal: aborter.signal,
    preventClose: false,
  });

  const remoteToWs = remote.readable.pipeTo(wsWritableStream(ws, touch), {
    signal: aborter.signal,
    preventClose: false,
  });

  try {
    await Promise.race([wsToRemote, remoteToWs]);
  } catch (_err) {
    // ignore
  } finally {
    try { remote.close(); } catch (_err) {}
    try { ws.close(); } catch (_err) {}
    if (idleTimer) clearTimeout(idleTimer);
  }
}

Deno.serve({ port: PORT }, async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  const path = url.pathname;

  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST",
    "Cache-Control": "no-store",
    "X-Accel-Buffering": "no",
    "X-Padding": generatePadding(100, 1000),
  };

  // /sub returns raw payload (not clipboard config)
  if (path === `/${SUB_PATH}`) {
    const payload =
`GET / HTTP/1.1[crlf]
Host:${SSH_HOST}[crlf]
Connection: Upgrade[crlf]
User-Agent: [ua][crlf]
Upgrade: websocket[crlf][crlf]
key=${WS_KEY}&user=${AUTH_USER}&pass=${AUTH_PASS}
`;
    return new Response(payload, {
      status: 200,
      headers: { "Content-Type": "text/plain" },
    });
  }

  // Root path = WebSocket
  if (path === "/") {
    const upgrade = req.headers.get("upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("WebSocket Upgrade Required", { status: 426 });
    }
    const { socket, response } = Deno.upgradeWebSocket(req);

    relay_ssh_ws(socket, url).catch(() => {
      try { socket.close(); } catch (_err) {}
    });

    return response;
  }

  return new Response("Not Found", { status: 404, headers });
});

console.log(`Server is running on port ${PORT}`);
