// server.ts
// Stable Render deployment (WebSocket version) with SSH Proxy

const UUID: string = Deno.env.get("UUID") || "f9a1ba12-7187-4b25-a5d5-7bafd82ffb4d";
const SUB_PATH: string = Deno.env.get("SUB_PATH") || "sub";
const DOMAIN: string = Deno.env.get("DOMAIN") || "render.santanudhibar.deno.net";
const NAME: string = Deno.env.get("NAME") || "Render";
// Updated default port to 443
const PORT: number = parseInt(Deno.env.get("PORT") || "443");

// WS path that clients connect to
const WS_PATH: string = "/ws";

// Optional timeouts (ms). Set to 0 to disable.
const HEADER_TIMEOUT_MS: number = parseInt(Deno.env.get("HEADER_TIMEOUT_MS") || "10000");
const IDLE_TIMEOUT_MS: number = parseInt(Deno.env.get("IDLE_TIMEOUT_MS") || "0");

interface Settings {
  UUID: string;
  LOG_LEVEL: "none" | "debug" | "info" | "warn" | "error";
  BUFFER_SIZE: number;
  WS_PATH: string;
  MAX_POST_SIZE: number;
  SESSION_TIMEOUT: number;
  CHUNK_SIZE: number;
  TCP_NODELAY: boolean;
  TCP_KEEPALIVE: boolean;
}

const SETTINGS: Settings = {
  UUID,
  LOG_LEVEL: "none",
  BUFFER_SIZE: 64 * 1024,
  WS_PATH: "%2Fws",
  MAX_POST_SIZE: 1000000,
  SESSION_TIMEOUT: 30000,
  CHUNK_SIZE: 1024 * 1024,
  TCP_NODELAY: true,
  TCP_KEEPALIVE: true,
};

function validate_uuid(left: Uint8Array, right: Uint8Array): boolean {
  for (let i = 0; i < 16; i++) {
    if (left[i] !== right[i]) return false;
  }
  return true;
}

function concat_typed_arrays(...args: Uint8Array[]): Uint8Array {
  let len = 0;
  for (const a of args) len += a.length;
  const r = new Uint8Array(len);
  let offset = 0;
  for (const a of args) {
    r.set(a, offset);
    offset += a.length;
  }
  return r;
}

function parse_uuid(uuid: string): Uint8Array {
  uuid = uuid.replaceAll("-", "");
  const r = new Uint8Array(16);
  for (let index = 0; index < 16; index++) {
    r[index] = parseInt(uuid.substr(index * 2, 2), 16);
  }
  return r;
}

async function read_vless_header(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  cfg_uuid_str: string
): Promise<{
  hostname: string;
  port: number;
  data: Uint8Array;
  resp: Uint8Array;
}> {
  let readed_len = 0;
  let header = new Uint8Array();

  async function inner_read_until(offset: number): Promise<void> {
    while (readed_len < offset) {
      const { value, done } = await reader.read();
      if (done) throw new Error("header length too short");
      header = concat_typed_arrays(header, value!);
      readed_len += value!.length;
    }
  }

  await inner_read_until(1 + 16 + 1);

  const version = header[0];
  const uuid = header.slice(1, 1 + 16);
  const cfg_uuid = parse_uuid(cfg_uuid_str);
  if (!validate_uuid(uuid, cfg_uuid)) {
    throw new Error("invalid UUID");
  }

  const pb_len = header[1 + 16];
  const addr_plus1 = 1 + 16 + 1 + pb_len + 1 + 2 + 1;
  await inner_read_until(addr_plus1 + 1);

  const cmd = header[1 + 16 + 1 + pb_len];
  const COMMAND_TYPE_TCP = 1;
  if (cmd !== COMMAND_TYPE_TCP) {
    throw new Error(`unsupported command: ${cmd}`);
  }

  const port = (header[addr_plus1 - 1 - 2] << 8) + header[addr_plus1 - 1 - 1];
  const atype = header[addr_plus1 - 1];

  const ADDRESS_TYPE_IPV4 = 1;
  const ADDRESS_TYPE_STRING = 2;
  const ADDRESS_TYPE_IPV6 = 3;
  let header_len = -1;

  if (atype === ADDRESS_TYPE_IPV4) {
    header_len = addr_plus1 + 4;
  } else if (atype === ADDRESS_TYPE_IPV6) {
    header_len = addr_plus1 + 16;
  } else if (atype === ADDRESS_TYPE_STRING) {
    header_len = addr_plus1 + 1 + header[addr_plus1];
  }

  if (header_len < 0) {
    throw new Error("read address type failed");
  }

  await inner_read_until(header_len);

  const idx = addr_plus1;
  let hostname = "";
  if (atype === ADDRESS_TYPE_IPV4) {
    hostname = Array.from(header.slice(idx, idx + 4))
      .map((b) => b.toString())
      .join(".");
  } else if (atype === ADDRESS_TYPE_STRING) {
    hostname = new TextDecoder().decode(header.slice(idx + 1, idx + 1 + header[idx]));
  } else if (atype === ADDRESS_TYPE_IPV6) {
    hostname = Array.from({ length: 8 }, (_, i) =>
      ((header[idx + i * 2] << 8) + header[idx + i * 2 + 1]).toString(16)
    ).join(":");
  }

  if (!hostname) {
    throw new Error("parse hostname failed");
  }

  return {
    hostname,
    port,
    data: header.slice(header_len),
    resp: new Uint8Array([version, 0]),
  };
}

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  if (ms <= 0) return promise;
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timeout`)), ms);
    promise.then(
      (v) => {
        clearTimeout(timer);
        resolve(v);
      },
      (e) => {
        clearTimeout(timer);
        reject(e);
      }
    );
  });
}

async function parse_header(
  uuid_str: string,
  client: { readable: ReadableStream<Uint8Array> }
): Promise<any> {
  const reader = client.readable.getReader();
  try {
    const vless = await withTimeout(read_vless_header(reader, uuid_str), HEADER_TIMEOUT_MS, "header");
    return vless;
  } catch (err) {
    throw new Error(`read vless header error: ${err.message}`);
  } finally {
    reader.releaseLock();
  }
}

async function connect_remote(hostname: string, port: number): Promise<Deno.Conn> {
  const conn = await Deno.connect({ hostname, port });

  // Reduce latency where possible
  try {
    const tcp = conn as Deno.TcpConn;
    if (SETTINGS.TCP_NODELAY && tcp.setNoDelay) tcp.setNoDelay(true);
    if (SETTINGS.TCP_KEEPALIVE && tcp.setKeepAlive) tcp.setKeepAlive(true);
  } catch (_err) {
    // ignore if unsupported
  }

  return conn;
}

let ISP = "";
try {
  const response = await fetch("https://speed.cloudflare.com/meta");
  if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
  const data = (await response.json()) as { country: string; asOrganization: string };
  ISP = `${data.country}-${data.asOrganization}`.replace(/ /g, "_");
} catch (_err) {
  ISP = "unknown";
}

let IP = DOMAIN;
if (!DOMAIN) {
  IP = Deno.env.get("RENDER_EXTERNAL_HOSTNAME") || "localhost";
}

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

      // cleanup
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
      if (ws.readyState !== WebSocket.OPEN) {
        throw new Error("websocket not open");
      }
      onActivity?.();
      ws.send(chunk);
    },
    close() {
      try {
        ws.close();
      } catch (_err) {}
    },
    abort() {
      try {
        ws.close();
      } catch (_err) {}
    },
  });
}

async function relay_ws(ws: WebSocket): Promise<void> {
  ws.binaryType = "arraybuffer";

  let idleTimer: number | undefined;
  const touch = () => {
    if (IDLE_TIMEOUT_MS <= 0) return;
    if (idleTimer) clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      try {
        ws.close();
      } catch (_err) {}
    }, IDLE_TIMEOUT_MS) as unknown as number;
  };

  const wsStream = wsReadableStream(ws, touch);

  // Parse header from full WS stream (not just first packet)
  const vless = await parse_header(SETTINGS.UUID, { readable: wsStream });

  const remote = await connect_remote(vless.hostname, vless.port);

  // Reply VLESS header
  ws.send(vless.resp);
  touch();

  // Send leftover data from header parsing (if any)
  if (vless.data && vless.data.length > 0) {
    const writer = remote.writable.getWriter();
    await writer.write(vless.data);
    writer.releaseLock();
  }

  const aborter = new AbortController();
  const abort = () => {
    try {
      aborter.abort();
    } catch (_err) {}
  };

  ws.addEventListener("close", abort);
  ws.addEventListener("error", abort);

  // ws -> remote
  const wsToRemote = wsStream.pipeTo(remote.writable, {
    signal: aborter.signal,
    preventClose: false,
  });

  // remote -> ws
  const remoteToWs = remote.readable.pipeTo(wsWritableStream(ws, touch), {
    signal: aborter.signal,
    preventClose: false,
  });

  try {
    await Promise.race([wsToRemote, remoteToWs]);
  } catch (_err) {
    // ignore: handled by cleanup
  } finally {
    try {
      remote.close();
    } catch (_err) {}
    try {
      ws.close();
    } catch (_err) {}
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

  if (path === "/") {
    return new Response("VLESS + SSH WS Server Running on Render\n", {
      status: 200,
      headers: { "Content-Type": "text/plain" },
    });
  }

  if (path === `/${SUB_PATH}`) {
    const serverIP = IP || url.hostname;
    const vlessURL =
      `vless://${UUID}@${serverIP}:443?encryption=none&security=tls&sni=${serverIP}` +
      `&fp=chrome&allowInsecure=1&type=ws&host=${serverIP}&path=${SETTINGS.WS_PATH}#${NAME}-${ISP}`;
    const base64Content = btoa(vlessURL);
    return new Response(base64Content + "\n", {
      status: 200,
      headers: { "Content-Type": "text/plain" },
    });
  }

  // ==== SSH Over WebSocket Handling ====
  if (path === "/ssh") {
    // Check Basic Auth for admin:12345 (YWRtaW46MTIzNDU=)
    const authHeader = req.headers.get("Authorization") || "";
    if (authHeader !== "Basic YWRtaW46MTIzNDU=") {
      return new Response("Unauthorized", {
        status: 401,
        headers: {
          "WWW-Authenticate": 'Basic realm="SSH Access"',
          ...headers,
        },
      });
    }

    const upgrade = req.headers.get("upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("WebSocket Upgrade Required", { status: 426, headers });
    }

    const { socket, response } = Deno.upgradeWebSocket(req);

    socket.onopen = async () => {
      try {
        socket.binaryType = "arraybuffer";
        // Connect to local SSH daemon
        const remote = await Deno.connect({ hostname: "127.0.0.1", port: 22 });
        const wsStream = wsReadableStream(socket);
        const aborter = new AbortController();

        const abort = () => {
          try { aborter.abort(); } catch {}
          try { remote.close(); } catch {}
        };

        socket.addEventListener("close", abort);
        socket.addEventListener("error", abort);

        // Pipe bidirectional traffic
        const wsToRemote = wsStream.pipeTo(remote.writable, { signal: aborter.signal, preventClose: false });
        const remoteToWs = remote.readable.pipeTo(wsWritableStream(socket), { signal: aborter.signal, preventClose: false });

        await Promise.race([wsToRemote, remoteToWs]);
      } catch (err) {
        console.error("SSH proxy error:", err);
      } finally {
        try { socket.close(); } catch {}
      }
    };

    return response;
  }
  // =====================================

  if (path === WS_PATH) {
    const upgrade = req.headers.get("upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("WebSocket Upgrade Required", { status: 426 });
    }
    const { socket, response } = Deno.upgradeWebSocket(req);

    // Handle connection in background
    relay_ws(socket).catch(() => {
      try {
        socket.close();
      } catch (_err) {}
    });

    return response;
  }

  return new Response("Not Found", { status: 404 });
});

console.log(`Server is running on port ${PORT}`);
