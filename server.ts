// server.ts
// For Render deployment (WebSocket version)

const UUID: string = Deno.env.get("UUID") || "f9a1ba12-7187-4b25-a5d5-7bafd82ffb4d";
const SUB_PATH: string = Deno.env.get("SUB_PATH") || "sub";  // Get subscription path
const DOMAIN: string = Deno.env.get("DOMAIN") || "render.santanudhibar.deno.net"; // Your Render domain (required)
const NAME: string = Deno.env.get("NAME") || "Render";
const PORT: number = parseInt(Deno.env.get("PORT") || "8080"); // Render uses 8080 by default
const WS_PATH: string = "/ws";

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
  BUFFER_SIZE: 2048,
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

async function parse_header(
  uuid_str: string,
  client: { readable: ReadableStream<Uint8Array> }
): Promise<any> {
  const reader = client.readable.getReader();
  try {
    const vless = await read_vless_header(reader, uuid_str);
    return vless;
  } catch (err) {
    throw new Error(`read vless header error: ${err.message}`);
  } finally {
    reader.releaseLock();
  }
}

async function connect_remote(hostname: string, port: number): Promise<Deno.Conn> {
  try {
    const conn = await Deno.connect({ hostname, port });
    return conn;
  } catch (err) {
    throw err;
  }
}

let ISP = "";

try {
  const response = await fetch("https://speed.cloudflare.com/meta");
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  const data = await response.json() as {
    country: string;
    asOrganization: string;
  };
  ISP = `${data.country}-${data.asOrganization}`.replace(/ /g, "_");
} catch (_err) {
  ISP = "unknown";
}

let IP = DOMAIN;
if (!DOMAIN) {
  // In Render, we'll use the environment variable or the host header
  IP = Deno.env.get("RENDER_EXTERNAL_HOSTNAME") || "localhost";
}

function generatePadding(min: number, max: number): string {
  const length = min + Math.floor(Math.random() * (max - min));
  return btoa(Array(length).fill("X").join(""));
}

async function relay_ws(ws: WebSocket, firstPacket: Uint8Array): Promise<void> {
  const readable = new ReadableStream({
    start(controller) {
      controller.enqueue(firstPacket);
      controller.close();
    },
  });

  const client = {
    readable,
  };

  const vless = await parse_header(SETTINGS.UUID, client);
  const remote = await connect_remote(vless.hostname, vless.port);

  // Send VLESS response header
  ws.send(vless.resp);

  // Send any remaining data after header to remote
  if (vless.data && vless.data.length > 0) {
    const writer = remote.writable.getWriter();
    await writer.write(vless.data);
    writer.releaseLock();
  }

  // Pipe remote -> ws
  const reader = remote.readable.getReader();
  (async () => {
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done || !value) break;
        ws.send(value);
      }
    } catch (_err) {
      // ignore
    } finally {
      try {
        ws.close();
      } catch (_err) {}
      try {
        remote.close();
      } catch (_err) {}
    }
  })();

  // ws -> remote
  ws.addEventListener("message", async (evt) => {
    if (!(evt.data instanceof ArrayBuffer)) return;
    const data = new Uint8Array(evt.data);
    try {
      const writer = remote.writable.getWriter();
      await writer.write(data);
      writer.releaseLock();
    } catch (_err) {
      try {
        ws.close();
      } catch (_e) {}
    }
  });

  ws.addEventListener("close", () => {
    try {
      remote.close();
    } catch (_err) {}
  });
}

// Use Deno.serve instead of serve from std library for better compatibility
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
    return new Response("VLESS WS Server Running on Render\n", {
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

  // WebSocket path
  if (path === WS_PATH) {
    const upgrade = req.headers.get("upgrade") || "";
    if (upgrade.toLowerCase() !== "websocket") {
      return new Response("WebSocket Upgrade Required", { status: 426 });
    }
    const { socket, response } = Deno.upgradeWebSocket(req);

    let initialized = false;

    socket.addEventListener("message", async (evt) => {
      if (initialized) return; // only handle first packet here
      if (!(evt.data instanceof ArrayBuffer)) return;
      initialized = true;
      const firstPacket = new Uint8Array(evt.data);
      try {
        await relay_ws(socket, firstPacket);
      } catch (_err) {
        try {
          socket.close();
        } catch (_e) {}
      }
    });

    return response;
  }

  return new Response("Not Found", { status: 404 });
});

console.log(`Server is running on port ${PORT}`);
