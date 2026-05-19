// server.ts
// For Render deployment

const UUID: string = Deno.env.get("UUID") || "f9a1ba12-7187-4b25-a5d5-7bafd82ffb4d";
const SUB_PATH: string = Deno.env.get("SUB_PATH") || "sub";  // Get subscription path
const WS_PATH: string = Deno.env.get("WS_PATH") || "vless";   // VLESS WebSocket path
const SSH_WS_PATH: string = Deno.env.get("SSH_WS_PATH") || "ssh"; // SSH WebSocket path
const DOMAIN: string = Deno.env.get("DOMAIN") || "render-pdj5.onrender.com";         // Your Render domain (required)
const NAME: string = Deno.env.get("NAME") || "Render";
const PORT: number = parseInt(Deno.env.get("PORT") || "8080"); // Render uses 8080 by default

interface Settings {
  UUID: string;
  LOG_LEVEL: "none" | "debug" | "info" | "warn" | "error";
  BUFFER_SIZE: number;
  WS_PATH: string;
  SSH_WS_PATH: string;
  MAX_BUFFERED_POSTS: number;
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
  WS_PATH: `/${WS_PATH}`,
  SSH_WS_PATH: `/${SSH_WS_PATH}`,
  MAX_BUFFERED_POSTS: 30,
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

function pipe_relay() {
  async function pump(
    src: ReadableStream<Uint8Array>,
    dest: WritableStream<Uint8Array>,
    first_packet: Uint8Array
  ): Promise<void> {
    if (first_packet.length > 0) {
      const writer = dest.getWriter();
      await writer.write(first_packet);
      writer.releaseLock();
    }

    try {
      await src.pipeTo(dest, {
        preventClose: false,
        preventAbort: false,
        preventCancel: false,
        signal: AbortSignal.timeout(SETTINGS.SESSION_TIMEOUT),
      });
    } catch (err) {
      throw err;
    }
  }
  return pump;
}

function relay(
  cfg: Settings,
  client: { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> },
  remote: Deno.Conn,
  vless: { data: Uint8Array; resp: Uint8Array }
): void {
  const pump = pipe_relay();
  let isClosing = false;

  const remoteStream = {
    readable: remote.readable,
    writable: remote.writable,
  };

  function cleanup(): void {
    if (!isClosing) {
      isClosing = true;
      try {
        remote.close();
      } catch (err) {
      }
    }
  }

  const uploader = pump(client.readable, remoteStream.writable, vless.data)
    .catch((err) => {
    })
    .finally(cleanup);

  const downloader = pump(remoteStream.readable, client.writable, vless.resp)
    .catch((err) => {
    });

  downloader.finally(() => uploader).finally(cleanup);
}

// WebSocket handler for VLESS
async function handleVLESSWebSocket(ws: WebSocket, uuid: string) {
  let remote: Deno.Conn | null = null;
  let vlessHeader: any = null;
  let initialized = false;
  
  try {
    // Wait for first message which contains VLESS header
    const firstMessage = await new Promise<Uint8Array>((resolve, reject) => {
      ws.onmessage = (event) => {
        if (event.data instanceof ArrayBuffer) {
          resolve(new Uint8Array(event.data));
        } else {
          reject(new Error("Invalid message format"));
        }
      };
      ws.onerror = (err) => reject(err);
      setTimeout(() => reject(new Error("Timeout waiting for VLESS header")), 10000);
    });

    // Parse VLESS header
    const readable = new ReadableStream({
      start(controller) {
        controller.enqueue(firstMessage);
        controller.close();
      },
    });

    const client = {
      readable,
      writable: new WritableStream(),
    };

    vlessHeader = await parse_header(SETTINGS.UUID, client);
    remote = await connect_remote(vlessHeader.hostname, vlessHeader.port);
    initialized = true;

    // Send response header
    ws.send(vlessHeader.resp.buffer);

    // Send remaining data
    if (vlessHeader.data.length > 0) {
      ws.send(vlessHeader.data.buffer);
    }

    // Handle bidirectional data transfer
    ws.onmessage = async (event) => {
      if (!remote || !initialized) return;
      
      if (event.data instanceof ArrayBuffer) {
        const data = new Uint8Array(event.data);
        const writer = remote.writable.getWriter();
        await writer.write(data);
        writer.releaseLock();
      }
    };

    // Read from remote and send to WebSocket
    (async () => {
      const reader = remote!.readable.getReader();
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          if (value && value.length > 0) {
            ws.send(value.buffer);
          }
        }
      } catch (err) {
        // Connection closed
      } finally {
        reader.releaseLock();
        ws.close();
      }
    })();

    ws.onclose = () => {
      if (remote) {
        try {
          remote.close();
        } catch (err) {}
        remote = null;
      }
    };

    ws.onerror = () => {
      if (remote) {
        try {
          remote.close();
        } catch (err) {}
        remote = null;
      }
    };

  } catch (err) {
    if (remote) {
      try {
        remote.close();
      } catch (err) {}
    }
    ws.close();
  }
}

// WebSocket handler for SSH
async function handleSSHWebSocket(ws: WebSocket, host?: string, port?: string) {
  let remote: Deno.Conn | null = null;
  
  try {
    // Default SSH connection parameters
    const sshHost = host || "localhost";
    const sshPort = port ? parseInt(port) : 22;
    
    // Connect to SSH server
    remote = await Deno.connect({ hostname: sshHost, port: sshPort });
    
    // Handle bidirectional data transfer
    ws.onmessage = async (event) => {
      if (!remote) return;
      
      if (event.data instanceof ArrayBuffer) {
        const data = new Uint8Array(event.data);
        const writer = remote.writable.getWriter();
        await writer.write(data);
        writer.releaseLock();
      } else if (typeof event.data === 'string') {
        // Handle text data if needed
        const encoder = new TextEncoder();
        const data = encoder.encode(event.data);
        const writer = remote.writable.getWriter();
        await writer.write(data);
        writer.releaseLock();
      }
    };

    // Read from remote and send to WebSocket
    (async () => {
      const reader = remote!.readable.getReader();
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          if (value && value.length > 0) {
            ws.send(value.buffer);
          }
        }
      } catch (err) {
        // Connection closed
      } finally {
        reader.releaseLock();
        ws.close();
      }
    })();

    ws.onclose = () => {
      if (remote) {
        try {
          remote.close();
        } catch (err) {}
        remote = null;
      }
    };

    ws.onerror = () => {
      if (remote) {
        try {
          remote.close();
        } catch (err) {}
        remote = null;
      }
    };

  } catch (err) {
    if (remote) {
      try {
        remote.close();
      } catch (err) {}
    }
    ws.close();
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
} catch (err) {
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

// Use Deno.serve with WebSocket upgrade support
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
    return new Response("VLESS + SSH WebSocket Server Running on Render\n", 
    { status: 200, 
      headers: { "Content-Type": "text/plain" }, }); 
  } 

  if (path === `/${SUB_PATH}`) {
    const serverIP = IP || url.hostname;
    // Generate VLESS WebSocket URL
    const vlessURL = `vless://${UUID}@${serverIP}:443?encryption=none&security=tls&sni=${serverIP}&fp=chrome&allowInsecure=1&type=ws&host=${serverIP}&path=${SETTINGS.WS_PATH}/${UUID}#${NAME}-${ISP}`;
    const base64Content = btoa(vlessURL);
    return new Response(base64Content + "\n", {
      status: 200,
      headers: { "Content-Type": "text/plain" },
    });
  }

  // Handle VLESS WebSocket connections
  if (path.startsWith(SETTINGS.WS_PATH)) {
    const uuidMatch = path.match(new RegExp(`^${SETTINGS.WS_PATH}/([^/]+)$`));
    if (!uuidMatch) {
      return new Response("Not Found", { status: 404 });
    }
    
    const clientUUID = uuidMatch[1];
    if (clientUUID !== UUID) {
      return new Response("Unauthorized", { status: 401 });
    }
    
    // Upgrade to WebSocket
    const { socket, response } = Deno.upgradeWebSocket(req);
    handleVLESSWebSocket(socket, clientUUID);
    return response;
  }

  // Handle SSH WebSocket connections
  if (path.startsWith(SETTINGS.SSH_WS_PATH)) {
    const sshMatch = path.match(new RegExp(`^${SETTINGS.SSH_WS_PATH}(?:/([^/]+)(?:/([0-9]+))?)?$`));
    
    // Upgrade to WebSocket
    const { socket, response } = Deno.upgradeWebSocket(req);
    
    if (sshMatch && sshMatch[1]) {
      // Connect to specified SSH host
      const sshHost = sshMatch[1];
      const sshPort = sshMatch[2] || "22";
      handleSSHWebSocket(socket, sshHost, sshPort);
    } else {
      // Connect to default local SSH
      handleSSHWebSocket(socket);
    }
    return response;
  }

  return new Response("Not Found", { status: 404 });
});

console.log(`Server is running on port ${PORT}`);
console.log(`VLESS WebSocket endpoint: ${SETTINGS.WS_PATH}/{uuid}`);
console.log(`SSH WebSocket endpoint: ${SETTINGS.SSH_WS_PATH}/{host}/{port}`);
console.log(`Subscription endpoint: /${SUB_PATH}`);
