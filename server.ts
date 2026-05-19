// server.ts
// For Render deployment with SSH WebSocket support

const UUID: string = Deno.env.get("UUID") || "f9a1ba12-7187-4b25-a5d5-7bafd82ffb4d";
const SUB_PATH: string = Deno.env.get("SUB_PATH") || "sub";  // Get subscription path
const XPATH: string = Deno.env.get("XPATH") || "xhttp";      // Node path
const DOMAIN: string = Deno.env.get("DOMAIN") || "render.santanudhibar.deno.net";         // Your Render domain (required)
const NAME: string = Deno.env.get("NAME") || "Render";
const PORT: number = parseInt(Deno.env.get("PORT") || "8080"); // Render uses 8080 by default
const SSH_PATH: string = Deno.env.get("SSH_PATH") || "ssh";   // SSH WebSocket path

// Random hardcoded SSH credentials (regenerated on each startup)
const SSH_USERNAME: string = generateRandomUsername();
const SSH_PASSWORD: string = generateRandomPassword();

function generateRandomUsername(): string {
  const prefixes = ["admin", "user", "guest", "ssh", "term", "shell", "client", "remote"];
  const suffix = Math.random().toString(36).substring(2, 8);
  return `${prefixes[Math.floor(Math.random() * prefixes.length)]}_${suffix}`;
}

function generateRandomPassword(): string {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()";
  let password = "";
  for (let i = 0; i < 16; i++) {
    password += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return password;
}

interface Settings {
  UUID: string;
  LOG_LEVEL: "none" | "debug" | "info" | "warn" | "error";
  BUFFER_SIZE: number;
  XPATH: string;
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
  XPATH: `%2F${XPATH}`,
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

const sessions = new Map<string, Session>();

class Session {
  uuid: string;
  nextSeq: number = 0;
  downstreamStarted: boolean = false;
  lastActivity: number = Date.now();
  vlessHeader: any = null;
  remote: Deno.Conn | null = null;
  initialized: boolean = false;
  responseHeader: Uint8Array | null = null;
  headerSent: boolean = false;
  bufferedData: Map<number, Uint8Array> = new Map();
  cleaned: boolean = false;
  pendingPackets: Uint8Array[] = [];
  currentStreamRes: { writable: WritableStream<Uint8Array> } | null = null;
  pendingBuffers: Map<number, Uint8Array> = new Map();

  constructor(uuid: string) {
    this.uuid = uuid;
  }

  async initializeVLESS(firstPacket: Uint8Array): Promise<boolean> {
    if (this.initialized) return true;

    try {
      const readable = new ReadableStream({
        start(controller) {
          controller.enqueue(firstPacket);
          controller.close();
        },
      });

      const client = {
        readable,
        writable: new WritableStream(),
      };

      this.vlessHeader = await parse_header(SETTINGS.UUID, client);
      this.remote = await connect_remote(this.vlessHeader.hostname, this.vlessHeader.port);
      this.initialized = true;
      return true;
    } catch (err) {
      return false;
    }
  }

  async processPacket(seq: number, data: Uint8Array): Promise<boolean> {
    try {
      this.pendingBuffers.set(seq, data);

      while (this.pendingBuffers.has(this.nextSeq)) {
        const nextData = this.pendingBuffers.get(this.nextSeq)!;
        this.pendingBuffers.delete(this.nextSeq);

        if (!this.initialized && this.nextSeq === 0) {
          if (!await this.initializeVLESS(nextData)) {
            throw new Error("Failed to initialize VLESS connection");
          }
          this.responseHeader = this.vlessHeader.resp;
          await this._writeToRemote(this.vlessHeader.data);

          if (this.currentStreamRes) {
            this._startDownstreamResponse();
          }
        } else {
          if (!this.initialized) {
            continue;
          }
          await this._writeToRemote(nextData);
        }

        this.nextSeq++;
      }

      if (this.pendingBuffers.size > SETTINGS.MAX_BUFFERED_POSTS) {
        throw new Error("Too many buffered packets");
      }

      return true;
    } catch (err) {
      throw err;
    }
  }

  startDownstream(res: { writable: WritableStream<Uint8Array> }): boolean {
    this.currentStreamRes = res;
    if (this.initialized && this.responseHeader) {
      this._startDownstreamResponse();
    }
    return true;
  }

  async _writeToRemote(data: Uint8Array): Promise<void> {
    if (!this.remote) {
      throw new Error("Remote connection not available");
    }
    const writer = this.remote.writable.getWriter();
    await writer.write(data);
    writer.releaseLock();
  }

  _startDownstreamResponse(): void {
    if (!this.currentStreamRes || !this.responseHeader) return;

    try {
      const writer = this.currentStreamRes.writable.getWriter();
      writer.write(this.responseHeader);
      this.headerSent = true;
      writer.releaseLock();

      this.remote!.readable.pipeTo(this.currentStreamRes.writable).catch((err) => {
      });
    } catch (err) {
      this.cleanup();
    }
  }

  cleanup(): void {
    if (!this.cleaned) {
      this.cleaned = true;
      if (this.remote) {
        this.remote.close();
        this.remote = null;
      }
      this.initialized = false;
      this.headerSent = false;
    }
  }
}

// SSH WebSocket Handler
async function handleSSHWebSocket(req: Request): Promise<Response> {
  const upgrade = req.headers.get("upgrade")?.toLowerCase();
  
  if (upgrade !== "websocket") {
    // Return SSH info page for non-WebSocket requests
    const html = `
<!DOCTYPE html>
<html>
<head>
    <title>SSH WebSocket Tunnel</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        .credentials { background: #f0f0f0; padding: 20px; border-radius: 5px; margin: 20px 0; }
        .username { color: #0066cc; font-weight: bold; }
        .password { color: #cc0000; font-weight: bold; }
        .warning { color: #ff6600; }
        code { background: #e0e0e0; padding: 2px 5px; border-radius: 3px; }
        .command { background: #333; color: #fff; padding: 10px; border-radius: 5px; font-family: monospace; }
    </style>
</head>
<body>
    <h1>🔐 SSH WebSocket Tunnel</h1>
    <div class="credentials">
        <h2>Access Credentials (Randomly Generated)</h2>
        <p><strong>Username:</strong> <span class="username">${SSH_USERNAME}</span></p>
        <p><strong>Password:</strong> <span class="password">${SSH_PASSWORD}</span></p>
        <p class="warning">⚠️ These credentials are regenerated on server restart</p>
    </div>
    <h3>How to use:</h3>
    <p>1. Use a WebSocket SSH client to connect to:</p>
    <div class="command">wss://${DOMAIN}/${SSH_PATH}</div>
    <p>2. Example using <code>wssh</code> (WebSocket SSH client):</p>
    <div class="command">wssh --server=wss://${DOMAIN}/${SSH_PATH} --username=${SSH_USERNAME} --password=${SSH_PASSWORD}</div>
    <p>3. Or use any WebSocket SSH client with basic authentication</p>
    <hr>
    <p><small>Note: This is a WebSocket to SSH tunnel. Your credentials are randomly generated on each startup.</small></p>
</body>
</html>
    `;
    return new Response(html, {
      status: 200,
      headers: { "Content-Type": "text/html" },
    });
  }
  
  // Handle WebSocket upgrade for SSH
  const { socket, response } = Deno.upgradeWebSocket(req);
  
  let sshConn: Deno.Conn | null = null;
  let authenticated = false;
  let authBuffer: string = "";
  
  socket.onopen = () => {
    console.log("SSH WebSocket connection opened");
    // Send authentication prompt
    socket.send("SSH-2.0-WebSocketTunnel\r\n");
  };
  
  socket.onmessage = async (event) => {
    try {
      const data = typeof event.data === "string" ? event.data : new TextDecoder().decode(event.data);
      
      if (!authenticated) {
        // Handle authentication
        authBuffer += data;
        
        // Check for username/password in format: "username:password" or basic auth
        if (authBuffer.includes("\n") || authBuffer.includes("\r")) {
          const lines = authBuffer.split(/\r?\n/);
          const authLine = lines[0];
          
          let username = "", password = "";
          
          if (authLine.includes(":")) {
            [username, password] = authLine.split(":");
          } else if (authLine.toLowerCase().includes("authorization: basic")) {
            // Handle Basic Auth header
            const base64Auth = authLine.split(" ")[2];
            if (base64Auth) {
              const decoded = atob(base64Auth);
              [username, password] = decoded.split(":");
            }
          }
          
          if (username === SSH_USERNAME && password === SSH_PASSWORD) {
            authenticated = true;
            socket.send("Authentication successful!\r\n");
            
            // Connect to SSH server (localhost:22)
            try {
              sshConn = await Deno.connect({ hostname: "127.0.0.1", port: 22 });
              socket.send("Connected to SSH server\r\n");
              
              // Start bidirectional data transfer
              const encoder = new TextEncoder();
              const decoder = new TextDecoder();
              
              // Read from SSH and send to WebSocket
              (async () => {
                const reader = sshConn!.readable.getReader();
                try {
                  while (true) {
                    const { value, done } = await reader.read();
                    if (done) break;
                    const text = decoder.decode(value);
                    socket.send(text);
                  }
                } catch (err) {
                  console.error("SSH read error:", err);
                } finally {
                  reader.releaseLock();
                }
              })();
            } catch (err) {
              socket.send(`Failed to connect to SSH server: ${err.message}\r\n`);
              socket.close();
            }
          } else {
            socket.send(`Authentication failed!\r\nUsername: ${SSH_USERNAME}\r\nPassword: ${SSH_PASSWORD}\r\n`);
            socket.close();
          }
        } else if (!authenticated) {
          // Request authentication
          socket.send("Please provide credentials (username:password): ");
        }
      } else if (sshConn) {
        // Forward data to SSH
        const encoder = new TextEncoder();
        await sshConn.writable.getWriter().write(encoder.encode(data));
      }
    } catch (err) {
      console.error("WebSocket message error:", err);
      socket.send(`Error: ${err.message}\r\n`);
    }
  };
  
  socket.onclose = () => {
    console.log("SSH WebSocket connection closed");
    if (sshConn) {
      sshConn.close();
    }
  };
  
  socket.onerror = (error) => {
    console.error("WebSocket error:", error);
    if (sshConn) {
      sshConn.close();
    }
  };
  
  return response;
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

console.log("🚀 Server Starting...");
console.log(`📡 VLESS UUID: ${UUID}`);
console.log(`🔐 SSH Credentials (Random):`);
console.log(`   Username: ${SSH_USERNAME}`);
console.log(`   Password: ${SSH_PASSWORD}`);
console.log(`🌐 WebSocket SSH Path: /${SSH_PATH}`);
console.log(`📁 Subscription Path: /${SUB_PATH}`);
console.log(`🖥️  Server running on port: ${PORT}`);

// Use Deno.serve with routing
Deno.serve({ port: PORT }, async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  const path = url.pathname;

  const headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, CONNECT",
    "Cache-Control": "no-store",
    "X-Accel-Buffering": "no",
    "X-Padding": generatePadding(100, 1000),
  };

  // SSH WebSocket endpoint
  if (path === `/${SSH_PATH}` || path === `/${SSH_PATH}/`) {
    return handleSSHWebSocket(req);
  }

  // Root endpoint
  if (path === "/") { 
    const infoHtml = `
<!DOCTYPE html>
<html>
<head>
    <title>VLESS + SSH WebSocket Server</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        .info { background: #e8f4f8; padding: 20px; border-radius: 5px; margin: 20px 0; }
        .ssh-info { background: #f0f8e8; padding: 20px; border-radius: 5px; margin: 20px 0; }
        code { background: #e0e0e0; padding: 2px 5px; border-radius: 3px; }
        .url { color: #0066cc; font-family: monospace; font-size: 16px; }
    </style>
</head>
<body>
    <h1>🚀 VLESS + SSH WebSocket Server</h1>
    <div class="info">
        <h2>📡 VLESS Configuration</h2>
        <p>Subscription URL: <code class="url">/${SUB_PATH}</code></p>
        <p>XHTTP Path: <code>/${XPATH}</code></p>
    </div>
    <div class="ssh-info">
        <h2>🔐 SSH WebSocket Tunnel</h2>
        <p>WebSocket URL: <code class="url">wss://${DOMAIN || "your-domain"}/${SSH_PATH}</code></p>
        <p>Username: <strong>${SSH_USERNAME}</strong></p>
        <p>Password: <strong>${SSH_PASSWORD}</strong></p>
        <p><small>⚠️ Credentials are regenerated on server restart</small></p>
    </div>
</body>
</html>
    `;
    return new Response(infoHtml, 
    { status: 200, 
      headers: { "Content-Type": "text/html" }, }); 
  } 

  // VLESS subscription endpoint
  if (path === `/${SUB_PATH}`) {
    const serverIP = IP || url.hostname;
    const vlessURL = `vless://${UUID}@${serverIP}:443?encryption=none&security=tls&sni=${serverIP}&fp=chrome&allowInsecure=1&type=xhttp&host=${serverIP}&path=${SETTINGS.XPATH}&mode=packet-up#${NAME}-${ISP}`;
    const base64Content = btoa(vlessURL);
    return new Response(base64Content + "\n", {
      status: 200,
      headers: { "Content-Type": "text/plain" },
    });
  }


  // VLESS XHTTP endpoint
  const pathMatch = path.match(new RegExp(`/${XPATH}/([^/]+)(?:/([0-9]+))?$`));
  if (!pathMatch) {
    return new Response("Not Found", { status: 404 });
  }

  const uuid = pathMatch[1];
  const seq = pathMatch[2] ? parseInt(pathMatch[2]) : null;

  if (req.method === "GET" && !seq) {
    let session = sessions.get(uuid);
    if (!session) {
      session = new Session(uuid);
      sessions.set(uuid, session);
    }

    session.downstreamStarted = true;
    const { readable, writable } = new TransformStream();
    session.startDownstream({ writable });

    return new Response(readable, {
      status: 200,
      headers: {
        ...headers,
        "Content-Type": "application/octet-stream",
        "Transfer-Encoding": "chunked",
      },
    });
  }

  if (req.method === "POST" && seq !== null) {
    let session = sessions.get(uuid);
    if (!session) {
      session = new Session(uuid);
      sessions.set(uuid, session);

      setTimeout(() => {
        const currentSession = sessions.get(uuid);
        if (currentSession && !currentSession.downstreamStarted) {
          currentSession.cleanup();
          sessions.delete(uuid);
        }
      }, SETTINGS.SESSION_TIMEOUT);
    }

    const data = await req.arrayBuffer();
    const buffer = new Uint8Array(data);

    try {
      await session.processPacket(seq, buffer);
      return new Response(null, { status: 200, headers });
    } catch (err) {
      session.cleanup();
      sessions.delete(uuid);
      return new Response(null, { status: 500 });
    }
  }
  return new Response("Not Found", { status: 404 });
});
