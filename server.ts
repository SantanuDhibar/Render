// server.ts
// For Render deployment - VLESS WebSocket

const UUID: string = Deno.env.get("UUID") || "f9a1ba12-7187-4b25-a5d5-7bafd82ffb4d";
const SUB_PATH: string = Deno.env.get("SUB_PATH") || "sub";
const WSPATH: string = Deno.env.get("WSPATH") || "ws";
const DOMAIN: string = Deno.env.get("DOMAIN") || "render-pdj5.onrender.com";
const NAME: string = Deno.env.get("NAME") || "Render";
const PORT: number = parseInt(Deno.env.get("PORT") || "8080");

const SETTINGS = {
  UUID,
  WSPATH: `/${WSPATH}`,
  SESSION_TIMEOUT: 30000,
};

function parse_uuid(uuid: string): Uint8Array {
  uuid = uuid.replaceAll("-", "");
  const r = new Uint8Array(16);
  for (let index = 0; index < 16; index++) {
    r[index] = parseInt(uuid.substr(index * 2, 2), 16);
  }
  return r;
}

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

async function read_vless_header(
  data: Uint8Array,
  cfg_uuid_str: string
): Promise<{
  hostname: string;
  port: number;
  data: Uint8Array;
  resp: Uint8Array;
}> {
  let offset = 0;
  
  if (data.length < 1 + 16 + 1) {
    throw new Error("header too short");
  }

  const version = data[offset];
  offset++;
  
  const uuid = data.slice(offset, offset + 16);
  offset += 16;
  
  const cfg_uuid = parse_uuid(cfg_uuid_str);
  if (!validate_uuid(uuid, cfg_uuid)) {
    throw new Error("invalid UUID");
  }
  
  const pb_len = data[offset];
  offset++;
  
  // Skip protobuf data
  offset += pb_len;
  
  if (data.length <= offset) {
    throw new Error("no command");
  }
  
  const cmd = data[offset];
  offset++;
  
  const COMMAND_TYPE_TCP = 1;
  if (cmd !== COMMAND_TYPE_TCP) {
    throw new Error(`unsupported command: ${cmd}`);
  }
  
  if (data.length < offset + 2) {
    throw new Error("no port");
  }
  
  const port = (data[offset] << 8) + data[offset + 1];
  offset += 2;
  
  if (data.length <= offset) {
    throw new Error("no address type");
  }
  
  const atype = data[offset];
  offset++;
  
  const ADDRESS_TYPE_IPV4 = 1;
  const ADDRESS_TYPE_STRING = 2;
  const ADDRESS_TYPE_IPV6 = 3;
  
  let hostname = "";
  
  if (atype === ADDRESS_TYPE_IPV4) {
    if (data.length < offset + 4) throw new Error("no ipv4");
    hostname = Array.from(data.slice(offset, offset + 4))
      .map((b) => b.toString())
      .join(".");
    offset += 4;
  } else if (atype === ADDRESS_TYPE_STRING) {
    if (data.length <= offset) throw new Error("no domain length");
    const domainLen = data[offset];
    offset++;
    if (data.length < offset + domainLen) throw new Error("no domain");
    hostname = new TextDecoder().decode(data.slice(offset, offset + domainLen));
    offset += domainLen;
  } else if (atype === ADDRESS_TYPE_IPV6) {
    if (data.length < offset + 16) throw new Error("no ipv6");
    hostname = Array.from({ length: 8 }, (_, i) =>
      ((data[offset + i * 2] << 8) + data[offset + i * 2 + 1]).toString(16)
    ).join(":");
    offset += 16;
  }
  
  if (!hostname) {
    throw new Error("parse hostname failed");
  }
  
  const remaining = data.slice(offset);
  
  return {
    hostname,
    port,
    data: remaining,
    resp: new Uint8Array([version, 0]),
  };
}

let ISP = "";
try {
  const response = await fetch("https://speed.cloudflare.com/meta");
  if (response.ok) {
    const data = await response.json() as {
      country: string;
      asOrganization: string;
    };
    ISP = `${data.country}-${data.asOrganization}`.replace(/ /g, "_");
  }
} catch (err) {
  ISP = "unknown";
}

let IP = DOMAIN;
if (!DOMAIN) {
  IP = Deno.env.get("RENDER_EXTERNAL_HOSTNAME") || "localhost";
}

// Store active connections
const connections = new Map<string, {
  remote: Deno.Conn | null;
  initialized: boolean;
  responseHeader: Uint8Array | null;
}>();

Deno.serve({ port: PORT }, async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  const path = url.pathname;
  
  // Root path
  if (path === "/") {
    return new Response("VLESS WebSocket Server Running on Render\n", {
      status: 200,
      headers: { "Content-Type": "text/plain" },
    });
  }
  
  // Subscription path
  if (path === `/${SUB_PATH}`) {
    const serverIP = IP || url.hostname;
    const vlessURL = `vless://${UUID}@${serverIP}:443?encryption=none&security=tls&sni=${serverIP}&fp=chrome&allowInsecure=1&type=ws&host=${serverIP}&path=${SETTINGS.WSPATH}#${NAME}-${ISP}`;
    const base64Content = btoa(vlessURL);
    return new Response(base64Content + "\n", {
      status: 200,
      headers: { "Content-Type": "text/plain" },
    });
  }
  
  // WebSocket path
  if (path === SETTINGS.WSPATH) {
    const upgrade = req.headers.get("upgrade");
    
    if (upgrade && upgrade.toLowerCase() === "websocket") {
      try {
        const { socket, response } = Deno.upgradeWebSocket(req);
        
        let sessionId = crypto.randomUUID();
        let remoteConn: Deno.Conn | null = null;
        let initialized = false;
        let responseHeader: Uint8Array | null = null;
        
        connections.set(sessionId, {
          remote: null,
          initialized: false,
          responseHeader: null,
        });
        
        socket.onopen = () => {
          console.log("WebSocket connection opened");
        };
        
        socket.onmessage = async (event) => {
          try {
            let data: Uint8Array;
            
            if (event.data instanceof ArrayBuffer) {
              data = new Uint8Array(event.data);
            } else if (event.data instanceof Blob) {
              data = new Uint8Array(await event.data.arrayBuffer());
            } else {
              return;
            }
            
            const session = connections.get(sessionId);
            if (!session) return;
            
            // Parse VLESS header on first message
            if (!session.initialized) {
              try {
                const vless = await read_vless_header(data, UUID);
                
                // Connect to remote
                remoteConn = await Deno.connect({
                  hostname: vless.hostname,
                  port: vless.port,
                });
                
                session.remote = remoteConn;
                session.initialized = true;
                session.responseHeader = vless.resp;
                
                // Send response header back to client
                if (socket.readyState === WebSocket.OPEN) {
                  socket.send(vless.resp);
                }
                
                // Send remaining data
                if (vless.data.length > 0 && socket.readyState === WebSocket.OPEN) {
                  socket.send(vless.data);
                }
                
                // Start reading from remote and sending to client
                (async () => {
                  try {
                    const reader = remoteConn.readable.getReader();
                    while (true) {
                      const { value, done } = await reader.read();
                      if (done) break;
                      if (socket.readyState === WebSocket.OPEN) {
                        socket.send(value);
                      }
                    }
                  } catch (err) {
                    console.error("Remote read error:", err);
                  } finally {
                    if (socket.readyState === WebSocket.OPEN) {
                      socket.close();
                    }
                  }
                })();
                
              } catch (err) {
                console.error("VLESS header parse error:", err);
                socket.close();
              }
            } else {
              // Send data to remote
              if (remoteConn && session.remote) {
                const writer = remoteConn.writable.getWriter();
                await writer.write(data);
                writer.releaseLock();
              }
            }
          } catch (err) {
            console.error("WebSocket message error:", err);
            socket.close();
          }
        };
        
        socket.onclose = () => {
          const session = connections.get(sessionId);
          if (session && session.remote) {
            try {
              session.remote.close();
            } catch (err) {
              // Ignore close errors
            }
          }
          connections.delete(sessionId);
          console.log("WebSocket connection closed");
        };
        
        socket.onerror = (error) => {
          console.error("WebSocket error:", error);
          const session = connections.get(sessionId);
          if (session && session.remote) {
            try {
              session.remote.close();
            } catch (err) {
              // Ignore close errors
            }
          }
          connections.delete(sessionId);
        };
        
        return response;
      } catch (error) {
        console.error("WebSocket upgrade error:", error);
        return new Response("WebSocket upgrade failed", { status: 500 });
      }
    }
  }
  
  return new Response("Not Found", { status: 404 });
});

console.log(`VLESS WebSocket Server running on port ${PORT}`);
console.log(`WebSocket path: ${SETTINGS.WSPATH}`);
console.log(`Subscription path: /${SUB_PATH}`);
console.log(`Connection URL: vless://${UUID}@${IP}:443?encryption=none&security=tls&sni=${IP}&fp=chrome&allowInsecure=1&type=ws&host=${IP}&path=${SETTINGS.WSPATH}#${NAME}-${ISP}`);
