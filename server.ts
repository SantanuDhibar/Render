// server.ts
// SSH WebSocket Tunnel for Render (works like XHTTP)

const UUID: string = Deno.env.get("UUID") || "f9a1ba12-7187-4b25-a5d5-7bafd82ffb4d";
const PORT: number = parseInt(Deno.env.get("PORT") || "8080");

// Simple session management
const sessions = new Map<string, WebSocket>();

// Handle SSH WebSocket connections
async function handleSSHWebSocket(socket: WebSocket, sessionId: string) {
  console.log(`New SSH WebSocket connection: ${sessionId}`);
  
  // Store the session
  sessions.set(sessionId, socket);
  
  socket.onmessage = async (event) => {
    try {
      const data = typeof event.data === 'string'
        ? new TextEncoder().encode(event.data)
        : new Uint8Array(await event.data.arrayBuffer());
      
      // Just echo for testing - you can modify this to forward to actual SSH
      // Since Render doesn't have SSH server, we'll create a tunnel endpoint
      console.log(`Received ${data.length} bytes from ${sessionId}`);
      
      // Echo back for testing (replace with actual SSH forwarding)
      socket.send(data);
    } catch (err) {
      console.error("Error handling message:", err);
    }
  };
  
  socket.onclose = () => {
    console.log(`SSH WebSocket closed: ${sessionId}`);
    sessions.delete(sessionId);
  };
  
  socket.onerror = (error) => {
    console.error(`WebSocket error for ${sessionId}:`, error);
    sessions.delete(sessionId);
  };
  
  // Send initial connection success message
  socket.send(new TextEncoder().encode("SSH WebSocket Tunnel Connected\n"));
}

// Handle HTTP requests for subscription (to work with VLESS clients)
async function handleSubscription(req: Request, url: URL): Promise<Response> {
  const host = req.headers.get("host") || url.hostname;
  const protocol = req.headers.get("x-forwarded-proto") || "https";
  const serverIP = `${protocol}://${host}`;
  
  // Generate VLESS config that works with WebSocket
  const vlessConfig = `vless://${UUID}@${host}:443?encryption=none&security=tls&sni=${host}&fp=chrome&allowInsecure=1&type=ws&host=${host}&path=%2Fssh&mode=packet-up#SSH-Tunnel`;
  
  // Return base64 encoded config
  const base64Config = btoa(vlessConfig);
  
  return new Response(base64Config, {
    status: 200,
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

// Start the server
Deno.serve({ port: PORT }, async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  const path = url.pathname;
  
  console.log(`${req.method} ${path}`);
  
  // Subscription endpoint for VLESS clients
  if (path === "/sub" || path === `/${Deno.env.get("SUB_PATH") || "sub"}`) {
    return await handleSubscription(req, url);
  }
  
  // SSH WebSocket endpoint
  if (path === "/ssh" || path === "/") {
    const upgrade = req.headers.get("upgrade");
    
    if (upgrade && upgrade.toLowerCase() === "websocket") {
      console.log("WebSocket upgrade request received");
      
      try {
        const { socket, response } = Deno.upgradeWebSocket(req);
        const sessionId = crypto.randomUUID();
        handleSSHWebSocket(socket, sessionId);
        return response;
      } catch (error) {
        console.error("WebSocket upgrade failed:", error);
        return new Response("WebSocket upgrade failed", { status: 500 });
      }
    }
    
    // Return info page for non-WebSocket requests
    return new Response(
      "SSH WebSocket Tunnel Server\n" +
      "=======================\n\n" +
      "WebSocket endpoint: wss://" + url.host + "/ssh\n" +
      "VLESS subscription: https://" + url.host + "/sub\n\n" +
      "Configure VLESS client with:\n" +
      "- Type: WebSocket (ws)\n" +
      "- Path: /ssh\n" +
      "- UUID: " + UUID + "\n",
      {
        status: 200,
        headers: { "Content-Type": "text/plain" },
      }
    );
  }
  
  // Health check endpoint
  if (path === "/health") {
    return new Response("OK", { status: 200 });
  }
  
  return new Response("Not Found", { status: 404 });
});

console.log(`SSH WebSocket Tunnel Server running on port ${PORT}`);
console.log(`WebSocket endpoint: ws://localhost:${PORT}/ssh`);
console.log(`Subscription endpoint: http://localhost:${PORT}/sub`);
console.log(`UUID: ${UUID}`);
