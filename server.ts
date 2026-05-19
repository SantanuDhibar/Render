// server.ts
// SSH WebSocket Server for Render

const PORT: number = parseInt(Deno.env.get("PORT") || "443");

// SSH WebSocket handler
async function handleSSHWebSocket(socket: WebSocket) {
  let sshConn: Deno.Conn | null = null;
  
  console.log("New WebSocket connection established");
  
  try {
    // Connect to local SSH server (port 22)
    sshConn = await Deno.connect({ hostname: "render.santanudhibar.deno.net", port: 22 });
    console.log("Connected to SSH server on port 22");
    
    // Handle incoming messages from WebSocket client
    socket.onmessage = async (event) => {
      if (!sshConn) return;
      
      try {
        const data = typeof event.data === 'string'
          ? new TextEncoder().encode(event.data)
          : new Uint8Array(await event.data.arrayBuffer());
        
        const writer = sshConn.writable.getWriter();
        await writer.write(data);
        writer.releaseLock();
      } catch (err) {
        console.error("SSH write error:", err);
        socket.close();
      }
    };
    
    // Pipe SSH output to WebSocket
    const reader = sshConn.readable.getReader();
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(value);
        }
      }
    } catch (err) {
      console.error("SSH read error:", err);
    } finally {
      reader.releaseLock();
      if (socket.readyState === WebSocket.OPEN) {
        socket.close();
      }
    }
  } catch (err) {
    console.error("SSH connection error:", err);
    const errorMsg = new TextEncoder().encode("SSH connection failed: " + err.message);
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(errorMsg);
      socket.close();
    }
  } finally {
    if (sshConn) {
      sshConn.close();
      console.log("SSH connection closed");
    }
  }
}

// Start server
Deno.serve({ port: PORT }, async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  const path = url.pathname;
  
  // Handle WebSocket upgrade on root path "/"
  if (path === "/") {
    const upgrade = req.headers.get("upgrade");
    if (upgrade && upgrade.toLowerCase() === "websocket") {
      console.log("WebSocket upgrade request received");
      const { socket, response } = Deno.upgradeWebSocket(req);
      handleSSHWebSocket(socket);
      return response;
    }
    
    // Return info for non-WebSocket requests
    return new Response("SSH WebSocket Server\nUsage: Connect using WebSocket client to ws://host:443/\n", 
    { status: 200, 
      headers: { "Content-Type": "text/plain" } }); 
  }
  
  // 404 for any other path
  return new Response("Not Found - Only root path '/' is available for WebSocket connections", { status: 404 });
});

console.log(`SSH WebSocket Server is running on port ${PORT}`);
console.log(`Connect using: ws://localhost:${PORT}/`);
console.log(`Waiting for WebSocket connections...`);
