/**
 * LizardCraft MP server — zero dependencies (pure Node.js)
 * Start: node server.js
 * PORT from env (Render sets this automatically)
 */
const http = require("http");
const crypto = require("crypto");
const PORT = process.env.PORT || 8765;

const rooms = new Map();

function roomGet(code) {
  code = String(code || "").toUpperCase();
  if (!rooms.has(code)) rooms.set(code, { clients: new Set(), edits: {}, hostId: null });
  return rooms.get(code);
}

function broadcast(room, data, except) {
  const raw = typeof data === "string" ? data : JSON.stringify(data);
  for (const c of room.clients) {
    if (c !== except && c.readyState === 1) {
      try { c.send(raw); } catch (e) {}
    }
  }
}

/* Minimal WebSocket (RFC6455) — no npm packages */
function acceptWs(req, socket, head) {
  const key = req.headers["sec-websocket-key"];
  if (!key) { socket.destroy(); return null; }
  const accept = crypto
    .createHash("sha1")
    .update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
    .digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
    "Upgrade: websocket\r\n" +
    "Connection: Upgrade\r\n" +
    "Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
  );
  if (head && head.length) socket.unshift(head);

  const ws = {
    readyState: 1,
    socket,
    meta: { id: crypto.randomBytes(4).toString("hex"), room: null },
    send(str) {
      const payload = Buffer.from(str);
      const len = payload.length;
      let header;
      if (len < 126) {
        header = Buffer.alloc(2);
        header[0] = 0x81;
        header[1] = len;
      } else if (len < 65536) {
        header = Buffer.alloc(4);
        header[0] = 0x81;
        header[1] = 126;
        header.writeUInt16BE(len, 2);
      } else {
        header = Buffer.alloc(10);
        header[0] = 0x81;
        header[1] = 127;
        header.writeUInt32BE(0, 2);
        header.writeUInt32BE(len, 6);
      }
      try { socket.write(Buffer.concat([header, payload])); } catch (e) {}
    },
    close() {
      this.readyState = 3;
      try { socket.end(); } catch (e) {}
    },
  };

  let buf = Buffer.alloc(0);
  socket.on("data", (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    while (buf.length >= 2) {
      const finOp = buf[0];
      const opcode = finOp & 0x0f;
      const maskBit = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f;
      let off = 2;
      if (len === 126) {
        if (buf.length < 4) return;
        len = buf.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (buf.length < 10) return;
        len = Number(buf.readUInt32BE(6));
        off = 10;
      }
      const maskLen = maskBit ? 4 : 0;
      if (buf.length < off + maskLen + len) return;
      let payload = buf.slice(off + maskLen, off + maskLen + len);
      if (maskBit) {
        const mask = buf.slice(off, off + 4);
        payload = Buffer.from(payload.map((b, i) => b ^ mask[i % 4]));
      }
      buf = buf.slice(off + maskLen + len);
      if (opcode === 0x8) {
        ws.readyState = 3;
        socket.end();
        return;
      }
      if (opcode === 0x9) {
        // ping -> pong
        const pong = Buffer.alloc(2 + payload.length);
        pong[0] = 0x8a;
        pong[1] = payload.length;
        payload.copy(pong, 2);
        try { socket.write(pong); } catch (e) {}
        continue;
      }
      if (opcode === 0x1) {
        let msg;
        try { msg = JSON.parse(payload.toString()); } catch (e) { continue; }
        onMessage(ws, msg);
      }
    }
  });
  socket.on("close", () => onClose(ws));
  socket.on("error", () => onClose(ws));

  ws.send(JSON.stringify({ type: "hello", id: ws.meta.id }));
  return ws;
}

function onMessage(ws, msg) {
  if (!msg || !msg.type) return;

  if (msg.type === "join") {
    const code = String(msg.code || "").trim().toUpperCase().slice(0, 12);
    if (!code || code.length < 2) {
      ws.send(JSON.stringify({ type: "err", text: "bad code" }));
      return;
    }
    if (ws.meta.room) {
      const old = rooms.get(ws.meta.room);
      if (old) {
        old.clients.delete(ws);
        broadcast(old, { type: "bye", id: ws.meta.id });
        if (old.clients.size === 0) rooms.delete(ws.meta.room);
      }
    }
    const room = roomGet(code);
    room.clients.add(ws);
    ws.meta.room = code;
    if (!room.hostId) room.hostId = ws.meta.id;
    ws.send(JSON.stringify({
      type: "joined",
      code,
      id: ws.meta.id,
      host: room.hostId === ws.meta.id,
      edits: room.edits,
      players: room.clients.size,
    }));
    broadcast(room, { type: "peer", id: ws.meta.id, n: room.clients.size }, ws);
    return;
  }

  const code = ws.meta.room;
  if (!code) return;
  const room = rooms.get(code);
  if (!room) return;

  if (msg.type === "blk") {
    const { x, y, z, t } = msg;
    if (![x, y, z, t].every(Number.isInteger)) return;
    if (y < 0 || y > 64 || t < 0 || t > 80) return;
    const k = (x >> 4) + "," + (z >> 4);
    const i = (y * 16 + (z & 15)) * 16 + (x & 15);
    if (!room.edits[k]) room.edits[k] = [];
    const arr = room.edits[k];
    let found = false;
    for (let j = 0; j < arr.length; j += 2) {
      if (arr[j] === i) { arr[j + 1] = t; found = true; break; }
    }
    if (!found) arr.push(i, t);
    broadcast(room, { type: "blk", x, y, z, t, from: ws.meta.id }, ws);
    return;
  }

  if (msg.type === "pres") {
    msg.id = ws.meta.id;
    broadcast(room, msg, ws);
    return;
  }
}

function onClose(ws) {
  if (ws.readyState === 3 && !ws.meta.room) return;
  ws.readyState = 3;
  const code = ws.meta.room;
  if (!code) return;
  const room = rooms.get(code);
  if (!room) return;
  room.clients.delete(ws);
  broadcast(room, { type: "bye", id: ws.meta.id });
  if (room.hostId === ws.meta.id) {
    room.hostId = room.clients.size ? [...room.clients][0].meta.id : null;
  }
  if (room.clients.size === 0) rooms.delete(code);
  ws.meta.room = null;
}

const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  if (req.url === "/" || req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({
      ok: true,
      game: "LizardCraft",
      rooms: rooms.size,
      players: [...rooms.values()].reduce((n, r) => n + r.clients.size, 0),
    }));
    return;
  }
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("LizardCraft MP server (no npm deps)");
});

server.on("upgrade", (req, socket, head) => {
  acceptWs(req, socket, head);
});

server.listen(PORT, "0.0.0.0", () => {
  console.log("LizardCraft MP on port", PORT);
});
