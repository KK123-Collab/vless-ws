// VLESS over WebSocket - Node version for Render free Web Service
// env: UUID (optional, default below), PORT (Render auto-injects)
const http = require('http');
const net = require('net');
const { WebSocketServer } = require('ws');

const UUID = process.env.UUID || 'bfc14c92-9382-4197-b2d1-7254b1bffd06';
const PORT = process.env.PORT || 3000;

function uuidBytes(u) {
  const hex = u.replace(/-/g, '');
  const b = Buffer.alloc(16);
  for (let i = 0; i < 16; i++) b[i] = parseInt(hex.substr(i * 2, 2), 16);
  return b;
}
const UID = uuidBytes(UUID);

const srv = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('OK');
});

const wss = new WebSocketServer({ noServer: true });

srv.on('upgrade', (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', (ws) => {
  let stage = 0;
  let remote = null;

  ws.on('message', (data) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);

    if (stage === 0) {
      stage = 1;
      if (buf.length < 24) { try { ws.close(); } catch (e) {} return; }
      if (buf[0] !== 0) { try { ws.close(); } catch (e) {} return; }
      if (!buf.slice(1, 17).equals(UID)) { try { ws.close(); } catch (e) {} return; }

      const optLen = buf[17];
      const cmdIdx = 18 + optLen;
      const cmd = buf[cmdIdx];
      const port = buf.readUInt16BE(cmdIdx + 1);
      const atyp = buf[cmdIdx + 3];
      let off = cmdIdx + 4;
      let host = '';

      if (atyp === 1) {
        host = buf[off] + '.' + buf[off + 1] + '.' + buf[off + 2] + '.' + buf[off + 3];
        off += 4;
      } else if (atyp === 2) {
        const len = buf[off];
        host = buf.slice(off + 1, off + 1 + len).toString();
        off += 1 + len;
      } else if (atyp === 3) {
        const parts = [];
        for (let i = 0; i < 8; i++) parts.push(buf.readUInt16BE(off + i * 2).toString(16));
        host = parts.join(':');
        off += 16;
      } else {
        try { ws.close(); } catch (e) {}
        return;
      }

      try { ws.send(Buffer.from([buf[0], 0])); } catch (e) {}

      if (cmd !== 1) { try { ws.close(); } catch (e) {} return; }

      remote = net.connect({ host: host, port: port }, () => {
        const payload = buf.slice(off);
        if (payload.length) remote.write(payload);
      });

      remote.on('data', (chunk) => {
        if (ws.readyState === ws.OPEN) ws.send(chunk);
      });
      remote.on('close', () => { try { ws.close(); } catch (e) {} });
      remote.on('error', () => { try { ws.close(); } catch (e) {} });
      return;
    }

    if (remote && remote.writable) remote.write(buf);
  });

  ws.on('close', () => { if (remote) remote.destroy(); });
  ws.on('error', () => { if (remote) remote.destroy(); });
});

srv.listen(PORT, () => console.log('vless-ws listening on ' + PORT));
