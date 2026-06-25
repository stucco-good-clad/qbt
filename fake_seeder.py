# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""
Minimal BitTorrent seeder — zero storage, on-demand piece serving.

Implements just enough of the BT wire protocol to:
1. Announce to trackers as a complete seeder
2. Accept incoming peer connections
3. Serve pieces fetched on-demand from the webseed proxy via HTTP Range requests

No libtorrent dependency. No local file storage.
"""

import hashlib
import http.client
import os
import random
import socket
import struct
import sys
import threading
import time
from typing import Optional

# ── Config ──────────────────────────────────────────────────────────
VIRTUAL_FILENAME = "code_1.125.1-1781859603_amd64.deb"
WEBSEED_URL = f"http://192.168.1.8:8080/{VIRTUAL_FILENAME}"
TORRENT_FILE = "/home/nexa/Downloads/code_1.125.1-1781859603_amd64.deb.torrent"
LISTEN_PORT = int(os.environ.get("BT_PORT", "6881"))
PROXY_HOST = "192.168.1.8"
PROXY_PORT = 8080
PIECE_SIZE = 1048576  # 1 MiB
# ────────────────────────────────────────────────────────────────────


# ── Bencode ─────────────────────────────────────────────────────────
def bdecode(data: bytes, idx: int = 0):
    """Decode bencoded data. Returns (value, new_index)."""
    if data[idx : idx + 1] == b"i":
        end = data.index(b"e", idx)
        return int(data[idx + 1 : end]), end + 1
    elif data[idx : idx + 1] == b"l":
        lst = []
        idx += 1
        while data[idx : idx + 1] != b"e":
            val, idx = bdecode(data, idx)
            lst.append(val)
        return lst, idx + 1
    elif data[idx : idx + 1] == b"d":
        d = {}
        idx += 1
        while data[idx : idx + 1] != b"e":
            key, idx = bdecode(data, idx)
            val, idx = bdecode(data, idx)
            d[key] = val
        return d, idx + 1
    else:
        colon = data.index(b":", idx)
        length = int(data[idx:colon])
        s = data[colon + 1 : colon + 1 + length]
        return s, colon + 1 + length


def bencode(value) -> bytes:
    """Encode a value to bencoded bytes."""
    if isinstance(value, int):
        return b"i" + str(value).encode() + b"e"
    elif isinstance(value, bytes):
        return str(len(value)).encode() + b":" + value
    elif isinstance(value, str):
        return bencode(value.encode())
    elif isinstance(value, list):
        return b"l" + b"".join(bencode(v) for v in value) + b"e"
    elif isinstance(value, dict):
        items = b"".join(bencode(k) + bencode(v) for k, v in sorted(value.items()))
        return b"d" + items + b"e"
    raise TypeError(f"Cannot bencode {type(value)}")


# ── Torrent metadata ───────────────────────────────────────────────
def load_torrent(path: str) -> dict:
    """Load and parse a .torrent file."""
    with open(path, "rb") as f:
        data = f.read()
    meta, _ = bdecode(data)
    return meta


def compute_info_hash(info_dict: bytes) -> bytes:
    """Compute SHA1 hash of the info dictionary."""
    return hashlib.sha1(info_dict).digest()


# ── Piece hashing ──────────────────────────────────────────────────
def load_piece_hashes(meta: dict) -> list[bytes]:
    """Extract piece hashes from torrent metadata."""
    info = meta[b"info"]
    pieces_raw = info[b"pieces"]
    return [pieces_raw[i : i + 20] for i in range(0, len(pieces_raw), 20)]


def get_total_size(info: dict) -> int:
    """Get total file size from torrent info."""
    if b"length" in info:
        return info[b"length"]
    return sum(f[b"length"] for f in info[b"files"])


# ── HTTP fetch from webseed proxy ──────────────────────────────────
def fetch_piece_range(start: int, end: int) -> bytes:
    """Fetch byte range from webseed proxy."""
    conn = http.client.HTTPConnection(PROXY_HOST, PROXY_PORT, timeout=30)
    conn.request(
        "GET",
        f"/{VIRTUAL_FILENAME}",
        headers={"Range": f"bytes={start}-{end}", "Host": f"{PROXY_HOST}:{PROXY_PORT}"},
    )
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return data


# ── BT Protocol Constants ──────────────────────────────────────────
BT_PROTOCOL = b"BitTorrent protocol"
MSG_CHOKE = 0
MSG_UNCHOKE = 1
MSG_INTERESTED = 2
MSG_NOT_INTERESTED = 3
MSG_HAVE = 4
MSG_BITFIELD = 5
MSG_REQUEST = 6
MSG_PIECE = 7
MSG_CANCEL = 8


def make_handshake(info_hash: bytes, peer_id: bytes) -> bytes:
    """Build BT handshake message."""
    reserved = b"\x00" * 8
    return (
        bytes([19]) + BT_PROTOCOL + reserved + info_hash + peer_id
    )


def make_bitfield(num_pieces: int) -> bytes:
    """Build bitfield message claiming we have all pieces."""
    num_bytes = (num_pieces + 7) // 8
    bitfield = b"\xff" * num_bytes
    # Clear trailing bits if needed
    if num_pieces % 8:
        bitfield = bitfield[:-1] + bytes([0xFF << (8 - num_pieces % 8)])
    return struct.pack(">IB", 1 + len(bitfield), MSG_BITFIELD) + bitfield


def make_unchoke() -> bytes:
    return struct.pack(">IB", 1, MSG_UNCHOKE)


def make_have(piece_index: int) -> bytes:
    return struct.pack(">IIB", 5, MSG_HAVE, piece_index)


# ── Peer handler ───────────────────────────────────────────────────
class PeerConnection:
    """Handles a single BT peer connection. Serves pieces on-demand."""

    def __init__(
        self,
        sock: socket.socket,
        addr: tuple,
        info_hash: bytes,
        num_pieces: int,
        total_size: int,
        piece_hashes: list[bytes],
        piece_size: int,
    ):
        self.sock = sock
        self.addr = addr
        self.info_hash = info_hash
        self.num_pieces = num_pieces
        self.total_size = total_size
        self.piece_hashes = piece_hashes
        self.piece_size = piece_size
        self.peer_id = None
        self.handshake_ok = False
        self.uploaded = 0
        self.served_pieces: set[int] = set()

    def handle(self):
        try:
            self.sock.settimeout(30)
            self._do_handshake()
            if not self.handshake_ok:
                print(f"  [debug] {self.addr}: handshake failed", flush=True)
                return
            self._send_handshake()
            self._send_bitfield()
            self._send_unchoke()
            print(f"  [debug] {self.addr}: handshake OK, entering message loop", flush=True)
            self._message_loop()
            print(f"  [debug] {self.addr}: message loop exited", flush=True)
        except Exception as e:
            print(f"  [error] {self.addr}: {type(e).__name__}: {e}", flush=True)
        finally:
            try:
                self.sock.close()
            except Exception:
                pass

    def _recv(self, n: int) -> bytes:
        """Receive exactly n bytes."""
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Peer closed connection")
            buf += chunk
        return buf

    def _do_handshake(self):
        """Receive and validate peer handshake."""
        # Length prefix (1 byte) + protocol (19) + reserved (8) + info_hash (20) + peer_id (20)
        # But BT handshake is NOT length-prefixed — it starts with byte 19
        pstrlen = self._recv(1)
        if pstrlen != b"\x13":
            return
        proto = self._recv(19)
        if proto != BT_PROTOCOL:
            return
        reserved = self._recv(8)
        self.info_hash = self._recv(20)
        self.peer_id = self._recv(20)
        self.handshake_ok = True

    def _send_handshake(self):
        """Send our handshake."""
        our_peer_id = b"-PYSEED01-" + bytes(random.randint(0, 255) for _ in range(10))
        msg = make_handshake(self.info_hash, our_peer_id)
        self.sock.sendall(msg)

    def _send_bitfield(self):
        """Announce we have all pieces."""
        self.sock.sendall(make_bitfield(self.num_pieces))

    def _send_unchoke(self):
        """Unchoke the peer so they can request pieces."""
        self.sock.sendall(make_unchoke())

    def _send_piece(self, index: int, begin: int, length: int):
        """Fetch piece data from webseed proxy and send to peer."""
        file_offset = index * self.piece_size + begin
        byte_end = file_offset + length - 1

        try:
            data = fetch_piece_range(file_offset, byte_end)
        except Exception as e:
            print(f"  [error] fetch failed piece {index}: {e}", flush=True)
            return

        if len(data) != length:
            print(f"  [error] piece {index}: wanted {length}, got {len(data)}", flush=True)
            return

        header = struct.pack(">IIBII", 9 + length, MSG_PIECE, index, begin, length)
        self.sock.sendall(header + data)
        self.uploaded += length
        if index not in self.served_pieces:
            self.served_pieces.add(index)
            print(f"  [serve] Piece {index} ({length} bytes) → {self.addr[0]}:{self.addr[1]}", flush=True)

    def _message_loop(self):
        """Process incoming messages from peer."""
        while True:
            try:
                raw_len = self._recv(4)
            except Exception as e:
                print(f"  [debug] {self.addr}: recv len failed: {e}", flush=True)
                break
            msg_len = struct.unpack(">I", raw_len)[0]

            if msg_len == 0:
                continue  # Keep-alive

            try:
                msg = self._recv(msg_len)
            except Exception as e:
                print(f"  [debug] {self.addr}: recv msg failed: {e}", flush=True)
                break
            msg_id = msg[0]

            if msg_id == MSG_INTERESTED:
                pass
            elif msg_id == MSG_NOT_INTERESTED:
                pass
            elif msg_id == MSG_REQUEST:
                index, begin, length = struct.unpack(">III", msg[1:13])
                if index >= self.num_pieces:
                    continue
                max_piece = self.piece_size if index < self.num_pieces - 1 else self.total_size - (self.num_pieces - 1) * self.piece_size
                if begin + length > max_piece:
                    continue
                self._send_piece(index, begin, length)
            elif msg_id == MSG_CANCEL:
                pass
            elif msg_id == MSG_HAVE:
                pass

    def stats(self) -> dict:
        return {
            "addr": f"{self.addr[0]}:{self.addr[1]}",
            "uploaded": self.uploaded,
            "served_pieces": len(self.served_pieces),
        }


# ── Tracker announce (HTTP) ────────────────────────────────────────
def udp_tracker_announce(tracker_url: str, info_hash: bytes, port: int) -> list[tuple[str, int]]:
    """Announce to UDP tracker (BEP 15). Returns list of (ip, port) peers."""
    # Parse host:port from udp://host:port/announce
    host_port = tracker_url.replace("udp://", "").split("/")[0]
    host, tport = host_port.split(":")
    tport = int(tport)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(8)

    try:
        # Step 1: Connect
        transaction_id = random.randint(0, 0xFFFFFFFF)
        connect_req = struct.pack(">QII", 0x41727101980, 0, transaction_id)
        sock.sendto(connect_req, (host, tport))

        data, _ = sock.recvfrom(65535)
        if len(data) < 16:
            return []
        action, resp_tid, connection_id = struct.unpack(">IIQ", data[:16])
        if action != 0 or resp_tid != transaction_id:
            return []

        # Step 2: Announce
        transaction_id = random.randint(0, 0xFFFFFFFF)
        peer_id = b"-PYSEED01-" + bytes(random.randint(0, 255) for _ in range(10))
        announce_req = struct.pack(
            ">QII20s20sQQQiiIIIH",
            connection_id,          # 8
            1,                      # 4 action=announce
            transaction_id,         # 4
            info_hash,              # 20
            peer_id,                # 20
            0,                      # 8 downloaded
            0,                      # 8 uploaded
            0,                      # 8 left (seeder)
            0,                      # 4 event=none
            0,                      # 4 IP (any)
            -1,                     # 4 num_key (use default)
            port,                   # 4 port
            200,                    # 2 num_want
        )
        sock.sendto(announce_req, (host, tport))

        data, _ = sock.recvfrom(65535)
        if len(data) < 20:
            return []
        action, resp_tid, interval, leechers, seeders = struct.unpack(">IIIII", data[:20])
        if action != 1 or resp_tid != transaction_id:
            return []

        # Parse compact peer list
        peers = []
        for i in range(20, len(data), 6):
            if i + 6 > len(data):
                break
            ip = ".".join(str(b) for b in data[i : i + 4])
            pport = struct.unpack(">H", data[i + 4 : i + 6])[0]
            if pport > 0:
                peers.append((ip, pport))

        return peers
    except Exception as e:
        return []
    finally:
        sock.close()


def http_tracker_announce(tracker_url: str, info_hash: bytes, port: int) -> list[tuple[str, int]]:
    """Announce to HTTP tracker. Returns list of (ip, port) peers."""
    from urllib.parse import quote

    params = {
        "info_hash": info_hash,
        "peer_id": b"-PYSEED01-" + bytes(random.randint(0, 255) for _ in range(10)),
        "port": port,
        "uploaded": 0,
        "downloaded": 0,
        "left": 0,
        "compact": 1,
        "numwant": 200,
    }

    parts = []
    for k, v in params.items():
        if isinstance(v, bytes):
            parts.append(f"{k}={quote(v)}")
        else:
            parts.append(f"{k}={v}")
    query = "&".join(parts)

    host = tracker_url.split("//")[1].split("/")[0]
    path = "/" + tracker_url.split("//")[1].split("/", 1)[1].split("?")[0]
    scheme = "https" if tracker_url.startswith("https") else "http"

    try:
        if scheme == "https":
            conn = http.client.HTTPSConnection(host, timeout=10)
        else:
            conn = http.client.HTTPConnection(host, timeout=10)
        conn.request("GET", f"{path}?{query}")
        resp = conn.getresponse()
        body = resp.read()
        conn.close()

        if resp.status != 200:
            return []

        result, _ = bdecode(body)
        if not isinstance(result, dict):
            return []

        peers_raw = result.get(b"peers", b"")
        if isinstance(peers_raw, bytes):
            peers = []
            for i in range(0, len(peers_raw), 6):
                ip = ".".join(str(b) for b in peers_raw[i : i + 4])
                pport = struct.unpack(">H", peers_raw[i + 4 : i + 6])[0]
                peers.append((ip, pport))
            return peers
    except Exception:
        pass
    return []


def tracker_announce(tracker_url: str, info_hash: bytes, port: int, num_pieces: int) -> list[tuple[str, int]]:
    """Announce to tracker (UDP or HTTP). Returns list of (ip, port) peers."""
    if tracker_url.startswith("udp://"):
        return udp_tracker_announce(tracker_url, info_hash, port)
    else:
        return http_tracker_announce(tracker_url, info_hash, port)


# ── Main ───────────────────────────────────────────────────────────
def main():
    print("=== Minimal BT Seeder (on-demand, zero storage) ===\n")

    # Load torrent
    meta = load_torrent(TORRENT_FILE)
    info = meta[b"info"]
    info_bytes = bencode(info)
    info_hash = compute_info_hash(info_bytes)
    piece_hashes_list = load_piece_hashes(meta)
    num_pieces = len(piece_hashes_list)
    total_size = get_total_size(info)
    piece_size = info.get(b"piece length", PIECE_SIZE)

    print(f"  Torrent:  {info.get(b'name', b'?').decode()}")
    print(f"  Pieces:   {num_pieces} x {piece_size} bytes")
    print(f"  Total:    {total_size} bytes ({total_size / 1048576:.1f} MiB)")
    print(f"  Info hash: {info_hash.hex()}")
    print(f"  Webseed:  {WEBSEED_URL}")
    print(f"  Listen:   port {LISTEN_PORT}")

    # Get trackers
    trackers = []
    if b"announce" in meta:
        trackers.append(meta[b"announce"].decode())
    if b"announce-list" in meta:
        for tier in meta[b"announce-list"]:
            for t in tier:
                url = t.decode()
                if url not in trackers:
                    trackers.append(url)
    print(f"  Trackers: {len(trackers)}")

    # Start tracker announce thread
    def tracker_loop():
        # Immediate first announce
        for turl in trackers:
            try:
                peers = tracker_announce(turl, info_hash, LISTEN_PORT, num_pieces)
                if peers:
                    print(f"  [tracker] {turl}: {len(peers)} peers found")
                    for ip, port in peers:
                        threading.Thread(target=connect_to_peer, args=(ip, port), daemon=True).start()
                else:
                    print(f"  [tracker] {turl}: 0 peers (announce sent)")
            except Exception as e:
                print(f"  [tracker] {turl}: {e}")
        # Periodic re-announce
        while True:
            time.sleep(30)
            for turl in trackers:
                try:
                    peers = tracker_announce(turl, info_hash, LISTEN_PORT, num_pieces)
                    if peers:
                        print(f"  [tracker] {turl}: {len(peers)} peers found")
                        for ip, port in peers:
                            threading.Thread(target=connect_to_peer, args=(ip, port), daemon=True).start()
                except Exception:
                    pass

    def connect_to_peer(ip: str, port: int):
        """Outgoing connection to a peer (for DHT/tracker-discovered peers)."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((ip, port))
            pc = PeerConnection(
                sock, (ip, port), info_hash, num_pieces, total_size,
                piece_hashes_list, piece_size,
            )
            pc.handle()
        except Exception:
            pass

    threading.Thread(target=tracker_loop, daemon=True).start()

    # Start TCP listener
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", LISTEN_PORT))
    server.listen(16)
    server.settimeout(1.0)
    print(f"\n  Listening on 0.0.0.0:{LISTEN_PORT}")
    print(f"  Seeder is LIVE. Zero storage. On-demand serving.")
    print(f"  Press Ctrl+C to stop.\n")

    connections: list[PeerConnection] = []

    try:
        while True:
            try:
                sock, addr = server.accept()
                pc = PeerConnection(
                    sock, addr, info_hash, num_pieces, total_size,
                    piece_hashes_list, piece_size,
                )
                t = threading.Thread(target=pc.handle, daemon=True)
                t.start()
                connections.append(pc)
                # Prune old entries
                connections = [c for c in connections if c.handshake_ok]
            except socket.timeout:
                pass

            # Print stats
            active = [c for c in connections if c.handshake_ok]
            total_up = sum(c.uploaded for c in active)
            total_served = set()
            for c in active:
                total_served |= c.served_pieces
            print(
                f"\r  Peers: {len(active):3d}  "
                f"Uploaded: {total_up / 1048576:8.1f} MiB  "
                f"Pieces served: {len(total_served)}/{num_pieces}",
                end="", flush=True,
            )
    except KeyboardInterrupt:
        print("\n\nShutting down ...")
        server.close()
        print("Done.")


if __name__ == "__main__":
    main()
