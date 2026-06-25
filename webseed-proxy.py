#!/usr/bin/env python3
"""
Webseed proxy for BitTorrent HTTP seeding (BEP 19).
Reads torrent metadata to serve the virtual byte stream from CDN piece files.
Optimized: connection pooling + concurrent piece fetching.
"""

import http.server
import http.client
import ssl
import threading
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fake_seeder import bdecode

HOST = "0.0.0.0"
PORT = int(os.environ.get("WEBSEED_PORT", "80"))

GITHUB_HOST = "raw.githubusercontent.com"
GITHUB_PATH_BASE = os.environ.get(
    "GITHUB_PATH_BASE",
    "/stucco-good-clad/qbt/refs/heads/main",
)
TORRENT_FILE = os.environ.get(
    "TORRENT_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "current.torrent"),
)

with open(TORRENT_FILE, "rb") as _f:
    _meta = bdecode(_f.read())[0]
_info = _meta[b"info"]
TORRENT_NAME = _info[b"name"].decode()
VIRTUAL_PATH = f"/{TORRENT_NAME}"
PIECE_SIZE = _info[b"piece length"]
_raw_pieces = _info[b"pieces"]
NUM_PIECES = len(_raw_pieces) // 20

IS_MULTI_FILE = b"files" in _info
if IS_MULTI_FILE:
    _total = sum(f[b"length"] for f in _info[b"files"])
else:
    _total = _info[b"length"]
TOTAL_SIZE = _total
PIECE_LAST_SIZE = TOTAL_SIZE - (NUM_PIECES - 1) * PIECE_SIZE

FILE_MAP = {}
if IS_MULTI_FILE:
    _offset = 0
    for _f in _info[b"files"]:
        _path = "/".join(p.decode() for p in _f[b"path"])
        _size = _f[b"length"]
        FILE_MAP[_path] = (_offset, _size)
        _offset += _size

POOL_SIZE = 16
MAX_WORKERS = 8


class ConnectionPool:
    def __init__(self, host, pool_size=POOL_SIZE):
        self.host = host
        self.pool_size = pool_size
        self._lock = threading.Lock()
        self._conn_count = 0
        self._pool = []
        self._ssl_ctx = ssl.create_default_context()

    def _make_conn(self):
        conn = http.client.HTTPSConnection(self.host, timeout=30, context=self._ssl_ctx)
        return conn

    def get(self):
        with self._lock:
            if self._pool:
                return self._pool.pop()
        return self._make_conn()

    def put(self, conn):
        with self._lock:
            if len(self._pool) < self.pool_size:
                self._pool.append(conn)
            else:
                try:
                    conn.close()
                except Exception:
                    pass

    def close_all(self):
        with self._lock:
            for c in self._pool:
                try:
                    c.close()
                except Exception:
                    pass
            self._pool.clear()


pool = ConnectionPool(GITHUB_HOST)


def fetch_piece(piece_index, offset=0, length=None):
    piece_len = PIECE_SIZE if piece_index < NUM_PIECES - 1 else PIECE_LAST_SIZE
    start = offset
    end = offset + (length if length is not None else piece_len - offset) - 1
    end = min(end, piece_len - 1)

    url_path = f"{GITHUB_PATH_BASE}/piece_{piece_index}.bin"
    range_val = f"bytes={start}-{end}"

    conn = pool.get()
    retries = 2
    for attempt in range(retries):
        try:
            conn.request("GET", url_path, headers={
                "Range": range_val,
                "Host": GITHUB_HOST,
                "Connection": "keep-alive",
            })
            resp = conn.getresponse()
            data = resp.read()
            pool.put(conn)
            return data
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            if attempt < retries - 1:
                conn = pool._make_conn()
    return b""


def fetch_range_concurrent(range_start, range_end):
    content_length = range_end - range_start + 1
    start_piece = range_start // PIECE_SIZE
    end_piece = range_end // PIECE_SIZE

    if start_piece == end_piece:
        piece_offset = range_start % PIECE_SIZE
        return fetch_piece(start_piece, piece_offset, content_length)

    pieces_to_fetch = []
    fetched_so_far = 0
    for piece_idx in range(start_piece, end_piece + 1):
        piece_total = PIECE_SIZE if piece_idx < NUM_PIECES - 1 else PIECE_LAST_SIZE
        piece_offset = range_start % PIECE_SIZE if piece_idx == start_piece else 0
        bytes_this = min(piece_total - piece_offset, content_length - fetched_so_far)
        pieces_to_fetch.append((piece_idx, piece_offset, bytes_this))
        fetched_so_far += bytes_this

    results = [None] * len(pieces_to_fetch)
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(pieces_to_fetch))) as ex:
        futures = {}
        for i, (pidx, poff, plen) in enumerate(pieces_to_fetch):
            futures[ex.submit(fetch_piece, pidx, poff, plen)] = i
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()

    return b"".join(results)


class WebseedHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        sys.stderr.write(f"[webseed] {self.client_address[0]} - {format % args}\n")
        sys.stderr.flush()

    def _resolve_file(self):
        from urllib.parse import unquote
        decoded = unquote(self.path)
        prefix = f"/{TORRENT_NAME}/"
        if decoded.startswith(prefix):
            remainder = decoded[len(prefix):]
            if remainder in FILE_MAP:
                return FILE_MAP[remainder]
            double_prefix = f"{TORRENT_NAME}/"
            if remainder.startswith(double_prefix):
                file_path = remainder[len(double_prefix):]
                if file_path in FILE_MAP:
                    return FILE_MAP[file_path]
        return None

    def _handle(self, method):
        file_info = self._resolve_file()

        if file_info is not None:
            file_offset, file_size = file_info
            self._serve_file(method, file_offset, file_size)
        elif self.path == VIRTUAL_PATH:
            self._serve_stream(method, 0, TOTAL_SIZE)
        else:
            self.send_error(404)

    def _serve_file(self, method, file_offset, file_size):
        if method == "HEAD":
            self.send_response(200)
            self.send_header("Content-Length", str(file_size))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            return

        range_header = self.headers.get("Range")
        if range_header and range_header.startswith("bytes="):
            range_spec = range_header[6:]
            parts = range_spec.split("-")
            local_start = int(parts[0])
            local_end = int(parts[1]) if parts[1] else file_size - 1
            local_start = max(0, local_start)
            local_end = min(local_end, file_size - 1)
            content_length = local_end - local_start + 1

            abs_start = file_offset + local_start
            abs_end = file_offset + local_end

            self.send_response(206)
            self.send_header("Content-Range", f"bytes {local_start}-{local_end}/{file_size}")
            self.send_header("Content-Length", str(content_length))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

            data = fetch_range_concurrent(abs_start, abs_end)
            self.wfile.write(data)
            self.wfile.flush()
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(file_size))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

            abs_end = file_offset + file_size - 1
            chunk_size = 4 * PIECE_SIZE
            for offset in range(file_offset, file_offset + file_size, chunk_size):
                end = min(offset + chunk_size - 1, abs_end)
                data = fetch_range_concurrent(offset, end)
                self.wfile.write(data)
            self.wfile.flush()

    def _serve_stream(self, method, stream_start, stream_size):
        stream_end = stream_start + stream_size - 1

        if method == "HEAD":
            self.send_response(200)
            self.send_header("Content-Length", str(stream_size))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            return

        range_header = self.headers.get("Range")

        if range_header and range_header.startswith("bytes="):
            range_spec = range_header[6:]
            parts = range_spec.split("-")
            range_start = stream_start + int(parts[0])
            range_end = stream_start + (int(parts[1]) if parts[1] else stream_size - 1)

            range_start = max(stream_start, range_start)
            range_end = min(range_end, stream_end)
            content_length = range_end - range_start + 1

            self.send_response(206)
            self.send_header("Content-Range", f"bytes {range_start - stream_start}-{range_end - stream_start}/{stream_size}")
            self.send_header("Content-Length", str(content_length))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

            data = fetch_range_concurrent(range_start, range_end)
            self.wfile.write(data)
            self.wfile.flush()
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(stream_size))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

            chunk_size = 4 * PIECE_SIZE
            for offset in range(stream_start, stream_start + stream_size, chunk_size):
                end = min(offset + chunk_size - 1, stream_end)
                data = fetch_range_concurrent(offset, end)
                self.wfile.write(data)
            self.wfile.flush()

    def do_GET(self):
        self._handle("GET")

    def do_HEAD(self):
        self._handle("HEAD")


class ThreadedHTTPServer(http.server.ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128


if __name__ == "__main__":
    server = ThreadedHTTPServer((HOST, PORT), WebseedHandler)
    print(f"Webseed proxy listening on {HOST}:{PORT}")
    print(f"  Torrent:     {TORRENT_FILE}")
    print(f"  Virtual URL: http://localhost:{PORT}{VIRTUAL_PATH}")
    print(f"  Piece size:  {PIECE_SIZE:,} bytes ({PIECE_SIZE / 1024:.0f} KiB)")
    print(f"  Pieces:      {NUM_PIECES}")
    print(f"  Total size:  {TOTAL_SIZE:,} bytes ({TOTAL_SIZE / 1048576:.1f} MB)")
    print(f"  Multi-file:  {IS_MULTI_FILE} ({len(FILE_MAP)} files)")
    print(f"  CDN:         {GITHUB_HOST}{GITHUB_PATH_BASE}")
    print(f"  Pool:        {POOL_SIZE} connections, {MAX_WORKERS} workers")
    sys.stdout.flush()
    server.serve_forever()
