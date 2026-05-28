"""
Non-Blocking I/O Primitives
============================
Async socket wrappers built on top of the EventLoop's selector integration.
Provides awaitable connect, read, write, and accept operations.
"""

from __future__ import annotations

import socket
import logging
from typing import Optional

from .event_loop import EventLoop, Future

logger = logging.getLogger(__name__)

_DEFAULT_BUFSIZE = 4096


class AsyncSocket:
    """
    Non-blocking socket wrapper with awaitable I/O operations.
    All operations integrate with the EventLoop's selector without
    blocking the calling thread.
    """

    def __init__(self, loop: EventLoop, sock: Optional[socket.socket] = None):
        self._loop = loop
        if sock is None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        self._sock = sock
        self._fd = sock.fileno()
        logger.debug(f"[AsyncSocket] Created fd={self._fd}")

    # ─── Connection ────────────────────────────────────────────────────────────

    def connect(self, host: str, port: int) -> Future:
        """
        Initiate a non-blocking connect.
        Returns a Future that resolves when the connection is established.
        """
        future = Future(self._loop)
        address = (host, port)

        try:
            self._sock.connect(address)
        except BlockingIOError:
            pass  # Expected for non-blocking sockets

        def _on_writable():
            self._loop.remove_writer(self._fd)
            err = self._sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err:
                future.set_exception(ConnectionError(f"Connect failed: errno={err}"))
            else:
                logger.debug(f"[AsyncSocket] Connected to {host}:{port}")
                future.set_result(None)

        self._loop.add_writer(self._fd, _on_writable)
        return future

    # ─── Read / Write ──────────────────────────────────────────────────────────

    def recv(self, bufsize: int = _DEFAULT_BUFSIZE) -> Future:
        """
        Read up to `bufsize` bytes.
        Returns a Future[bytes] that resolves when data is available.
        """
        future = Future(self._loop)

        def _on_readable():
            self._loop.remove_reader(self._fd)
            try:
                data = self._sock.recv(bufsize)
                future.set_result(data)
            except Exception as e:
                future.set_exception(e)

        self._loop.add_reader(self._fd, _on_readable)
        return future

    def sendall(self, data: bytes) -> Future:
        """
        Send all bytes, handling partial writes.
        Returns a Future that resolves when all data has been sent.
        """
        future = Future(self._loop)
        remaining = bytearray(data)

        def _on_writable():
            nonlocal remaining
            try:
                sent = self._sock.send(remaining)
                remaining = remaining[sent:]
                if not remaining:
                    self._loop.remove_writer(self._fd)
                    future.set_result(None)
                # else: keep writer registered, send more on next iteration
            except Exception as e:
                self._loop.remove_writer(self._fd)
                future.set_exception(e)

        self._loop.add_writer(self._fd, _on_writable)
        return future

    # ─── Server ────────────────────────────────────────────────────────────────

    def bind_and_listen(self, host: str, port: int, backlog: int = 5):
        """Configure as a listening server socket."""
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))
        self._sock.listen(backlog)
        logger.info(f"[AsyncSocket] Listening on {host}:{port}")

    def accept(self) -> Future:
        """
        Accept an incoming connection.
        Returns a Future[(AsyncSocket, address)] when a client connects.
        """
        future = Future(self._loop)

        def _on_readable():
            self._loop.remove_reader(self._fd)
            try:
                conn, addr = self._sock.accept()
                client = AsyncSocket(self._loop, sock=conn)
                logger.debug(f"[AsyncSocket] Accepted connection from {addr}")
                future.set_result((client, addr))
            except Exception as e:
                future.set_exception(e)

        self._loop.add_reader(self._fd, _on_readable)
        return future

    # ─── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self):
        try:
            self._loop.remove_reader(self._fd)
            self._loop.remove_writer(self._fd)
        except Exception:
            pass
        self._sock.close()
        logger.debug(f"[AsyncSocket] Closed fd={self._fd}")

    def fileno(self) -> int:
        return self._fd

    def __repr__(self):
        return f"<AsyncSocket fd={self._fd}>"


class AsyncServer:
    """
    High-level async TCP server.
    Accepts connections in a loop and dispatches each to a handler coroutine.
    """

    def __init__(self, loop: EventLoop, host: str, port: int):
        self._loop = loop
        self._host = host
        self._port = port
        self._socket = AsyncSocket(loop)
        self._running = False

    def start(self, handler) -> Future:
        """
        Start accepting connections. `handler(client_socket, addr)` is called
        as a new task for each incoming client.
        Returns a Future that resolves when the server is stopped.
        """
        self._socket.bind_and_listen(self._host, self._port)
        self._running = True
        future = Future(self._loop)

        def _accept_loop():
            if not self._running:
                future.set_result("Server stopped")
                return
            accept_fut = self._socket.accept()

            def _on_accepted(f: Future):
                if f._exception:
                    logger.error(f"[AsyncServer] Accept error: {f._exception}")
                else:
                    client, addr = f.result()
                    self._loop.create_task(handler(client, addr))
                # Schedule next accept
                self._loop.call_soon(_accept_loop)

            accept_fut.add_done_callback(_on_accepted)

        self._loop.call_soon(_accept_loop)
        return future

    def stop(self):
        self._running = False
        self._socket.close()
        logger.info(f"[AsyncServer] Stopped")
