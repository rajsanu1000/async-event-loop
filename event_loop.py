"""
Custom Async Event Loop Runtime
================================
A fully functional async runtime built from scratch in Python.
Implements coroutine scheduling, non-blocking I/O, Futures,
selectors, and event-driven task orchestration.
"""

import selectors
import socket
import time
import heapq
import logging
from collections import deque
from typing import Any, Callable, Generator, Optional

logger = logging.getLogger(__name__)


class Future:
    """
    Represents a result that will be available in the future.
    Supports callbacks, chaining, and cancellation.
    """

    _PENDING = "PENDING"
    _DONE = "DONE"
    _CANCELLED = "CANCELLED"

    def __init__(self, loop: "EventLoop"):
        self._loop = loop
        self._state = self._PENDING
        self._result = None
        self._exception = None
        self._callbacks: list[Callable] = []

    def done(self) -> bool:
        return self._state != self._PENDING

    def cancelled(self) -> bool:
        return self._state == self._CANCELLED

    def result(self) -> Any:
        if self._state == self._CANCELLED:
            raise CancelledError("Future was cancelled")
        if self._state == self._PENDING:
            raise InvalidStateError("Future is not done yet")
        if self._exception:
            raise self._exception
        return self._result

    def set_result(self, result: Any):
        if self._state != self._PENDING:
            raise InvalidStateError(f"Cannot set result on {self._state} future")
        self._result = result
        self._state = self._DONE
        self._schedule_callbacks()

    def set_exception(self, exception: Exception):
        if self._state != self._PENDING:
            raise InvalidStateError(f"Cannot set exception on {self._state} future")
        self._exception = exception
        self._state = self._DONE
        self._schedule_callbacks()

    def cancel(self) -> bool:
        if self._state != self._PENDING:
            return False
        self._state = self._CANCELLED
        self._schedule_callbacks()
        return True

    def add_done_callback(self, callback: Callable):
        if self.done():
            self._loop.call_soon(callback, self)
        else:
            self._callbacks.append(callback)

    def _schedule_callbacks(self):
        for cb in self._callbacks:
            self._loop.call_soon(cb, self)
        self._callbacks.clear()

    def __await__(self):
        if not self.done():
            yield self  # Suspend coroutine until future is resolved
        return self.result()


class Task:
    """
    Wraps a coroutine as a schedulable unit of work.
    Drives the coroutine step-by-step and handles awaited Futures.
    """

    _task_counter = 0

    def __init__(self, coro: Generator, loop: "EventLoop", name: Optional[str] = None):
        Task._task_counter += 1
        self._id = Task._task_counter
        self._coro = coro
        self._loop = loop
        self._name = name or f"Task-{self._id}"
        self._future = Future(loop)
        self._cancelled = False
        logger.debug(f"[Task] Created: {self._name}")
        loop.call_soon(self._step)

    @property
    def name(self) -> str:
        return self._name

    def cancel(self) -> bool:
        if self._future.done():
            return False
        self._cancelled = True
        return True

    def done(self) -> bool:
        return self._future.done()

    def result(self) -> Any:
        return self._future.result()

    def add_done_callback(self, callback: Callable):
        self._future.add_done_callback(callback)

    def _step(self, exc: Optional[Exception] = None):
        if self._cancelled:
            self._future.cancel()
            return

        try:
            if exc is None:
                # Drive coroutine forward
                result = next(self._coro)
            else:
                result = self._coro.throw(type(exc), exc)

            # Coroutine yielded a Future — register callback to resume when ready
            if isinstance(result, Future):
                result.add_done_callback(self._wakeup)
            else:
                # Yielded something else — reschedule immediately
                self._loop.call_soon(self._step)

        except StopIteration as e:
            self._future.set_result(e.value)
            logger.debug(f"[Task] Completed: {self._name} => {e.value}")
        except CancelledError:
            self._future.cancel()
            logger.debug(f"[Task] Cancelled: {self._name}")
        except Exception as e:
            self._future.set_exception(e)
            logger.error(f"[Task] Failed: {self._name} => {e}")

    def _wakeup(self, future: Future):
        exc = future._exception if future._exception else None
        self._step(exc=exc)

    def __await__(self):
        return self._future.__await__()


class TimerHandle:
    """Represents a delayed callback scheduled via call_later."""

    def __init__(self, when: float, callback: Callable, args: tuple, loop: "EventLoop"):
        self.when = when
        self.callback = callback
        self.args = args
        self._cancelled = False
        self._loop = loop

    def cancel(self):
        self._cancelled = True

    def __lt__(self, other: "TimerHandle"):
        return self.when < other.when


class EventLoop:
    """
    Core event loop engine.

    Responsibilities:
    - Ready queue: callbacks scheduled via call_soon()
    - Timer heap: callbacks scheduled via call_later()
    - I/O selector: non-blocking socket/file descriptor monitoring
    - Task lifecycle: creation, scheduling, and teardown
    """

    def __init__(self):
        self._ready: deque[tuple[Callable, tuple]] = deque()
        self._scheduled: list[TimerHandle] = []  # min-heap by .when
        self._selector = selectors.DefaultSelector()
        self._running = False
        self._stopping = False
        self._io_callbacks: dict[int, tuple[Callable, Callable]] = {}  # fd -> (reader, writer)

    # ─── Scheduling ────────────────────────────────────────────────────────────

    def call_soon(self, callback: Callable, *args):
        """Schedule callback to run on the next iteration."""
        self._ready.append((callback, args))

    def call_later(self, delay: float, callback: Callable, *args) -> TimerHandle:
        """Schedule callback to run after `delay` seconds."""
        when = self.time() + delay
        handle = TimerHandle(when, callback, args, self)
        heapq.heappush(self._scheduled, handle)
        return handle

    def call_at(self, when: float, callback: Callable, *args) -> TimerHandle:
        """Schedule callback to run at absolute time `when`."""
        handle = TimerHandle(when, callback, args, self)
        heapq.heappush(self._scheduled, handle)
        return handle

    def time(self) -> float:
        return time.monotonic()

    # ─── I/O Registration ──────────────────────────────────────────────────────

    def add_reader(self, fd: int, callback: Callable, *args):
        """Watch fd for readability and invoke callback when ready."""
        self._ensure_fd_registered(fd)
        readers, writers = self._io_callbacks.get(fd, (None, None))
        self._io_callbacks[fd] = ((callback, args), writers)
        self._update_selector(fd)

    def remove_reader(self, fd: int):
        if fd in self._io_callbacks:
            _, writers = self._io_callbacks[fd]
            self._io_callbacks[fd] = (None, writers)
            self._update_selector(fd)

    def add_writer(self, fd: int, callback: Callable, *args):
        """Watch fd for writability and invoke callback when ready."""
        self._ensure_fd_registered(fd)
        readers, _ = self._io_callbacks.get(fd, (None, None))
        self._io_callbacks[fd] = (readers, (callback, args))
        self._update_selector(fd)

    def remove_writer(self, fd: int):
        if fd in self._io_callbacks:
            readers, _ = self._io_callbacks[fd]
            self._io_callbacks[fd] = (readers, None)
            self._update_selector(fd)

    def _ensure_fd_registered(self, fd: int):
        if fd not in self._io_callbacks:
            self._io_callbacks[fd] = (None, None)

    def _update_selector(self, fd: int):
        readers, writers = self._io_callbacks.get(fd, (None, None))
        events = 0
        if readers:
            events |= selectors.EVENT_READ
        if writers:
            events |= selectors.EVENT_WRITE
        try:
            if events:
                try:
                    self._selector.modify(fd, events)
                except KeyError:
                    self._selector.register(fd, events)
            else:
                try:
                    self._selector.unregister(fd)
                except KeyError:
                    pass
                self._io_callbacks.pop(fd, None)
        except Exception as e:
            logger.warning(f"Selector update failed for fd={fd}: {e}")

    # ─── Task Creation ─────────────────────────────────────────────────────────

    def create_task(self, coro: Generator, name: Optional[str] = None) -> Task:
        """Wrap a coroutine in a Task and schedule it."""
        return Task(coro, self, name=name)

    def create_future(self) -> Future:
        return Future(self)

    # ─── Sleep / Yield ─────────────────────────────────────────────────────────

    def sleep(self, delay: float) -> Future:
        """
        Returns an awaitable Future that resolves after `delay` seconds.
        Usage: await loop.sleep(1.0)
        """
        future = Future(self)
        self.call_later(delay, future.set_result, None)
        return future

    # ─── Run Loop ──────────────────────────────────────────────────────────────

    def run_forever(self):
        """Run the event loop until stop() is called."""
        self._running = True
        logger.info("[EventLoop] Starting run_forever()")
        try:
            while not self._stopping:
                self._run_once()
        finally:
            self._running = False
            logger.info("[EventLoop] Stopped")

    def run_until_complete(self, coro_or_future) -> Any:
        """
        Run the loop until the given coroutine/future completes.
        Returns the result.
        """
        if hasattr(coro_or_future, 'send'):
            # It's a coroutine
            task = self.create_task(coro_or_future)
            future = task._future
        else:
            future = coro_or_future

        # Stop the loop once the future is done
        future.add_done_callback(lambda f: self.stop())
        self.run_forever()

        return future.result()

    def stop(self):
        self._stopping = True

    def is_running(self) -> bool:
        return self._running

    def close(self):
        self._selector.close()
        logger.info("[EventLoop] Closed selector")

    def _run_once(self):
        """
        Single iteration of the event loop:
        1. Fire due timers
        2. Poll I/O with a computed timeout
        3. Drain the ready queue
        """
        now = self.time()

        # 1. Fire all timers whose time has come
        while self._scheduled and self._scheduled[0].when <= now:
            handle = heapq.heappop(self._scheduled)
            if not handle._cancelled:
                self._ready.append((handle.callback, handle.args))

        # 2. Compute I/O poll timeout
        timeout = 0.0
        if not self._ready:
            if self._scheduled:
                timeout = max(0.0, self._scheduled[0].when - self.time())
            else:
                timeout = None  # Block indefinitely until I/O event

        # 3. Poll I/O
        if self._selector.get_map():
            events = self._selector.select(timeout)
            for key, mask in events:
                fd = key.fd
                readers, writers = self._io_callbacks.get(fd, (None, None))
                if mask & selectors.EVENT_READ and readers:
                    cb, args = readers
                    self._ready.append((cb, args))
                if mask & selectors.EVENT_WRITE and writers:
                    cb, args = writers
                    self._ready.append((cb, args))
        elif timeout:
            time.sleep(min(timeout, 0.01))

        # 4. Drain ready queue (snapshot to avoid mutation during iteration)
        ntodo = len(self._ready)
        for _ in range(ntodo):
            callback, args = self._ready.popleft()
            try:
                callback(*args)
            except Exception as e:
                logger.error(f"[EventLoop] Unhandled exception in callback: {e}")


# ─── Exceptions ────────────────────────────────────────────────────────────────

class CancelledError(Exception):
    pass


class InvalidStateError(Exception):
    pass


# ─── Convenience: gather ───────────────────────────────────────────────────────

class GatherFuture(Future):
    """Future that completes when all children complete."""

    def __init__(self, children: list[Future], loop: "EventLoop"):
        super().__init__(loop)
        self._children = children
        self._results = [None] * len(children)
        self._done_count = 0

        if not children:
            self.set_result([])
            return

        for i, child in enumerate(children):
            child.add_done_callback(lambda f, idx=i: self._child_done(f, idx))

    def _child_done(self, future: Future, idx: int):
        if self.done():
            return
        if future.cancelled():
            self.cancel()
            return
        if future._exception:
            self.set_exception(future._exception)
            return
        self._results[idx] = future._result
        self._done_count += 1
        if self._done_count == len(self._children):
            self.set_result(self._results)


def gather(*coros_or_futures, loop: "EventLoop") -> GatherFuture:
    """Run multiple coroutines concurrently and await all results."""
    futures = []
    for item in coros_or_futures:
        if hasattr(item, 'send'):
            task = loop.create_task(item)
            futures.append(task._future)
        else:
            futures.append(item)
    return GatherFuture(futures, loop)
