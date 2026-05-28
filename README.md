# ⚙️ Custom Async Event Loop Runtime

> A fully functional asynchronous runtime built from scratch in Python — no `asyncio`, no third-party libraries.

[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen)]()

---

## 🧠 Why Build This?

Python's `asyncio` is powerful, but understanding *why* it works requires looking inside. This project reimplements the core machinery of an async runtime from scratch:

- **Coroutine scheduling** — how `yield` drives task execution
- **Non-blocking I/O** — using `selectors` to watch file descriptors without blocking
- **Futures & callbacks** — the pub/sub glue between I/O events and coroutines
- **Task lifecycle** — creation, stepping, cancellation, and teardown
- **Fairness** — round-robin and priority-based scheduling policies

This is not a toy — it handles real TCP connections, concurrent tasks, and cancellation correctly.

---

## 🏗️ Architecture

```
EventLoop (core engine)
├── Ready Queue         ─── callbacks to run this tick (deque)
├── Scheduled Heap      ─── timer callbacks, min-heap by .when
├── Selector            ─── OS-level I/O readiness (selectors.DefaultSelector)
└── I/O Callbacks       ─── reader/writer callbacks per fd

Future                  ─── single result, callback chain, cancellable
Task                    ─── coroutine wrapper; drives .send()/.throw() step-by-step
AsyncSocket             ─── non-blocking TCP socket over the EventLoop selector
FairScheduler           ─── round-robin across named task groups
PriorityScheduler       ─── HIGH / NORMAL / LOW task dispatch
TaskGroup               ─── structured concurrency: await all or cancel on error
gather()                ─── GatherFuture: concurrent fan-out, ordered results
```

### Single Loop Iteration (`_run_once`)

```
1. Expire timers  →  move due TimerHandles into ready queue
2. Poll I/O       →  selector.select(timeout) fires reader/writer callbacks
3. Drain queue    →  execute all ready callbacks (snapshot to prevent starvation)
```

---

## 📂 Project Structure

```
async-event-loop/
├── src/
│   ├── __init__.py         # Public API
│   ├── event_loop.py       # EventLoop, Future, Task, gather()
│   ├── scheduler.py        # FairScheduler, PriorityScheduler, TaskGroup
│   └── io_handler.py       # AsyncSocket, AsyncServer
├── tests/
│   └── test_event_loop.py  # Unit tests: Futures, Tasks, Timers, Schedulers
├── examples/
│   └── basic_usage.py      # Runnable examples (sleep, gather, cancel, priority)
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

```bash
git clone https://github.com/<your-username>/async-event-loop.git
cd async-event-loop
pip install -r requirements.txt

# Run examples
python examples/basic_usage.py

# Run tests
pytest tests/ -v
```

---

## 💡 Usage Examples

### Basic task + sleep

```python
from src import EventLoop

loop = EventLoop()

def greet(name, delay):
    yield from loop.sleep(delay).__await__()
    print(f"Hello, {name}!")
    return name

result = loop.run_until_complete(greet("World", 0.1))
loop.close()
```

### Concurrent tasks with `gather()`

```python
from src import EventLoop, gather

loop = EventLoop()

def fetch(id, delay):
    yield from loop.sleep(delay).__await__()
    return f"data-{id}"

def main():
    results = yield from gather(
        fetch(1, 0.1),
        fetch(2, 0.05),
        fetch(3, 0.08),
        loop=loop
    ).__await__()
    print(results)  # ['data-1', 'data-2', 'data-3']

loop.run_until_complete(main())
loop.close()
```

### Priority scheduling

```python
from src import EventLoop
from src.scheduler import PriorityScheduler, Priority

loop = EventLoop()
sched = PriorityScheduler(loop)

def job(name):
    yield from loop.sleep(0).__await__()
    print(f"Running: {name}")

sched.submit(job("background"), Priority.LOW)
sched.submit(job("critical"),   Priority.HIGH)
sched.submit(job("normal"),     Priority.NORMAL)
```

### Task cancellation

```python
from src import EventLoop

loop = EventLoop()

def long_running():
    yield from loop.sleep(60).__await__()
    return "never"

def supervisor(task):
    yield from loop.sleep(0.1).__await__()
    task.cancel()
    print(f"Cancelled: {task._future.cancelled()}")  # True
    loop.stop()

t = loop.create_task(long_running())
loop.create_task(supervisor(t))
loop.run_forever()
loop.close()
```

---

## 🔬 Key Design Decisions

| Decision | Rationale |
|---|---|
| `heapq` for timers | O(log n) insert/pop; timers are sparse, not sequential |
| `deque` for ready queue | O(1) append/popleft; typical loop processes 100s of callbacks per tick |
| Snapshot-then-drain | Prevents newly scheduled callbacks from running in the same tick (fairness) |
| Callback-per-fd dict | Avoids repeated selector lookups; O(1) dispatch on I/O events |
| Generator-based coroutines | No `async/await` syntax — shows the underlying protocol explicitly |

---

## 📊 Scheduling Fairness

The `FairScheduler` partitions tasks into named groups and uses round-robin to ensure no group monopolizes the event loop. This mirrors patterns used in production event loops (Twisted's `cooperate`, Tornado's `IOLoop`) to handle mixed I/O and CPU workloads without starvation.

```
Tick N:   Group A (task 1) → Group B (task 1) → Group C (task 1)
Tick N+1: Group A (task 2) → Group B (task 2) → Group C (task 2)
```

---

## 🧪 Tests

```
tests/test_event_loop.py
├── TestFuture        (8 tests)  ─── state, callbacks, cancellation, errors
├── TestTask          (4 tests)  ─── coroutines, sleep, cancellation, exceptions
├── TestGather        (4 tests)  ─── empty, single, multiple, exception propagation
├── TestTimers        (3 tests)  ─── firing, ordering, cancellation
└── TestSchedulers    (2 tests)  ─── FairScheduler, PriorityScheduler
```

Run with coverage:

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

---

## 📖 Further Reading

- [PEP 342 – Coroutines via Enhanced Generators](https://peps.python.org/pep-0342/)
- [PEP 3156 – Asynchronous I/O Support Rework](https://peps.python.org/pep-3156/)
- [CPython asyncio source](https://github.com/python/cpython/tree/main/Lib/asyncio)
- [How the Python event loop works (Brett Cannon)](https://snarky.ca/how-the-heck-does-async-await-work-in-python/)

---

## 📄 License

MIT — see [LICENSE](LICENSE).

---

*Built to understand async from the ground up. Every line is intentional.*
