"""
examples/basic_usage.py
========================
Demonstrates core async runtime features:
- Task creation and scheduling
- Sleep / timer-based delays
- gather() for concurrent execution
- FairScheduler for round-robin task groups
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import logging
from src import EventLoop, gather, FairScheduler, PriorityScheduler, Priority

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ── Example 1: Basic coroutine and sleep ─────────────────────────────────────

def simple_task(loop, name, delay):
    """Coroutine that sleeps and prints a message."""
    logger.info(f"[{name}] Starting, will sleep {delay}s")
    yield from loop.sleep(delay).__await__()
    logger.info(f"[{name}] Done after {delay}s")
    return f"{name}_result"


def run_basic_example():
    print("\n" + "="*60)
    print("Example 1: Basic Tasks + Sleep")
    print("="*60)
    loop = EventLoop()

    def main():
        yield from loop.sleep(0).__await__()  # yield to let loop start
        result = yield from simple_task(loop, "Alpha", 0.05).__await__()
        logger.info(f"Result: {result}")
        loop.stop()

    loop.create_task(main())
    loop.run_forever()
    loop.close()


# ── Example 2: Concurrent tasks with gather() ────────────────────────────────

def run_gather_example():
    print("\n" + "="*60)
    print("Example 2: Concurrent Tasks via gather()")
    print("="*60)
    loop = EventLoop()

    def task_a():
        logger.info("[A] Started")
        yield from loop.sleep(0.1).__await__()
        logger.info("[A] Finished")
        return "A_done"

    def task_b():
        logger.info("[B] Started")
        yield from loop.sleep(0.05).__await__()
        logger.info("[B] Finished")
        return "B_done"

    def task_c():
        logger.info("[C] Started")
        yield from loop.sleep(0.08).__await__()
        logger.info("[C] Finished")
        return "C_done"

    def main():
        g = gather(task_a(), task_b(), task_c(), loop=loop)
        results = yield from g.__await__()
        logger.info(f"All results: {results}")

    loop.run_until_complete(main())
    loop.close()


# ── Example 3: FairScheduler round-robin ─────────────────────────────────────

def run_fair_scheduler_example():
    print("\n" + "="*60)
    print("Example 3: FairScheduler (Round-Robin Groups)")
    print("="*60)
    loop = EventLoop()
    scheduler = FairScheduler(loop)

    completed = []

    def worker(name, delay, group):
        logger.info(f"[{group}/{name}] Queued")
        yield from loop.sleep(delay).__await__()
        logger.info(f"[{group}/{name}] Completed")
        completed.append(name)

    # Add tasks to different groups
    for i in range(3):
        scheduler.add_task(worker(f"IO-{i}", 0.02 * (i+1), "io"), group="io")
    for i in range(3):
        scheduler.add_task(worker(f"CPU-{i}", 0.01 * (i+1), "cpu"), group="cpu")

    def stopper():
        yield from loop.sleep(0.3).__await__()
        logger.info(f"Completed tasks: {completed}")
        loop.stop()

    loop.create_task(stopper())
    loop.run_forever()
    loop.close()


# ── Example 4: Priority Scheduler ────────────────────────────────────────────

def run_priority_scheduler_example():
    print("\n" + "="*60)
    print("Example 4: PriorityScheduler")
    print("="*60)
    loop = EventLoop()
    scheduler = PriorityScheduler(loop)

    order = []

    def task(name, priority_label):
        logger.info(f"[{priority_label}] {name} running")
        yield from loop.sleep(0.01).__await__()
        order.append(name)
        return name

    scheduler.submit(task("low-1", "LOW"), Priority.LOW, name="low-1")
    scheduler.submit(task("high-1", "HIGH"), Priority.HIGH, name="high-1")
    scheduler.submit(task("normal-1", "NORMAL"), Priority.NORMAL, name="normal-1")
    scheduler.submit(task("high-2", "HIGH"), Priority.HIGH, name="high-2")

    def stopper():
        yield from loop.sleep(0.2).__await__()
        logger.info(f"Execution order: {order}")
        loop.stop()

    loop.create_task(stopper())
    loop.run_forever()
    loop.close()


# ── Example 5: Task cancellation ─────────────────────────────────────────────

def run_cancellation_example():
    print("\n" + "="*60)
    print("Example 5: Task Cancellation")
    print("="*60)
    loop = EventLoop()

    def long_task():
        logger.info("[LongTask] Starting...")
        yield from loop.sleep(10).__await__()  # Would run for 10s
        logger.info("[LongTask] This should NOT print")
        return "should_not_reach"

    def canceller(task):
        yield from loop.sleep(0.05).__await__()
        logger.info("[Canceller] Cancelling long task")
        cancelled = task.cancel()
        logger.info(f"[Canceller] Cancel result: {cancelled}")
        yield from loop.sleep(0.05).__await__()
        logger.info(f"[Canceller] Task done: {task.done()}, cancelled: {task._future.cancelled()}")
        loop.stop()

    t = loop.create_task(long_task(), name="long-task")
    loop.create_task(canceller(t))
    loop.run_forever()
    loop.close()


if __name__ == "__main__":
    run_basic_example()
    run_gather_example()
    run_fair_scheduler_example()
    run_priority_scheduler_example()
    run_cancellation_example()
    print("\n✅ All examples completed successfully.")
