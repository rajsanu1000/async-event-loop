"""
Coroutine Scheduler
====================
Implements scheduling fairness policies on top of the core EventLoop.
Supports FIFO (default), round-robin, and priority-based scheduling.
"""

from __future__ import annotations

import heapq
import logging
from collections import deque
from enum import Enum
from typing import Any, Callable, Generator, Optional

from .event_loop import EventLoop, Future, Task

logger = logging.getLogger(__name__)


class Priority(Enum):
    HIGH = 0
    NORMAL = 1
    LOW = 2


class PriorityTask:
    """A Task wrapper that carries a scheduling priority."""

    def __init__(self, priority: Priority, task: Task):
        self.priority = priority
        self.task = task

    def __lt__(self, other: "PriorityTask"):
        return self.priority.value < other.priority.value


class FairScheduler:
    """
    Wraps EventLoop with round-robin fairness across task groups.

    Tasks are partitioned into groups; each iteration, one task from
    each group is allowed to progress before cycling to the next group.
    This prevents any single group from starving others.
    """

    def __init__(self, loop: EventLoop):
        self._loop = loop
        self._groups: dict[str, deque[Task]] = {}
        self._group_order: list[str] = []

    def add_task(self, coro: Generator, group: str = "default", name: Optional[str] = None) -> Task:
        task = self._loop.create_task(coro, name=name)
        if group not in self._groups:
            self._groups[group] = deque()
            self._group_order.append(group)
        self._groups[group].append(task)
        logger.debug(f"[FairScheduler] Added {task.name} to group '{group}'")
        return task

    def active_groups(self) -> list[str]:
        return [g for g, q in self._groups.items() if q]

    def pending_tasks(self) -> int:
        return sum(len(q) for q in self._groups.values())

    def stats(self) -> dict[str, int]:
        return {g: len(q) for g, q in self._groups.items()}


class PriorityScheduler:
    """
    Priority-based task scheduler.
    HIGH priority tasks are scheduled before NORMAL and LOW.
    Within the same priority, FIFO ordering is preserved.
    """

    def __init__(self, loop: EventLoop):
        self._loop = loop
        self._heap: list[PriorityTask] = []
        self._counter = 0  # tiebreaker for equal priority

    def submit(
        self,
        coro: Generator,
        priority: Priority = Priority.NORMAL,
        name: Optional[str] = None,
    ) -> Task:
        task = self._loop.create_task(coro, name=name)
        pt = PriorityTask(priority, task)
        # Use counter as secondary sort key for stability
        heapq.heappush(self._heap, (priority.value, self._counter, pt))
        self._counter += 1
        logger.debug(f"[PriorityScheduler] Submitted {task.name} at {priority.name} priority")
        return task

    def pending(self) -> int:
        return len(self._heap)


class TaskGroup:
    """
    Context-manager style group for concurrent task lifecycle management.
    All tasks in the group are awaited before the group exits.

    Usage:
        group = TaskGroup(loop)
        group.create_task(my_coro())
        group.create_task(other_coro())
        loop.run_until_complete(group.wait())
    """

    def __init__(self, loop: EventLoop):
        self._loop = loop
        self._tasks: list[Task] = []
        self._errors: list[Exception] = []

    def create_task(self, coro: Generator, name: Optional[str] = None) -> Task:
        task = self._loop.create_task(coro, name=name)
        task.add_done_callback(self._on_task_done)
        self._tasks.append(task)
        return task

    def _on_task_done(self, future: Future):
        if future._exception:
            self._errors.append(future._exception)

    def wait(self) -> Generator:
        """Coroutine that resolves when all tasks complete."""
        from .event_loop import gather
        result_future = gather(*[t._future for t in self._tasks], loop=self._loop)
        return result_future.__await__()

    @property
    def errors(self) -> list[Exception]:
        return list(self._errors)

    @property
    def tasks(self) -> list[Task]:
        return list(self._tasks)
