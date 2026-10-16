"""
Scheduler module.
Handles task distribution (map and reduce tasks) to available workers, tracks progress, and manages fault tolerance/retries.
"""
import threading
import logging
import time

logger = logging.getLogger("Scheduler")

class TaskState:
    PENDING = "pending"
    ASSIGNED = "assigned"
    COMPLETED = "completed"
    FAILED = "failed"

def validate_map_result(result):
    """
    Validates that a MAP_RESULT is formatted correctly:
    A list of 2-element tuples/lists [word (str), count (int)].
    """
    if not isinstance(result, list):
        return False, "Result must be a list"
    for idx, item in enumerate(result):
        if not (isinstance(item, (list, tuple)) and len(item) == 2):
            return False, f"Item at index {idx} must be a 2-element sequence"
        k, v = item
        if not isinstance(k, str):
            return False, f"Key at index {idx} must be a string"
        if not (isinstance(v, int) and not isinstance(v, bool)):
            return False, f"Value at index {idx} must be an integer"
    return True, "Valid"

class Task(dict):
    """
    Represents a task with explicit state tracking, retry counters, and worker assignment.
    Inherits from dict for full backward compatibility with dict-based access.
    """
    def __init__(self, task_id, data="", max_retries=3):
        super().__init__()
        self["task_id"] = task_id
        self["data"] = data
        self["state"] = TaskState.PENDING
        self["assigned_worker"] = None
        self["retries"] = 0
        self["max_retries"] = max_retries
        self["result"] = None
        self["failure_reason"] = None
        self["failure_history"] = []
        self["assigned_time"] = None

    @property
    def task_id(self):
        return self["task_id"]

    @property
    def data(self):
        return self["data"]

    @property
    def state(self):
        return self["state"]

    @state.setter
    def state(self, val):
        self["state"] = val

    @property
    def assigned_worker(self):
        return self["assigned_worker"]

    @assigned_worker.setter
    def assigned_worker(self, val):
        self["assigned_worker"] = val

    @property
    def retries(self):
        return self["retries"]

    @retries.setter
    def retries(self, val):
        self["retries"] = val

    @property
    def max_retries(self):
        return self["max_retries"]

    @property
    def failure_reason(self):
        return self["failure_reason"]

    @failure_reason.setter
    def failure_reason(self, val):
        self["failure_reason"] = val

    @property
    def result(self):
        return self["result"]

    @result.setter
    def result(self, val):
        self["result"] = val

class TaskScheduler:
    def __init__(self, max_retries=3):
        self.max_retries = max_retries
        self.tasks = {} # task_id -> Task
        self.pending_tasks = [] # list of Task
        self.assigned_tasks = {} # worker_id -> Task
        self.completed_tasks = [] # list of Task
        self.failed_tasks = [] # list of Task
        self.job_failed = False
        self.failure_reason = None
        self.lock = threading.RLock()

    def add_tasks(self, tasks, max_retries=None):
        retries_limit = max_retries if max_retries is not None else self.max_retries
        with self.lock:
            for t in tasks:
                if isinstance(t, Task):
                    task_obj = t
                elif isinstance(t, dict):
                    task_obj = Task(
                        task_id=t.get("task_id"),
                        data=t.get("data", ""),
                        max_retries=t.get("max_retries", retries_limit)
                    )
                else:
                    raise ValueError(f"Unsupported task format: {type(t)}")
                
                self.tasks[task_obj.task_id] = task_obj
                self.pending_tasks.append(task_obj)
            logger.info(f"Added {len(tasks)} tasks to scheduler (max_retries={retries_limit})")

    def get_task(self, worker_id):
        with self.lock:
            if worker_id in self.assigned_tasks:
                return None # Already has a task assigned
            if not self.pending_tasks:
                return None
            task = self.pending_tasks.pop(0)
            task.state = TaskState.ASSIGNED
            task.assigned_worker = worker_id
            task["assigned_time"] = time.time()
            self.assigned_tasks[worker_id] = task
            logger.info(f"Task {task.task_id} assigned to {worker_id} (attempt {task.retries + 1})")
            return task

    def complete_task(self, task_id, worker_id, result=None, validate=True):
        """
        Completes a task after validating result and ensuring worker assignment matches.
        Prevents duplicate or late completions from unassigned/failed workers.
        """
        with self.lock:
            if task_id not in self.tasks:
                logger.warning(f"Attempted to complete unknown task {task_id} by {worker_id}")
                return False, f"Unknown task: {task_id}"

            task = self.tasks[task_id]

            if task.state == TaskState.COMPLETED:
                logger.warning(f"Duplicate result ignored: Task {task_id} already marked completed")
                return False, "Duplicate completion: task already completed"

            if task.state != TaskState.ASSIGNED:
                logger.warning(f"Cannot complete task {task_id} in state {task.state}")
                return False, f"Task {task_id} is in state {task.state}, not assigned"

            if task.assigned_worker != worker_id:
                logger.warning(
                    f"Late/stale result rejected for task {task_id} from worker {worker_id}. "
                    f"Currently assigned to {task.assigned_worker}"
                )
                return False, f"Task {task_id} assigned to {task.assigned_worker}, not {worker_id}"

            if validate and result is not None:
                is_valid, err = validate_map_result(result)
                if not is_valid:
                    logger.error(f"Task {task_id} returned invalid result from {worker_id}: {err}")
                    self._fail_task_locked(task, worker_id, reason=f"Validation failed: {err}")
                    return False, f"Invalid result: {err}"

            # Successfully complete
            task.state = TaskState.COMPLETED
            task.result = result
            if worker_id in self.assigned_tasks:
                self.assigned_tasks.pop(worker_id)
            self.completed_tasks.append(task)
            logger.info(f"Task {task_id} marked COMPLETED by {worker_id}")
            return True, task

    def mark_completed(self, worker_id, task_id=None, result=None):
        """
        Backward-compatible convenience method.
        """
        with self.lock:
            if task_id is None:
                task = self.assigned_tasks.get(worker_id)
                if not task:
                    return None
                task_id = task.task_id

        success, res = self.complete_task(task_id, worker_id, result=result, validate=(result is not None))
        if success:
            return res
        return None

    def fail_task(self, task_id, worker_id=None, reason="Task failed"):
        with self.lock:
            task = self.tasks.get(task_id)
            if not task:
                return False
            return self._fail_task_locked(task, worker_id, reason)

    def _fail_task_locked(self, task, worker_id, reason):
        """
        Internal failure handling with lock already held.
        """
        task.retries += 1
        task.failure_reason = reason
        if "failure_history" not in task:
            task["failure_history"] = []
        task["failure_history"].append({
            "attempt": task.retries,
            "worker_id": worker_id or task.assigned_worker,
            "reason": reason,
            "timestamp": time.time()
        })

        if worker_id and worker_id in self.assigned_tasks:
            self.assigned_tasks.pop(worker_id)
        elif task.assigned_worker in self.assigned_tasks:
            self.assigned_tasks.pop(task.assigned_worker)

        if task.retries <= task.max_retries:
            task.state = TaskState.PENDING
            task.assigned_worker = None
            self.pending_tasks.append(task)
            logger.warning(
                f"Task {task.task_id} failed ({reason}). Requeued for retry "
                f"({task.retries}/{task.max_retries})"
            )
            return False # Requeued, not permanently failed
        else:
            task.state = TaskState.FAILED
            task.failure_reason = reason
            self.failed_tasks.append(task)
            self.job_failed = True
            self.failure_reason = f"Task {task.task_id} exceeded max retries ({task.max_retries}): {reason}"
            logger.error(
                f"Task {task.task_id} reached maximum retries ({task.max_retries}) and FAILED: {reason}"
            )
            return True # Permanently failed

    def handle_worker_failure(self, worker_id, reason="Worker failed"):
        """
        Handles worker disconnect or timeout failure by reassigning its assigned task.
        """
        with self.lock:
            task = self.assigned_tasks.get(worker_id)
            if task:
                logger.warning(f"Worker {worker_id} failed during task {task.task_id}: {reason}")
                self._fail_task_locked(task, worker_id, reason=f"Worker {worker_id} failure: {reason}")
                return task
            return None

    def handle_worker_disconnect(self, worker_id):
        """
        Backward-compatible alias for worker disconnect.
        """
        return self.handle_worker_failure(worker_id, reason="Worker disconnected")

    def is_done(self):
        with self.lock:
            if not self.tasks:
                return len(self.pending_tasks) == 0 and len(self.assigned_tasks) == 0
            return (
                len(self.completed_tasks) == len(self.tasks)
                and len(self.pending_tasks) == 0
                and len(self.assigned_tasks) == 0
                and len(self.failed_tasks) == 0
            )

    def is_failed(self):
        with self.lock:
            return self.job_failed or len(self.failed_tasks) > 0

    def get_failure_reason(self):
        with self.lock:
            return self.failure_reason

    def get_task_state(self, task_id):
        with self.lock:
            task = self.tasks.get(task_id)
            return task.state if task else None

    def get_stats(self):
        with self.lock:
            return {
                "total": len(self.tasks),
                "pending": len(self.pending_tasks),
                "assigned": len(self.assigned_tasks),
                "completed": len(self.completed_tasks),
                "failed": len(self.failed_tasks),
                "is_failed": self.is_failed(),
                "is_done": self.is_done()
            }

    def get_all_tasks(self):
        with self.lock:
            task_list = []
            for tid, t in self.tasks.items():
                task_list.append({
                    "task_id": t.task_id,
                    "state": t.state,
                    "assigned_worker": t.assigned_worker,
                    "retries": t.retries,
                    "max_retries": t.max_retries,
                    "failure_reason": t.failure_reason,
                    "failure_history": list(t.get("failure_history", [])),
                    "assigned_time": t.get("assigned_time")
                })
            return sorted(task_list, key=lambda x: str(x["task_id"]))

