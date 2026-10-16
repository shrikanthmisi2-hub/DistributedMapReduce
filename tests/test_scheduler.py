import unittest
from master.scheduler import TaskScheduler, TaskState, validate_map_result

class TestScheduler(unittest.TestCase):
    def test_add_and_get_task(self):
        scheduler = TaskScheduler()
        tasks = [{"task_id": "1"}, {"task_id": "2"}]
        scheduler.add_tasks(tasks)
        
        task1 = scheduler.get_task("worker_A")
        self.assertEqual(task1["task_id"], "1")
        self.assertEqual(task1.state, TaskState.ASSIGNED)
        
        # Worker A shouldn't get another task if one is already assigned to it
        self.assertIsNone(scheduler.get_task("worker_A"))
        
        # Worker B gets the next task
        task2 = scheduler.get_task("worker_B")
        self.assertEqual(task2["task_id"], "2")
        self.assertEqual(task2.state, TaskState.ASSIGNED)
        
        # No more tasks available
        self.assertIsNone(scheduler.get_task("worker_C"))

    def test_mark_completed(self):
        scheduler = TaskScheduler()
        scheduler.add_tasks([{"task_id": "1"}])
        scheduler.get_task("worker_A")
        
        self.assertFalse(scheduler.is_done())
        scheduler.mark_completed("worker_A")
        self.assertTrue(scheduler.is_done())
        self.assertEqual(scheduler.get_task_state("1"), TaskState.COMPLETED)

    def test_worker_disconnect(self):
        scheduler = TaskScheduler()
        scheduler.add_tasks([{"task_id": "1"}])
        scheduler.get_task("worker_A")
        
        # Worker A disconnects before completing
        requeued = scheduler.handle_worker_disconnect("worker_A")
        self.assertEqual(requeued["task_id"], "1")
        self.assertEqual(requeued.state, TaskState.PENDING)
        self.assertEqual(requeued.retries, 1)
        
        # Task should be available for Worker B now
        task_b = scheduler.get_task("worker_B")
        self.assertEqual(task_b["task_id"], "1")
        self.assertEqual(task_b.state, TaskState.ASSIGNED)
        self.assertEqual(task_b.assigned_worker, "worker_B")

    def test_task_states_explicit(self):
        scheduler = TaskScheduler()
        scheduler.add_tasks([{"task_id": "task_states"}])
        
        # 1. Pending state
        self.assertEqual(scheduler.get_task_state("task_states"), TaskState.PENDING)
        
        # 2. Assigned state
        task = scheduler.get_task("worker_1")
        self.assertIsNotNone(task)
        self.assertEqual(task.state, TaskState.ASSIGNED)
        self.assertEqual(scheduler.get_task_state("task_states"), TaskState.ASSIGNED)
        
        # 3. Completed state
        valid_result = [("hello", 1), ("world", 1)]
        success, completed_task = scheduler.complete_task("task_states", "worker_1", result=valid_result)
        self.assertTrue(success)
        self.assertEqual(completed_task.state, TaskState.COMPLETED)
        self.assertEqual(scheduler.get_task_state("task_states"), TaskState.COMPLETED)
        self.assertTrue(scheduler.is_done())

    def test_result_validation(self):
        # Test helper directly
        self.assertTrue(validate_map_result([("word", 1)])[0])
        self.assertTrue(validate_map_result([])[0]) # Empty result from blank input is valid
        self.assertFalse(validate_map_result("not a list")[0])
        self.assertFalse(validate_map_result([("word",)])[0])
        self.assertFalse(validate_map_result([(123, 1)])[0]) # Key must be string
        self.assertFalse(validate_map_result([("word", "1")])[0]) # Value must be int
        self.assertFalse(validate_map_result([("word", True)])[0]) # bool is not a valid count

        # Test validation inside scheduler
        scheduler = TaskScheduler(max_retries=1)
        scheduler.add_tasks([{"task_id": "val_task"}])
        scheduler.get_task("worker_val")
        
        # Submitting invalid result should fail validation and not mark completed
        success, msg = scheduler.complete_task("val_task", "worker_val", result="invalid_format", validate=True)
        self.assertFalse(success)
        self.assertIn("Invalid result", msg)
        self.assertNotEqual(scheduler.get_task_state("val_task"), TaskState.COMPLETED)

    def test_duplicate_results(self):
        scheduler = TaskScheduler()
        scheduler.add_tasks([{"task_id": "dup_task"}])
        scheduler.get_task("worker_dup")
        
        valid_result = [("apple", 1)]
        # First completion succeeds
        success1, _ = scheduler.complete_task("dup_task", "worker_dup", result=valid_result)
        self.assertTrue(success1)
        self.assertEqual(scheduler.get_task_state("dup_task"), TaskState.COMPLETED)
        
        # Duplicate completion is rejected safely
        success2, err = scheduler.complete_task("dup_task", "worker_dup", result=valid_result)
        self.assertFalse(success2)
        self.assertIn("already completed", err)
        # Should still have only 1 completed task
        self.assertEqual(len(scheduler.completed_tasks), 1)

    def test_late_result_from_failed_worker_rejected(self):
        scheduler = TaskScheduler()
        scheduler.add_tasks([{"task_id": "late_task"}])
        
        # Assigned to worker 1
        scheduler.get_task("worker_1")
        
        # Worker 1 times out / fails -> task is requeued
        scheduler.handle_worker_failure("worker_1", reason="Heartbeat timeout")
        
        # Task reassigned to worker 2
        task2 = scheduler.get_task("worker_2")
        self.assertEqual(task2["task_id"], "late_task")
        self.assertEqual(task2.assigned_worker, "worker_2")
        
        # Late result arrives from worker 1 -> MUST BE REJECTED
        success_late, msg = scheduler.complete_task("late_task", "worker_1", result=[("late", 1)])
        self.assertFalse(success_late)
        self.assertIn("assigned to worker_2", msg)
        self.assertNotEqual(scheduler.get_task_state("late_task"), TaskState.COMPLETED)
        
        # Worker 2 successfully completes
        success_valid, _ = scheduler.complete_task("late_task", "worker_2", result=[("fresh", 1)])
        self.assertTrue(success_valid)
        self.assertEqual(scheduler.get_task_state("late_task"), TaskState.COMPLETED)

    def test_retry_limits_and_job_failure(self):
        max_retries = 2
        scheduler = TaskScheduler(max_retries=max_retries)
        scheduler.add_tasks([{"task_id": "retry_task"}])
        
        # Attempt 1: get task and fail
        scheduler.get_task("worker_1")
        failed1 = scheduler.fail_task("retry_task", worker_id="worker_1", reason="Crash 1")
        self.assertFalse(failed1) # Requeued, not permanently failed
        self.assertFalse(scheduler.is_failed())
        self.assertEqual(scheduler.tasks["retry_task"].retries, 1)
        self.assertEqual(scheduler.get_task_state("retry_task"), TaskState.PENDING)
        
        # Attempt 2: get task and fail
        scheduler.get_task("worker_2")
        failed2 = scheduler.fail_task("retry_task", worker_id="worker_2", reason="Crash 2")
        self.assertFalse(failed2) # Requeued (retries=2 == max_retries)
        self.assertFalse(scheduler.is_failed())
        self.assertEqual(scheduler.tasks["retry_task"].retries, 2)
        
        # Attempt 3: get task and fail -> exceeds max_retries (3 > 2)
        scheduler.get_task("worker_3")
        failed3 = scheduler.fail_task("retry_task", worker_id="worker_3", reason="Crash 3")
        self.assertTrue(failed3) # Permanently failed!
        self.assertTrue(scheduler.is_failed())
        self.assertFalse(scheduler.is_done())
        self.assertEqual(scheduler.get_task_state("retry_task"), TaskState.FAILED)
        self.assertIn("exceeded max retries", scheduler.get_failure_reason().lower())

if __name__ == "__main__":
    unittest.main()
