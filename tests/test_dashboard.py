"""
Unit and integration tests for the MapReduce Web Dashboard.
Verifies API routes, JSON response structures, live master integration,
standalone TCP fallback, worker tracking, task status lifecycle, and error handling.
"""
import unittest
import socket
import threading
import time
import json
import urllib.request
import urllib.error

from master.master import MasterServer, WorkerStatus
from master.scheduler import TaskState
from dashboard.app import create_dashboard_app


def find_free_port():
    """Helper to find an unused local TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class TestDashboardAPI(unittest.TestCase):
    def setUp(self):
        self.master = MasterServer(host='127.0.0.1', port=find_free_port(), enable_dashboard=False)
        self.app = create_dashboard_app(master_server=self.master)
        self.client = self.app.test_client()

    def tearDown(self):
        if self.master.running:
            self.master.stop()

    def test_empty_initial_state(self):
        """
        Verify that dashboard endpoints return valid, clean initial states
        when no jobs are submitted and no workers are registered.
        """
        # Test index page
        res = self.client.get('/')
        self.assertEqual(res.status_code, 200)
        self.assertIn(b"Distributed MapReduce", res.data)

        # Test /api/status
        res = self.client.get('/api/status')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertIsInstance(data, dict)
        self.assertFalse(data["job_in_progress"])
        self.assertFalse(data["job_completed"])
        self.assertFalse(data["job_failed"])
        self.assertIsNone(data["job_error"])
        self.assertIn("stats", data)
        self.assertEqual(data["stats"]["total"], 0)
        self.assertEqual(data["stats"]["pending"], 0)
        self.assertEqual(data["stats"]["completed"], 0)
        self.assertTrue(data["stats"]["is_done"])

        # Test /api/workers
        res = self.client.get('/api/workers')
        self.assertEqual(res.status_code, 200)
        workers = res.get_json()
        self.assertIsInstance(workers, list)
        self.assertEqual(len(workers), 0)

        # Test /api/tasks
        res = self.client.get('/api/tasks')
        self.assertEqual(res.status_code, 200)
        tasks = res.get_json()
        self.assertIsInstance(tasks, list)
        self.assertEqual(len(tasks), 0)

    def test_standalone_app_when_master_offline(self):
        """
        Verify that create_dashboard_app handles master being offline gracefully.
        """
        unused_port = find_free_port()
        standalone_app = create_dashboard_app(master_server=None, master_port=unused_port)
        client = standalone_app.test_client()

        res = client.get('/api/status')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["stats"]["total"], 0)
        self.assertFalse(data["server_running"])

        res = client.get('/api/workers')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json(), [])

        res = client.get('/api/tasks')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json(), [])

    def test_standalone_app_tcp_fallback_to_live_master(self):
        """
        Verify that a standalone dashboard process connects to the live MasterServer
        over TCP port 5000 and displays live workers, tasks, and job status.
        """
        tcp_port = find_free_port()
        live_master = MasterServer(host='127.0.0.1', port=tcp_port, enable_dashboard=False)
        master_thread = threading.Thread(target=live_master.start, daemon=True)
        master_thread.start()
        time.sleep(0.2)

        try:
            # Register worker on live master
            dummy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            live_master.register_worker("tcp-worker-99", dummy_sock, ("127.0.0.1", 49999))

            # Add task to live master
            live_master.scheduler.add_tasks([{"task_id": "map_tcp_1", "data": "tcp test"}])

            # Standalone dashboard with NO in-memory master reference
            standalone_app = create_dashboard_app(master_server=None, master_port=tcp_port)
            client = standalone_app.test_client()

            # 1. Status via TCP
            res = client.get('/api/status')
            self.assertEqual(res.status_code, 200)
            status_data = res.get_json()
            self.assertTrue(status_data["server_running"])
            self.assertEqual(status_data["stats"]["total"], 1)

            # 2. Workers via TCP
            res = client.get('/api/workers')
            self.assertEqual(res.status_code, 200)
            workers = res.get_json()
            self.assertEqual(len(workers), 1)
            self.assertEqual(workers[0]["worker_id"], "tcp-worker-99")

            # 3. Tasks via TCP
            res = client.get('/api/tasks')
            self.assertEqual(res.status_code, 200)
            tasks = res.get_json()
            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0]["task_id"], "map_tcp_1")

            # 4. Summary via TCP
            res = client.get('/api/summary')
            self.assertEqual(res.status_code, 200)
            summary = res.get_json()
            self.assertEqual(len(summary["workers"]), 1)
            self.assertEqual(len(summary["tasks"]), 1)
        finally:
            live_master.stop()

    def test_worker_information_and_safety(self):
        """
        Verify that worker state, status badges, and metadata are correctly serialized
        and internal sockets/locks are never exposed.
        """
        worker_id = "test-worker-101"
        dummy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.master.register_worker(worker_id, dummy_sock, ("127.0.0.1", 54321))
            
            res = self.client.get('/api/workers')
            self.assertEqual(res.status_code, 200)
            workers = res.get_json()
            self.assertEqual(len(workers), 1)

            w = workers[0]
            self.assertEqual(w["worker_id"], worker_id)
            self.assertEqual(w["status"], WorkerStatus.IDLE)
            self.assertIsNone(w["current_task"])
            self.assertIsInstance(w["last_heartbeat"], (int, float))
            self.assertIsInstance(w["seconds_since_heartbeat"], (int, float))
            self.assertIsInstance(w["uptime_seconds"], (int, float))
            self.assertEqual(w["address"], "127.0.0.1:54321")

            # Crucial requirement: socket must NEVER be exposed
            self.assertNotIn("socket", w)
        finally:
            dummy_sock.close()

    def test_task_status_lifecycle_and_progress(self):
        """
        Verify that task creation, assignment, completion, and statistics
        accurately reflect across /api/tasks and /api/status.
        """
        # Add 2 tasks
        tasks_input = [
            {"task_id": "map_0", "data": "word count sample"},
            {"task_id": "map_1", "data": "another test sample"}
        ]
        self.master.scheduler.add_tasks(tasks_input, max_retries=3)

        # 1. Check pending tasks in API
        res = self.client.get('/api/tasks')
        self.assertEqual(res.status_code, 200)
        tasks = res.get_json()
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0]["task_id"], "map_0")
        self.assertEqual(tasks[0]["state"], TaskState.PENDING)
        self.assertIsNone(tasks[0]["assigned_worker"])

        res = self.client.get('/api/status')
        stats = res.get_json()["stats"]
        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["pending"], 2)
        self.assertEqual(stats["completed"], 0)

        # 2. Assign task to worker
        worker_id = "worker-1"
        dummy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.master.register_worker(worker_id, dummy_sock, ("127.0.0.1", 54322))
            task = self.master.scheduler.get_task(worker_id)
            self.assertIsNotNone(task)
            self.assertEqual(task.task_id, "map_0")

            res = self.client.get('/api/tasks')
            tasks = res.get_json()
            task_0 = next(t for t in tasks if t["task_id"] == "map_0")
            self.assertEqual(task_0["state"], TaskState.ASSIGNED)
            self.assertEqual(task_0["assigned_worker"], worker_id)

            # 3. Complete task
            result_data = [["word", 1], ["count", 1], ["sample", 1]]
            success, _ = self.master.scheduler.complete_task(
                "map_0", worker_id, result=result_data, validate=True
            )
            self.assertTrue(success)

            res = self.client.get('/api/tasks')
            tasks = res.get_json()
            task_0 = next(t for t in tasks if t["task_id"] == "map_0")
            self.assertEqual(task_0["state"], TaskState.COMPLETED)

            res = self.client.get('/api/status')
            stats = res.get_json()["stats"]
            self.assertEqual(stats["completed"], 1)
            self.assertEqual(stats["pending"], 1)
            self.assertEqual(stats["assigned"], 0)
        finally:
            dummy_sock.close()

    def test_frontend_field_contracts(self):
        """
        Verify that all fields consumed by script.js exist in API responses.
        """
        # Register worker and add a task
        dummy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.master.register_worker("w-contract", dummy_sock, ("127.0.0.1", 50001))
            self.master.scheduler.add_tasks([{"task_id": "t-contract", "data": "abc"}])

            # Status contract
            res = self.client.get('/api/status')
            status = res.get_json()
            for key in ["job_in_progress", "job_completed", "job_failed", "job_error", "server_running", "stats"]:
                self.assertIn(key, status, f"Missing key '{key}' in /api/status")
            for stat_key in ["total", "pending", "assigned", "completed", "failed"]:
                self.assertIn(stat_key, status["stats"], f"Missing stat '{stat_key}' in stats")

            # Worker contract
            res = self.client.get('/api/workers')
            workers = res.get_json()
            self.assertTrue(len(workers) > 0)
            w = workers[0]
            for key in ["worker_id", "status", "current_task", "seconds_since_heartbeat", "address", "uptime_seconds"]:
                self.assertIn(key, w, f"Missing key '{key}' in /api/workers")

            # Task contract
            res = self.client.get('/api/tasks')
            tasks = res.get_json()
            self.assertTrue(len(tasks) > 0)
            t = tasks[0]
            for key in ["task_id", "state", "assigned_worker", "retries", "max_retries", "failure_reason"]:
                self.assertIn(key, t, f"Missing key '{key}' in /api/tasks")
        finally:
            dummy_sock.close()

    def test_summary_endpoint(self):
        """
        Verify /api/summary returns status, workers, and tasks in a single JSON payload.
        """
        res = self.client.get('/api/summary')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertIn("status", data)
        self.assertIn("workers", data)
        self.assertIn("tasks", data)

    def test_worker_task_assignment_lifecycle_and_multi_worker(self):
        """
        Verify Problem 1 requirements:
        - Idle workers display current_task as None (renders as '--')
        - Busy workers display their actual assigned task ID
        - The value updates when tasks start and finish
        - The dashboard correctly handles multiple workers simultaneously
        """
        # Register 2 workers
        sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.master.register_worker("worker-alpha", sock1, ("127.0.0.1", 51001))
            self.master.register_worker("worker-beta", sock2, ("127.0.0.1", 51002))

            # Step 1: Initial state - both workers IDLE and current_task is None
            res = self.client.get('/api/workers')
            self.assertEqual(res.status_code, 200)
            workers = res.get_json()
            self.assertEqual(len(workers), 2)
            w_alpha = next(w for w in workers if w["worker_id"] == "worker-alpha")
            w_beta = next(w for w in workers if w["worker_id"] == "worker-beta")
            self.assertEqual(w_alpha["status"], WorkerStatus.IDLE)
            self.assertIsNone(w_alpha["current_task"])
            self.assertEqual(w_beta["status"], WorkerStatus.IDLE)
            self.assertIsNone(w_beta["current_task"])

            # Step 2: Add 2 tasks and assign task 0 to worker-alpha
            self.master.scheduler.add_tasks([
                {"task_id": "map_0", "data": "alpha task data"},
                {"task_id": "map_1", "data": "beta task data"}
            ])
            task_0 = self.master.scheduler.get_task("worker-alpha")
            self.assertIsNotNone(task_0)
            self.assertEqual(task_0.task_id, "map_0")

            # Verify worker-alpha is now BUSY with map_0, while worker-beta remains IDLE
            res = self.client.get('/api/workers')
            workers = res.get_json()
            w_alpha = next(w for w in workers if w["worker_id"] == "worker-alpha")
            w_beta = next(w for w in workers if w["worker_id"] == "worker-beta")
            self.assertEqual(w_alpha["status"], WorkerStatus.BUSY)
            self.assertEqual(w_alpha["current_task"], "map_0")
            self.assertEqual(w_beta["status"], WorkerStatus.IDLE)
            self.assertIsNone(w_beta["current_task"])

            # Verify /api/summary also reflects this
            sum_res = self.client.get('/api/summary')
            sum_workers = sum_res.get_json()["workers"]
            sum_alpha = next(w for w in sum_workers if w["worker_id"] == "worker-alpha")
            self.assertEqual(sum_alpha["status"], WorkerStatus.BUSY)
            self.assertEqual(sum_alpha["current_task"], "map_0")

            # Step 3: Assign task 1 to worker-beta
            task_1 = self.master.scheduler.get_task("worker-beta")
            self.assertIsNotNone(task_1)
            self.assertEqual(task_1.task_id, "map_1")

            # Verify BOTH workers are BUSY with their respective assigned tasks
            res = self.client.get('/api/workers')
            workers = res.get_json()
            w_alpha = next(w for w in workers if w["worker_id"] == "worker-alpha")
            w_beta = next(w for w in workers if w["worker_id"] == "worker-beta")
            self.assertEqual(w_alpha["status"], WorkerStatus.BUSY)
            self.assertEqual(w_alpha["current_task"], "map_0")
            self.assertEqual(w_beta["status"], WorkerStatus.BUSY)
            self.assertEqual(w_beta["current_task"], "map_1")

            # Step 4: Complete task 0 for worker-alpha
            success, _ = self.master.scheduler.complete_task("map_0", "worker-alpha", result=[("word", 1)])
            self.assertTrue(success)

            # Verify worker-alpha transitions back to IDLE (current_task=None), while worker-beta remains BUSY
            res = self.client.get('/api/workers')
            workers = res.get_json()
            w_alpha = next(w for w in workers if w["worker_id"] == "worker-alpha")
            w_beta = next(w for w in workers if w["worker_id"] == "worker-beta")
            self.assertEqual(w_alpha["status"], WorkerStatus.IDLE)
            self.assertIsNone(w_alpha["current_task"])
            self.assertEqual(w_beta["status"], WorkerStatus.BUSY)
            self.assertEqual(w_beta["current_task"], "map_1")

            # Step 5: Complete task 1 for worker-beta
            success, _ = self.master.scheduler.complete_task("map_1", "worker-beta", result=[("data", 1)])
            self.assertTrue(success)

            # Both workers are now IDLE with current_task=None
            res = self.client.get('/api/workers')
            workers = res.get_json()
            w_alpha = next(w for w in workers if w["worker_id"] == "worker-alpha")
            w_beta = next(w for w in workers if w["worker_id"] == "worker-beta")
            self.assertEqual(w_alpha["status"], WorkerStatus.IDLE)
            self.assertIsNone(w_alpha["current_task"])
            self.assertEqual(w_beta["status"], WorkerStatus.IDLE)
            self.assertIsNone(w_beta["current_task"])
        finally:
            sock1.close()
            sock2.close()

    def test_task_failure_reason_reflected_in_api(self):
        """
        Verify Problem 2 requirements:
        - Real task failure records its actual error reason
        - The failure reason is returned by /api/tasks and /api/summary
        - Retry and reassignment behavior follows scheduler policy (requeued as PENDING with retries=1)
        - On successful recovery, task state is COMPLETED and failure reason history is preserved
        - Successful tasks that never failed have failure_reason as None (renders as '--')
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.master.register_worker("worker-err-test", sock, ("127.0.0.1", 51003))

            # Add two tasks: task_fail (will fail and retry) and task_ok (will succeed directly)
            self.master.scheduler.add_tasks([
                {"task_id": "task_fail", "data": "fail test data"},
                {"task_id": "task_ok", "data": "ok test data"}
            ], max_retries=3)

            # Assign task_fail to worker
            task = self.master.scheduler.get_task("worker-err-test")
            self.assertEqual(task.task_id, "task_fail")

            # Fail the task with a genuine error reason
            error_reason = "ZeroDivisionError: division by zero in mapper parse_tokens()"
            perm_failed = self.master.scheduler.fail_task("task_fail", worker_id="worker-err-test", reason=error_reason)
            self.assertFalse(perm_failed) # Requeued, not permanently failed

            # Check /api/tasks: failure reason MUST be present and equal to the real error
            res = self.client.get('/api/tasks')
            self.assertEqual(res.status_code, 200)
            tasks = res.get_json()
            t_fail = next(t for t in tasks if t["task_id"] == "task_fail")
            t_ok = next(t for t in tasks if t["task_id"] == "task_ok")

            self.assertEqual(t_fail["state"], TaskState.PENDING)
            self.assertEqual(t_fail["retries"], 1)
            self.assertEqual(t_fail["failure_reason"], error_reason)
            self.assertIn("failure_history", t_fail)
            self.assertEqual(len(t_fail["failure_history"]), 1)
            self.assertEqual(t_fail["failure_history"][0]["reason"], error_reason)

            # Normal task that has not failed MUST have failure_reason as None
            self.assertIsNone(t_ok["failure_reason"])
            self.assertEqual(t_ok["retries"], 0)

            # Check /api/summary also has the exact failure_reason
            sum_res = self.client.get('/api/summary')
            sum_tasks = sum_res.get_json()["tasks"]
            sum_fail = next(t for t in sum_tasks if t["task_id"] == "task_fail")
            self.assertEqual(sum_fail["failure_reason"], error_reason)

            # In FIFO queue, task_ok is at the head, followed by requeued task_fail
            task_ok_assigned = self.master.scheduler.get_task("worker-err-test")
            self.assertEqual(task_ok_assigned.task_id, "task_ok")
            # Complete task_ok so worker becomes idle again
            self.master.scheduler.complete_task("task_ok", "worker-err-test", result=[("ok", 1)])

            # Reassign task_fail to worker (attempt 2)
            task_retry = self.master.scheduler.get_task("worker-err-test")
            self.assertEqual(task_retry.task_id, "task_fail")

            # Task successfully completes on retry
            success, comp_task = self.master.scheduler.complete_task("task_fail", "worker-err-test", result=[("word", 1)])
            self.assertTrue(success)

            # Verify API shows task_fail COMPLETED, not marked as failed, with failure_reason preserved
            res = self.client.get('/api/tasks')
            tasks = res.get_json()
            t_fail = next(t for t in tasks if t["task_id"] == "task_fail")
            self.assertEqual(t_fail["state"], TaskState.COMPLETED)
            self.assertEqual(t_fail["retries"], 1)
            self.assertEqual(t_fail["failure_reason"], error_reason)

            # Overall scheduler is not failed
            self.assertFalse(self.master.scheduler.is_failed())
        finally:
            sock.close()

    def test_error_handling(self):
        """
        Verify that API endpoints return clean fallback states
        if the master raises an unexpected exception during data snapshotting.
        """
        class FaultyMaster:
            def get_dashboard_data(self):
                raise RuntimeError("Simulated internal master error")

        faulty_app = create_dashboard_app(master_server=FaultyMaster())
        faulty_client = faulty_app.test_client()

        res = faulty_client.get('/api/status')
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertFalse(data["server_running"])

        res = faulty_client.get('/api/workers')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json(), [])

        res = faulty_client.get('/api/tasks')
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_json(), [])


class TestDashboardLiveServer(unittest.TestCase):
    def setUp(self):
        self.tcp_port = find_free_port()
        self.dashboard_port = find_free_port()
        self.master = MasterServer(
            host='127.0.0.1',
            port=self.tcp_port,
            dashboard_port=self.dashboard_port,
            enable_dashboard=True
        )
        self.master_thread = threading.Thread(target=self.master.start, daemon=True)
        self.master_thread.start()
        # Give server time to bind and listen
        time.sleep(0.3)

    def tearDown(self):
        if self.master.running:
            self.master.stop()

    def test_live_dashboard_http_endpoints_while_master_running(self):
        """
        Verify that HTTP clients can connect to the running dashboard server
        on its dashboard port while the master is simultaneously running TCP.
        """
        base_url = f"http://127.0.0.1:{self.dashboard_port}"

        # 1. Test index page via HTTP
        with urllib.request.urlopen(f"{base_url}/", timeout=3.0) as resp:
            self.assertEqual(resp.status, 200)
            html = resp.read().decode('utf-8')
            self.assertIn("Distributed MapReduce", html)

        # 2. Test /api/status via HTTP
        with urllib.request.urlopen(f"{base_url}/api/status", timeout=3.0) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode('utf-8'))
            self.assertIn("stats", data)
            self.assertTrue(data["server_running"])

        # 3. Test /api/workers via HTTP
        with urllib.request.urlopen(f"{base_url}/api/workers", timeout=3.0) as resp:
            self.assertEqual(resp.status, 200)
            workers = json.loads(resp.read().decode('utf-8'))
            self.assertIsInstance(workers, list)

        # 4. Test /api/tasks via HTTP
        with urllib.request.urlopen(f"{base_url}/api/tasks", timeout=3.0) as resp:
            self.assertEqual(resp.status, 200)
            tasks = json.loads(resp.read().decode('utf-8'))
            self.assertIsInstance(tasks, list)

        # 5. Verify TCP master socket is still receptive
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_sock:
            tcp_sock.connect(('127.0.0.1', self.tcp_port))
            # Connected successfully to port 5000 / tcp_port


if __name__ == "__main__":
    unittest.main()
