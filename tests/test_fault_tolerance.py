import unittest
import socket
import threading
import time
import os
import tempfile
import shutil

from master.master import MasterServer, WorkerStatus
from master.scheduler import TaskState
from workers.worker import WorkerNode
from common.protocol import send_message, MessageReader

def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

class TestFaultTolerance(unittest.TestCase):
    def setUp(self):
        self.port = find_free_port()
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        if hasattr(self, 'master') and self.master.running:
            self.master.stop()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_worker_heartbeat_and_timeout_detection(self):
        """
        Verify that MasterServer tracks worker heartbeats and marks unresponsive
        workers as UNAVAILABLE once worker_timeout expires.
        """
        # Create master with a very short timeout for quick testing
        self.master = MasterServer(host='127.0.0.1', port=self.port, worker_timeout=1.0)
        master_thread = threading.Thread(target=self.master.start, daemon=True)
        master_thread.start()
        time.sleep(0.2)

        # Connect a simulated worker
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', self.port))
        worker_id = "test-timeout-worker"
        
        # 1. Register worker
        send_message(sock, {"type": "REGISTER", "worker_id": worker_id})
        reader = MessageReader(sock)
        ack = reader.read_message()
        self.assertEqual(ack.get("type"), "REGISTER_ACK")

        # Worker should now be registered and IDLE
        with self.master.lock:
            self.assertIn(worker_id, self.master.worker_info)
            self.assertEqual(self.master.worker_info[worker_id]["status"], WorkerStatus.IDLE)

        # 2. Send heartbeat
        send_message(sock, {"type": "HEARTBEAT", "worker_id": worker_id})
        time.sleep(0.2)
        with self.master.lock:
            self.assertEqual(self.master.worker_info[worker_id]["status"], WorkerStatus.IDLE)

        # 3. Stop sending heartbeats and wait for timeout to trigger (> 1.0s)
        time.sleep(1.4)

        # Master should detect timeout and mark worker as UNAVAILABLE
        with self.master.lock:
            self.assertEqual(self.master.worker_info[worker_id]["status"], WorkerStatus.UNAVAILABLE)
            self.assertNotIn(worker_id, self.master.workers)

        sock.close()

    def test_worker_task_reassignment_on_disconnect(self):
        """
        Verify that when a worker holding an assigned task disconnects without returning
        a result, the task is safely requeued and reassigned to an available worker.
        """
        self.master = MasterServer(host='127.0.0.1', port=self.port, worker_timeout=3.0)
        master_thread = threading.Thread(target=self.master.start, daemon=True)
        master_thread.start()
        time.sleep(0.2)

        # Worker 1: simulated worker socket that will disconnect immediately upon getting a task
        w1_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        w1_sock.connect(('127.0.0.1', self.port))
        send_message(w1_sock, {"type": "REGISTER", "worker_id": "failing-worker-1"})
        w1_reader = MessageReader(w1_sock)
        ack = w1_reader.read_message()
        self.assertEqual(ack.get("type"), "REGISTER_ACK")

        # Create input file with 2 lines -> 1 task
        input_file = os.path.join(self.test_dir, "input.txt")
        output_file = os.path.join(self.test_dir, "output.txt")
        with open(input_file, "w", encoding="utf-8") as f:
            f.write("distributed systems\nmapreduce fault tolerance\n")

        # Submit job via TCP
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect(('127.0.0.1', self.port))
        send_message(client_sock, {
            "type": "SUBMIT_JOB",
            "input_file": input_file,
            "output_file": output_file
        })
        resp = MessageReader(client_sock).read_message()
        self.assertEqual(resp.get("type"), "JOB_ACCEPTED")
        client_sock.close()

        # Worker 1 reads MAP_TASK
        msg = w1_reader.read_message()
        self.assertEqual(msg.get("type"), "MAP_TASK")
        self.assertEqual(msg.get("task_id"), "map_0")

        # Abruptly close Worker 1's socket without sending MAP_RESULT
        w1_sock.close()
        time.sleep(0.4)

        # Verify failing-worker-1 is marked UNAVAILABLE and task is requeued as PENDING with retry=1
        with self.master.lock:
            self.assertEqual(self.master.worker_info["failing-worker-1"]["status"], WorkerStatus.UNAVAILABLE)
            task = self.master.scheduler.tasks["map_0"]
            self.assertEqual(task.state, TaskState.PENDING)
            self.assertEqual(task.retries, 1)

        # Start Worker 2 to pick up the reassigned task
        worker2 = WorkerNode(master_host='127.0.0.1', master_port=self.port, worker_id="worker-2")
        w2_thread = threading.Thread(target=worker2.connect, daemon=True)
        w2_thread.start()

        # Wait for job to complete
        for _ in range(20):
            time.sleep(0.3)
            if self.master.job_completed:
                break

        self.assertTrue(self.master.job_completed)
        self.assertTrue(os.path.exists(output_file))
        self.assertEqual(self.master.scheduler.tasks["map_0"].state, TaskState.COMPLETED)
        worker2.stop()

    def test_duplicate_result_handling(self):
        """
        Verify that duplicate results for the same task are ignored and
        do not corrupt the aggregated map results.
        """
        self.master = MasterServer(host='127.0.0.1', port=self.port)
        master_thread = threading.Thread(target=self.master.start, daemon=True)
        master_thread.start()
        time.sleep(0.2)

        # Connect a simulated worker
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', self.port))
        send_message(sock, {"type": "REGISTER", "worker_id": "worker-dup"})
        MessageReader(sock).read_message()

        # Queue a task manually into scheduler
        self.master.scheduler.add_tasks([{"task_id": "task_dup_1", "data": "word word"}])
        self.master.job_in_progress = True

        # Assign task to worker
        task = self.master.scheduler.get_task("worker-dup")
        self.assertIsNotNone(task)
        self.master.worker_info["worker-dup"]["status"] = WorkerStatus.BUSY
        self.master.worker_info["worker-dup"]["current_task"] = "task_dup_1"

        # Send first MAP_RESULT
        result_payload = [("word", 1), ("word", 1)]
        send_message(sock, {
            "type": "MAP_RESULT",
            "task_id": "task_dup_1",
            "result": result_payload
        })
        time.sleep(0.3)

        self.assertEqual(len(self.master.task_results["task_dup_1"]), 2)
        self.assertEqual(len(self.master.map_results), 2)

        # Send second duplicate MAP_RESULT
        send_message(sock, {
            "type": "MAP_RESULT",
            "task_id": "task_dup_1",
            "result": result_payload
        })
        time.sleep(0.3)

        # Results must NOT be duplicated!
        self.assertEqual(len(self.master.task_results["task_dup_1"]), 2)
        self.assertEqual(len(self.master.map_results), 2)
        sock.close()

    def test_deliberate_worker_termination_and_recovery(self):
        """
        Integration test demonstrating recovery when a worker is deliberately terminated
        during a task:
        1. Master distributes multi-task job.
        2. Worker 1 is killed during execution.
        3. Master detects failure and reassigns task to Worker 2.
        4. Job completes with 100% correct word counts.
        """
        self.master = MasterServer(host='127.0.0.1', port=self.port, worker_timeout=1.5, max_retries=3)
        master_thread = threading.Thread(target=self.master.start, daemon=True)
        master_thread.start()
        time.sleep(0.2)

        # Prepare dataset: 4 lines -> 2 tasks (chunk_size=2)
        input_file = os.path.join(self.test_dir, "fault_input.txt")
        output_file = os.path.join(self.test_dir, "fault_output.txt")
        input_data = (
            "distributed systems fault tolerance\n"
            "mapreduce master worker heartbeat\n"
            "distributed systems task reassignment\n"
            "mapreduce worker timeout recovery\n"
        )
        with open(input_file, "w", encoding="utf-8") as f:
            f.write(input_data)

        # Start Worker 1
        worker1 = WorkerNode(master_host='127.0.0.1', master_port=self.port, worker_id="victim-worker-1")
        w1_thread = threading.Thread(target=worker1.connect, daemon=True)
        w1_thread.start()
        time.sleep(0.3)

        # Start Worker 2
        worker2 = WorkerNode(master_host='127.0.0.1', master_port=self.port, worker_id="healthy-worker-2")
        w2_thread = threading.Thread(target=worker2.connect, daemon=True)
        w2_thread.start()
        time.sleep(0.3)

        # Submit job
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect(('127.0.0.1', self.port))
        send_message(client_sock, {
            "type": "SUBMIT_JOB",
            "input_file": input_file,
            "output_file": output_file
        })
        resp = MessageReader(client_sock).read_message()
        self.assertEqual(resp.get("type"), "JOB_ACCEPTED")
        client_sock.close()

        # Deliberately terminate Worker 1 immediately while tasks are in flight
        time.sleep(0.1)
        worker1.stop()

        # Wait for job completion through recovery
        max_wait = 15.0
        start_time = time.time()
        while time.time() - start_time < max_wait:
            time.sleep(0.5)
            if self.master.job_completed:
                break

        self.assertTrue(self.master.job_completed, "Job should complete successfully despite worker termination")
        self.assertTrue(os.path.exists(output_file), "Output file should be generated")

        # Read output and verify correctness
        word_counts = {}
        with open(output_file, "r", encoding="utf-8") as f:
            for line in f:
                if ":" in line:
                    word, count = line.strip().split(":", 1)
                    word_counts[word.strip()] = int(count.strip())

        # Expected word counts:
        # "distributed" -> 2
        # "systems" -> 2
        # "fault" -> 1
        # "tolerance" -> 1
        # "mapreduce" -> 2
        # "master" -> 1
        # "worker" -> 2
        # "heartbeat" -> 1
        # "task" -> 1
        # "reassignment" -> 1
        # "timeout" -> 1
        # "recovery" -> 1
        expected = {
            "distributed": 2,
            "systems": 2,
            "fault": 1,
            "tolerance": 1,
            "mapreduce": 2,
            "master": 1,
            "worker": 2,
            "heartbeat": 1,
            "task": 1,
            "reassignment": 1,
            "timeout": 1,
            "recovery": 1
        }
        self.assertEqual(word_counts, expected)
        worker2.stop()

    def test_controlled_fault_injection_and_recovery(self):
        """
        Verify controlled, reversible fault-injection mechanism:
        1. Worker configured with one-time fault on 'map_0' exercises the actual TCP TASK_FAILED protocol path.
        2. Master records genuine failure reason and requeues task.
        3. Dashboard API /api/tasks and /api/summary return the actual failure reason.
        4. Reassignment and retry execute successfully.
        5. Job completes successfully with 100% correct MapReduce results.
        6. Recovered task has state COMPLETED and retains failure reason without being marked as failed.
        """
        import urllib.request
        import json

        dashboard_port = find_free_port()
        self.master = MasterServer(
            host='127.0.0.1',
            port=self.port,
            worker_timeout=4.0,
            max_retries=3,
            dashboard_port=dashboard_port,
            enable_dashboard=True
        )
        master_thread = threading.Thread(target=self.master.start, daemon=True)
        master_thread.start()
        time.sleep(0.3)

        # Dataset for 2 map tasks
        input_file = os.path.join(self.test_dir, "fi_input.txt")
        output_file = os.path.join(self.test_dir, "fi_output.txt")
        input_data = (
            "distributed fault injection test\n"
            "systems mapreduce recovery verification\n"
            "resilience cluster master worker\n"
            "scheduler heartbeat reassignment monitoring\n"
        )
        with open(input_file, "w", encoding="utf-8") as f:
            f.write(input_data)

        # Start worker with one-time fault injection on map_0
        fault_reason = "Simulated ValueError: Corrupted token parsing in map_0"
        worker = WorkerNode(
            master_host='127.0.0.1',
            master_port=self.port,
            worker_id="fi-worker-1",
            heartbeat_interval=0.5,
            fault_inject_task="map_0",
            fault_inject_error=fault_reason
        )
        w_thread = threading.Thread(target=worker.connect, daemon=True)
        w_thread.start()
        time.sleep(0.3)

        # Submit job via TCP
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect(('127.0.0.1', self.port))
        send_message(client_sock, {
            "type": "SUBMIT_JOB",
            "input_file": input_file,
            "output_file": output_file
        })
        resp = MessageReader(client_sock).read_message()
        self.assertEqual(resp.get("type"), "JOB_ACCEPTED")
        client_sock.close()

        # Wait for job completion through fault and recovery
        max_wait = 15.0
        start_t = time.time()
        saw_failure_in_api = False
        while time.time() - start_t < max_wait:
            time.sleep(0.2)
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{dashboard_port}/api/tasks", timeout=1.0) as http_resp:
                    tasks_data = json.loads(http_resp.read().decode('utf-8'))
                    for t in tasks_data:
                        if t.get("task_id") == "map_0" and t.get("failure_reason") == fault_reason:
                            saw_failure_in_api = True
                            self.assertGreaterEqual(t.get("retries", 0), 1)
            except Exception:
                pass

            if self.master.job_completed:
                break

        self.assertTrue(saw_failure_in_api, "Actual failure reason must reach dashboard /api/tasks")
        self.assertTrue(self.master.job_completed, "Job should complete successfully after retry recovery")
        self.assertTrue(os.path.exists(output_file))

        # Verify final task state via dashboard API
        with urllib.request.urlopen(f"http://127.0.0.1:{dashboard_port}/api/tasks", timeout=1.0) as http_resp:
            final_tasks = json.loads(http_resp.read().decode('utf-8'))
            t_map0 = next(t for t in final_tasks if t["task_id"] == "map_0")
            t_map1 = next(t for t in final_tasks if t["task_id"] == "map_1")

            # map_0 succeeded on retry: state is completed, failure_reason is preserved, retries >= 1
            self.assertEqual(t_map0["state"], TaskState.COMPLETED)
            self.assertEqual(t_map0["failure_reason"], fault_reason)
            self.assertEqual(t_map0["retries"], 1)

            # map_1 succeeded first try: state is completed, failure_reason is None, retries == 0
            self.assertEqual(t_map1["state"], TaskState.COMPLETED)
            self.assertIsNone(t_map1["failure_reason"])
            self.assertEqual(t_map1["retries"], 0)

        # Verify results correctness
        word_counts = {}
        with open(output_file, "r", encoding="utf-8") as f:
            for line in f:
                if ":" in line:
                    w, c = line.strip().split(":", 1)
                    word_counts[w.strip()] = int(c.strip())

        expected_words = [
            "distributed", "fault", "injection", "test",
            "systems", "mapreduce", "recovery", "verification",
            "resilience", "cluster", "master", "worker",
            "scheduler", "heartbeat", "reassignment", "monitoring"
        ]
        for ew in expected_words:
            self.assertEqual(word_counts.get(ew), 1)

        worker.stop()

if __name__ == "__main__":
    unittest.main()
