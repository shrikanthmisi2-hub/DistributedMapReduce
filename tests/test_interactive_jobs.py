"""
Automated test suite for Interactive Job Submission, Dynamic Execution,
Result Retrieval, History Tracking, and Concurrency Protection.
"""
import unittest
import socket
import threading
import time
import os
import shutil
import tempfile
import io
import json

from master.master import MasterServer
from workers.worker import WorkerNode
from dashboard.app import create_dashboard_app


def find_free_port():
    """Finds an unused local port for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class TestInteractiveJobs(unittest.TestCase):
    def setUp(self):
        self.tcp_port = find_free_port()
        self.master = MasterServer(
            host='127.0.0.1',
            port=self.tcp_port,
            worker_timeout=4.0,
            enable_dashboard=False
        )
        self.master_thread = threading.Thread(target=self.master.start, daemon=True)
        self.master_thread.start()
        time.sleep(0.2)

        self.app = create_dashboard_app(master_server=self.master)
        self.client = self.app.test_client()

    def tearDown(self):
        if self.master.running:
            self.master.stop()

    def test_submit_custom_text(self):
        """Test submitting custom text input via /api/submit-job."""
        payload = {
            "input_text": "distributed systems mapreduce cluster",
            "num_mappers": 2
        }
        res = self.client.post('/api/submit-job', json=payload)
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data["success"])
        self.assertIn("job_id", data)
        self.assertEqual(data["num_map_tasks"], 2)

        # Verify job is tracked in master
        with self.master.lock:
            self.assertEqual(self.master.current_job_id, data["job_id"])
            self.assertTrue(len(self.master.job_history) >= 1)

    def test_submit_file_upload(self):
        """Test uploading a .txt file via multipart /api/submit-job."""
        file_content = b"cloud computing parallel map reduce\nworker threads fault tolerance"
        data = {
            'file': (io.BytesIO(file_content), 'sample_input.txt'),
            'num_mappers': '3'
        }
        res = self.client.post('/api/submit-job', data=data, content_type='multipart/form-data')
        self.assertEqual(res.status_code, 200)
        resp_data = res.get_json()
        self.assertTrue(resp_data["success"])
        self.assertIn("job_id", resp_data)
        self.assertEqual(resp_data["num_map_tasks"], 3)

    def test_invalid_and_empty_inputs(self):
        """Test validation for empty text, empty file, non-txt files, and invalid mapper counts."""
        # 1. Completely empty text
        res = self.client.post('/api/submit-job', json={"input_text": "   "})
        self.assertEqual(res.status_code, 400)
        self.assertFalse(res.get_json()["success"])
        self.assertIn("cannot be empty", res.get_json()["error"].lower())

        # 2. Empty file
        data_empty_file = {
            'file': (io.BytesIO(b"   "), 'empty.txt'),
            'num_mappers': '2'
        }
        res = self.client.post('/api/submit-job', data=data_empty_file, content_type='multipart/form-data')
        self.assertEqual(res.status_code, 400)
        self.assertIn("empty", res.get_json()["error"].lower())

        # 3. Non-txt file (e.g., .pdf)
        data_pdf = {
            'file': (io.BytesIO(b"%PDF-1.4..."), 'document.pdf'),
            'num_mappers': '2'
        }
        res = self.client.post('/api/submit-job', data=data_pdf, content_type='multipart/form-data')
        self.assertEqual(res.status_code, 400)
        self.assertIn(".txt", res.get_json()["error"].lower())

        # 4. Invalid mapper counts (negative, 0, or excessive)
        res = self.client.post('/api/submit-job', json={"input_text": "valid text", "num_mappers": 0})
        self.assertEqual(res.status_code, 400)
        self.assertIn("between 1 and 32", res.get_json()["error"])

        res = self.client.post('/api/submit-job', json={"input_text": "valid text", "num_mappers": 50})
        self.assertEqual(res.status_code, 400)
        self.assertIn("between 1 and 32", res.get_json()["error"])

        res = self.client.post('/api/submit-job', json={"input_text": "valid text", "num_mappers": "abc"})
        self.assertEqual(res.status_code, 400)

    def test_dynamic_task_counts(self):
        """Test that configuring 1 vs 4 map tasks creates the exact task count."""
        # 4 mappers on 4 lines
        four_lines = "line one\nline two\nline three\nline four\n"
        res = self.client.post('/api/submit-job', json={"input_text": four_lines, "num_mappers": 4})
        self.assertEqual(res.status_code, 200)

        tasks = self.master.scheduler.get_all_tasks()
        self.assertEqual(len(tasks), 4)

    def test_end_to_end_execution_and_results(self):
        """
        Verify that actual workers connect, process tasks, and return real word-count results.
        Different inputs produce distinct, accurate outputs.
        """
        # Start 2 worker nodes
        w1 = WorkerNode(master_host='127.0.0.1', master_port=self.tcp_port, worker_id="test-worker-1")
        w2 = WorkerNode(master_host='127.0.0.1', master_port=self.tcp_port, worker_id="test-worker-2")
        t1 = threading.Thread(target=w1.connect, daemon=True)
        t2 = threading.Thread(target=w2.connect, daemon=True)
        t1.start()
        t2.start()
        time.sleep(0.4)

        try:
            # 1. Submit Job 1: Fruits
            fruits_text = "apple banana apple orange apple banana"
            res1 = self.client.post('/api/submit-job', json={"input_text": fruits_text, "num_mappers": 2})
            self.assertEqual(res1.status_code, 200)
            job1_id = res1.get_json()["job_id"]

            # Wait for Job 1 completion
            for _ in range(25):
                time.sleep(0.2)
                if self.master.job_completed:
                    break
            self.assertTrue(self.master.job_completed)

            # Retrieve results for Job 1
            res_job1 = self.client.get(f'/api/results/{job1_id}')
            self.assertEqual(res_job1.status_code, 200)
            data1 = res_job1.get_json()
            self.assertTrue(data1["available"])
            self.assertEqual(data1["unique_words"], 3)  # apple, banana, orange
            self.assertEqual(data1["total_words"], 6)

            counts1 = {item["word"]: item["count"] for item in data1["results"]}
            self.assertEqual(counts1["apple"], 3)
            self.assertEqual(counts1["banana"], 2)
            self.assertEqual(counts1["orange"], 1)

            # 2. Submit Job 2: Animals (different input producing different results)
            animals_text = "cat dog dog dog bird elephant"
            res2 = self.client.post('/api/submit-job', json={"input_text": animals_text, "num_mappers": 2})
            self.assertEqual(res2.status_code, 200)
            job2_id = res2.get_json()["job_id"]
            self.assertNotEqual(job1_id, job2_id)

            # Wait for Job 2 completion
            for _ in range(25):
                time.sleep(0.2)
                if self.master.job_completed and self.master.current_job_id == job2_id:
                    break
            self.assertTrue(self.master.job_completed)

            # Retrieve results for Job 2
            res_job2 = self.client.get(f'/api/results/{job2_id}')
            data2 = res_job2.get_json()
            self.assertTrue(data2["available"])
            self.assertEqual(data2["unique_words"], 4)  # bird, cat, dog, elephant
            self.assertEqual(data2["total_words"], 6)

            counts2 = {item["word"]: item["count"] for item in data2["results"]}
            self.assertEqual(counts2["dog"], 3)
            self.assertEqual(counts2["cat"], 1)
            self.assertEqual(counts2["bird"], 1)
            self.assertEqual(counts2["elephant"], 1)
            self.assertNotIn("apple", counts2)

            # 3. Verify Job 1's results are still preserved and unaltered in history!
            res_job1_again = self.client.get(f'/api/results/{job1_id}')
            counts1_again = {item["word"]: item["count"] for item in res_job1_again.get_json()["results"]}
            self.assertEqual(counts1_again["apple"], 3)

            # 4. Check /api/jobs history table
            res_history = self.client.get('/api/jobs')
            history = res_history.get_json()
            self.assertEqual(len(history), 2)
            jids = [j["job_id"] for j in history]
            self.assertIn(job1_id, jids)
            self.assertIn(job2_id, jids)

        finally:
            w1.stop()
            w2.stop()

    def test_concurrent_submission_protection(self):
        """Verify that submitting a job while another job is in progress returns HTTP 409 Conflict."""
        # Manually set master state to in progress
        with self.master.lock:
            self.master.job_in_progress = True

        res = self.client.post('/api/submit-job', json={"input_text": "conflict test"})
        self.assertEqual(res.status_code, 409)
        self.assertIn("already in progress", res.get_json()["error"].lower())

    def test_master_offline_error_handling(self):
        """Verify that submitting a job when the master is offline returns HTTP 503."""
        # Dashboard connected to an offline master port
        unused_port = find_free_port()
        offline_app = create_dashboard_app(master_server=None, master_port=unused_port)
        offline_client = offline_app.test_client()

        res = offline_client.post('/api/submit-job', json={"input_text": "offline test"})
        self.assertEqual(res.status_code, 503)
        self.assertIn("offline", res.get_json()["error"].lower())

    def test_relative_frequency_calculation(self):
        """
        Verify relative frequency formula:
        relative_frequency = (word_count / total_words) * 100
        Using exact prompt example:
        apple: 3, banana: 2, mango: 1 -> total: 6
        apple: 50.00%, banana: 33.33%, mango: 16.67%
        """
        w1 = WorkerNode(master_host="127.0.0.1", master_port=self.tcp_port, worker_id="freq-worker-1", heartbeat_interval=0.5)
        w2 = WorkerNode(master_host="127.0.0.1", master_port=self.tcp_port, worker_id="freq-worker-2", heartbeat_interval=0.5)
        threading.Thread(target=w1.connect, daemon=True).start()
        threading.Thread(target=w2.connect, daemon=True).start()
        time.sleep(0.4)

        try:
            prompt_input = "apple apple apple banana banana mango"
            res = self.client.post('/api/submit-job', json={"input_text": prompt_input, "num_mappers": 2})
            self.assertEqual(res.status_code, 200)
            job_id = res.get_json()["job_id"]

            for _ in range(25):
                time.sleep(0.2)
                if self.master.job_completed and self.master.current_job_id == job_id:
                    break
            self.assertTrue(self.master.job_completed)

            # Retrieve results
            res_job = self.client.get(f'/api/results/{job_id}')
            self.assertEqual(res_job.status_code, 200)
            data = res_job.get_json()
            self.assertTrue(data["available"])
            self.assertEqual(data["total_words"], 6)
            self.assertEqual(data["unique_words"], 3)

            results_by_word = {item["word"]: item for item in data["results"]}
            self.assertIn("apple", results_by_word)
            self.assertIn("banana", results_by_word)
            self.assertIn("mango", results_by_word)

            # Verify counts
            self.assertEqual(results_by_word["apple"]["count"], 3)
            self.assertEqual(results_by_word["banana"]["count"], 2)
            self.assertEqual(results_by_word["mango"]["count"], 1)

            # Verify relative frequencies: (count / total_words) * 100
            # apple: (3 / 6) * 100 = 50.00%
            self.assertAlmostEqual(results_by_word["apple"]["relative_frequency"], 50.00, places=2)
            # banana: (2 / 6) * 100 = 33.33%
            self.assertAlmostEqual(results_by_word["banana"]["relative_frequency"], 33.33, places=2)
            # mango: (1 / 6) * 100 = 16.67%
            self.assertAlmostEqual(results_by_word["mango"]["relative_frequency"], 16.67, places=2)

        finally:
            w1.stop()
            w2.stop()

    def test_relative_frequency_empty_results_safe(self):
        """Verify that empty results or missing job IDs are handled safely without division by zero."""
        # Non-existent job
        res_missing = self.client.get('/api/results/non_existent_job_12345')
        self.assertEqual(res_missing.status_code, 200)
        data_missing = res_missing.get_json()
        self.assertFalse(data_missing["available"])

        # No job submitted yet (initial state)
        res_initial = self.client.get('/api/results')
        self.assertEqual(res_initial.status_code, 200)
        data_initial = res_initial.get_json()
        # Should return safely without error
        self.assertIn("available", data_initial)


    def test_job_metadata_input_size_map_tasks_duration(self):
        """
        Verify that input size (in UTF-8 bytes), map task count, and elapsed duration
        are accurately recorded and returned for a completed job.
        """
        w1 = WorkerNode(master_host="127.0.0.1", master_port=self.tcp_port, worker_id="meta-worker-1", heartbeat_interval=0.5)
        threading.Thread(target=w1.connect, daemon=True).start()
        time.sleep(0.4)

        try:
            custom_text = "distributed system mapreduce cluster parallel computation"
            expected_bytes = len(custom_text.encode('utf-8'))
            configured_mappers = 3

            res = self.client.post('/api/submit-job', json={"input_text": custom_text, "num_mappers": configured_mappers})
            self.assertEqual(res.status_code, 200)
            job_id = res.get_json()["job_id"]

            for _ in range(25):
                time.sleep(0.2)
                if self.master.job_completed and self.master.current_job_id == job_id:
                    break
            self.assertTrue(self.master.job_completed)

            # Query results for this specific job
            res_job = self.client.get(f'/api/results/{job_id}')
            self.assertEqual(res_job.status_code, 200)
            data = res_job.get_json()

            self.assertTrue(data["available"])
            self.assertEqual(data["job_id"], job_id)
            self.assertEqual(data["status"], "COMPLETED")

            # 1. Input size
            self.assertEqual(data["input_size_bytes"], expected_bytes)
            self.assertGreater(data["input_size_bytes"], 0)

            # 2. Map tasks
            self.assertEqual(data["num_map_tasks"], configured_mappers)

            # 3. Job duration
            self.assertIsNotNone(data["duration_seconds"])
            self.assertGreater(data["duration_seconds"], 0.0)

            # Query latest results endpoint without job_id
            res_latest = self.client.get('/api/results')
            self.assertEqual(res_latest.status_code, 200)
            latest_data = res_latest.get_json()
            self.assertEqual(latest_data["input_size_bytes"], expected_bytes)
            self.assertEqual(latest_data["num_map_tasks"], configured_mappers)
            self.assertGreater(latest_data["duration_seconds"], 0.0)

        finally:
            w1.stop()

    def test_job_specific_metadata_isolation(self):
        """
        Verify that multiple jobs retain their own distinct input size, task count,
        and duration metadata, and viewing older jobs returns each job's own values.
        """
        w1 = WorkerNode(master_host="127.0.0.1", master_port=self.tcp_port, worker_id="iso-worker-1", heartbeat_interval=0.5)
        threading.Thread(target=w1.connect, daemon=True).start()
        time.sleep(0.4)

        try:
            # Job 1: Short text, 2 tasks
            text1 = "cat dog"
            bytes1 = len(text1.encode('utf-8'))
            res1 = self.client.post('/api/submit-job', json={"input_text": text1, "num_mappers": 2})
            self.assertEqual(res1.status_code, 200)
            job1_id = res1.get_json()["job_id"]

            for _ in range(25):
                time.sleep(0.2)
                if self.master.job_completed and self.master.current_job_id == job1_id:
                    break
            self.assertTrue(self.master.job_completed)

            # Job 2: Longer text, 4 tasks
            text2 = "one two three four five six seven eight nine ten"
            bytes2 = len(text2.encode('utf-8'))
            res2 = self.client.post('/api/submit-job', json={"input_text": text2, "num_mappers": 4})
            self.assertEqual(res2.status_code, 200)
            job2_id = res2.get_json()["job_id"]

            for _ in range(25):
                time.sleep(0.2)
                if self.master.job_completed and self.master.current_job_id == job2_id:
                    break
            self.assertTrue(self.master.job_completed)

            # Retrieve Job 1: Must show Job 1's own metadata
            res_j1 = self.client.get(f'/api/results/{job1_id}')
            data1 = res_j1.get_json()
            self.assertEqual(data1["job_id"], job1_id)
            self.assertEqual(data1["input_size_bytes"], bytes1)
            self.assertEqual(data1["num_map_tasks"], 2)
            self.assertGreater(data1["duration_seconds"], 0.0)

            # Retrieve Job 2: Must show Job 2's own metadata
            res_j2 = self.client.get(f'/api/results/{job2_id}')
            data2 = res_j2.get_json()
            self.assertEqual(data2["job_id"], job2_id)
            self.assertEqual(data2["input_size_bytes"], bytes2)
            self.assertEqual(data2["num_map_tasks"], 4)
            self.assertGreater(data2["duration_seconds"], 0.0)

            # Confirm isolation
            self.assertNotEqual(data1["input_size_bytes"], data2["input_size_bytes"])
            self.assertNotEqual(data1["num_map_tasks"], data2["num_map_tasks"])

        finally:
            w1.stop()

    def test_cluster_visualization_metrics_and_worker_task_counts(self):
        """
        Verify that cluster map metrics (master host, port, total completed tasks)
        and worker completed task counts are tracked accurately and exposed via /api/summary.
        """
        w1 = WorkerNode(master_host="127.0.0.1", master_port=self.tcp_port, worker_id="vis-worker-1", heartbeat_interval=0.5)
        w2 = WorkerNode(master_host="127.0.0.1", master_port=self.tcp_port, worker_id="vis-worker-2", heartbeat_interval=0.5)
        t1 = threading.Thread(target=w1.connect, daemon=True)
        t2 = threading.Thread(target=w2.connect, daemon=True)
        t1.start()
        t2.start()
        time.sleep(0.4)

        try:
            # Query summary before job
            sum_res_before = self.client.get('/api/summary')
            self.assertEqual(sum_res_before.status_code, 200)
            data_before = sum_res_before.get_json()
            self.assertEqual(len(data_before["workers"]), 2)
            self.assertEqual(data_before["status"]["master_port"], self.tcp_port)
            self.assertEqual(data_before["status"]["total_completed_tasks"], 0)

            # Submit job with 2 tasks
            res = self.client.post('/api/submit-job', json={
                "input_text": "distributed map reduce cluster topology visualization",
                "num_mappers": 2
            })
            self.assertEqual(res.status_code, 200)
            jid = res.get_json()["job_id"]

            for _ in range(25):
                time.sleep(0.2)
                if self.master.job_completed and self.master.current_job_id == jid:
                    break
            self.assertTrue(self.master.job_completed)

            # Query summary after job
            sum_res = self.client.get('/api/summary')
            self.assertEqual(sum_res.status_code, 200)
            data_after = sum_res.get_json()

            # Verify master status metrics
            status = data_after["status"]
            self.assertEqual(status["master_host"], "127.0.0.1")
            self.assertEqual(status["master_port"], self.tcp_port)
            self.assertGreaterEqual(status["total_completed_tasks"], 2)

            # Verify phase timestamps
            pts = status.get("phase_timestamps")
            self.assertIsNotNone(pts)
            self.assertIn("submitted", pts)
            self.assertIn("map_start", pts)
            self.assertIn("map_end", pts)
            self.assertIn("complete", pts)
            self.assertGreaterEqual(pts["complete"], pts["submitted"])

            # Verify worker completed tasks
            workers = data_after["workers"]
            self.assertEqual(len(workers), 2)
            total_worker_completed = sum(w.get("completed_tasks", 0) for w in workers)
            self.assertEqual(total_worker_completed, status["total_completed_tasks"])
            self.assertGreaterEqual(total_worker_completed, 2)

        finally:
            w1.stop()
            w2.stop()


if __name__ == "__main__":
    unittest.main()

