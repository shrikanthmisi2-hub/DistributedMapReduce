"""
Master node for the Distributed MapReduce System.
Responsible for accepting jobs, managing workers, and coordinating the overall MapReduce workflow.
"""
import socket
import threading
import logging
import argparse
import time
import sys
import os
import uuid
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from common.protocol import send_message, MessageReader, ProtocolException
try:
    from master.scheduler import TaskScheduler, TaskState, validate_map_result
except ImportError:
    from scheduler import TaskScheduler, TaskState, validate_map_result
from workers.reducer import reduce_function

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Master")

class WorkerStatus:
    IDLE = "IDLE"
    BUSY = "BUSY"
    UNAVAILABLE = "UNAVAILABLE"

class MasterServer:
    def __init__(self, host='127.0.0.1', port=5000, worker_timeout=4.0, max_retries=3, dashboard_port=8080, enable_dashboard=False):
        self.host = host
        self.port = port
        self.worker_timeout = worker_timeout
        self.max_retries = max_retries
        self.dashboard_port = dashboard_port
        self.enable_dashboard = enable_dashboard
        self.dashboard_server = None
        self.dashboard_thread = None
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.workers = {} # map worker_id -> socket (active workers only)
        self.worker_info = {} # map worker_id -> metadata dict
        self.scheduler = TaskScheduler(max_retries=self.max_retries)
        self.task_results = {} # map task_id -> list of (key, value) pairs
        self.map_results = [] # aggregated list of (key, value) pairs
        self.lock = threading.Lock()
        self.running = False
        self.job_in_progress = False
        self.job_completed = False
        self.job_failed = False
        self.job_error = None
        self.current_job_id = None
        self.current_stage = "IDLE"
        self.start_time = None
        self.end_time = None
        self.duration_seconds = None
        self.job_history = []
        self.latest_results = None
        self.phase_timestamps = {}

    def start(self):
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        logger.info(f"Master server started on {self.host}:{self.port} (timeout={self.worker_timeout}s, max_retries={self.max_retries})")
        
        # Start web dashboard if enabled
        if self.enable_dashboard and self.dashboard_port:
            self.start_dashboard()

        # Start a thread to continuously assign pending tasks to idle workers
        threading.Thread(target=self.assign_tasks_loop, daemon=True).start()

        # Start a thread to detect worker heartbeat timeouts
        threading.Thread(target=self.monitor_worker_timeouts_loop, daemon=True).start()

        try:
            while self.running:
                client_sock, addr = self.server_socket.accept()
                logger.info(f"New TCP connection from {addr}")
                threading.Thread(target=self.handle_client, args=(client_sock, addr), daemon=True).start()
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            if self.running:
                logger.error(f"Server accept error: {e}")
    
    def stop(self):
        self.running = False
        if self.dashboard_server:
            try:
                self.dashboard_server.shutdown()
            except Exception:
                pass
            self.dashboard_server = None
        try:
            self.server_socket.close()
        except Exception:
            pass
        with self.lock:
            for wid, sock in list(self.workers.items()):
                try:
                    sock.close()
                except Exception:
                    pass
            self.workers.clear()
        logger.info("Master server stopped.")

    def start_dashboard(self):
        """
        Starts the Flask web dashboard on a background daemon thread.
        Uses werkzeug make_server for safe shutdown.
        """
        if not self.enable_dashboard or not self.dashboard_port:
            return

        try:
            from dashboard.app import create_dashboard_app
            from werkzeug.serving import make_server

            logging.getLogger('werkzeug').setLevel(logging.WARNING)

            app = create_dashboard_app(master_server=self)
            self.dashboard_server = make_server(self.host, self.dashboard_port, app)
            self.dashboard_thread = threading.Thread(target=self.dashboard_server.serve_forever, daemon=True)
            self.dashboard_thread.start()
            logger.info(f"Dashboard web server started on http://{self.host}:{self.dashboard_port}")
        except Exception as e:
            logger.error(f"Failed to start dashboard server on port {self.dashboard_port}: {e}")

    def get_dashboard_data(self):
        """
        Thread-safe, read-only snapshot of master, worker, and task state for the dashboard.
        Returns only JSON-serializable data. Socket and internal lock objects are never exposed.
        """
        now = time.time()
        
        # 1. Snapshot scheduler assigned tasks
        assigned_tasks_snapshot = {}
        if hasattr(self, 'scheduler') and self.scheduler:
            with getattr(self.scheduler, 'lock', threading.RLock()):
                for wid, t in self.scheduler.assigned_tasks.items():
                    assigned_tasks_snapshot[wid] = t.task_id if hasattr(t, 'task_id') else t.get('task_id')

        # 2. Snapshot workers under self.lock
        workers_list = []
        total_cluster_completed_tasks = 0
        with self.lock:
            for wid, info in self.worker_info.items():
                last_hb = info.get("last_heartbeat")
                connected_at = info.get("connected_at")
                addr = info.get("addr")
                if isinstance(addr, (list, tuple)) and len(addr) >= 2:
                    addr_str = f"{addr[0]}:{addr[1]}"
                elif addr:
                    addr_str = str(addr)
                else:
                    addr_str = "unknown"

                comp_tasks = info.get("completed_tasks", 0)
                total_cluster_completed_tasks += comp_tasks

                status = info.get("status", WorkerStatus.UNAVAILABLE)
                if status != WorkerStatus.UNAVAILABLE:
                    cur_task = assigned_tasks_snapshot.get(wid)
                    if cur_task:
                        status = WorkerStatus.BUSY
                        info["current_task"] = cur_task
                        info["status"] = WorkerStatus.BUSY
                    else:
                        status = WorkerStatus.IDLE
                        info["current_task"] = None
                        info["status"] = WorkerStatus.IDLE
                else:
                    cur_task = None

                workers_list.append({
                    "worker_id": wid,
                    "status": status,
                    "current_task": cur_task,
                    "completed_tasks": comp_tasks,
                    "last_heartbeat": last_hb,
                    "seconds_since_heartbeat": round(now - last_hb, 1) if last_hb else None,
                    "connected_at": connected_at,
                    "uptime_seconds": round(now - connected_at, 1) if connected_at else None,
                    "address": addr_str
                })

        with self.lock:
            job_in_progress = self.job_in_progress
            job_completed = self.job_completed
            job_failed = self.job_failed
            job_error = self.job_error
            running = self.running
            current_job_id = self.current_job_id
            current_stage = self.current_stage
            start_time = self.start_time
            duration = self.duration_seconds or (round(now - start_time, 2) if (start_time and job_in_progress) else None)
            total_jobs = len(self.job_history)
            job_history_copy = [dict(j) for j in self.job_history]
            latest_results_copy = dict(self.latest_results) if self.latest_results else None
            phase_ts_copy = dict(self.phase_timestamps) if self.phase_timestamps else None

        workers_list.sort(key=lambda w: str(w["worker_id"]))

        # 2. Snapshot tasks and stats via scheduler thread-safe methods
        stats = self.scheduler.get_stats()
        tasks_list = self.scheduler.get_all_tasks()

        # 3. Overall job state
        job_status = {
            "job_id": current_job_id,
            "current_stage": current_stage,
            "job_in_progress": job_in_progress,
            "job_completed": job_completed,
            "job_failed": job_failed,
            "job_error": job_error,
            "duration_seconds": duration,
            "total_jobs_submitted": total_jobs,
            "server_running": running,
            "master_host": self.host,
            "master_port": self.port,
            "total_completed_tasks": total_cluster_completed_tasks,
            "phase_timestamps": phase_ts_copy,
            "stats": stats
        }

        return {
            "status": job_status,
            "workers": workers_list,
            "tasks": tasks_list,
            "current_job_id": current_job_id,
            "current_stage": current_stage,
            "job_history": job_history_copy,
            "latest_results": latest_results_copy
        }

    def get_job_results(self, job_id=None):
        with self.lock:
            if job_id:
                for job in self.job_history:
                    if job.get("job_id") == job_id:
                        return dict(job)
                return None
            if self.latest_results:
                res_copy = dict(self.latest_results)
                for job in self.job_history:
                    if job.get("job_id") == res_copy.get("job_id"):
                        for k in ["input_size_bytes", "num_map_tasks", "duration_seconds", "submission_time"]:
                            if (res_copy.get(k) is None or res_copy.get(k) == 0) and job.get(k) is not None:
                                res_copy[k] = job.get(k)
                        break
                return res_copy
            for job in self.job_history:
                if job.get("status") == "COMPLETED" and "results" in job:
                    return dict(job)
            return None

    def get_job_history(self):
        with self.lock:
            return [dict(j) for j in self.job_history]

    def register_worker(self, worker_id, client_sock, addr):
        with self.lock:
            existing_completed = self.worker_info.get(worker_id, {}).get("completed_tasks", 0)
            self.workers[worker_id] = client_sock
            self.worker_info[worker_id] = {
                "worker_id": worker_id,
                "socket": client_sock,
                "addr": addr,
                "status": WorkerStatus.IDLE,
                "current_task": None,
                "completed_tasks": existing_completed,
                "last_heartbeat": time.time(),
                "connected_at": time.time()
            }
        logger.info(f"Worker {worker_id} registered successfully from {addr}")

    def mark_worker_unavailable(self, worker_id, reason="Worker unavailable"):
        sock = None
        with self.lock:
            if worker_id not in self.worker_info:
                return
            info = self.worker_info[worker_id]
            if info["status"] == WorkerStatus.UNAVAILABLE:
                return # Already handled
            info["status"] = WorkerStatus.UNAVAILABLE
            info["current_task"] = None
            sock = info.get("socket")
            if worker_id in self.workers:
                del self.workers[worker_id]

        if sock:
            try:
                sock.close()
            except Exception:
                pass

        logger.warning(f"Worker {worker_id} marked UNAVAILABLE: {reason}")
        reassigned_task = self.scheduler.handle_worker_failure(worker_id, reason=reason)
        if reassigned_task:
            if reassigned_task.state == TaskState.PENDING:
                logger.warning(
                    f"Reassigned unfinished task {reassigned_task.task_id} to pending queue "
                    f"(retry attempt {reassigned_task.retries}/{reassigned_task.max_retries})"
                )
            elif reassigned_task.state == TaskState.FAILED:
                logger.error(
                    f"Task {reassigned_task.task_id} reached maximum retries and FAILED. Failing job."
                )
                self.job_failed = True
                self.job_error = reassigned_task.failure_reason

    def monitor_worker_timeouts_loop(self):
        while self.running:
            time.sleep(0.5)
            now = time.time()
            timed_out_workers = []
            with self.lock:
                for wid, info in self.worker_info.items():
                    if info["status"] != WorkerStatus.UNAVAILABLE:
                        elapsed = now - info["last_heartbeat"]
                        if elapsed > self.worker_timeout:
                            timed_out_workers.append((wid, elapsed))

            for wid, elapsed in timed_out_workers:
                logger.warning(f"Worker {wid} heartbeat timeout ({elapsed:.1f}s > {self.worker_timeout}s)")
                self.mark_worker_unavailable(wid, reason=f"Heartbeat timeout ({elapsed:.1f}s)")

    def handle_client(self, client_sock, addr):
        reader = MessageReader(client_sock)
        worker_id = None
        try:
            msg = reader.read_message()
            if not msg:
                return

            msg_type = msg.get("type")
            
            if msg_type == "REGISTER":
                worker_id = msg.get("worker_id", str(addr))
                self.register_worker(worker_id, client_sock, addr)
                send_message(client_sock, {"type": "REGISTER_ACK", "status": "OK"})
                self.handle_worker_loop(client_sock, reader, worker_id)
                
            elif msg_type == "SUBMIT_JOB":
                input_file = msg.get("input_file")
                output_file = msg.get("output_file")
                job_id = msg.get("job_id")
                num_map_tasks = msg.get("num_map_tasks") or msg.get("num_mappers")
                input_text = msg.get("input_text")
                input_size_bytes = msg.get("input_size_bytes")
                if input_text is not None and not input_file:
                    generated_id = job_id or f"job_{int(time.time())}_{uuid.uuid4().hex[:4]}"
                    os.makedirs("datasets", exist_ok=True)
                    input_file = os.path.join("datasets", f"{generated_id}_input.txt")
                    encoded_bytes = input_text.encode('utf-8')
                    with open(input_file, "wb") as f:
                        f.write(encoded_bytes)
                    if not output_file:
                        os.makedirs("output", exist_ok=True)
                        output_file = os.path.join("output", f"{generated_id}_result.txt")
                    job_id = generated_id
                    input_size_bytes = len(encoded_bytes)
                elif not output_file and input_file:
                    output_file = os.path.join("output", "result.txt")
                self.start_job(input_file, output_file, client_sock, job_id=job_id, num_map_tasks=num_map_tasks, input_size_bytes=input_size_bytes)

            elif msg_type == "GET_JOB_STATUS":
                status_response = {
                    "type": "JOB_STATUS_RESPONSE",
                    "job_id": self.current_job_id,
                    "current_stage": self.current_stage,
                    "in_progress": self.job_in_progress,
                    "is_completed": self.job_completed,
                    "is_failed": self.job_failed,
                    "error": self.job_error,
                    "stats": self.scheduler.get_stats()
                }
                send_message(client_sock, status_response)

            elif msg_type == "GET_JOB_RESULTS":
                req_job_id = msg.get("job_id")
                send_message(client_sock, {
                    "type": "JOB_RESULTS_RESPONSE",
                    "results": self.get_job_results(req_job_id)
                })

            elif msg_type == "GET_JOB_HISTORY":
                send_message(client_sock, {
                    "type": "JOB_HISTORY_RESPONSE",
                    "history": self.get_job_history()
                })

            elif msg_type == "GET_DASHBOARD_DATA":
                send_message(client_sock, {
                    "type": "DASHBOARD_DATA_RESPONSE",
                    "data": self.get_dashboard_data()
                })
                
            else:
                logger.warning(f"Unexpected initial message from {addr}: {msg}")
                
        except ProtocolException as e:
            logger.error(f"Protocol error with {addr}: {e}")
        finally:
            if worker_id:
                self.mark_worker_unavailable(worker_id, reason="Worker connection closed")
            client_sock.close()

    def handle_worker_loop(self, client_sock, reader, worker_id):
        while self.running:
            try:
                msg = reader.read_message()
                if not msg:
                    logger.info(f"Worker {worker_id} disconnected (EOF).")
                    break

                msg_type = msg.get("type")

                if msg_type == "HEARTBEAT":
                    hb_task = msg.get("current_task")
                    with self.lock:
                        if worker_id in self.worker_info:
                            self.worker_info[worker_id]["last_heartbeat"] = time.time()
                            if hb_task:
                                self.worker_info[worker_id]["current_task"] = hb_task
                                self.worker_info[worker_id]["status"] = WorkerStatus.BUSY
                    try:
                        send_message(client_sock, {"type": "HEARTBEAT_ACK", "status": "OK"})
                    except Exception:
                        pass

                elif msg_type == "MAP_RESULT":
                    task_id = msg.get("task_id")
                    result = msg.get("result", [])
                    logger.info(f"Received MAP_RESULT for {task_id} from {worker_id}")
                    self.handle_map_result(worker_id, task_id, result)

                elif msg_type == "TASK_FAILED":
                    task_id = msg.get("task_id")
                    err = msg.get("error", "Unknown error")
                    logger.warning(f"Worker {worker_id} reported failure for task {task_id}: {err}")
                    perm_failed = self.scheduler.fail_task(task_id, worker_id=worker_id, reason=err)
                    if perm_failed:
                        with self.lock:
                            self.job_failed = True
                            self.job_error = self.scheduler.get_failure_reason() or err
                    with self.lock:
                        if worker_id in self.worker_info:
                            self.worker_info[worker_id]["status"] = WorkerStatus.IDLE
                            self.worker_info[worker_id]["current_task"] = None

                else:
                    logger.warning(f"Unknown message type from {worker_id}: {msg_type}")

            except ProtocolException as e:
                logger.error(f"Worker {worker_id} protocol error: {e}")
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} socket error: {e}")
                break

    def handle_map_result(self, worker_id, task_id, result):
        # Validate result format
        is_valid, val_err = validate_map_result(result)
        if not is_valid:
            logger.error(f"Invalid MAP_RESULT received for {task_id} from {worker_id}: {val_err}")
            perm_failed = self.scheduler.fail_task(task_id, worker_id=worker_id, reason=f"Invalid result: {val_err}")
            if perm_failed:
                with self.lock:
                    self.job_failed = True
                    self.job_error = self.scheduler.get_failure_reason() or f"Invalid result: {val_err}"
            with self.lock:
                if worker_id in self.worker_info:
                    self.worker_info[worker_id]["status"] = WorkerStatus.IDLE
                    self.worker_info[worker_id]["current_task"] = None
            return

        success, res = self.scheduler.complete_task(task_id, worker_id, result=result, validate=True)
        if not success:
            logger.warning(f"MAP_RESULT rejected for {task_id} from {worker_id}: {res}")
            # If the task is no longer assigned to this worker, make sure worker is not marked stuck
            with self.lock:
                if worker_id in self.worker_info and self.worker_info[worker_id]["status"] == WorkerStatus.BUSY:
                    if self.worker_info[worker_id]["current_task"] == task_id:
                        self.worker_info[worker_id]["status"] = WorkerStatus.IDLE
                        self.worker_info[worker_id]["current_task"] = None
            return

        # Store validated result uniquely by task_id to guarantee idempotency
        with self.lock:
            self.task_results[task_id] = result
            # Update flat map_results for compatibility
            self.map_results = []
            for tid, pairs in self.task_results.items():
                self.map_results.extend(pairs)
            if worker_id in self.worker_info:
                self.worker_info[worker_id]["status"] = WorkerStatus.IDLE
                self.worker_info[worker_id]["current_task"] = None
                self.worker_info[worker_id]["completed_tasks"] = self.worker_info[worker_id].get("completed_tasks", 0) + 1

        logger.info(f"Task {task_id} result validated and saved ({len(result)} pairs). Worker {worker_id} is now IDLE.")
        # Immediately try to assign the next task
        self.assign_task_to_worker(worker_id)

    def start_job(self, input_file, output_file, client_sock=None, job_id=None, num_map_tasks=None, input_size_bytes=None):
        if not job_id:
            job_id = f"job_{int(time.time())}_{uuid.uuid4().hex[:4]}"

        with self.lock:
            if self.job_in_progress:
                if client_sock:
                    try:
                        send_message(client_sock, {"type": "ERROR", "message": "Job already in progress"})
                    except Exception:
                        pass
                return False, "Job already in progress"

            if not os.path.exists(input_file):
                if client_sock:
                    try:
                        send_message(client_sock, {"type": "ERROR", "message": f"File {input_file} not found"})
                    except Exception:
                        pass
                return False, f"File {input_file} not found"

            self.job_in_progress = True
            self.job_completed = False
            self.job_failed = False
            self.job_error = None
            self.current_job_id = job_id
            self.current_stage = "MAPPING"
            self.start_time = time.time()
            self.end_time = None
            self.duration_seconds = None
            self.task_results = {}
            self.map_results = []
            self.scheduler = TaskScheduler(max_retries=self.max_retries)

        logger.info(f"Starting job {job_id}. Input: {input_file}, Output: {output_file}")

        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            if input_size_bytes is None or input_size_bytes <= 0:
                try:
                    if os.path.exists(input_file):
                        input_size_bytes = os.path.getsize(input_file)
                    else:
                        input_size_bytes = len("".join(lines).encode('utf-8'))
                except Exception:
                    input_size_bytes = len("".join(lines).encode('utf-8'))
            tasks = []
            if num_map_tasks is not None and int(num_map_tasks) > 0:
                k = int(num_map_tasks)
                n = len(lines)
                if n == 0:
                    tasks = []
                elif n == 1 and k > 1:
                    words = lines[0].split()
                    if len(words) >= k:
                        words_per_chunk = max(1, (len(words) + k - 1) // k)
                        for i in range(0, len(words), words_per_chunk):
                            chunk_text = " ".join(words[i:i + words_per_chunk])
                            if chunk_text.strip():
                                tasks.append({
                                    "task_id": f"map_{len(tasks)}",
                                    "data": chunk_text,
                                    "max_retries": self.max_retries
                                })
                    else:
                        tasks.append({
                            "task_id": "map_0",
                            "data": lines[0],
                            "max_retries": self.max_retries
                        })
                else:
                    actual_k = min(k, n)
                    chunk_size = max(1, (n + actual_k - 1) // actual_k)
                    for i in range(0, n, chunk_size):
                        chunk_text = "".join(lines[i:i + chunk_size])
                        if chunk_text.strip():
                            tasks.append({
                                "task_id": f"map_{len(tasks)}",
                                "data": chunk_text,
                                "max_retries": self.max_retries
                            })
            else:
                chunk_size = 2
                for i in range(0, len(lines), chunk_size):
                    chunk = lines[i:i + chunk_size]
                    if "".join(chunk).strip():
                        tasks.append({
                            "task_id": f"map_{len(tasks)}",
                            "data": "".join(chunk),
                            "max_retries": self.max_retries
                        })

            logger.info(f"Created {len(tasks)} map tasks for job {job_id}.")
            self.scheduler.add_tasks(tasks, max_retries=self.max_retries)

            now_ts = time.time()
            self.phase_timestamps = {
                "ready": now_ts - 0.05,
                "submitted": now_ts,
                "map_start": now_ts,
                "map_end": None,
                "shuffle_start": None,
                "shuffle_end": None,
                "reduce_start": None,
                "complete": None,
                "failed": None
            }

            job_record = {
                "job_id": job_id,
                "submission_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp": self.start_time,
                "status": "RUNNING",
                "current_stage": "MAPPING",
                "input_file": input_file,
                "output_file": output_file,
                "input_size_bytes": input_size_bytes,
                "num_map_tasks": len(tasks),
                "duration_seconds": None,
                "unique_words": 0,
                "total_words": 0,
                "results": [],
                "error": None,
                "phase_timestamps": dict(self.phase_timestamps)
            }
            with self.lock:
                self.job_history.insert(0, job_record)

            if client_sock:
                try:
                    send_message(client_sock, {
                        "type": "JOB_ACCEPTED",
                        "job_id": job_id,
                        "message": f"Job {job_id} started with {len(tasks)} tasks",
                        "task_count": len(tasks)
                    })
                except Exception as e:
                    logger.warning(f"Failed to send JOB_ACCEPTED: {e}")

            # Start background thread to monitor completion
            threading.Thread(target=self.monitor_job_completion, args=(output_file, job_id), daemon=True).start()
            return True, job_id

        except Exception as e:
            logger.error(f"Error starting job: {e}")
            with self.lock:
                self.job_in_progress = False
                self.job_failed = True
                self.job_error = str(e)
                self.current_stage = "FAILED"
            if client_sock:
                try:
                    send_message(client_sock, {"type": "ERROR", "message": str(e)})
                except Exception:
                    pass
            return False, str(e)

    def assign_tasks_loop(self):
        while self.running:
            time.sleep(0.2)
            if self.job_in_progress:
                with self.lock:
                    available_workers = [
                        wid for wid, info in self.worker_info.items()
                        if info["status"] == WorkerStatus.IDLE and wid in self.workers
                    ]
                for worker_id in available_workers:
                    self.assign_task_to_worker(worker_id)

    def assign_task_to_worker(self, worker_id):
        with self.lock:
            if worker_id not in self.workers:
                return
            if worker_id not in self.worker_info or self.worker_info[worker_id]["status"] != WorkerStatus.IDLE:
                return
            sock = self.workers[worker_id]
        
        task = self.scheduler.get_task(worker_id)
        if task:
            with self.lock:
                if worker_id in self.worker_info:
                    self.worker_info[worker_id]["status"] = WorkerStatus.BUSY
                    self.worker_info[worker_id]["current_task"] = task.task_id

            try:
                msg = {
                    "type": "MAP_TASK",
                    "task_id": task.task_id,
                    "data": task.data
                }
                send_message(sock, msg)
                logger.info(f"Assigned task {task.task_id} (attempt {task.retries + 1}/{task.max_retries}) to worker {worker_id}")
            except (ProtocolException, socket.error) as e:
                logger.error(f"Failed to send task {task.task_id} to worker {worker_id}: {e}")
                self.mark_worker_unavailable(worker_id, reason=f"Send failure: {e}")

    def monitor_job_completion(self, output_file, job_id=None):
        target_job_id = job_id or self.current_job_id
        while self.running and self.job_in_progress:
            time.sleep(0.3)
            if self.scheduler.is_failed():
                err = self.scheduler.get_failure_reason() or "Job failed due to unrecoverable task errors."
                logger.error(f"Job {target_job_id} FAILED: {err}")
                with self.lock:
                    self.job_failed = True
                    self.job_error = err
                    self.job_in_progress = False
                    self.current_stage = "FAILED"
                    self.end_time = time.time()
                    if self.phase_timestamps:
                        self.phase_timestamps["failed"] = self.end_time
                    job_start = None
                    for job in self.job_history:
                        if job.get("job_id") == target_job_id:
                            job_start = job.get("timestamp")
                            break
                    if not job_start:
                        job_start = self.start_time or self.end_time

                    raw_dur = self.end_time - job_start
                    dur = round(raw_dur, 2)
                    if dur <= 0:
                        dur = round(raw_dur, 3)
                    if dur <= 0:
                        dur = 0.01
                    self.duration_seconds = dur

                    for job in self.job_history:
                        if job.get("job_id") == target_job_id:
                            job["status"] = "FAILED"
                            job["current_stage"] = "FAILED"
                            job["duration_seconds"] = self.duration_seconds
                            job["error"] = err
                            if self.phase_timestamps:
                                job["phase_timestamps"] = dict(self.phase_timestamps)
                            break

                    if self.latest_results and self.latest_results.get("job_id") == target_job_id:
                        self.latest_results["status"] = "FAILED"
                        self.latest_results["current_stage"] = "FAILED"
                        self.latest_results["duration_seconds"] = self.duration_seconds
                        if self.phase_timestamps:
                            self.latest_results["phase_timestamps"] = dict(self.phase_timestamps)
                break

            if self.scheduler.is_done():
                logger.info(f"All map tasks completed successfully for {target_job_id}. Starting Shuffle & Reduce...")
                with self.lock:
                    self.current_stage = "SHUFFLE_AND_REDUCE"
                    now_map_end = time.time()
                    if self.phase_timestamps:
                        self.phase_timestamps["map_end"] = now_map_end
                        self.phase_timestamps["shuffle_start"] = now_map_end
                    for job in self.job_history:
                        if job.get("job_id") == target_job_id:
                            job["current_stage"] = "SHUFFLE_AND_REDUCE"
                            if self.phase_timestamps:
                                job["phase_timestamps"] = dict(self.phase_timestamps)
                            break

                self.shuffle_and_reduce(output_file, job_id=target_job_id)
                with self.lock:
                    self.job_in_progress = False
                    self.job_completed = True
                    self.current_stage = "COMPLETED"
                    self.end_time = time.time()
                    if self.phase_timestamps:
                        self.phase_timestamps["reduce_end"] = self.end_time
                        self.phase_timestamps["complete"] = self.end_time
                    job_start = None
                    for job in self.job_history:
                        if job.get("job_id") == target_job_id:
                            job_start = job.get("timestamp")
                            break
                    if not job_start:
                        job_start = self.start_time or self.end_time

                    raw_dur = self.end_time - job_start
                    dur = round(raw_dur, 2)
                    if dur <= 0:
                        dur = round(raw_dur, 3)
                    if dur <= 0:
                        dur = 0.01
                    self.duration_seconds = dur

                    for job in self.job_history:
                        if job.get("job_id") == target_job_id:
                            job["status"] = "COMPLETED"
                            job["current_stage"] = "COMPLETED"
                            job["duration_seconds"] = self.duration_seconds
                            if self.phase_timestamps:
                                job["phase_timestamps"] = dict(self.phase_timestamps)
                            break

                    if self.latest_results and self.latest_results.get("job_id") == target_job_id:
                        self.latest_results["status"] = "COMPLETED"
                        self.latest_results["current_stage"] = "COMPLETED"
                        self.latest_results["duration_seconds"] = self.duration_seconds
                        if self.phase_timestamps:
                            self.latest_results["phase_timestamps"] = dict(self.phase_timestamps)

                logger.info(f"Job {target_job_id} completed successfully in {self.duration_seconds}s! Results written to {output_file}")
                break
                
    def shuffle_and_reduce(self, output_file, job_id=None):
        try:
            target_job_id = job_id or self.current_job_id
            # Reconstruct map results from validated task results
            all_pairs = []
            with self.lock:
                for tid, pairs in self.task_results.items():
                    all_pairs.extend(pairs)
                self.map_results = all_pairs

            # Shuffle
            grouped = defaultdict(list)
            for key, value in all_pairs:
                grouped[key].append(value)

            now_shuffle_end = time.time()
            if self.phase_timestamps:
                self.phase_timestamps["shuffle_end"] = now_shuffle_end
                self.phase_timestamps["reduce_start"] = now_shuffle_end

            # Reduce
            reduced = []
            for key, values in grouped.items():
                reduced.append(reduce_function(key, values))
                
            # Sort
            reduced.sort(key=lambda x: x[0])
            
            # Write Output
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                for key, count in reduced:
                    f.write(f"{key}: {count}\n")

            # Also maintain output/result.txt for backward compatibility
            default_out = os.path.join("output", "result.txt")
            if os.path.abspath(output_file) != os.path.abspath(default_out):
                try:
                    os.makedirs(os.path.dirname(default_out), exist_ok=True)
                    with open(default_out, 'w', encoding='utf-8') as f:
                        for key, count in reduced:
                            f.write(f"{key}: {count}\n")
                except Exception:
                    pass

            unique_words = len(reduced)
            total_words = sum(count for _, count in reduced)
            results_list = [
                {
                    "word": key,
                    "count": count,
                    "relative_frequency": round((count / total_words) * 100, 2) if total_words > 0 else 0.0
                }
                for key, count in reduced
            ]

            with self.lock:
                target_job = None
                for job in self.job_history:
                    if job.get("job_id") == target_job_id:
                        target_job = job
                        break

                input_size = target_job.get("input_size_bytes", 0) if target_job else 0
                num_tasks = target_job.get("num_map_tasks", len(self.task_results)) if target_job else len(self.task_results)
                sub_time = target_job.get("submission_time") if target_job else time.strftime("%Y-%m-%d %H:%M:%S")

                results_payload = {
                    "job_id": target_job_id,
                    "status": "COMPLETED",
                    "current_stage": "COMPLETED",
                    "unique_words": unique_words,
                    "total_words": total_words,
                    "input_size_bytes": input_size,
                    "num_map_tasks": num_tasks,
                    "duration_seconds": self.duration_seconds,
                    "submission_time": sub_time,
                    "results": results_list,
                    "output_file": output_file,
                    "phase_timestamps": dict(self.phase_timestamps) if self.phase_timestamps else None
                }

                self.latest_results = results_payload
                if target_job:
                    target_job["unique_words"] = unique_words
                    target_job["total_words"] = total_words
                    target_job["results"] = results_list

            logger.info(f"Job {target_job_id} complete! {unique_words} unique words written to {output_file}")
        except Exception as e:
            logger.error(f"Error in shuffle & reduce: {e}")
            with self.lock:
                self.job_failed = True
                self.job_error = str(e)
                self.current_stage = "FAILED"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MapReduce Master")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--worker-timeout", type=float, default=4.0, help="Worker timeout in seconds")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retries per task")
    parser.add_argument("--dashboard-port", type=int, default=8080, help="Port for web dashboard (default: 8080)")
    parser.add_argument("--no-dashboard", action="store_true", help="Disable the web dashboard")
    args = parser.parse_args()
    
    enable_dashboard = not args.no_dashboard and (args.dashboard_port > 0)
    master = MasterServer(
        args.host,
        args.port,
        worker_timeout=args.worker_timeout,
        max_retries=args.max_retries,
        dashboard_port=args.dashboard_port,
        enable_dashboard=enable_dashboard
    )
    master.start()

