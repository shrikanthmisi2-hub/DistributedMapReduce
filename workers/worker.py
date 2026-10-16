"""
Worker node entry point.
Connects to the master node, receives tasks, and executes either map or reduce operations.
"""
import socket
import logging
import argparse
import uuid
import time
import sys
import os
import threading

# Add the parent directory to sys.path so we can import common and workers modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import send_message, MessageReader, ProtocolException
from workers.mapper import map_function

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Worker")

class WorkerNode:
    def __init__(self, master_host='127.0.0.1', master_port=5000, worker_id=None, heartbeat_interval=1.0, task_delay=0.0, fault_inject_task=None, fault_inject_error=None):
        self.master_host = master_host
        self.master_port = master_port
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:6]}"
        self.heartbeat_interval = heartbeat_interval
        self.task_delay = task_delay
        self.fault_inject_task = fault_inject_task
        self.fault_inject_error = fault_inject_error or "Simulated mapper execution failure: division by zero"
        self._fault_injected = False
        self.sock = None
        self.running = False
        self.current_task = None
        self.status = "IDLE"
        self.send_lock = threading.Lock()
        self.heartbeat_thread = None

    def set_fault_injection(self, task_id, error="Simulated mapper execution failure: division by zero"):
        """
        Configures a one-time controlled fault injection for a specific task_id.
        Fails exactly once, then automatically clears itself.
        """
        self.fault_inject_task = task_id
        self.fault_inject_error = error
        self._fault_injected = False

    def clear_fault_injection(self):
        """Clears/reverses fault injection."""
        self.fault_inject_task = None
        self.fault_inject_error = None
        self._fault_injected = False

    def send(self, msg_dict):
        """
        Thread-safe message sender over the TCP socket.
        """
        with self.send_lock:
            if not self.sock:
                raise ProtocolException("Socket is not connected")
            send_message(self.sock, msg_dict)

    def start_heartbeat(self):
        """
        Starts the background thread to send periodic heartbeats to the master.
        """
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def _heartbeat_loop(self):
        while self.running:
            time.sleep(self.heartbeat_interval)
            if not self.running or not self.sock:
                break
            try:
                self.send({
                    "type": "HEARTBEAT",
                    "worker_id": self.worker_id,
                    "timestamp": time.time(),
                    "current_task": self.current_task,
                    "status": self.status
                })
            except Exception as e:
                if self.running:
                    logger.debug(f"Heartbeat send failed: {e}")
                break

    def connect(self, retry_forever=True):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while True:
            try:
                self.sock.connect((self.master_host, self.master_port))
                logger.info(f"Connected to master at {self.master_host}:{self.master_port}")
                break
            except ConnectionRefusedError:
                if not retry_forever:
                    raise
                logger.warning("Master not available, retrying in 2 seconds...")
                time.sleep(2)
        
        self.running = True
        
        try:
            # Register with master
            register_msg = {"type": "REGISTER", "worker_id": self.worker_id}
            self.send(register_msg)
            
            reader = MessageReader(self.sock)
            ack = reader.read_message()
            if ack and ack.get("type") == "REGISTER_ACK":
                logger.info(f"Registration successful for {self.worker_id}. Waiting for tasks...")
            else:
                logger.error("Failed to register with master.")
                return

            # Start heartbeat sender
            self.start_heartbeat()

            # Listen for tasks
            while self.running:
                msg = reader.read_message()
                if not msg:
                    logger.info("Master disconnected.")
                    break
                
                msg_type = msg.get("type")
                if msg_type == "MAP_TASK":
                    task_id = msg.get("task_id")
                    data = msg.get("data", "")
                    logger.info(f"Received MAP_TASK {task_id}")
                    self.current_task = task_id
                    self.status = "BUSY"
                    
                    try:
                        # Controlled reversible fault injection
                        if self.fault_inject_task and task_id == self.fault_inject_task and not self._fault_injected:
                            self._fault_injected = True
                            err_msg = self.fault_inject_error
                            self.fault_inject_task = None
                            logger.warning(f"Controlled fault injection triggered for task {task_id}: {err_msg}")
                            raise RuntimeError(err_msg)

                        if self.task_delay and self.task_delay > 0:
                            time.sleep(self.task_delay)

                        # Process task using existing map_function line by line
                        results = []
                        for line in data.split("\n"):
                            results.extend(map_function(line))
                        
                        # Send result back
                        result_msg = {
                            "type": "MAP_RESULT",
                            "task_id": task_id,
                            "result": results
                        }
                        self.send(result_msg)
                        logger.info(f"Sent MAP_RESULT for {task_id}")
                    except Exception as e:
                        logger.error(f"Error processing task {task_id}: {e}")
                        try:
                            self.send({
                                "type": "TASK_FAILED",
                                "task_id": task_id,
                                "error": str(e)
                            })
                        except Exception:
                            pass
                    finally:
                        self.current_task = None
                        self.status = "IDLE"
                elif msg_type == "HEARTBEAT_ACK":
                    # Heartbeat acknowledged
                    pass
                else:
                    logger.warning(f"Unknown task/message type: {msg_type}")

        except ProtocolException as e:
            if self.running:
                logger.error(f"Protocol error: {e}")
        except KeyboardInterrupt:
            logger.info("Worker shutting down.")
        finally:
            self.stop()

    def stop(self):
        self.running = False
        self.current_task = None
        self.status = "IDLE"
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
        logger.info(f"Worker {self.worker_id} stopped.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MapReduce Worker")
    parser.add_argument("--master-host", default="127.0.0.1", help="Master host to connect to")
    parser.add_argument("--master-port", type=int, default=5000, help="Master port to connect to")
    parser.add_argument("--worker-id", default=None, help="Optional worker ID")
    parser.add_argument("--heartbeat-interval", type=float, default=1.0, help="Heartbeat interval in seconds")
    parser.add_argument("--task-delay", type=float, default=0.0, help="Optional artificial task execution delay in seconds")
    parser.add_argument("--fault-task", default=None, help="Task ID for one-time controlled fault injection")
    parser.add_argument("--fault-error", default=None, help="Error message for controlled fault injection")
    args = parser.parse_args()
    
    worker = WorkerNode(
        args.master_host,
        args.master_port,
        worker_id=args.worker_id,
        heartbeat_interval=args.heartbeat_interval,
        task_delay=args.task_delay,
        fault_inject_task=args.fault_task,
        fault_inject_error=args.fault_error
    )
    worker.connect()
