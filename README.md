# Distributed MapReduce System

A college project for the Distributed Systems subject with built-in fault tolerance, worker failure detection, automatic task reassignment, and a real-time web dashboard.

## Project Structure
- `master/`: Contains the master node logic (`master.py`) and fault-tolerant task scheduler (`scheduler.py`).
- `workers/`: Contains the worker node logic (`worker.py`), mapper (`mapper.py`), and reducer (`reducer.py`).
- `common/`: Contains the shared JSON newline-delimited communication protocol (`protocol.py`).
- `dashboard/`: Flask web application (`app.py`), templates (`index.html`), and static assets (`style.css`, `script.js`).
- `datasets/`: Sample input data (`sample.txt`).
- `output/`: Directory for job output results (`result.txt`).
- `tests/`: Unit and integration tests covering MapReduce, communication protocol, task scheduling, fault tolerance, and the web dashboard.
- `run_local.py`: Driver script to run MapReduce locally without networking.
- `submit_job.py`: CLI script to submit jobs to the running cluster and monitor completion.

---

## Installation Requirements

- **Python 3.10+** (tested up to Python 3.14)
- **Dependencies:** Flask and Werkzeug (listed in `requirements.txt`)

Install dependencies using pip:
```powershell
pip install -r requirements.txt
# or via Python Launcher on Windows:
py -m pip install -r requirements.txt
```

---

## Web Dashboard Features

The dashboard runs on HTTP port `8080` alongside the TCP master server (port `5000`):

1. **Job Status Cards:** Instant visibility into overall job state (`IDLE`, `MAPPING`, `SHUFFLE & REDUCE`, `COMPLETED`, `FAILED`), task completion percentage, queue counts, and worker pool availability.
2. **Visual Task Progress Bar & Pipeline Stepper:** Real-time multi-stage pipeline tracking (`Workers Ready` &rarr; `Job Submitted` &rarr; `Map Phase` &rarr; `Shuffle & Reduce` &rarr; `Complete`).
3. **Live Worker Health Table:** Displays worker ID, status badges (`IDLE`, `BUSY`, `UNAVAILABLE`), currently assigned task, heartbeat timing with color indicators, IP address, and uptime.
4. **Task Scheduler Table with Interactive Filters:** Shows every task's ID, state (`pending`, `assigned`, `completed`, `failed`), assigned worker, retry count (`attempts / max`), and failure reasons. Filter tabs allow filtering by state.
5. **Auto-refresh & Connection Monitoring:** Automatically updates every 1.5 seconds via `/api/status`, `/api/workers`, and `/api/tasks`. Features automatic offline detection and error banner recovery when the master is disconnected.
6. **Thread-Safe Master Integration:** The dashboard reads in-memory master state through thread-safe locks (`self.lock` and `scheduler.lock`), never exposes raw sockets or non-serializable objects, and runs without blocking TCP operations.

### Dashboard REST API Endpoints

- `GET /` &mdash; Serves the dashboard HTML interface.
- `GET /api/status` &mdash; Returns high-level job state, completion flags, and aggregate scheduler metrics.
- `GET /api/workers` &mdash; Returns an array of registered workers with health and task metadata.
- `GET /api/tasks` &mdash; Returns an array of scheduled tasks with states, workers, and retry statistics.
- `GET /api/summary` &mdash; Returns all status, worker, and task data in a single JSON payload.

---

## Fault Tolerance & Reliability Features
1. **Worker Status Tracking:** Tracks each worker's status (`IDLE`, `BUSY`, `UNAVAILABLE`) and assigned tasks in real-time.
2. **Heartbeat & Timeout Detection:** Workers continuously send periodic heartbeats. If a worker drops out or becomes unresponsive beyond the configurable timeout, the master flags the worker as `UNAVAILABLE`.
3. **Explicit Task Lifecycle:** Tasks transition through explicit states: `pending` &rarr; `assigned` &rarr; `completed` (or `failed`).
4. **Automatic Task Reassignment:** Any unfinished task from a failed or disconnected worker is automatically requeued for available workers.
5. **Strict Result Validation:** Results are checked for proper list structure and key-value formats before a task is marked completed.
6. **Duplicate & Late Result Protection:** Stale or late results from previously failed workers cannot overwrite newer successful tasks or cause duplicate counts.
7. **Configurable Retry Limits:** Each task tracks retry attempts against a maximum retry limit (`max_retries`). If all retries fail, the job fails gracefully with an explicit failure reason.

---

## Running the Distributed System (Windows PowerShell)

### Normal Execution

1. **Terminal 1 - Start the Master Node:**
   ```powershell
   python master\master.py
   # or with custom dashboard port / disable dashboard:
   # python master\master.py --dashboard-port 8080
   # python master\master.py --no-dashboard
   ```
   *The TCP master starts on port `5000` and the web dashboard starts on `http://127.0.0.1:8080`.*

2. **Open the Web Dashboard:**
   Open your browser and navigate to:
   ```text
   http://localhost:8080
   ```
   You will see the cluster dashboard with the live "Master Online" status pill and an empty worker queue.

3. **Terminal 2 & Terminal 3 - Start Worker Nodes:**
   Open separate PowerShell terminals and launch workers:
   ```powershell
   # In Terminal 2:
   python workers\worker.py --worker-id worker-1

   # In Terminal 3:
   python workers\worker.py --worker-id worker-2
   ```
   *Watch the dashboard update immediately: `worker-1` and `worker-2` appear in the Active Workers table with green `IDLE` status badges and active heartbeat indicators.*

4. **Terminal 4 - Submit a Job and View Progress:**
   In another terminal, submit the job:
   ```powershell
   python submit_job.py --wait
   ```
   *Watch the dashboard live:*
   - The Job State badge changes to `MAPPING`.
   - The progress bar advances as tasks complete.
   - Worker statuses switch to `BUSY` while processing and return to `IDLE` when finished.
   - The pipeline stepper advances to `Shuffle & Reduce` and finally `Complete`.
   - The final output is saved to `output\result.txt`.

---

## Demonstrating Worker Failure & Recovery on the Dashboard

Follow these steps to observe real-time worker failure detection and task reassignment:

1. **Terminal 1 - Start Master with a Short Timeout:**
   ```powershell
   python master\master.py --worker-timeout 3.0 --max-retries 3
   ```

2. **Open Dashboard:**
   Navigate to `http://localhost:8080`.

3. **Terminal 2 - Start Worker 1 (The Worker That Will Fail):**
   ```powershell
   python workers\worker.py --worker-id worker-fail-1
   ```

4. **Terminal 3 - Start Worker 2 (Healthy Worker):**
   ```powershell
   python workers\worker.py --worker-id worker-healthy-2
   ```

5. **Terminal 4 - Submit the Job with `--wait`:**
   ```powershell
   python submit_job.py --wait
   ```

6. **Trigger Worker Failure:**
   Immediately switch to **Terminal 2** (running `worker-fail-1`) and press `Ctrl+C` to terminate the worker process while tasks are in flight.

7. **Observe Recovery on the Dashboard:**
   - `worker-fail-1` status immediately switches to `UNAVAILABLE` in the Registered Workers table.
   - The unfinished task retry counter increments (e.g., `1 / 3`).
   - The task is reassigned to `worker-healthy-2`.
   - `worker-healthy-2` finishes the task, the progress bar reaches 100%, and the job completes successfully.

---

## Running Locally without Networking
To test the MapReduce logic sequentially in a single process:
```powershell
python run_local.py
```

## Running Unit, Integration, and Dashboard Tests
To run the complete test suite (25 tests covering MapReduce, TCP protocol, task scheduling, worker timeouts, failure recovery, and dashboard REST APIs):
```powershell
python -m unittest discover -s tests
# or:
py -m unittest discover -s tests
```
