"""
Flask application for the MapReduce Web Dashboard.
Provides real-time monitoring of job progress, worker statuses, and task execution states.
Supports both embedded in-process MasterServer access and standalone out-of-process TCP querying.
"""
import os
import sys
import socket
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, render_template, jsonify, request
import time
import uuid
from common.protocol import send_message, MessageReader

logger = logging.getLogger("Dashboard")

def fetch_master_data_from_tcp(host='127.0.0.1', port=5000, timeout=1.5):
    """
    Connects to MasterServer over TCP on port 5000 to retrieve live dashboard data
    when the dashboard is running in standalone mode (separate process).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        send_message(sock, {"type": "GET_DASHBOARD_DATA"})
        reader = MessageReader(sock)
        resp = reader.read_message()
        if resp and resp.get("type") == "DASHBOARD_DATA_RESPONSE":
            return resp.get("data")
    except Exception as e:
        logger.debug(f"TCP query to master at {host}:{port} failed: {e}")
        return None
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return None

def submit_job_to_tcp_master(host='127.0.0.1', port=5000, input_file=None, output_file=None, job_id=None, num_map_tasks=None, input_text=None, input_size_bytes=None, timeout=2.5):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        msg = {
            "type": "SUBMIT_JOB",
            "input_file": input_file,
            "output_file": output_file,
            "job_id": job_id,
            "num_map_tasks": num_map_tasks,
            "input_text": input_text,
            "input_size_bytes": input_size_bytes
        }
        send_message(sock, msg)
        reader = MessageReader(sock)
        resp = reader.read_message()
        return resp or {"type": "ERROR", "message": "No response from master"}
    except Exception as e:
        logger.error(f"TCP job submission to {host}:{port} failed: {e}")
        return {"type": "ERROR", "message": f"Connection to master failed: {e}"}
    finally:
        try:
            sock.close()
        except Exception:
            pass

def fetch_job_results_from_tcp(host='127.0.0.1', port=5000, job_id=None, timeout=2.0):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        send_message(sock, {"type": "GET_JOB_RESULTS", "job_id": job_id})
        reader = MessageReader(sock)
        resp = reader.read_message()
        if resp and resp.get("type") == "JOB_RESULTS_RESPONSE":
            return resp.get("results")
    except Exception as e:
        logger.debug(f"TCP query job results failed: {e}")
        return None
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return None

def fetch_job_history_from_tcp(host='127.0.0.1', port=5000, timeout=2.0):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        send_message(sock, {"type": "GET_JOB_HISTORY"})
        reader = MessageReader(sock)
        resp = reader.read_message()
        if resp and resp.get("type") == "JOB_HISTORY_RESPONSE":
            return resp.get("history")
    except Exception as e:
        logger.debug(f"TCP query job history failed: {e}")
        return None
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return None

def create_dashboard_app(master_server=None, master_host='127.0.0.1', master_port=5000):
    """
    Factory to create and configure the Flask dashboard app.
    Takes an optional MasterServer instance to fetch real-time state directly,
    or falls back to TCP communication on master_host:master_port if running standalone.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    app = Flask(
        __name__,
        template_folder=os.path.join(base_dir, 'templates'),
        static_folder=os.path.join(base_dir, 'static')
    )

    # Attach master reference to app config for access in routes
    app.config['MASTER_SERVER'] = master_server
    app.config['MASTER_HOST'] = master_host
    app.config['MASTER_PORT'] = master_port

    def get_live_data():
        """
        Retrieves real-time dashboard data.
        1. If MasterServer instance is provided in-memory, read from it directly.
        2. Otherwise, connect over TCP to the MasterServer on port 5000.
        """
        master = app.config.get('MASTER_SERVER')
        if master:
            try:
                return master.get_dashboard_data()
            except Exception as e:
                logger.error(f"Error fetching data from embedded master: {e}")
                return None

        # Standalone mode: fetch via TCP from MasterServer on configured TCP port
        host = app.config.get('MASTER_HOST', '127.0.0.1')
        port = app.config.get('MASTER_PORT', 5000)
        return fetch_master_data_from_tcp(host=host, port=port)

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/api/status', methods=['GET'])
    def get_status():
        data = get_live_data()
        if data and "status" in data:
            return jsonify(data["status"])

        # When master is unreachable or offline
        return jsonify({
            "job_in_progress": False,
            "job_completed": False,
            "job_failed": False,
            "job_error": None,
            "job_id": None,
            "current_stage": "IDLE",
            "duration_seconds": None,
            "total_jobs_submitted": 0,
            "server_running": False,
            "master_host": app.config.get('MASTER_HOST', '127.0.0.1'),
            "master_port": app.config.get('MASTER_PORT', 5000),
            "total_completed_tasks": 0,
            "phase_timestamps": None,
            "stats": {
                "total": 0,
                "pending": 0,
                "assigned": 0,
                "completed": 0,
                "failed": 0,
                "is_failed": False,
                "is_done": True
            }
        })

    @app.route('/api/workers', methods=['GET'])
    def get_workers():
        data = get_live_data()
        if data and "workers" in data:
            return jsonify(data["workers"])
        return jsonify([])

    @app.route('/api/tasks', methods=['GET'])
    def get_tasks():
        data = get_live_data()
        if data and "tasks" in data:
            return jsonify(data["tasks"])
        return jsonify([])

    @app.route('/api/summary', methods=['GET'])
    def get_summary():
        data = get_live_data()
        if data:
            return jsonify(data)
        return jsonify({
            "status": {
                "job_in_progress": False,
                "job_completed": False,
                "job_failed": False,
                "job_error": None,
                "job_id": None,
                "current_stage": "IDLE",
                "duration_seconds": None,
                "total_jobs_submitted": 0,
                "server_running": False,
                "master_host": app.config.get('MASTER_HOST', '127.0.0.1'),
                "master_port": app.config.get('MASTER_PORT', 5000),
                "total_completed_tasks": 0,
                "phase_timestamps": None,
                "stats": {
                    "total": 0,
                    "pending": 0,
                    "assigned": 0,
                    "completed": 0,
                    "failed": 0,
                    "is_failed": False,
                    "is_done": True
                }
            },
            "workers": [],
            "tasks": [],
            "current_job_id": None,
            "current_stage": "IDLE",
            "job_history": [],
            "latest_results": None
        })

    @app.route('/api/submit-job', methods=['POST'])
    @app.route('/api/jobs', methods=['POST'])
    def submit_job():
        # 1. Check if master is running
        live_data = get_live_data()
        if not live_data or not live_data.get("status", {}).get("server_running", False):
            return jsonify({
                "success": False,
                "error": "Master server is offline. Please start the master before submitting jobs."
            }), 503

        # 2. Check if a job is already in progress (prevent overlapping jobs)
        if live_data.get("status", {}).get("job_in_progress", False):
            return jsonify({
                "success": False,
                "error": "A MapReduce job is already in progress. Please wait for it to complete."
            }), 409

        # 3. Extract and validate input
        input_text = None
        uploaded_file = request.files.get('file') if request.files else None

        if uploaded_file and uploaded_file.filename:
            filename = uploaded_file.filename.lower()
            if not filename.endswith('.txt'):
                return jsonify({
                    "success": False,
                    "error": "Invalid file format. Only plain text (.txt) files are accepted."
                }), 400
            try:
                raw_bytes = uploaded_file.read()
                input_text = raw_bytes.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    input_text = raw_bytes.decode('latin-1')
                except Exception as e:
                    return jsonify({
                        "success": False,
                        "error": f"Failed to read file encoding: {e}"
                    }), 400

            if not input_text or not input_text.strip():
                return jsonify({
                    "success": False,
                    "error": "Uploaded text file is empty. Please provide a file with valid text content."
                }), 400
        else:
            # Check form data or JSON payload
            if request.is_json and request.json:
                input_text = request.json.get('input_text')
            elif request.form:
                input_text = request.form.get('input_text')

            if not input_text or not str(input_text).strip():
                return jsonify({
                    "success": False,
                    "error": "Input text cannot be empty. Please enter text or upload a .txt file."
                }), 400

        # 4. Extract and validate mapper count
        mappers_raw = None
        if request.is_json and isinstance(request.json, dict):
            if 'num_mappers' in request.json:
                mappers_raw = request.json['num_mappers']
            elif 'num_map_tasks' in request.json:
                mappers_raw = request.json['num_map_tasks']
        elif request.form:
            if 'num_mappers' in request.form:
                mappers_raw = request.form['num_mappers']
            elif 'num_map_tasks' in request.form:
                mappers_raw = request.form['num_map_tasks']

        num_mappers = 2
        if mappers_raw is not None and str(mappers_raw).strip() != "":
            try:
                num_mappers = int(mappers_raw)
                if num_mappers < 1 or num_mappers > 32:
                    return jsonify({
                        "success": False,
                        "error": "Number of map tasks must be between 1 and 32."
                    }), 400
            except (ValueError, TypeError):
                return jsonify({
                    "success": False,
                    "error": "Number of map tasks must be a valid integer."
                }), 400

        # 5. Prepare job files
        job_id = f"job_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:4]}"
        proj_root = os.path.dirname(base_dir)
        upload_dir = os.path.join(proj_root, "datasets", "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        input_file = os.path.join(upload_dir, f"{job_id}_input.txt")
        input_bytes = input_text.encode('utf-8')
        input_size_bytes = len(input_bytes)
        with open(input_file, "wb") as f:
            f.write(input_bytes)

        output_dir = os.path.join(proj_root, "output")
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"{job_id}_result.txt")

        # 6. Execute job via master (embedded or TCP)
        master = app.config.get('MASTER_SERVER')
        if master:
            success, res = master.start_job(
                input_file=input_file,
                output_file=output_file,
                job_id=job_id,
                num_map_tasks=num_mappers,
                input_size_bytes=input_size_bytes
            )
            if not success:
                err_msg = str(res)
                if "already in progress" in err_msg.lower():
                    return jsonify({"success": False, "error": err_msg}), 409
                return jsonify({"success": False, "error": err_msg}), 400
        else:
            host = app.config.get('MASTER_HOST', '127.0.0.1')
            port = app.config.get('MASTER_PORT', 5000)
            tcp_resp = submit_job_to_tcp_master(
                host=host,
                port=port,
                input_file=input_file,
                output_file=output_file,
                job_id=job_id,
                num_map_tasks=num_mappers,
                input_size_bytes=input_size_bytes
            )
            if tcp_resp.get("type") == "ERROR":
                err_msg = tcp_resp.get("message", "Master rejected job")
                if "already in progress" in err_msg.lower():
                    return jsonify({"success": False, "error": err_msg}), 409
                return jsonify({"success": False, "error": err_msg}), 400

        return jsonify({
            "success": True,
            "job_id": job_id,
            "num_map_tasks": num_mappers,
            "input_size_bytes": input_size_bytes,
            "message": f"Job {job_id} submitted successfully with {num_mappers} map tasks."
        }), 200

    @app.route('/api/results', methods=['GET'])
    @app.route('/api/results/<req_job_id>', methods=['GET'])
    def get_results(req_job_id=None):
        target_id = req_job_id or request.args.get('job_id')
        master = app.config.get('MASTER_SERVER')
        if master:
            res_data = master.get_job_results(job_id=target_id)
        else:
            host = app.config.get('MASTER_HOST', '127.0.0.1')
            port = app.config.get('MASTER_PORT', 5000)
            res_data = fetch_job_results_from_tcp(host=host, port=port, job_id=target_id)

        if not res_data:
            return jsonify({
                "available": False,
                "message": f"No results found for job '{target_id}'" if target_id else "No job results available yet"
            }), 200

        raw_results = res_data.get("results", [])
        total_words = res_data.get("total_words")
        if total_words is None or total_words <= 0:
            total_words = sum(item.get("count", 0) for item in raw_results)

        enriched_results = []
        for item in raw_results:
            cnt = item.get("count", 0)
            rf = item.get("relative_frequency")
            if rf is None:
                rf = round((cnt / total_words) * 100, 2) if total_words > 0 else 0.0
            enriched_results.append({
                "word": item.get("word"),
                "count": cnt,
                "relative_frequency": rf
            })

        return jsonify({
            "available": True,
            "job_id": res_data.get("job_id"),
            "status": res_data.get("status", "COMPLETED"),
            "unique_words": res_data.get("unique_words", len(raw_results)),
            "total_words": total_words,
            "input_size_bytes": res_data.get("input_size_bytes", 0),
            "num_map_tasks": res_data.get("num_map_tasks", 0),
            "duration_seconds": res_data.get("duration_seconds"),
            "submission_time": res_data.get("submission_time"),
            "phase_timestamps": res_data.get("phase_timestamps"),
            "results": enriched_results
        }), 200

    @app.route('/api/jobs', methods=['GET'])
    def get_jobs_history():
        master = app.config.get('MASTER_SERVER')
        if master:
            history = master.get_job_history()
        else:
            host = app.config.get('MASTER_HOST', '127.0.0.1')
            port = app.config.get('MASTER_PORT', 5000)
            history = fetch_job_history_from_tcp(host=host, port=port)

        return jsonify(history or []), 200

    return app

# Default app instance for WSGI/direct usage
app = create_dashboard_app()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="MapReduce Web Dashboard")
    parser.add_argument("--port", type=int, default=8080, help="Port to run the dashboard on (default: 8080)")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind dashboard to")
    parser.add_argument("--master-host", default="127.0.0.1", help="Master TCP host (default: 127.0.0.1)")
    parser.add_argument("--master-port", type=int, default=5000, help="Master TCP port (default: 5000)")
    args = parser.parse_args()

    app = create_dashboard_app(master_host=args.master_host, master_port=args.master_port)
    print(f"Starting standalone dashboard on http://{args.host}:{args.port} (connecting to Master TCP at {args.master_host}:{args.master_port})")
    app.run(host=args.host, port=args.port, debug=False)
