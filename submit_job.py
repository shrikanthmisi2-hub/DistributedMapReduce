"""
CLI tool to submit a MapReduce job to the master.
"""
import socket
import argparse
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from common.protocol import send_message, MessageReader

def check_status(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        send_message(sock, {"type": "GET_JOB_STATUS"})
        reader = MessageReader(sock)
        return reader.read_message()
    finally:
        sock.close()

def submit_job(host, port, input_file, output_file, wait=False):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
        msg = {
            "type": "SUBMIT_JOB",
            "input_file": input_file,
            "output_file": output_file
        }
        send_message(sock, msg)
        
        reader = MessageReader(sock)
        response = reader.read_message()
        if response and response.get("type") == "JOB_ACCEPTED":
            print(f"Success: {response.get('message')}")
        else:
            print(f"Error submitting job: {response}")
            return
    except Exception as e:
        print(f"Connection error: {e}")
        return
    finally:
        sock.close()

    if wait:
        print("Waiting for job completion...")
        while True:
            time.sleep(0.5)
            try:
                status = check_status(host, port)
                if not status:
                    break
                stats = status.get("stats", {})
                if status.get("is_completed"):
                    print(f"Job completed successfully! (Total tasks: {stats.get('total', 0)}, Completed: {stats.get('completed', 0)})")
                    break
                if status.get("is_failed"):
                    print(f"Job failed: {status.get('error')}")
                    sys.exit(1)
            except Exception as e:
                print(f"Error checking job status: {e}")
                break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit a MapReduce job")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--input", default=os.path.join("datasets", "sample.txt"))
    parser.add_argument("--output", default=os.path.join("output", "result.txt"))
    parser.add_argument("--wait", action="store_true", help="Wait for job to complete")
    args = parser.parse_args()
    
    submit_job(args.host, args.port, args.input, args.output, wait=args.wait)
