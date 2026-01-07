import subprocess
import os
import sys
import threading
import time

_process = None
_lock = threading.Lock()
PORT = 8501

def get_runner_path():
    # Assuming this file is in backend/services/
    # backend/ is parent
    current_dir = os.path.dirname(os.path.abspath(__file__))
    backend_dir = os.path.dirname(current_dir)
    return os.path.join(backend_dir, "streamlit_runner.py")

def start_streamlit():
    global _process
    with _lock:
        if _process is not None:
            if _process.poll() is None:
                print("Streamlit is already running.")
                return
            
        runner_path = get_runner_path()
        if not os.path.exists(runner_path):
            with open(runner_path, "w") as f:
                f.write("import streamlit as st\nst.write('Streamlit Service Initialized')")
        
        cmd = [
            sys.executable, "-m", "streamlit", "run",
            runner_path,
            "--server.port", str(PORT),
            "--server.headless", "true",
            "--server.address", "0.0.0.0",
            "--server.runOnSave", "true"
        ]
        
        print(f"Starting Streamlit: {' '.join(cmd)}")
        
        # Use Popen. We don't wait.
        # Redirect output to DEVNULL to avoid buffer filling issues if we don't read it.
        # Or inherit to see logs in console. Let's inherit for now to help debug.
        # Note: If running in a background service, inherit might not work well, but here it's fine.
        _process = subprocess.Popen(
            cmd,
            stdout=sys.stdout, 
            stderr=sys.stderr,
            text=True
        )
        print(f"Streamlit started on port {PORT} with PID {_process.pid}")

def stop_streamlit():
    global _process
    with _lock:
        if _process:
            print(f"Stopping Streamlit process (PID: {_process.pid})...")
            _process.terminate()
            try:
                _process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("Streamlit process did not terminate gracefully. Killing...")
                _process.kill()
            _process = None
            print("Streamlit stopped.")
        else:
            print("Streamlit is not running.")

def update_streamlit_code(content: str):
    runner_path = get_runner_path()
    # Write the file. Streamlit watchdog will reload it.
    with open(runner_path, "w", encoding="utf-8") as f:
        f.write(content)
    return f"http://localhost:{PORT}"

def get_streamlit_url():
    return f"http://localhost:{PORT}"
