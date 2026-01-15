import subprocess
import os
import sys
import threading
import time

_process = None
_lock = threading.Lock()
PORT = 8501

_PRELUDE = (
    "import os\n"
    "import sys\n"
    "try:\n"
    "    getattr(sys.stderr, 'flush', lambda: None)()\n"
    "except Exception:\n"
    "    try:\n"
    "        sys.stderr = open(os.devnull, 'w')\n"
    "    except Exception:\n"
    "        pass\n"
    "os.environ.setdefault('TQDM_DISABLE', '1')\n"
    "_backend_dir = os.path.dirname(os.path.abspath(__file__))\n"
    "if _backend_dir not in sys.path:\n"
    "    sys.path.insert(0, _backend_dir)\n"
    "from pymr_compat import ensure_py_mini_racer\n"
    "ensure_py_mini_racer()\n"
)

def _inject_prelude(content: str) -> str:
    if not content:
        return _PRELUDE

    if "ensure_py_mini_racer" in content and "pymr_compat" in content:
        return content

    lines = content.splitlines(True)
    insert_at = 0

    if lines and lines[0].startswith("#!"):
        insert_at = 1

    if insert_at < len(lines) and (
        lines[insert_at].startswith("# -*- coding:")
        or lines[insert_at].startswith("# coding=")
        or lines[insert_at].startswith("# coding:")
    ):
        insert_at += 1

    while insert_at < len(lines) and lines[insert_at].startswith("from __future__ import"):
        insert_at += 1

    return "".join(lines[:insert_at]) + _PRELUDE + "".join(lines[insert_at:])

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
                f.write(_inject_prelude("import streamlit as st\nst.write('Streamlit Service Initialized')\n"))
        
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
        f.write(_inject_prelude(content))
    return f"http://localhost:{PORT}"

def get_streamlit_url():
    return f"http://localhost:{PORT}"
