#!/usr/bin/env python3
"""
run_servers.py

Automatically launch all CrashReplicator nodes defined in your local_testing configs.
Opens each in a separate terminal window on Windows, macOS, or Linux.
Usage:
  python run_servers.py [config_dir]
If no directory is provided, defaults to ./configs/local_testing
"""

import os
import sys
import subprocess
import time
import platform

def main():
    # Determine config directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.path.join(base_dir, "configs", "local_testing")
    )
    if not os.path.isdir(config_dir):
        print(f"Error: config directory not found: {config_dir!r}", file=sys.stderr)
        sys.exit(1)

    # Gather all .json files in directory
    configs = [
        os.path.join(config_dir, f)
        for f in os.listdir(config_dir)
        if f.lower().endswith(".json")
    ]
    if not configs:
        print(f"No JSON config files found in {config_dir!r}", file=sys.stderr)
        sys.exit(1)

    python_exec = sys.executable
    system = platform.system()

    print(f"Launching {len(configs)} server processes in separate terminals...")
    for cfg in configs:
        cmd = [python_exec, "-m", "server.server", cfg]
        print(f"Opening terminal for: {' '.join(cmd)}")

        if system == "Windows":
            flags = subprocess.CREATE_NEW_CONSOLE | subprocess.CREATE_NEW_PROCESS_GROUP
            subprocess.Popen(cmd, creationflags=flags)
        elif system == "Darwin":
            # macOS: open new Terminal window with cd command first
            script = f'tell application "Terminal" to do script "cd Documents/vsProjects/cmpe275/mini3 && {python_exec} -m server.server {cfg}"'
            subprocess.Popen(["osascript", "-e", script])
        else:
            # Linux: try gnome-terminal, fall back to xterm
            try:
                subprocess.Popen(["gnome-terminal", "--"] + cmd)
            except FileNotFoundError:
                subprocess.Popen(["xterm", "-e"] + cmd)

    print("All servers launched. Press Ctrl+C to exit launcher.")
    try:
        # Keep launcher alive; child terminals remain open
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nLauncher exiting. Servers remain running in their terminals.")
        sys.exit(0)


if __name__ == "__main__":
    main()
