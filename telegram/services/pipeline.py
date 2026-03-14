"""
services/pipeline.py — async subprocess runner for merger.py.
"""

import asyncio
import sys

from config import MERGER_PY, PROJECT_DIR, VENV_PYTHON


_TIMEOUT = 1800   # 30 minutes hard cap — kills the subprocess if exceeded


async def run_pipeline(algo_flag: str) -> tuple[bool, str]:
    """
    Run  merger.py <algo_flag>  as an async subprocess.

    Returns
    -------
    (success: bool, output: str)
        success — True if returncode == 0
        output  — combined stdout + stderr (last ~800 chars shown in bot)
    """
    python = str(VENV_PYTHON) if VENV_PYTHON.exists() else sys.executable

    proc = await asyncio.create_subprocess_exec(
        python, str(MERGER_PY), algo_flag,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(PROJECT_DIR),
    )
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=_TIMEOUT)
    except asyncio.TimeoutError:
        proc.kill()
        return False, f"Pipeline killed — exceeded {_TIMEOUT // 60}-minute timeout."

    output = stdout.decode(errors="replace") if stdout else ""
    return proc.returncode == 0, output
