"""Файл гоняет flake8 через pytest, чтобы проверка стиля выглядела как обычный тест."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_flake8_clean() -> None:
    project_root = Path(__file__).resolve().parents[1]
    cmd = [
        sys.executable,
        "-m",
        "flake8",
        "--max-line-length=100",
        "--extend-ignore=E203,W503",
        "flows",
        "dask_jobs",
        "tests",
    ]
    result = subprocess.run(
        cmd,
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )
    details = f"{result.stdout}\n{result.stderr}".strip()
    assert result.returncode == 0, f"flake8 нашёл проблемы:\n{details}"
