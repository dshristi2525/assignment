"""Convenience entry point so the project runs from a clean checkout without installing anything.

    python main.py demo
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from schema_drift.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
