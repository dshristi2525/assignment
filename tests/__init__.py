import sys
from pathlib import Path

# Make `schema_drift` importable for both `python -m unittest` and pytest, with nothing installed.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
