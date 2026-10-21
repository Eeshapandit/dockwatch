import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # import repo modules
FIXTURES = Path(__file__).parent / "fixtures"
