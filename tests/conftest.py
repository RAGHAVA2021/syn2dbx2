import copy
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))


@pytest.fixture
def inventory():
    return copy.deepcopy(json.loads((ROOT / "config" / "inventory.example.json").read_text()))
