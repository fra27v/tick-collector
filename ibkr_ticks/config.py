from dataclasses import dataclass, field
from pathlib import Path
import math
import re
import tomllib


@dataclass
class Config:
    ibkr: dict = field(default_factory=lambda: dict(host="127.0.0.1", port=4002, client_id=21))
    contract: dict = field(default_factory=lambda: dict(symbol="EUR", sec_type="CASH", currency="USD", exchange="IDEALPRO"))
    storage: dict = field(default_factory=lambda: dict(directory="data", queue_capacity=10000,
        flush_interval_seconds=2, segment_max_seconds=900, segment_max_bytes=67108864,
        minimum_free_bytes=1073741824, status_interval_seconds=10))

    @property
    def instrument(self):
        return "_".join(self.contract.values())


def load(path):
    path = Path(path).resolve()
    with path.open("rb") as f:
        raw = tomllib.load(f)
    c = Config()
    for section, values in raw.items():
        if section not in ("ibkr", "contract", "storage") or not isinstance(values, dict):
            raise ValueError(f"Invalid section: {section}")
        target = getattr(c, section)
        if values.keys() - target.keys():
            raise ValueError(f"Unknown options in {section}: {values.keys() - target.keys()}")
        target.update(values)
    for name in ("port", "client_id"):
        v = c.ibkr[name]
        if type(v) is not int or v < (1 if name == "port" else 0) or (name == "port" and v > 65535):
            raise ValueError(f"Invalid {name}")
    if not isinstance(c.ibkr["host"], str) or not c.ibkr["host"].strip():
        raise ValueError("Invalid host")
    for v in c.contract.values():
        if not isinstance(v, str) or not re.fullmatch(r"[A-Za-z0-9.-]+", v):
            raise ValueError("Invalid contract field")
    for k, v in c.storage.items():
        if k == "directory":
            if not isinstance(v, str) or not v.strip():
                raise ValueError("Invalid directory")
        elif type(v) not in (int, float) or not math.isfinite(v) or v < 0 or (k != "minimum_free_bytes" and v == 0):
            raise ValueError(f"Invalid storage.{k}")
    for k in ("queue_capacity", "segment_max_bytes", "minimum_free_bytes"):
        if type(c.storage[k]) is not int:
            raise ValueError(f"storage.{k} must be integer")
    c.storage["directory"] = str((path.parent / c.storage["directory"]).resolve())
    return c
