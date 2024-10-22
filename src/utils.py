import csv
import functools
import random
import secrets
import time
from pathlib import Path

import numpy as np
import torch


def timeit(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"{func.__name__} took {end - start} seconds")
        return result

    return wrapper


def set_seed(seed: int = -1) -> None:
    if seed == -1:
        seed = secrets.randbelow(1_000_000_000)

    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False


def get_device(disable_mps=False) -> str:
    if torch.backends.mps.is_available() and not disable_mps:
        return "mps"
    elif torch.cuda.is_available():
        return "cuda"
    else:
        return "cpu"


def dump_to_csv(filepath: Path, data: list[dict]) -> None:
    filepath.unlink(missing_ok=True)
    with open(filepath, "w") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys(), quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        writer.writerows(data)
