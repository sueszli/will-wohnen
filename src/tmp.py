import json
import csv
import glob
from pathlib import Path

inputpath = glob.glob(str(Path.cwd() / "data" / "pages_*.csv"))
inputpath = list(filter(lambda p: Path(p).name.startswith("pages_") and Path(p).name.endswith(".csv"), inputpath))
assert len(inputpath) > 0
inputpath.sort()
inputpath = inputpath[-1]
inputfile = list(csv.reader(open(inputpath, "r")))
header = [word for word in inputfile[0]]
body = inputfile[1:]

# turn to jsons
for row in body:
    json_elem = dict(zip(header, row))
    json_elem = {k.lower(): v.lower() for k, v in json_elem.items() if isinstance(k, str) and isinstance(v, str)}
    json_elem = {k: (v if v != "" else None) for k, v in json_elem.items()}
    print(json.dumps(json_elem, ensure_ascii=False, indent=4))
    print()
