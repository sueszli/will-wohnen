import json
from glob import glob
from pathlib import Path


def get_keys(file: str) -> list:
    ks: set = set()
    file = open(file, "r").readlines()
    for line in file:
        k = json.loads(line)
        ks = ks.union(k.keys())
    return list(ks)


inputpath = glob(str(Path("./data/*.jsonl")))
inputpath = list(filter(lambda p: Path(p).name.startswith("pages_") and Path(p).name.endswith(".jsonl"), inputpath))
assert len(inputpath) > 0
inputpath.sort()
inputpath = inputpath[-1]

ks = get_keys(inputpath)

print("keys:")
for k in ks:
    print("\t" + k)
print("\n" * 5)

file = open(inputpath, "r").readlines()
fstelem = json.loads(file[0])
print(json.dumps(fstelem, indent=4, ensure_ascii=False))

# df = pd.DataFrame(columns=ks)
# for line in tqdm(file):
#     k = json.loads(line)
#     df = df._append(k, ignore_index=True)
