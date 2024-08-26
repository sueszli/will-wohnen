from pathlib import Path
import argparse

# parser = argparse.ArgumentParser()
# parser.add_argument("csvpath", type=str, help="path to csv file")
# args = parser.parse_args()
# inputpath = Path(args.csvpath)
# assert inputpath.exists(), f"{inputpath} does not exist"


inputpath = Path("./data/data/links_2024-08-26_05-52-05.csv")

print(f"reading from {inputpath}")
