import os
import sys
import json

import pandas as pd


def read_latest_pages():
    files = os.listdir("data")
    files = [f for f in files if f.endswith(".json")]
    filenname = max(files)
    path = os.path.join("data", filenname)

    dict = json.load(open(path))
    print(dict)


read_latest_pages()
