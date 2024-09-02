from tqdm import tqdm
import datetime
import dateparser

import json
from glob import glob
from pathlib import Path


def get_keys(file: str) -> list:
    ks: set = set()
    file = open(file, "r").readlines()
    for line in file:
        k = json.loads(line)
        ks = ks.union(k.keys())
    ks = list(ks)
    ks.sort()
    return ks

def parse_commission_fee(string: str) -> float:
    string = string.lower() if string else None
    is_commission_free = any([word in string for word in ["abgeber", "provisionsfrei", "direkt", "keine"]]) if string else None
    if not string:
        string = None
    elif is_commission_free:
        string = 0
    elif "%" in string and not "€" in string and not "eur" in string:
        string = string.split("%")[0].strip()
        string = "".join([c for c in string if c.isdigit() or c in [".", ","]])
        string = string[string.find([c for c in string if c.isdigit()][0]):]
        string = string.split(",")[0].split(".")[0]
        if len(string) > 2: # error
            string = None
        else:
            string = float(string) if string else None
            if elem["Kaufpreis"]:
                string = elem["Kaufpreis"] * string / 100
    elif "eur" in string:
        string = string.split("eur")[0].strip()
        string = "".join([c for c in string if c.isdigit() or c in [".", ","]]) if string else None
        string = string.replace(".", "").replace(",", ".").strip() if string else None
        string = float(string) if string else None
    elif "€" in string:
        string = string.split("€")[1].strip()
        string = "".join([c for c in string if c.isdigit() or c in [".", ","]]) if string else None
        string = string.replace(".", "").replace(",", ".").strip() if string else None
        string = float(string) if string else None
    else:
        string = None
    return string
    

inputpath = glob(str(Path("./data/*.jsonl")))
inputpath = list(filter(lambda p: Path(p).name.startswith("pages_") and Path(p).name.endswith(".jsonl"), inputpath))
assert len(inputpath) > 0
inputpath.sort()
inputpath = inputpath[-1]

ks = get_keys(inputpath)
file = open(inputpath, "r").readlines()
for line in tqdm(file):
    elem = json.loads(line)
    elem = {key: elem.get(key) for key in ks}

    # duplicate
    elem.pop("links_url")
    elem.pop("links_title")
    elem.pop("links_seller_name")
    elem.pop("links_price")
    elem.pop("address")

    # preprocess
    elem["company_broker_name"] = elem["company_broker_name"].replace("Kontakt", "").strip() if elem["company_broker_name"] else None
    elem["company_reference_id"] = elem["company_reference_id"].replace("Referenz ID", "").strip() if elem["company_reference_id"] else None
    elem["company_address"] = elem["company_address"].replace("Adresse", "").strip() if elem["company_address"] else None
    elem["last_update"] = elem["last_update"].replace("Zuletzt geändert: ", "").replace(" Uhr", "").replace(", ", " ").strip() if elem["last_update"] else None
    elem["last_update"] = str(dateparser.parse(elem["last_update"], languages=["de"])) if elem["last_update"] else None
    elem["Zimmer"] = elem["Zimmer"].replace("Zimmer", "").strip() if elem["Zimmer"] else None
    elem["Kaufpreis"] = elem["Kaufpreis"].replace(".", "").replace(",", ".").replace("€ ", "").strip() if elem["Kaufpreis"] else None
    elem["Kaufpreis"] = float(elem["Kaufpreis"]) if elem["Kaufpreis"] else None
    elem["Monatliche Kosten (inkl. MWSt)"] = elem["Monatliche Kosten (inkl. MWSt)"].replace(".", "").replace(",", ".").replace("€ ", "").strip() if elem["Monatliche Kosten (inkl. MWSt)"] else None
    elem["Monatliche Kosten (inkl. MWSt)"] = float(elem["Monatliche Kosten (inkl. MWSt)"]) if elem["Monatliche Kosten (inkl. MWSt)"] else None
    elem["Nutzfläche"] = elem["Nutzfläche"].replace("Nutzfläche", "").strip() if elem["Nutzfläche"] else None
    elem["Nutzfläche"] = elem["Nutzfläche"].replace(".", "").replace(",", ".").replace(" m²", "").strip() if elem["Nutzfläche"] else None
    elem["Maklerprovision:"] = parse_commission_fee(elem["Maklerprovision:"])

    # rename
    elem["address"] = elem.pop("links_address")
    elem["price"] = elem.pop("Kaufpreis")


    elem.pop("description_general") # todo: process later
    print(json.dumps(elem, indent=4, ensure_ascii=False))
    break








# df = pd.DataFrame(columns=ks)
# for line in tqdm(file):
#     k = json.loads(line)
#     df = df._append(k, ignore_index=True)
