import csv
import json
import re
from glob import glob
from pathlib import Path
from typing import Optional

from tqdm import tqdm


def get_keys(file: str) -> list:
    # get shared keys from all jsonl lines
    ks: set = set()
    file = open(file, "r").readlines()
    for line in file:
        k = json.loads(line)
        ks = ks.union(k.keys())
    ks = list(ks)
    ks.sort()
    return ks


def get_available_keys(dicts: list[dict], threshold: float) -> dict:
    # get {key: non-null ratio} for all keys
    available_keys = {k: sum([1 for j in dicts if j[k] is not None]) / len(dicts) for k in dicts[0].keys()}
    available_keys = dict(sorted(available_keys.items(), key=lambda item: item[1], reverse=True))
    available_keys = {k: v for k, v in available_keys.items() if v > threshold}
    return available_keys


def parse_float(string: str) -> float:
    string = "".join([c for c in string if c.isdigit() or c in [".", ","]]) if string else None
    for s in ["¹", "²", "³", "⁴", "⁵", "⁶", "⁷", "⁸", "⁹", "⁰"]:
        string = string.replace(s, "") if string else None
    if not string or not any([c.isdigit() for c in string]):
        return None
    string = string.replace(".", "").replace(",", ".").strip() if string else None
    string = float(string) if string else None
    return string


def parse_int(string: str) -> int:
    string = "".join([c for c in string if c.isdigit()]) if string else None
    string = int(string) if string else None
    return string


def parse_commission_fee(string: str, kaufpreis: Optional[float] = None) -> Optional[float]:
    if not string:
        return None
    string = string.lower()
    is_commission_free = any([word in string for word in ["abgeber", "provisionsfrei", "direkt", "keine"]])
    if is_commission_free:
        return 0.0
    elif "%" in string and not "€" in string and not "eur" in string:
        string = string.split("%")[0].strip()
        string = "".join([c for c in string if c.isdigit() or c in [".", ","]])
        string = string[string.find([c for c in string if c.isdigit()][0]) :]
        string = string.split(",")[0].split(".")[0]
        if len(string) > 2:  # error
            return None
        else:
            return float(string) * kaufpreis / 100 if kaufpreis else None
    elif "eur" in string:
        string = string.split("eur")[0].strip()
        return parse_float(string)
    elif "€" in string:
        string = string.split("€")[1].strip()
        return parse_float(string)
    return None


inputpath = glob(str(Path("./data/*.jsonl")))
inputpath = list(filter(lambda p: Path(p).name.startswith("pages_") and Path(p).name.endswith(".jsonl"), inputpath))
assert len(inputpath) > 0
inputpath.sort()
inputpath = inputpath[-1]
outputpath = Path.cwd() / "data" / (str(Path(inputpath).stem) + ".csv")
outputpath.unlink(missing_ok=True)

dicts = list(map(lambda line: json.loads(line), open(inputpath, "r").readlines()))
required_keys = get_keys(inputpath)
dicts = list(map(lambda elem: {key: elem.get(key) for key in required_keys}, dicts))  # use same keys in all jsonl lines
assert all([set(dicts[0].keys()) == set(elem.keys()) for elem in dicts])

threshold = 0.5  # only keep keys that are available in more than 50% of the data (good treshhold based on manual inspection)
available_keys = get_available_keys(dicts, threshold)

for elem in tqdm(dicts):
    # preprocess
    elem["Ablöse"] = parse_float(elem["Ablöse"])
    elem["Balkon"] = parse_float(elem["Balkon"])
    elem["Betriebskosten (exkl. MWSt)"] = parse_float(elem["Betriebskosten (exkl. MWSt)"])
    elem["Betriebskosten (inkl. MWSt)"] = parse_float(elem["Betriebskosten (inkl. MWSt)"])
    elem["Dachterrasse"] = parse_float(elem["Dachterrasse"])
    elem["Fertigstellung"] = re.search(r"\b\d{4}(?=[.,\s\-/]|$)", elem["Fertigstellung"]) if elem["Fertigstellung"] else None
    elem["Fertigstellung"] = elem["Fertigstellung"].group(0) if elem["Fertigstellung"] else None
    elem["Garten"] = parse_float(elem["Garten"])
    elem["Gesamtfläche"] = parse_float(elem["Gesamtfläche"])
    elem["Grundfläche"] = parse_float(elem["Grundfläche"])
    elem["Heizkosten (exkl. MWSt)"] = parse_float(elem["Heizkosten (exkl. MWSt)"])
    elem["Loggia"] = parse_float(elem["Loggia"])
    elem["Maklerprovision:"] = parse_commission_fee(elem["Maklerprovision:"], parse_float(elem["links_price"]))
    elem["Miete"] = elem["Miete"].split("-")[0].strip() if elem["Miete"] else None
    elem["Miete"] = parse_float(elem["Miete"])
    elem["Monatliche Kosten (MWSt)"] = parse_float(elem["Monatliche Kosten (MWSt)"])
    elem["Monatliche Kosten (inkl. MWSt)"] = parse_float(elem["Monatliche Kosten (inkl. MWSt)"])
    elem["Nutzfläche"] = parse_float(elem["Nutzfläche"])
    elem["Preis"] = elem["Preis"].split("-")[0].strip() if elem["Preis"] else None
    elem["Preis"] = parse_float(elem["Preis"])
    elem["Sonstige Kosten (exkl. MWSt)"] = parse_float(elem["Sonstige Kosten (exkl. MWSt)"])
    elem["Stockwerk(e)"] = parse_int(elem["Stockwerk(e)"])
    elem["Stockwerk(e)"] = None if elem["Stockwerk(e)"] and elem["Stockwerk(e)"] > 15 else elem["Stockwerk(e)"]
    elem["Terrasse"] = parse_float(elem["Terrasse"])
    elem["Topnummer"] = parse_int(elem["Topnummer"])
    elem["Topnummer"] = None if elem["Topnummer"] and elem["Topnummer"] > 100 else elem["Topnummer"]
    elem["Verfügbar"] = re.search(r"\b\d{4}(?=[.,\s\-/]|$)", elem["Verfügbar"]) if elem["Verfügbar"] else None
    elem["Verfügbar"] = elem["Verfügbar"].group(0) if elem["Verfügbar"] else None
    elem["Wintergarten"] = parse_float(elem["Wintergarten"])
    elem["Wohneinheiten"] = parse_int(elem["Wohneinheiten"])
    elem["Wohnfläche"] = elem["Wohnfläche"].split("-")[0].strip() if elem["Wohnfläche"] else None
    elem["Wohnfläche"] = parse_float(elem["Wohnfläche"])
    elem["Zimmer"] = parse_float(elem["Zimmer"])
    elem["company_address"] = elem["company_address"].replace("Adresse", "").strip() if elem["company_address"] else None
    elem["company_broker_name"] = elem["company_broker_name"].replace("Kontakt", "").strip() if elem["company_broker_name"] else None
    elem["company_reference_id"] = elem["company_reference_id"].replace("Referenz ID", "").strip() if elem["company_reference_id"] else None
    elem["description_price"] = re.findall(r"(\d+(?:,\d+)?)\s*Eur", elem["description_price"]) if elem["description_price"] else None
    elem["description_price"] = sum([parse_float(p) for p in elem["description_price"]]) if elem["description_price"] else None  # sum of all utilities
    elem["energy_certificate"] = re.search(r"Energieklasse:\s*([A-Z])", elem["energy_certificate"]) if elem["energy_certificate"] else None
    elem["energy_certificate"] = elem["energy_certificate"].group(1) if elem["energy_certificate"] else None
    elem["last_update"] = elem["last_update"].replace("Zuletzt geändert: ", "").replace(" Uhr", "").replace(", ", " ").strip() if elem["last_update"] else None
    elem["links_num_rooms"] = parse_float(elem["links_num_rooms"])
    elem["links_price"] = parse_float(elem["links_price"])

    elem.pop("Kaufpreis")  # links_price is more structured
    elem.pop("address")  # links_address is structured
    elem.pop("links_m2")  # wohnfläche
    elem.pop("links_url")  # url
    elem.pop("links_num_rooms")  # zimmer
    elem.pop("links_seller_name")  # company_name
    elem.pop("Preis")  # links_price
    elem.pop("Monatliche Kosten (MWSt)")  # doesn't mean anything

    # merge columns
    if elem["Betriebskosten (exkl. MWSt)"] and not elem["Betriebskosten (inkl. MWSt)"]:
        elem["Betriebskosten (inkl. MWSt)"] = elem["Betriebskosten (exkl. MWSt)"] * 1.2
    elem["betriebskosten"] = elem.pop("Betriebskosten (inkl. MWSt)")
    elem.pop("Betriebskosten (exkl. MWSt)")

    # lowercase all vals that aren't descriptions
    elem = {k: v.lower() if isinstance(v, str) and not k.startswith("description_") else v for k, v in elem.items()}

    # drop inavailable keys
    for k in list(elem.keys()):
        if k not in available_keys:
            elem.pop(k)

    # rename keys
    rename = {
        "links_address": "address",
        "links_price": "price",
        "links_title": "title",
        "links_type": "type",
        "description_price": "total_additional_costs",
    }
    elem = {rename.get(k, k): v for k, v in elem.items()}
    elem = {k.replace(":", "").strip(): v for k, v in elem.items()}
    elem = {k.replace("(", "").replace(")", "").strip(): v for k, v in elem.items()}
    elem = {k.lower(): v for k, v in elem.items()}

    # round floats
    elem = {k: round(v, 2) if isinstance(v, float) else v for k, v in elem.items()}

    # store
    with open(outputpath, "a") as f:
        writer = csv.DictWriter(f, fieldnames=elem.keys(), quoting=csv.QUOTE_NONNUMERIC)
        if f.tell() == 0:
            writer.writeheader()
        writer.writerow(elem)
