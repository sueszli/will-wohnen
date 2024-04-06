import os
import json

import pandas as pd


def read_latest_pages() -> dict:
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(parent_dir, "data")
    assert os.path.exists(data_dir), "data directory not found"

    files = os.listdir(data_dir)
    files = [f for f in files if f.endswith(".json")]
    filenname = max(files)
    path = os.path.join("data", filenname)

    dic = json.load(open(path))
    return dic


def clean_last_change(df: pd.DataFrame):
    # last update as pd.Timestamp
    lcs = []
    for i in range(len(df)):
        lc = df.iloc[i]["last_change"].strip().lower()
        lc = lc.split(" ")[2:4]
        lc[0] = lc[0].replace(",", "")
        lc = " ".join(lc)
        lc = pd.to_datetime(lc, format="%d.%m.%Y %H:%M")
        lcs.append(lc)
    df["last_change"] = lcs


def clean_price(df: pd.DataFrame):
    # get price, has_broker_commission
    prices = []
    for i in range(len(df)):
        price = df.iloc[i]["price_info"]["Kaufpreis"]
        price = price.replace("€", "").replace(".", "").replace(",", ".").strip()
        price = float(price)
        prices.append(price)
    df["price"] = prices

    has_broker_commission = []
    for i in range(len(df)):
        bc = df.iloc[i]["price_info"]
        bc_keys = [k.strip().lower() for k in bc.keys()]
        has_bc = any(["provision" in k for k in bc_keys])
        has_broker_commission.append(has_bc)
    df["broker"] = has_broker_commission

    df.drop(columns=["price_info"], inplace=True)


def clean_address(df: pd.DataFrame):
    # get district
    districts = []
    for i in range(len(df)):
        address = df.iloc[i]["address"].strip()
        address = address.split(",")
        address = [a.strip() for a in address]
        address = [a for a in address if a[0].isdigit() and len(a.split(" ")[0]) == 4]
        address = None if len(address) == 0 else address[0].split(" ")[0]
        districts.append(address)

    df["district"] = districts
    df.drop(columns=["address"], inplace=True)


def clean_attributes(df: pd.DataFrame):
    # get neubau, area, num_rooms, needs_renovation
    neubau = []
    areas = []
    num_rooms = []
    needs_renovation = []

    for i in range(len(df)):
        attrs = df.iloc[i]["attributes"]
        lower_attrs = {}
        for k, v in attrs.items():
            lower_attrs[k.strip().lower()] = v.strip().lower()  # type: ignore
        attrs = lower_attrs

        # neubau
        constr_type = attrs.get("bautyp", None)
        if constr_type == "neubau":
            neubau.append(True)
        elif constr_type == "altbau":
            neubau.append(False)
        else:
            neubau.append(None)

        # area
        area = attrs.get("wohnfläche", None)
        if area:
            area = area.split(" ")[0].replace(",", ".").strip()
            try:
                area = float(area)
            except:
                area = None
        areas.append(area)

        # num rooms
        nr = attrs.get("zimmer", None)
        if nr:
            try:
                nr = int(nr)
            except:
                nr = None
        num_rooms.append(nr)

        # needs renovation
        needs_ren = attrs.get("zustand", None)
        if needs_ren == "sanierungsbedürftig":
            needs_ren = True
        elif needs_ren != None:
            needs_ren = False
        else:
            needs_ren = None

        needs_renovation.append(needs_ren)

    df["neubau"] = neubau
    df["area"] = areas
    df["rooms"] = num_rooms
    df["needs_fix"] = needs_renovation

    df.drop(columns=["attributes"], inplace=True)


def remove_outliers(df: pd.DataFrame):
    # too expensive
    df = df[df["price"] < 150_000]

    # too far away

    return df


def get_cleaned_data() -> pd.DataFrame:
    print("cleaning data...")

    dic = read_latest_pages()
    df = pd.DataFrame(dic).T

    df["title"] = df["title"].apply(lambda x: x.strip().lower())
    df["descriptions"] = df["descriptions"].apply(lambda x: x["description_general"].strip().lower())
    df.drop(columns=["energy_certificate"], inplace=True)

    clean_last_change(df)
    clean_price(df)
    clean_address(df)
    clean_attributes(df)

    df = remove_outliers(df)

    return df


df = get_cleaned_data()
print(df.head())
