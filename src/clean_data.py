import os
import json
import itertools

import pandas as pd


# -------------------------------------------------------------------------------------------------------- utils


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


# -------------------------------------------------------------------------------------------------------- cleaning


def clean_descriptions(df: pd.DataFrame):  # TESTED
    descriptions = []
    for i in range(len(df)):
        desc = df.iloc[i]["descriptions"]
        vals = [v for v in list(desc.values()) if v]
        merged = " ".join(vals)
        merged = merged.strip().lower()
        descriptions.append(merged)
    df["descriptions"] = descriptions


def clean_last_change(df: pd.DataFrame):  # TESTED
    lcs = []
    for i in range(len(df)):
        lc = df.iloc[i]["last_change"].strip().lower()
        lc = lc.split(" ")[2:4]
        lc[0] = lc[0].replace(",", "")
        lc = " ".join(lc)
        lc = pd.to_datetime(lc, format="%d.%m.%Y %H:%M")
        lcs.append(lc)
    df["last_change"] = lcs


def clean_price(df: pd.DataFrame):  # TESTED
    prices = []
    for i in range(len(df)):
        price = df.iloc[i]["price_info"]["Kaufpreis"]
        price = price.replace("€", "").replace(".", "").replace(",", ".").strip()
        price = float(price)
        prices.append(price)
    df["price"] = prices

    ## don't derive: they all have brokers
    # has_broker = []
    # for i in range(len(df)):
    #     bk = df.iloc[i]["price_info"]
    #     bk_keys = [k.strip().lower() for k in bk.keys()]
    #     has_bk = any(["provision" in k for k in [k.strip().lower() for k in bk.keys()]])
    #     has_broker.append(has_bk)
    # df["broker"] = has_broker

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


# -------------------------------------------------------------------------------------------------------- filtering rows


def remove_outliers(df: pd.DataFrame):
    # too expensive
    df = df[df["price"] < 150_000]

    # too far away
    # fmt: off
    valid_districts = [
        1210, 2100, 2103, # korneuburg, langenzersdorf
        2100, 2102, # korneuburg, bisamberg
        2102, 2201, 2202, # korneuburg, hagenbrunn
        2201, # korneuburg, gerasdorf bei wien
        1140, 3400, 3420, 3421, # st.pölten land, purkersdorf
        3003, # st.pölten land, gablitz
        3001, # st.pölten land, mauerbach
    ]
    # fmt: on
    is_valid_district = lambda x: int(x) < 2000 or int(x) in valid_districts
    df = df[df["district"].apply(is_valid_district)]

    return df


def get_cleaned_data() -> pd.DataFrame:
    print("cleaning data...")

    dic = read_latest_pages()
    df = pd.DataFrame(dic).T

    df["title"] = df["title"].apply(lambda x: x.strip().lower())
    clean_descriptions(df)
    clean_last_change(df)
    clean_price(df)
    clean_address(df)
    clean_attributes(df)

    df = remove_outliers(df)

    return df


if __name__ == "__main__":
    df = get_cleaned_data()
    print(df.head())
