import os
import json


import pandas as pd


# -------------------------------------------------------------------------------------- utils


def load_pages() -> pd.DataFrame:
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(parent_dir, "data")
    assert os.path.exists(data_dir), "data directory not found"

    files = os.listdir(data_dir)
    files = [f for f in files if f.endswith(".json")]
    filenname = max(files)
    path = os.path.join("data", filenname)

    dic = json.load(open(path))
    df = pd.DataFrame(dic).T

    return df


# -------------------------------------------------------------------------------------- cleaning


def clean_price(df: pd.DataFrame):
    prices = []

    # ignore everything else: there are always brokers even if the ad says "no broker"
    for i in range(len(df)):
        price = df.iloc[i]["price_info"]["Kaufpreis"]
        price = price.replace("€", "").replace(".", "").replace(",", ".").strip()
        price = float(price)
        prices.append(price)

    df["price"] = prices
    df.drop(columns=["price_info"], inplace=True)


def clean_district(df: pd.DataFrame):
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


def clean_bag_of_words(df: pd.DataFrame):

    bag_of_words = []

    for i in range(len(df)):
        merged = ""

        # title
        merged += df.iloc[i]["title"]

        # descriptions
        desc = df.iloc[i]["descriptions"]
        vals = [v for v in list(desc.values()) if v]
        merged += " ".join(vals) + " "

        # energy_certificate
        energy_cert = df.iloc[i]["energy_certificate"]
        merged += (" " + energy_cert) if energy_cert else ""

        merged = merged.strip().lower()
        bag_of_words.append(merged.strip().lower())

    df["bag_of_words"] = bag_of_words

    df.drop(columns=["title"], inplace=True)
    df.drop(columns=["descriptions"], inplace=True)
    df.drop(columns=["energy_certificate"], inplace=True)


def clean_last_update(df: pd.DataFrame):
    lcs = []

    for i in range(len(df)):
        lc = df.iloc[i]["last_update"].strip().lower()
        lc = lc.split(" ")[2:4]
        lc[0] = lc[0].replace(",", "")
        lc = " ".join(lc)
        lc = pd.to_datetime(lc, format="%d.%m.%Y %H:%M")
        lcs.append(lc)

    df["last_update"] = lcs


def clean_attributes(df: pd.DataFrame):
    neubau = []
    areas = []
    rooms = []
    needs_fix = []

    for i in range(len(df)):
        attrs = df.iloc[i]["attributes"]
        lower_attrs = {}
        for k, v in attrs.items():
            lower_attrs[k.strip().lower()] = v.strip().lower()  # type: ignore
        attrs = lower_attrs

        # neubau
        bt = attrs.get("bautyp")
        if bt == "neubau":
            neubau.append(True)
        elif bt == "altbau":
            neubau.append(False)
        else:
            neubau.append(None)

        # area
        area = attrs.get("wohnfläche")
        if area:
            area = area.split(" ")[0].replace(",", ".").strip()
            try:
                area = float(area)
            except:
                area = None
        areas.append(area)

        # num rooms
        nr = attrs.get("zimmer")
        if nr:
            try:
                nr = int(nr)
            except:
                nr = None
        rooms.append(nr)

        # needs fix
        needs_ren = attrs.get("zustand")
        if needs_ren == "sanierungsbedürftig":
            needs_ren = True
        elif needs_ren != None:
            needs_ren = False
        else:
            needs_ren = None

        needs_fix.append(needs_ren)

    df["neubau"] = neubau
    df["area"] = areas
    df["rooms"] = rooms
    df["needs_fix"] = needs_fix

    df.drop(columns=["attributes"], inplace=True)


# -------------------------------------------------------------------------------------- filtering rows


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
    df = load_pages()

    clean_price(df)
    clean_district(df)
    clean_bag_of_words(df)
    clean_last_update(df)
    clean_attributes(df)

    # df = remove_outliers(df)

    # df.drop(columns=["energy_certificate"], inplace=True)
    return df


if __name__ == "__main__":
    df = get_cleaned_data()
    print(df.head())
