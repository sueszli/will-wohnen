import json
import os

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
    # ignore everything else: "no broker" flags are not reliable
    prices = []

    for i in range(len(df)):
        price = df.iloc[i]["price_info"]["Kaufpreis"]
        price = price.replace("€", "").replace(".", "").replace(",", ".").strip()
        price = float(price)
        prices.append(price)
        # print("price: ", df.iloc[i]["price_info"]["Kaufpreis"])
        # print("\tprice: ", prices[i])

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
        # print("address: ", df.iloc[i]["address"])
        # print("\tdistrict: ", districts[i])

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
        # print("last_update: ", df.iloc[i]["last_update"])
        # print("\tlast_update: ", lcs[i])

    df["last_update"] = lcs


def clean_attributes(df: pd.DataFrame):
    neubau = []
    areas = []
    rooms = []
    leased = []
    needs_fix = []

    for i in range(len(df)):
        attrs = df.iloc[i]["attributes"]

        # strip and lower everything
        lower_attrs = {}
        for k, v in attrs.items():
            lower_attrs[k.strip().lower()] = v.strip().lower()  # type: ignore
        attrs = lower_attrs

        # neubau
        b = attrs.get("bautyp")
        if b == "neubau":
            neubau.append(True)
        elif b == "altbau":
            neubau.append(False)
        else:
            neubau.append(None)

        # area
        w = attrs.get("wohnfläche")
        if w:
            w = w.split(" ")[0].replace(",", ".").strip()
            try:
                w = float(w)
            except:
                w = None
        areas.append(w)

        # rooms
        r = attrs.get("zimmer")
        if r:
            try:
                r = int(r)
            except:
                r = None
        rooms.append(r)

        # needs_fix
        z = attrs.get("zustand")
        if z == "sanierungsbedürftig":
            z = True
        elif z != None:
            z = False
        else:
            z = None
        needs_fix.append(z)

        # leased
        l = attrs.get("verfügbar")
        if l and "vermietet" in l:
            l = True
        elif l and l == "ab sofort":
            l = False
        else:
            l = None
        leased.append(l)

        # print("url: ", df.iloc[i]["url"])
        # print("attrs: ", attrs)
        # print("\tneubau: ", neubau[i])
        # print("\tarea: ", areas[i])
        # print("\trooms: ", rooms[i])
        # print("\tneeds_fix: ", needs_fix[i])
        # print("\tleased: ", leased[i])

    df["neubau"] = neubau
    df["area"] = areas
    df["rooms"] = rooms
    df["needs_fix"] = needs_fix
    df["leased"] = leased

    df.drop(columns=["attributes"], inplace=True)


# -------------------------------------------------------------------------------------- wrong scrapes


def remove_outliers(df: pd.DataFrame):
    # too expensive
    max_limit = 150_000
    illegal_dfs = df[df["price"] >= max_limit]
    print(f"removing {len(illegal_dfs)} rows with price >= {max_limit}...")
    df = df[df["price"] < max_limit]

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
    illegal_dfs = df[~df["district"].apply(is_valid_district)]
    print(f"removing {len(illegal_dfs)} rows with invalid district...")
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

    df = remove_outliers(df)
    print(f"final length: {len(df)}")
    return df


if __name__ == "__main__":
    df = get_cleaned_data()
    print(df.head())
