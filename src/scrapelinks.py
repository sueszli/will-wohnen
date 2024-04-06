import os
import sys
import itertools
import json
from typing import List

import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import asyncio
import aiohttp
from aiohttp import ClientSession


def get_init_url() -> str:
    assert os.path.exists("config.json"), "config.json not found"
    config = json.load(open("config.json"))

    URL = "https://www.willhaben.at/iad/immobilien/eigentumswohnung/eigentumswohnung-angebote?"
    URL += "rows=5"  #  avoid buffered page loading
    URL += "&areaId=900"  # vienna
    URL += "" if not config["suburbs"] else "&areaId=312&areaId=319&areaId=321"  # st.pölten land, tulln, korneuburg
    URL += f"&PRICE_FROM={config['price_range']['from']}"
    URL += f"&PRICE_TO={config['price_range']['to']}"
    return URL


def get_rate_limit() -> int:
    assert os.path.exists("config.json"), "config.json not found"
    config = json.load(open("config.json"))
    return config["rate_limit_delay_ms"]


def get_total_ads(url: str) -> int:
    response = requests.get(url)
    assert response.status_code == 200, f"status code: {response.status_code}"

    soup = BeautifulSoup(response.text, "html.parser")
    elem = soup.find("h1", {"data-testid": "result-list-title"})
    assert elem
    return int(elem.text.split()[0])


def set_user_agent() -> None:
    os.environ["HTTP_USER_AGENT"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    os.environ["HTTP_ACCEPT_LANGUAGE"] = "en-US,en;q=0.5"


def parse_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")

    elems = soup.find_all("div", {"class": "fvLiku"})
    elems = [elem.find("a") for elem in elems if elem.find("a")]
    hrefs = [elem["href"] for elem in elems]

    new_links = [urljoin("https://www.willhaben.at/iad", href) for href in hrefs]
    new_links = [link for link in new_links if link.startswith("https://www.willhaben.at/iad/immobilien/d/eigentumswohnung/")]

    return new_links


def dump_links(links: set[str]) -> None:
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if not os.path.exists(os.path.join(parent_dir, "data")):
        os.makedirs(os.path.join(parent_dir, "data"))

    curr_time = time.strftime("%Y-%m-%d_%H-%M-%S")
    path = os.path.join(parent_dir, "data", f"links_{curr_time}.csv")
    with open(path, "w") as f:
        f.write("\n".join(links))


url = get_init_url()
set_user_agent()

total_count = get_total_ads(url)

links = set()

curr_page = 1
while True:
    response = requests.get(url + f"&page={curr_page}")
    assert response.status_code == 200, f"status code: {response.status_code}"

    new_links = parse_links(response.text)
    links.update(new_links)

    if len(new_links) == 0:
        print("no more pages")
        break

    # print(f"\rpage {str(curr_page).zfill(2)}: {str(len(links)).zfill(3)} / {total_count} (+{len(new_links)})")
    print("progress: " + "%.2f" % (len(links) / total_count * 100) + "%", end="\r")
    curr_page += 1

print("scraped", len(links), "/", total_count, "links")

dump_links(links)
