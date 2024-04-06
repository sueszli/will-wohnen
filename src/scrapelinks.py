import os
import json
from typing import List

import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

import asyncio
import aiohttp


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


def get_total_ads(url: str) -> int:
    response = requests.get(url)
    assert response.status_code == 200, f"status code: {response.status_code}"

    soup = BeautifulSoup(response.text, "html.parser")
    elem = soup.find("h1", {"data-testid": "result-list-title"})
    assert elem
    return int(elem.text.split()[0])


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


async def main():
    os.environ["HTTP_USER_AGENT"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"

    async def fetch_async(url: str) -> str:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                assert response.status == 200, f"status code: {response.status}"
                return await response.text()

    url = get_init_url()
    total_count = get_total_ads(url)

    print("running async fetches...")
    tasks = [fetch_async(url + f"&page={i}") for i in range(1, total_count // 5 + 1)]
    results = await asyncio.gather(*tasks)

    print("parsing all links...")
    links = set()
    for result in results:
        new_links = parse_links(result)
        links.update(new_links)

    print(f"dumping {len(links)}/{total_count} links to file...")
    dump_links(links)


asyncio.run(main())
