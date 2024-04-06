import os
import json
from typing import List

import time

import asyncio
import aiohttp

from bs4 import BeautifulSoup


def load_links() -> List[str]:
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(parent_dir, "data")
    assert os.path.exists(data_dir), "data directory not found"

    data_files = [f for f in os.listdir(data_dir) if f.startswith("links_")]
    assert data_files, "no data files found"

    latest_file = max(data_files)
    path = os.path.join(data_dir, latest_file)
    links = open(path).read().splitlines()
    return links


def parse_page(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    data = {}

    # url

    data["url"] = url

    # title

    elem = soup.find("h1", {"data-testid": "ad-detail-header"})
    data["title"] = elem.text if elem else None

    # last change section

    elem = soup.find("span", {"data-testid": "ad-detail-ad-edit-date-top"})
    data["last_change"] = elem.text if elem else None

    # price info section

    price_info = {}

    elem = soup.find("div", {"data-testid": "price-information-box"})
    assert elem
    elem = elem.find("div")
    assert elem
    elems = elem.find_all("div")  # type: ignore
    assert len(elems) > 0
    for elem in elems:
        spans = elem.find_all("span")
        fst = spans[0].text
        snd = spans[1].text
        price_info[fst] = snd

    data["price_info"] = price_info

    # address section

    elem = soup.find("div", {"data-testid": "object-location-address"})
    data["address"] = elem.text if elem else None

    # attributes section

    attrs = {}

    elems = soup.find_all("li", {"data-testid": "attribute-item"})
    for elem in elems:
        key_elem = elem.find("div", {"data-testid": "attribute-title"})
        key = key_elem.text if key_elem else None
        value_elem = elem.find("div", {"data-testid": "attribute-value"})
        value = value_elem.text if value_elem else None
        if key and value:
            attrs[key] = value

    data["attributes"] = attrs

    # energy certificate section

    elem = soup.find("div", {"data-testid": "energy-pass-box"})
    data["energy_certificate"] = elem.text if elem else None

    # description sections

    desc = {}

    elem = soup.find("div", {"data-testid": "ad-description-Objektbeschreibung"})
    desc["description_general"] = elem.text if elem else None

    elem = soup.find("div", {"data-testid": "ad-description-Lage"})
    desc["description_location"] = elem.text if elem else None

    elem = soup.find("div", {"data-testid": "ad-description-Ausstattung"})
    desc["description_equipment"] = elem.text if elem else None

    elem = soup.find("div", {"data-testid": "ad-description-Zusatzinformationen"})
    desc["description_additional"] = elem.text if elem else None

    elem = soup.find("div", {"data-testid": "ad-description-Preis und Detailinformation"})
    desc["description_price"] = elem.text if elem else None

    data["descriptions"] = desc

    return data


def dump_pages(pages: dict) -> None:
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if not os.path.exists(os.path.join(parent_dir, "data")):
        os.makedirs(os.path.join(parent_dir, "data"))

    curr_time = time.strftime("%Y-%m-%d_%H-%M-%S")
    path = os.path.join(parent_dir, "data", f"pages_{curr_time}.json")

    f = open(path, "w")
    json.dump(pages, f, indent=4, ensure_ascii=False)


async def fetch_async(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
        "Accept-Language": "en-US,en;q=0.5",
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            assert response.status == 200, f"status code: {response.status}"
            return await response.text()


async def main():
    links = load_links()

    print("running async fetches...")

    # tasks = [fetch_async(url) for url in links]

    # avoid being rate limited
    tasks = []
    for i, url in enumerate(links):
        tasks.append(fetch_async(url))
        print(f"fetching page {i + 1}/{len(links)}")
        if i % 5 == 0:
            await asyncio.sleep(0.125)

    results = await asyncio.gather(*tasks)

    print("parsing all pages...")
    pages: List[dict] = [parse_page(url, html) for url, html in zip(links, results)]
    flat_pages: dict = dict(enumerate(pages))

    print("dumping all pages...")
    dump_pages(flat_pages)


if __name__ == "__main__":
    asyncio.run(main())
