"""
too fast, will get you rate limited
"""

import asyncio
import csv
import glob
import json
import random
from pathlib import Path
from typing import List

import aiohttp
from backoff import expo, on_exception
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm


def load_links() -> List[str]:
    datapath = glob.glob(str(Path("./data/*.csv")))
    datapath = list(filter(lambda p: Path(p).name.startswith("links_") and Path(p).name.endswith(".csv"), datapath))
    assert len(datapath) > 0
    datapath.sort()
    datapath = datapath[-1]

    csvfile = csv.reader(open(datapath, "r"))
    header = next(csvfile)
    body = list(csvfile)
    links = list(map(lambda row: row[0], body))
    return links


@on_exception(expo, (aiohttp.ClientError, AssertionError), max_tries=3)  # retry on exceptions
async def fetch_async(url: str) -> str:
    await asyncio.sleep(random.uniform(0.125, 1))  # throttle requests

    async with aiohttp.ClientSession(raise_for_status=True) as session:
        await asyncio.sleep(random.uniform(0.125, 1))  # throttle requests

        async with session.get(url) as response:
            assert response.status == 200, f"status code: {response.status}"  # check status
            await asyncio.sleep(random.uniform(0.125, 1))  # throttle requests

            return await response.text()


def parse_page(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    data = {}

    # url

    data["url"] = url

    # title

    elem = soup.find("h1", {"data-testid": "ad-detail-header"})
    data["title"] = elem.text if elem else None

    # last update section

    elem = soup.find("span", {"data-testid": "ad-detail-ad-edit-date-top"})
    data["last_update"] = elem.text if elem else None

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


async def main():
    links = load_links()
    print(f"total links: {len(links)}")

    print("running async fetches...")
    for url in tqdm(links):
        html = await fetch_async(url)
        data = parse_page(url, html)
        print(json.dumps(data, indent=4, ensure_ascii=False))

    # tasks = [fetch_async(url) for url in links]
    # results = await tqdm.gather(*tasks)

    # print("parsing all pages...")
    # pages: List[dict] = [parse_page(url, html) for url, html in zip(links, results)]
    # flat_pages: dict = dict(enumerate(pages))

    # print("dumping all pages...")
    # # print(json.dumps(flat_pages, indent=4, ensure_ascii=False))
    # dump_pages(flat_pages)


if __name__ == "__main__":
    asyncio.run(main())
