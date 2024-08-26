import asyncio
import csv
import glob
import json
import random
import time
from pathlib import Path

import aiohttp
from backoff import expo, on_exception
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm


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
    data = {"url": url}

    def safe_extract(elem):
        return elem.text.strip() if elem else None

    # title
    data["title"] = safe_extract(soup.find("h1", {"data-testid": "ad-detail-header"}))

    # last update section
    data["last_update"] = safe_extract(soup.find("span", {"data-testid": "ad-detail-ad-edit-date-top"}))

    # price info section
    price_info = soup.find("div", {"data-testid": "price-information-box"})
    if price_info:
        price_details = price_info.find("div")
        if price_details:
            for elem in price_details.find_all("div"):
                spans = elem.find_all("span")
                if len(spans) == 2:
                    data[spans[0].text.strip()] = spans[1].text.strip()

    # address section
    data["address"] = safe_extract(soup.find("div", {"data-testid": "object-location-address"}))

    # Attributes section
    for elem in soup.find_all("li", {"data-testid": "attribute-item"}):
        key = safe_extract(elem.find("div", {"data-testid": "attribute-title"}))
        value = safe_extract(elem.find("div", {"data-testid": "attribute-value"}))
        if key and value:
            data[key] = value

    # energy certificate section
    data["energy_certificate"] = safe_extract(soup.find("div", {"data-testid": "energy-pass-box"}))

    # description sections
    description_sections = [("description_general", "ad-description-Objektbeschreibung"), ("description_location", "ad-description-Lage"), ("description_equipment", "ad-description-Ausstattung"), ("description_additional", "ad-description-Zusatzinformationen"), ("description_price", "ad-description-Preis und Detailinformation")]
    for key, testid in description_sections:
        data[key] = safe_extract(soup.find("div", {"data-testid": testid}))

    return data


async def main():
    outputpath = Path.cwd() / "data" / ("pages_" + time.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")

    inputpath = glob.glob(str(Path("./data/*.csv")))
    inputpath = list(filter(lambda p: Path(p).name.startswith("links_") and Path(p).name.endswith(".csv"), inputpath))
    assert len(inputpath) > 0
    inputpath.sort()
    inputpath = inputpath[-1]

    inputfile = csv.reader(open(inputpath, "r"))
    header = next(inputfile)

    for row in tqdm(inputfile):
        url = row[0]
        html = await fetch_async(url)
        data = parse_page(url, html)
        print(json.dumps(data, indent=4, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
