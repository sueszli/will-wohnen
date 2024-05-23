import os
from typing import List

import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm.asyncio import tqdm
from pathlib import Path

import random
import asyncio
import aiohttp
import backoff
from backoff import on_exception, expo


filename = "links" + time.strftime("%Y-%m-%d_%H-%M-%S") + ".csv"
output_path = Path.cwd() / "output" / filename


def get_init_url() -> str:
    URL = "https://www.willhaben.at/iad/immobilien/eigentumswohnung/eigentumswohnung-angebote?"
    URL += "rows=5"  #  5 ads per page, avoid buffered page loading
    URL += "&areaId=900"  # vienna
    return URL


def get_total_count(url: str) -> int:
    response = requests.get(url)
    assert response.status_code == 200, f"status code: {response.status_code}"

    soup = BeautifulSoup(response.text, "html.parser")
    elem = soup.find("h1", {"data-testid": "result-list-title"})
    assert elem
    return int(elem.text.split()[0].replace(".", ""))


def parse_page(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")

    elems = soup.find_all("div", {"class": "fvLiku"})
    is_valid_elem = lambda elem: elem.find("a") and elem.find("a").has_attr("href")
    hrefs = [elem.find("a")["href"] for elem in elems if is_valid_elem(elem)]

    hrefs = [href for href in hrefs if href.startswith("/iad/immobilien/d/eigentumswohnung/")]
    hrefs = [urljoin("https://www.willhaben.at/iad", href) for href in hrefs]
    return hrefs


# @on_exception(expo, (aiohttp.ClientError, AssertionError), max_tries=3)
async def fetch_async(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
    # await asyncio.sleep(random.uniform(0.125, 1))

    async with aiohttp.ClientSession(raise_for_status=True, headers=headers) as session:

        # await asyncio.sleep(random.uniform(0.125, 1))

        async with session.get(url) as response:

            # await asyncio.sleep(random.uniform(0.125, 1))

            assert response.status == 200, f"status code: {response.status}"
            resp = await response.text()
            print(resp)
            return resp


async def main():
    url = get_init_url()
    total_count = get_total_count(url)
    print(f"url: {url}")
    print(f"total ads: {total_count}")

    tasks = [fetch_async(url + f"&page={i}") for i in range(1, total_count // 5 + 2)]
    results = await tqdm.gather(*tasks)

    # print("parsing all links...")
    # links = set()
    # for result in results:
    #     new_links = parse_page(result)
    #     links.update(new_links)


if __name__ == "__main__":
    asyncio.run(main())
