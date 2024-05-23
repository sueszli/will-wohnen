import os
from typing import List

import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm.asyncio import tqdm

import random
import asyncio
import aiohttp
import backoff
from backoff import on_exception, expo


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


def parse_links(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")

    elems = soup.find_all("div", {"class": "fvLiku"})
    is_valid_elem = lambda elem: elem.find("a") and elem.find("a").has_attr("href")
    hrefs = [elem.find("a")["href"] for elem in elems if is_valid_elem(elem)]

    hrefs = [href for href in hrefs if href.startswith("/iad/immobilien/d/eigentumswohnung/")]
    hrefs = [urljoin("https://www.willhaben.at/iad", href) for href in hrefs]
    return hrefs


def dump_links(links: set[str]) -> None:
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(parent_dir, "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    curr_time = time.strftime("%Y-%m-%d_%H-%M-%S")
    path = os.path.join(data_dir, f"links_{curr_time}.csv")

    with open(path, "w") as f:
        f.write("\n".join(links))


# @on_exception(expo, (aiohttp.ClientError, AssertionError), max_tries=3)  # retry on exceptions
async def fetch_async(url: str) -> str:
    global counter

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
        "Accept-Language": "en-US,en;q=0.5",
    }
    get_throttle = lambda: random.uniform(0.125, 1)
    await asyncio.sleep(get_throttle())

    return "yo"

    # await asyncio.sleep(get_throttle())

    # async with aiohttp.ClientSession(raise_for_status=True) as session:

    #     await asyncio.sleep(get_throttle())

    #     async with session.get(url, headers=headers) as response:
    #         assert response.status == 200, f"status code: {response.status}"

    #         await asyncio.sleep(get_throttle())

    #         counter += 1
    #         print(f"progress: {counter}/{total_count}", end="\r")
    #         return await response.text()


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
    #     new_links = parse_links(result)
    #     links.update(new_links)

    # print(f"dumping {len(links)}/{total_count} links to file...")
    # dump_links(links)


if __name__ == "__main__":
    asyncio.run(main())
