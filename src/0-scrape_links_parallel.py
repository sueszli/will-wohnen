"""
too fast, will get you rate limited
"""

import asyncio
import time
from pathlib import Path
from typing import List
from urllib.parse import urljoin

import aiohttp
import requests
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm


def get_init_url() -> str:
    URL = "https://www.willhaben.at/iad/immobilien/eigentumswohnung/eigentumswohnung-angebote?"
    URL += "rows=5"  #  5 ads per page, avoid buffered page loading
    URL += "&areaId=900"  # vienna
    # URL += "" if not CONFIG["suburbs"] else "&areaId=312&areaId=319&areaId=321"  # st.pölten land, tulln, korneuburg
    # URL += f"&PRICE_FROM={CONFIG['price_range']['from']}"
    # URL += f"&PRICE_TO={CONFIG['price_range']['to']}"
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
    url = get_init_url()
    total_count = get_total_count(url)
    print(f"url: {url}")
    print(f"total ads: {total_count}")

    tasks = [fetch_async(url + f"&page={i}") for i in range(1, total_count // 5 + 2)]
    results = await tqdm.gather(*tasks, desc="fetching")

    links = set()
    for result in tqdm(results, desc="parsing"):
        new_links = parse_links(result)
        links.update(new_links)

    filepath = Path.cwd() / "data" / ("links_" + time.strftime("%Y-%m-%d_%H-%M-%S") + ".txt")
    with open(filepath, "w") as f:
        f.write("\n".join(links))


if __name__ == "__main__":
    asyncio.run(main())
