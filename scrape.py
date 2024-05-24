import os
from typing import List
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path
import itertools
import functools

from tqdm import tqdm
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync


output_path = Path.cwd() / "data" / ("links_" + time.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")


def get_num_ads(url: str) -> int:
    response = requests.get(url)
    assert response.status_code == 200, f"status code: {response.status_code}"
    soup = BeautifulSoup(response.text, "html.parser")
    elem = soup.find("h1", {"data-testid": "result-list-title"})
    assert elem
    count: int = int(elem.text.split()[0].replace(".", ""))
    return count


def get_urls() -> List[str]:
    url = "https://www.willhaben.at/iad/immobilien/eigentumswohnung/eigentumswohnung-angebote?"
    ads_per_page = 90
    url += f"rows={ads_per_page}"
    url += "&areaId=900"  # vienna
    num_ads = get_num_ads(url)
    num_pages = num_ads // ads_per_page + 1

    urls = [url + f"&page={i}" for i in range(1, num_pages + 1)]
    return urls


urls = get_urls()
with sync_playwright() as p:

    browser = p.firefox.launch()
    page = browser.new_page()
    stealth_sync(page)
    page.goto("https://www.google.com/")
    page.screenshot(path=f"example-hi.png")
    browser.close()
