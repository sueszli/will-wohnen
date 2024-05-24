import os
from typing import List
import time
import requests
from urllib.parse import urljoin
from pathlib import Path
import random

from tqdm import tqdm
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
from bs4 import BeautifulSoup


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
    browser = p.firefox.launch(headless=False)
    page = browser.new_page()

    # reduce bot detection
    stealth_sync(page)

    is_fst_page = True

    urls = tqdm(urls)
    for url in urls:
        page.goto(url)

        # reduce cookies
        if is_fst_page:
            page.wait_for_selector("[id=didomi-notice-disagree-button]")
            page.click("[id=didomi-notice-disagree-button]")
            is_fst_page = False

        # slowly scroll to bottom of page for elems to load
        for _ in range(100):
            page.evaluate(f"window.scrollBy(0, 200)")
            time.sleep(random.uniform(0.1, 0.2))
        time.sleep(random.uniform(0.5, 1.5))
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

        # load all ad elems
        elements = page.query_selector_all("[data-testid^=search-result-entry-header]")
        urls.set_description(f"read {len(elements)}/90 links on page")

        # extract links

    browser.close()
