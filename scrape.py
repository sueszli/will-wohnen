import os
from typing import List
import time
import requests
from pathlib import Path
import random
import csv

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


def extract_content(elem) -> dict:
    # txt = elem.inner_text()

    content = {}

    content["link"] = "https://www.willhaben.at" + str(elem.get_attribute("href"))

    title_elem = elem.query_selector("h3[class^=Text-sc-]")
    content["title"] = title_elem.inner_text() if title_elem else None

    address_elem = elem.query_selector("[data-testid^=search-result-entry-subheader]")
    content["address"] = address_elem.inner_text() if address_elem else None

    price_elem = elem.query_selector("[data-testid^=search-result-entry-price]")
    content["price"] = price_elem.inner_text() if price_elem else None

    teaser_elems = elem.query_selector_all("[data-testid^=search-result-entry-teaser-attributes]")
    for teaser_elem in teaser_elems:
        data_testid = teaser_elem.get_attribute("data-testid")
        if not data_testid:
            continue
        if data_testid.endswith("-0"):
            content["m2"] = teaser_elem.inner_text()
        elif data_testid.endswith("-1"):
            content["num_rooms"] = teaser_elem.inner_text()
        elif data_testid.endswith("-2"):
            content["type"] = teaser_elem.inner_text()

    return content


def write_to_csv(content: dict):
    delim = ";"
    for key, value in content.items():
        if value:
            content[key] = value.replace(delim, "")

    with open(output_path, "a") as f:
        writer = csv.DictWriter(f, fieldnames=content.keys(), delimiter=delim)
        if f.tell() == 0:
            writer.writeheader()
        writer.writerow(content)


urls = get_urls()
with sync_playwright() as p:
    browser = p.firefox.launch(headless=True)
    page = browser.new_page()

    # reduce bot detection
    stealth_sync(page)

    is_fst_page = True

    for url in tqdm(urls):
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
        page_num = url.split("=")[-1]
        elements = page.query_selector_all("[data-testid^=search-result-entry-header]")
        if len(elements) < 90:
            print(f"warning: found {len(elements)}/90 links on page {page_num}, url: {url}")

        # store content
        for elem in elements:
            content: dict = extract_content(elem)
            write_to_csv(content)

    browser.close()
