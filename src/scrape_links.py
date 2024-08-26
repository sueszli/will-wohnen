import csv
import random
import time
from pathlib import Path
from typing import List

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
from tqdm import tqdm


def get_urls() -> List[str]:
    url = "https://www.willhaben.at/iad/immobilien/eigentumswohnung/eigentumswohnung-angebote?"
    ads_per_page = 90
    url += f"rows={ads_per_page}"
    url += "&areaId=900"  # vienna

    def get_num_ads(url: str) -> int:
        response = requests.get(url)
        assert response.status_code == 200, f"status code: {response.status_code}"
        soup = BeautifulSoup(response.text, "html.parser")
        elem = soup.find("h1", {"data-testid": "result-list-title"})
        assert elem
        count: int = int(elem.text.split()[0].replace(".", ""))
        return count

    num_ads = get_num_ads(url)
    num_pages = num_ads // ads_per_page + 1

    urls = [url + f"&page={i}" for i in range(1, num_pages + 1)]
    return urls


def extract_content(elem) -> dict:
    # txt = elem.inner_text()

    content = {
        "link": None,
        "title": None,
        "address": None,
        "price": None,
        "m2": None,
        "num_rooms": None,
        "type": None,
    }

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

    # clean up
    content = {k: " ".join(v.split()).strip() if v else None for k, v in content.items()}
    return content


def write_to_csv(content: dict, output_path: Path):
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=content.keys(), strict=True, quoting=csv.QUOTE_NONNUMERIC)
        if f.tell() == 0:
            writer.writeheader()
        writer.writerow(content)


def main():
    output_path = Path.cwd() / "data" / ("links_" + time.strftime("%Y-%m-%d_%H-%M-%S") + ".csv")

    urls = get_urls()

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()
        stealth_sync(page)

        is_fst_page = True

        for url in tqdm(urls):
            page.goto(url)

            # no cookies
            if is_fst_page:
                page.wait_for_selector("[id=didomi-notice-disagree-button]")
                page.click("[id=didomi-notice-disagree-button]")
                is_fst_page = False

            # slowly scroll to bottom of page for all elems to load
            for _ in range(100):
                page.evaluate(f"window.scrollBy(0, 200)")
                time.sleep(random.uniform(0.1, 0.2))
            time.sleep(random.uniform(0.5, 1.5))
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(random.uniform(1.5, 2.5))

            # read advertisements
            page_num = url.split("=")[-1]
            elements = page.query_selector_all("[data-testid^=search-result-entry-header]")
            if len(elements) < 90:
                print(f"warning: found {len(elements)}/90 links on page {page_num}, url: {url}")

            # store content
            for elem in elements:
                content: dict = extract_content(elem)
                write_to_csv(content, output_path)

        browser.close()


if __name__ == "__main__":
    main()