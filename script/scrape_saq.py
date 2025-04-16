import time
import os
import requests
import json
from bs4 import BeautifulSoup
from zenrows import ZenRowsClient
from tqdm import tqdm
import re
import argparse
import sys
import asyncio
from aiohttp import ClientError, ClientTimeout
from datetime import datetime

DATADIR = "../data/saq/products"
PROGRESS_FILE = os.path.join(DATADIR, "scraped_urls.json")
os.makedirs(DATADIR, exist_ok=True)

ZENROWS_API_KEY = "4974bf17a231d52157b7e296435f98c61fcc80e7"
client = ZenRowsClient(ZENROWS_API_KEY)


MAX_RETRIES = 5
INITIAL_DELAY = 1


def extract_price(soup):
    price_tag = soup.find("span", {"data-price-type": "finalPrice"})
    if price_tag and price_tag.has_attr("data-price-amount"):
        return float(price_tag["data-price-amount"])
    return None


def extract_breadcrumb(script_text):
    match = re.search(r'"ecomm_category":\s*"([^"]+)"', script_text)
    return match.group(1) if match else None

def extract_product_details(soup):
    details = {}
    for li in soup.select("ul.list-attributs li"):
        key = li.find("span").get_text(strip=True)
        val = li.find("strong").get_text(strip=True)
        details[key] = val
    return details

def extract_tasting_notes(soup):
    notes = {}
    tasting_section = soup.select_one("#tasting")
    if tasting_section:
        for li in tasting_section.select("ul.tasting-container li"):
            label = li.find("span", class_="in-line") or li.find("span")
            value = li.find("strong")
            if label and value:
                notes[label.get_text(strip=True)] = value.get_text(strip=True)
    return notes

def extract_pairings(soup):
    pairings = []
    for item in soup.select("#product-data-item-product-recipe li.item.product"):
        name = item.select_one(".product-item-name span")
        url = item.select_one("a.product-item-link")["href"] if item.select_one("a.product-item-link") else None
        pairings.append({"name": name.text.strip(), "url": url})
    return pairings

def extract_product_name(soup):
    h1 = soup.select_one("h1.page-title")
    return h1.get_text(strip=True) if h1 else None




def scrape_saq_product(url):
    r = client.get(url, params={"js_render": "true"})
    soup = BeautifulSoup(r.text, "html.parser")

    # Extract components
    script_tags = soup.find_all("script")
    product_name = extract_product_name(soup)
    price = extract_price(soup)
    breadcrumb = next((extract_breadcrumb(s.text) for s in script_tags if "ecomm_category" in s.text), None)
    product_details = extract_product_details(soup)
    tasting_notes = extract_tasting_notes(soup)
    pairings = extract_pairings(soup)

    return {
        "url": url,
        "product_name": product_name,
        "price": price,
        "breadcrumb": breadcrumb,
        "product_details": product_details,
        "tasting_notes": tasting_notes,
        "pairings": pairings
    }

def load_scraped_urls():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_scraped_urls(scraped):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(scraped), f, indent=2)


def scrape_with_retries(url):
    delay = INITIAL_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return scrape_saq_product(url)
        except Exception as e:
            if "timeout" in str(e).lower():
                print(f"[Timeout] {url} retry {attempt}/{MAX_RETRIES} in {delay:.1f}s")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"[Skip] {url}: {str(e)}")
                return None
    print(f"[Failed] {url} after {MAX_RETRIES} retries")
    return None



def main(sitemap: str, batch_size: int = 20):
    print(f"Processing sitemap: {sitemap}")
    
    with open(sitemap, "r", encoding="utf-8") as f:
        sitemap_content = f.read()
    
    soup = BeautifulSoup(sitemap_content, "xml")
    urls = [loc.text for loc in soup.find_all("loc")]
    filtered_urls = [url for url in urls if not url.startswith("https://www.saq.com/media/")]

    scraped_urls = load_scraped_urls()
    urls_to_scrape = [url for url in filtered_urls if url not in scraped_urls]

    total = len(urls_to_scrape)
    batch = []
    batch_id = len(os.listdir(DATADIR))  # crude guess
    if batch_id > 0:
        batch_id = max([int(f.split("_")[1].split(".")[0]) for f in os.listdir(DATADIR) if f.startswith("batch_") and f.endswith(".json")], default=0) + 1
    else:
        batch_id = 1

    for i, url in enumerate(tqdm(urls_to_scrape, desc="Scraping SAQ products")):
        product_data = scrape_with_retries(url)
        if product_data:
            batch.append(product_data)
            scraped_urls.add(url)
        time.sleep(INITIAL_DELAY)

        if len(batch) == batch_size or (i + 1) == total:
            batch_file = os.path.join(DATADIR, f"batch_{batch_id}.json")
            with open(batch_file, "w", encoding="utf-8") as f:
                json.dump(batch, f, indent=2, ensure_ascii=False)
            save_scraped_urls(scraped_urls)
            batch = []
            batch_id += 1

        if (i + 1) % 100 == 0:
            print(f"{i + 1} products processed...")

    save_scraped_urls(scraped_urls)


    


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a sitemap.")
    parser.add_argument("sitemap", help="URL or path to sitemap")
    args = parser.parse_args()

    try:
        main(args.sitemap, batch_size=20)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
