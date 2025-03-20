import time
import os
import requests
import json
from bs4 import BeautifulSoup
from zenrows import ZenRowsClient
import tqdm

# Load product data
with open("../saq_cat.json") as f:
    data = json.load(f)


client = ZenRowsClient("4974bf17a231d52157b7e296435f98c61fcc80e7")
skipped_codes = []  # List to store product codes that failed

def get_saq_image(product_code, retries=3, delay=1, save_dir="../images"):
    os.makedirs(save_dir, exist_ok=True)  # Ensure directory exists
    image_path = os.path.join(save_dir, f"{product_code}.jpg")

    # Skip download if image already exists
    if os.path.exists(image_path):
        print(f"Image already exists for {product_code}, skipping...")
        return image_path

    url = f"https://www.saq.com/fr/{product_code}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    for attempt in range(retries):
        try:
            response = client.get(url, headers=headers, timeout=5)
            
            if response.status_code == 404:
                print(f"404 Not Found: {product_code} - Skipping retries.")
                skipped_codes.append(product_code)
                return None  # Exit immediately

            if response.status_code == 200:
                break  # Success, exit loop

            print(f"Attempt {attempt + 1}: Received {response.status_code}, retrying...")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1}: Error {e}, retrying...")
        time.sleep(delay * (2 ** attempt))  # Exponential backoff
    else:
        print(f"Failed to fetch page after {retries} retries.")
        skipped_codes.append(product_code)
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    img_tag = soup.select_one('a.MagicZoom')

    if img_tag and 'href' in img_tag.attrs:
        image_url = img_tag['href']

        try:
            img_response = client.get(image_url, headers=headers, timeout=5)
            
            if img_response.status_code == 404:
                print(f"404 Not Found: Image for {product_code} - Skipping.")
                skipped_codes.append(product_code)
                return None  # Exit immediately

            if img_response.status_code == 200:
                with open(image_path, "wb") as f:
                    f.write(img_response.content)
                return image_path
            else:
                print(f"Failed to download image, status code: {img_response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading image: {e}")

    print(f"Image not found for product {product_code}")
    skipped_codes.append(product_code)
    return None

def save_skipped_codes(filename="skipped_codes.txt"):
    if skipped_codes:
        with open(filename, "w") as f:
            f.write("\n".join(skipped_codes))
        print(f"Skipped codes saved to {filename}")
    else:
        print("No skipped codes.")

# Process images
for item in tqdm.tqdm(data, desc="Fetching images", unit="item"):
    if item.get('code'):
        get_saq_image(item['code'])

# Save failed downloads
save_skipped_codes()
