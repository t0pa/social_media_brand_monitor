import os
import requests
import time
import json
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


API_KEY = os.getenv("API_KEY")
BASE_URL = "https://newsapi.org/v2/everything"

def fetch_brand_articles(brand, pages=3, save_dir="data/raw/api", retry_limit=3, sleep_time=1, page_size=20):
    os.makedirs(save_dir, exist_ok=True)
    all_articles = []
    for page in range(1, pages + 1):
        params = {
            "apiKey": API_KEY,
            "q": brand,
            "sortBy": "publishedAt",
            "page": page,
            "pageSize": page_size
        }
        for attempt in range(retry_limit):
            try:
                response = requests.get(BASE_URL, params=params)
                response.raise_for_status()
                data = response.json()
                # Save raw data for this page
                with open(f"{save_dir}/newsapi_{brand}_page_{page}.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                articles = data.get("articles", [])
                all_articles.extend(articles)
                break  # Success, break retry loop
            except requests.exceptions.RequestException as e:
                print(f"Error fetching page {page} (attempt {attempt+1}): {e}")
                if attempt < retry_limit - 1:
                    time.sleep(sleep_time)
                else:
                    print(f"Failed to fetch page {page} after {retry_limit} attempts.")
    return all_articles

if __name__ == "__main__":
    brand = "Apple"  # Change to your brand of interest
    articles = fetch_brand_articles(brand, pages=3)
    print(f"Fetched {len(articles)} articles for brand '{brand}'.")
    if articles:
        print("Here are some article titles:")
        for article in articles[:5]:
            print(article.get("title", "No Title"))
