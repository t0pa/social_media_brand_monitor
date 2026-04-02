import sys
import os
import requests
from bs4 import BeautifulSoup
import time

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.scraping.robots_utils import can_fetch
from src.utils.logger import get_logger

USER_AGENT = "ResearchBot/1.0"
REQUEST_DELAY = 1.5

logger = get_logger(__name__)

def scrape_url(url: str):
    """
    Scrapes a single URL, extracts structured data, and returns it.
    
    Args:
        url (str): The URL to scrape.
        
    Returns:
        list: A list of dictionaries containing the extracted data, or an empty list if scraping fails.
    """
    if not can_fetch(url, USER_AGENT):
        logger.warning(f"Scraping not allowed for {url} by robots.txt.")
        return []
    
    headers = {
        "User-Agent": USER_AGENT
    }
    
    try:
        logger.info(f"Scraping {url}...")
        time.sleep(REQUEST_DELAY)
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Example of extracting structured data from a table
        # This will need to be adapted for the actual structure of the target page
        data = []
        # Find all book containers
        books = soup.find_all('article', class_='product_pod')
        
        for book in books:
            # Extract the title from the 'title' attribute of the link within the h3 tag
            title_element = book.find('h3').find('a')
            title = title_element['title'] if title_element else "No Title Found"
            
            # Extract the price from the <p> tag with the class 'price_color'
            price_element = book.find('p', class_='price_color')
            price = price_element.get_text(strip=True) if price_element else "No Price Found"
            
            record = {
                "title": title,
                "price": price,
            }
            data.append(record)
        return data
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error scraping {url}: {e}")
        return []

def scrape_multiple_pages(base_url: str, num_pages: int):
    """
    Scrapes multiple pages of a website.
    
    Args:
        base_url (str): The base URL of the website.
        num_pages (int): The number of pages to scrape.
        
    Returns:
        list: A list of all scraped records.
    """
    all_data = []
    for page_num in range(1, num_pages + 1):
        # Construct the URL for each page
        if page_num == 1:
            url = f"{base_url}index.html"
        else:
            url = f"{base_url}catalogue/page-{page_num}.html"
        
        logger.info(f"Scraping page {page_num}: {url}")
        data = scrape_url(url)
        if data:
            all_data.extend(data)
        else:
            logger.warning(f"No data scraped from page {page_num}. Stopping.")
            break # Stop if a page has no data
            
    return all_data

if __name__ == '__main__':
    # Example usage with a URL that has a table and allows scraping
    base_scrape_url = "https://books.toscrape.com/"
    scraped_data = scrape_multiple_pages(base_scrape_url, num_pages=3)
    
    if scraped_data:
        # Print the first 5 records
        print(f"Successfully scraped {len(scraped_data)} records.")
        print("Sample of scraped data:")
        for item in scraped_data[:5]:
            print(item)
    else:
        logger.info("No data was scraped. This could be due to scraping restrictions or incorrect selectors.")
