import sys
import os
import time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.scraping.robots_utils import can_fetch
from src.utils.logger import get_logger

USER_AGENT = "ResearchBot/1.0"
REQUEST_DELAY = 1.5

logger = get_logger(__name__)

def scrape_ajax_endpoint(url: str, params: dict = None):
    """
    Scrapes a JSON endpoint directly using requests.
    
    Args:
        url (str): The API endpoint URL.
        params (dict): A dictionary of query parameters.
        
    Returns:
        list: A list of dictionaries containing the extracted data, or an empty list if scraping fails.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "X-Requested-With": "XMLHttpRequest"
    }
    
    try:
        logger.info(f"Requesting data from AJAX endpoint: {url} with params: {params}")
        time.sleep(REQUEST_DELAY)
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error requesting AJAX endpoint {url}: {e}")
        return []

def scrape_dynamic_url(url: str):
    """
    Scrapes a single URL using a headless browser after checking robots.txt.
    This should be used as a fallback.
    
    Args:
        url (str): The URL to scrape.
        
    Returns:
        BeautifulSoup: The parsed HTML content of the page, or None if scraping is not allowed or an error occurs.
    """
    if not can_fetch(url, USER_AGENT):
        logger.warning(f"Scraping not allowed for {url} by robots.txt.")
        return None

    chrome_options = Options()
    chrome_options.add_argument(f"user-agent={USER_AGENT}")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    
    driver = None
    try:
        logger.info(f"Scraping dynamic content from {url}...")
        
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        time.sleep(REQUEST_DELAY)
        driver.get(url)
        
        # Wait for dynamic content to load if necessary.
        time.sleep(5) 
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        return soup
        
    except Exception as e:
        logger.error(f"Error scraping dynamic content from {url}: {e}")
        return None
    finally:
        if driver:
            driver.quit()

if __name__ == '__main__':
    # Example usage for scraping the AJAX endpoint
    ajax_url = "https://www.scrapethissite.com/pages/ajax-javascript/"
    
    # The actual endpoint is the same URL with a query parameter
    # ?ajax=true&year=<year>
    
    year_to_scrape = 2015
    params = {
        "ajax": "true",
        "year": year_to_scrape
    }
    
    film_data = scrape_ajax_endpoint(ajax_url, params=params)
    
    if film_data:
        print(f"Successfully scraped {len(film_data)} films for the year {year_to_scrape}:")
        # Print the first 5 films
        for film in film_data[:5]:
            print(f"- Title: {film['title']}, Year: {film['year']}, Awards: {film['awards']}")
    else:
        logger.info("No data was scraped from the AJAX endpoint.")

    # Example of fallback using Selenium (if needed)
    # soup = scrape_dynamic_url(ajax_url)
    # if soup:
    #     print("\nSelenium fallback got page title:", soup.title.string)
