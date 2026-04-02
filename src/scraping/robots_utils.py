import urllib.robotparser
from urllib.parse import urljoin

def get_robots_txt_url(url):
    """
    Get the URL for the robots.txt file from a base URL.
    """
    return urljoin(url, "/robots.txt")

def can_fetch(url, user_agent):
    """
    Check if the user agent is allowed to fetch the URL according to robots.txt.
    """
    rp = urllib.robotparser.RobotFileParser()
    robots_url = get_robots_txt_url(url)
    rp.set_url(robots_url)
    try:
        rp.read()
        # Check if the user agent can fetch the specific URL
        if rp.can_fetch(user_agent, url):
            # Check for any crawl-delay directive
            delay = rp.crawl_delay(user_agent)
            if delay:
                print(f"Crawl-delay of {delay} seconds requested by {robots_url}")
            return True
        else:
            return False
    except Exception as e:
        print(f"Error reading or parsing robots.txt from {robots_url}: {e}")
        # Default to not fetching if robots.txt is unreadable or has errors
        return False

if __name__ == '__main__':
    # Example usage:
    test_url = "https://books.toscrape.com/"
    user_agent = "ResearchBot/1.0"
    
    print(f"Checking if '{user_agent}' can fetch '{test_url}'...")
    
    if can_fetch(test_url, user_agent):
        print("Scraping is allowed.")
    else:
        print("Scraping is not allowed.")





