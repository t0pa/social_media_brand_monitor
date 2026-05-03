import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.utils.logger import get_logger
from src.api.client import fetch_brand_articles
from src.parsing.parsers import (
    parse_json_files, 
    parse_csv_file, 
    parse_xml_file, 
    parse_pdf_files, 
    parse_docx_files, 
    parse_excel_files
)
from src.ocr.ocr_utils import extract_text_from_image, extract_text_from_pdf
from src.storage.mongo import save_to_mongo
from src.scraping.scraper import scrape_multiple_pages
from src.scraping.dynamic_scraper import scrape_ajax_endpoint, scrape_dynamic_url
from src.analytics.numpy_ops import main as run_numpy_demo
from src.analytics.data_loader import run_data_loader_pipeline
from src.analytics.explorer import run_explorer_pipeline
from src.analytics.quality_report import run_quality_pipeline
from src.analytics.regex_ops import run_regex_pipeline
from src.cleaning.clean_pipeline import run_cleaning_pipeline

import pandas as pd

logger = get_logger(__name__)

def run_pipeline():
    brand = "Apple"  # You can change this to your target brand
    logger.info(f"Pipeline started for brand: {brand}")
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # --- Data Fetching and Parsing ---
    logger.info("--- Starting Data Ingestion and Parsing ---")

    # 1. Fetch data from API
    try:
        logger.info("Fetching data from NewsAPI...")
        api_dir = os.path.join(project_root, "data", "raw", "api")
        if not os.path.exists(api_dir): os.makedirs(api_dir)
        fetch_brand_articles(brand, pages=3, save_dir=api_dir)
        logger.info("API data fetched. Parsing JSON files...")
        parse_json_files(api_dir)
    except Exception as e:
        logger.error(f"Error in API data fetching or parsing: {e}")

    # 2. Parse local structured and semi-structured files
    file_parsers = {
        "csv": {"func": parse_csv_file, "mode": "file"},
        "xml": {"func": parse_xml_file, "mode": "file"},
        "pdf": {"func": parse_pdf_files, "mode": "dir"},
        "docx": {"func": parse_docx_files, "mode": "dir"},
        "excel": {"func": parse_excel_files, "mode": "file"}
    }

    for file_type, config in file_parsers.items():
        try:
            logger.info(f"Parsing {file_type.upper()} files...")
            data_dir = os.path.join(project_root, "data", "raw", file_type)
            if not os.path.exists(data_dir):
                logger.warning(f"Directory not found: {data_dir}")
                continue

            parser_func = config["func"]
            if config["mode"] == "dir":
                # This parser handles the whole directory
                parser_func(data_dir)
            else:
                # This parser handles one file at a time
                for filename in os.listdir(data_dir):
                    if filename.lower().endswith(f".{file_type}"):
                        file_path = os.path.join(data_dir, filename)
                        parser_func(file_path)
        except Exception as e:
            logger.error(f"Error parsing {file_type} files: {e}")

    # --- Web Scraping ---
    logger.info("--- Starting Web Scraping ---")
    try:
        base_scrape_url = "https://books.toscrape.com/"
        scraped_data = scrape_multiple_pages(base_scrape_url, num_pages=3) # Scrape 3 pages for demonstration
        if scraped_data:
            logger.info(f"Successfully scraped {len(scraped_data)} records from {base_scrape_url}.")
            for record in scraped_data:
                metadata = {
                    "source": base_scrape_url,
                    "type": "web_scrape",
                    "file_name": None
                }
                save_to_mongo(record, metadata=metadata)
        else:
            logger.warning("No data was scraped. Check scraper configuration and website availability.")
    except Exception as e:
        logger.error(f"An error occurred during the web scraping process: {e}")

    # --- Dynamic Content Handling ---
    logger.info("--- Starting Dynamic Content Handling ---")
    try:
        # 1. Scrape a known AJAX endpoint
        ajax_url = "https://www.scrapethissite.com/pages/ajax-javascript/"
        year_to_scrape = 2015
        params = {"ajax": "true", "year": year_to_scrape}
        
        logger.info(f"Scraping AJAX endpoint for year {year_to_scrape}...")
        film_data = scrape_ajax_endpoint(ajax_url, params=params)
        
        if film_data:
            logger.info(f"Successfully scraped {len(film_data)} films from AJAX endpoint.")
            for film in film_data:
                # The data is a list of dicts, each can be saved.
                metadata = {
                    "source": ajax_url,
                    "type": "ajax_scrape",
                    "query_params": params,
                    "file_name": None
                }
                save_to_mongo(film, metadata=metadata)
        else:
            logger.warning("No data was scraped from the AJAX endpoint.")

        # 2. Scrape a dynamic page with Selenium as a fallback/example
        dynamic_page_url = "https://www.scrapethissite.com/pages/javascript/" # A page that requires JS
        logger.info(f"Scraping dynamic page with Selenium: {dynamic_page_url}")
        soup = scrape_dynamic_url(dynamic_page_url)
        if soup:
            # Example: Extract the text from the h3 tag
            h3_text = soup.find('h3', class_='page-title')
            if h3_text:
                logger.info(f"Selenium successfully extracted title: {h3_text.text.strip()}")
                # In a real scenario, you would parse the soup object and save the data
                metadata = {"source": dynamic_page_url, "type": "selenium_scrape"}
                save_to_mongo({"page_title": h3_text.text.strip()}, metadata=metadata)
            else:
                logger.warning("Could not find the expected element on the dynamic page.")
        else:
            logger.warning("Selenium scraper did not return any content.")

    except Exception as e:
        logger.error(f"An error occurred during dynamic content scraping: {e}")


    # --- OCR Processing ---
    logger.info("--- Starting OCR Processing ---")

    # 3. OCR on individual images
    try:
        logger.info("Performing OCR on images...")
        image_dir = os.path.join(project_root, "data", "raw", "images")
        if os.path.exists(image_dir):
            for filename in os.listdir(image_dir):
                if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.tiff', '.bmp')):
                    file_path = os.path.join(image_dir, filename)
                    text = extract_text_from_image(file_path)
                    if text:
                        metadata = {"source": file_path, "file_name": filename, "type": "ocr_image"}
                        save_to_mongo({"text": text}, metadata=metadata)
        else:
            logger.warning(f"Image directory not found: {image_dir}")
    except Exception as e:
        logger.error(f"Error during image OCR: {e}")

    # 4. OCR on scanned PDFs
    try:
        logger.info("Performing OCR on scanned PDFs...")
        scanned_dir = os.path.join(project_root, "data", "raw", "scanned")
        if os.path.exists(scanned_dir):
            for filename in os.listdir(scanned_dir):
                if filename.lower().endswith('.pdf'):
                    file_path = os.path.join(scanned_dir, filename)
                    text_data = extract_text_from_pdf(file_path)
                    if text_data:
                        metadata = {"source": file_path, "file_name": filename, "type": "ocr_pdf"}
                        save_to_mongo(text_data, metadata=metadata)
        else:
            logger.warning(f"Scanned PDF directory not found: {scanned_dir}")
    except Exception as e:
        logger.error(f"Error during scanned PDF OCR: {e}")

    # --- S3 Upload (Commented Out) ---
    # logger.info("--- Starting S3 Upload ---")
    # try:
    #     raw_data_dirs = [
    #         os.path.join(project_root, "data", "raw", d) 
    #         for d in ["api", "csv", "xml", "pdf", "docx", "excel", "images", "scanned"]
    #     ]
    #     for data_dir in raw_data_dirs:
    #         if os.path.exists(data_dir):
    #             for filename in os.listdir(data_dir):
    #                 file_path = os.path.join(data_dir, filename)
    #                 if os.path.isfile(file_path):
    #                     s3_object_name = f"{os.path.basename(data_dir)}/{filename}"
    #                     # upload_file_to_s3(file_path, s3_object_name)
    #     logger.info("S3 upload process skipped as requested.")
    # except Exception as e:
    #     logger.error(f"An error occurred during the S3 upload process: {e}")
    
    # --- Analytics and Reporting ---
    logger.info("--- Starting Analytics and Reporting ---")

    try:
        logger.info("Running NumPy foundations demo...")
        run_numpy_demo()
    except Exception as e:
        logger.error(f"Error running NumPy analytics demo: {e}")

    try:
        logger.info("Running analytics data loading, CSV export, chunk processing, and dtype optimization...")
        run_data_loader_pipeline()
    except Exception as e:
        logger.error(f"Error in analytics data-loader stage: {e}")

    try:
        logger.info("Running EDA explorer module...")
        run_explorer_pipeline()
    except Exception as e:
        logger.error(f"Error in analytics explorer stage: {e}")

    try:
        logger.info("Running quality report module...")
        run_quality_pipeline()
    except Exception as e:
        logger.error(f"Error in analytics quality-report stage: {e}")

    try:
        logger.info("Running regex analytics module...")
        run_regex_pipeline()
    except Exception as e:
        logger.error(f"Error in analytics regex stage: {e}")

    try:
        logger.info("Running cleaning pipeline...")
        raw_csv_path = os.path.join(project_root, "data", "raw", "csv", "raw_brand_mentions.csv")
        if not os.path.exists(raw_csv_path):
            logger.warning(f"Cleaning stage skipped because raw CSV was not found: {raw_csv_path}")
        else:
            raw_dataframe = pd.read_csv(raw_csv_path)
            cleaned_dataframe = run_cleaning_pipeline(raw_dataframe)
            logger.info(
                "Cleaning stage complete | input_path=%s | cleaned_rows=%s | cleaned_columns=%s",
                raw_csv_path,
                len(cleaned_dataframe),
                len(cleaned_dataframe.columns),
            )
    except Exception as e:
        logger.error(f"Error in cleaning stage: {e}")

    logger.info("--- Pipeline finished successfully. ---")

if __name__ == "__main__":
    run_pipeline()
