import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import logging
from storage.mongo import save_to_mongo
from api.client import fetch_brand_articles
from parsing.parsers import parse_json_files, parse_csv_file, parse_xml_file
from storage.s3 import upload_file_to_s3

def run_pipeline():
    brand = "Apple"  # You can change this to your target brand
    logging.info(f"Pipeline started for brand: {brand}")

    # Step 1: Fetch data from API
    logging.info("Fetching data from API...")
    fetch_brand_articles(brand, pages=3)
    logging.info("API data fetched and saved as JSON files.")

    # Step 2: Parse JSON, CSV, XML files and save to MongoDB
    logging.info("Parsing JSON files and saving to MongoDB...")
    parse_json_files()
    logging.info("Parsing CSV file and saving to MongoDB...")
    parse_csv_file(os.path.join("data", "raw", "csv", "sample.csv"))
    logging.info("Parsing XML file and saving to MongoDB...")
    parse_xml_file(os.path.join("data", "raw", "xml", "sample.xml"))
    logging.info("All data parsed and saved to MongoDB.")

    # Step 3: Upload a sample file to S3 (you can loop or upload more as needed)
    file_path = os.path.join("data", "raw", "api", f"newsapi_{brand}_page_1.json")
    file_name = f"newsapi_{brand}_page_1.json"
    logging.info(f"Uploading {file_name} to S3...")
    upload_file_to_s3(file_path, file_name)
    logging.info("S3 upload complete.")

    logging.info("Pipeline finished successfully.")

if __name__ == "__main__":
    run_pipeline()
