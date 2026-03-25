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

    # Step 3: Upload all raw data files to S3
    logging.info("Starting upload of all raw data to S3...")
    raw_data_dirs = [
        os.path.join("data", "raw", "api"),
        os.path.join("data", "raw", "csv"),
        os.path.join("data", "raw", "xml"),
        os.path.join("data", "raw", "pdf")
    ]

    for data_dir in raw_data_dirs:
        if os.path.exists(data_dir):
            for filename in os.listdir(data_dir):
                file_path = os.path.join(data_dir, filename)
                if os.path.isfile(file_path):
                    logging.info(f"Uploading {filename} to S3...")
                    # The object name in S3 will be the directory and the filename
                    s3_object_name = f"{os.path.basename(data_dir)}/{filename}"
                    upload_file_to_s3(file_path, s3_object_name)

    logging.info("S3 upload complete.")

    logging.info("Pipeline finished successfully.")

if __name__ == "__main__":
    run_pipeline()
