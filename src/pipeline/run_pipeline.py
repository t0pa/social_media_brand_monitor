import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.logger import logging
from api.client import fetch_brand_articles
from parsing.parsers import (
    parse_json_files, 
    parse_csv_file, 
    parse_xml_file, 
    parse_pdf_files, 
    parse_docx_files, 
    parse_excel_files
)
from storage.s3 import upload_file_to_s3

def run_pipeline():
    brand = "Apple"  # You can change this to your target brand
    logging.info(f"Pipeline started for brand: {brand}")
    # Correctly define project_root to be the top-level directory
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Step 1: Fetch data from API
    logging.info("Fetching data from API...")
    api_dir = os.path.join(project_root, "data", "raw", "api")
    fetch_brand_articles(brand, pages=3, save_dir=api_dir)
    logging.info("API data fetched and saved as JSON files.")

    # Step 2: Parse all local files and save to MongoDB
    logging.info("Starting parsing of all local files...")
    
    # Define directories relative to the correct project root
    csv_dir = os.path.join(project_root, "data", "raw", "csv")
    xml_dir = os.path.join(project_root, "data", "raw", "xml")
    pdf_dir = os.path.join(project_root, "data", "raw", "pdf")
    docx_dir = os.path.join(project_root, "data", "raw", "docx")
    excel_dir = os.path.join(project_root, "data", "raw", "excel")

    # Run parsers
    if os.path.exists(api_dir): parse_json_files(api_dir)
    if os.path.exists(csv_dir):
        for f in os.listdir(csv_dir):
            if f.endswith(".csv"): parse_csv_file(os.path.join(csv_dir, f))
    if os.path.exists(xml_dir):
        for f in os.listdir(xml_dir):
            if f.endswith(".xml"): parse_xml_file(os.path.join(xml_dir, f))
    if os.path.exists(pdf_dir): parse_pdf_files(pdf_dir)
    if os.path.exists(docx_dir): parse_docx_files(docx_dir)
    if os.path.exists(excel_dir): parse_excel_files(excel_dir)

    logging.info("All data parsed and saved to MongoDB.")

    # Step 3: Upload all raw data files to S3
    logging.info("Starting upload of all raw data to S3...")
    raw_data_dirs = [api_dir, csv_dir, xml_dir, pdf_dir, docx_dir, excel_dir]

    for data_dir in raw_data_dirs:
        if os.path.exists(data_dir):
            for filename in os.listdir(data_dir):
                file_path = os.path.join(data_dir, filename)
                if os.path.isfile(file_path):
                    s3_object_name = f"{os.path.basename(data_dir)}/{filename}"
                    upload_file_to_s3(file_path, s3_object_name)

    logging.info("S3 upload complete.")
    logging.info("Pipeline finished successfully.")

if __name__ == "__main__":
    run_pipeline()
