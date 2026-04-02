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
    
    logger.info("--- Pipeline finished successfully. ---")

if __name__ == "__main__":
    run_pipeline()
