import sys
import os
from pymongo import MongoClient

# Add the project root to the Python path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.storage.mongo import save_to_mongo

def verify_saved_data():
    """
    Connects to MongoDB and verifies that the test data exists.
    """
    client = MongoClient("mongodb://localhost:27017/")
    db = client["social_media_brand_monitor"]
    col = db["brand_mentions"]

    print("\n--- Verifying Data in MongoDB ---")

    # Verification for Test 1
    scraped_count = col.count_documents({"type": "scraped_web_data"})
    print(f"[Verification 1] Found {scraped_count} documents of type 'scraped_web_data'. (Expected: 2)")
    assert scraped_count >= 2

    # Verification for Test 2
    ocr_count = col.count_documents({"type": "OCR"})
    print(f"[Verification 2] Found {ocr_count} documents of type 'OCR'. (Expected: 3)")
    assert ocr_count >= 3

    # Verification for Test 3
    doc_count = col.count_documents({"type": "document_data"})
    print(f"[Verification 3] Found {doc_count} documents of type 'document_data'. (Expected: 1)")
    assert doc_count >= 1
    
    print("--- Data Verification Successful ---")

if __name__ == '__main__':
    # This block will run the tests to save data and then verify it.
    
    # --- Running MongoDB Save Tests ---
    print("--- Running MongoDB Save Tests ---")

    # Test 1: Saving a list of dictionaries (like scraped web data)
    print("\n[Test 1: Saving a list of scraped data]")
    scraped_articles = [
        {"url": "https://test.com/news/1", "title": "First Test Article", "body": "This is the content of the first article."},
        {"url": "https://test.com/news/2", "title": "Second Test Article", "body": "This is the content of the second article."}
    ]
    scraped_metadata = {
        "source": "web_scraper_v1",
        "type": "scraped_web_data"
    }
    save_to_mongo(scraped_articles, collection_name="brand_mentions", metadata=scraped_metadata)

    # Test 2: Saving OCR results from a PDF (dictionary of pages)
    print("\n[Test 2: Saving OCR results from a PDF]")
    pdf_ocr_results = {
        1: "This is the extracted text from page one of the PDF.",
        2: "And this is the text from the second page.",
        3: "Finally, the content of the third page."
    }
    pdf_metadata = {
        "source": "local_file_system",
        "file_name": "annual_report_scanned.pdf",
        "type": "OCR"
    }
    save_to_mongo(pdf_ocr_results, collection_name="brand_mentions", metadata=pdf_metadata)

    # Test 3: Saving a single document data (like from a DOCX file)
    print("\n[Test 3: Saving a single document]")
    doc_data = {
        "title": "Internal Memo",
        "text": "This is the full text extracted from a Word document."
    }
    doc_metadata = {
        "source": "local_file_system",
        "file_name": "memo.docx",
        "type": "document_data"
    }
    save_to_mongo(doc_data, collection_name="brand_mentions", metadata=doc_metadata)

    print("\n--- MongoDB Save Tests Finished ---")

    # Verify the data that was just saved
    verify_saved_data()
