import sys
import os
from pymongo import MongoClient
from pprint import pprint
from src.parsing.parsers import parse_excel_files

# Ensure the script can find the logger utility
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.utils.logger import logging

def verify_mongo_data():
    """
    Connects to MongoDB and prints sample documents for each parsed data type.
    """
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        db = client["social_media_brand_monitor"]
        collection = db["raw_data"]
        logging.info("Successfully connected to MongoDB.")
    except Exception as e:
        logging.error(f"Could not connect to MongoDB. Please ensure it is running. Error: {e}")
        return

    print("-" * 50)
    
    # --- Verify PDF Text ---
    print("\n📄 Verifying PDF Text Content...")
    pdf_text_doc = collection.find_one({"document_type": "pdf", "type": "text"})
    if pdf_text_doc:
        print(f"Found text from: {pdf_text_doc.get('source')}")
        print("Content (first 200 chars):")
        pprint(pdf_text_doc['content'][:200] + "...")
    else:
        print("No PDF text documents found.")

    print("-" * 50)

    # --- Verify PDF Table ---
    print("\n📊 Verifying PDF Table Content...")
    pdf_table_doc = collection.find_one({"document_type": "pdf", "type": "table"})
    if pdf_table_doc:
        print(f"Found a table from: {pdf_table_doc.get('source')} on page {pdf_table_doc.get('page')}")
        print("Table Content:")
        pprint(pdf_table_doc['content'])
    else:
        print("No PDF table documents found.")

    print("-" * 50)

    # --- Verify DOCX Text ---
    print("\n📄 Verifying Word (DOCX) Text Content...")
    docx_text_doc = collection.find_one({"document_type": "docx", "type": "text"})
    if docx_text_doc:
        print(f"Found text from: {docx_text_doc.get('source')}")
        print("Content (first 200 chars):")
        pprint(docx_text_doc['content'][:200] + "...")
    else:
        print("No DOCX text documents found.")
        
    print("-" * 50)

    # --- Verify DOCX Table ---
    print("\n📊 Verifying Word (DOCX) Table Content...")
    docx_table_doc = collection.find_one({"document_type": "docx", "type": "table"})
    if docx_table_doc:
        print(f"Found a table from: {docx_table_doc.get('source')}")
        print("Table Content:")
        pprint(docx_table_doc['content'])
    else:
        print("No DOCX table documents found.")

    print("-" * 50)

    # --- Verify Excel Data ---
    print("\n📈 Verifying Excel Content...")
    excel_doc = collection.find_one({"document_type": "excel"})
    if excel_doc:
        print(f"Found data from: {excel_doc.get('source')}")
        # Print the name of the first sheet and the first row of its data
        first_sheet_name = next(iter(excel_doc.keys()), None)
        if first_sheet_name and first_sheet_name not in ['_id', 'source', 'document_type', 'extraction_timestamp']:
             print(f"Data from sheet '{first_sheet_name}' (first row):")
             pprint(excel_doc[first_sheet_name][0] if excel_doc[first_sheet_name] else "Sheet is empty")
        else:
            pprint(excel_doc)
    else:
        print("No Excel documents found.")
        
    print("-" * 50)

if __name__ == "__main__":
    verify_mongo_data()

    # --- New Test for Excel File ---
    print("\n[Test 4: Parsing and Saving an Excel file]")
    try:
        # Define the path to your sample Excel file
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        excel_file_path = os.path.join(project_root, "data", "raw", "excel", "sample.xlsx")
        
        if os.path.exists(excel_file_path):
            # Run the parser function directly on the file
            parse_excel_files(excel_file_path)
            
            # Verify the data was saved
            client = MongoClient("mongodb://localhost:27017/")
            db = client["social_media_brand_monitor"]
            col = db["brand_mentions"]
            excel_doc_count = col.count_documents({"type": "excel"})
            print(f"[Verification 4] Found {excel_doc_count} documents of type 'excel'. (Expected > 0)")
            assert excel_doc_count > 0
            print("--- Excel Data Verification Successful ---")
        else:
            print(f"Excel test file not found at: {excel_file_path}")
    except Exception as e:
        print(f"An error occurred during the Excel test: {e}")

    print("\n--- All Tests Finished ---")
