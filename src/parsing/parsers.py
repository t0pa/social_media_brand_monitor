
import os
import json
import csv
import xml.etree.ElementTree as ET
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'storage')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.storage.mongo import save_to_mongo

def parse_json_files(directory=None):
    # Use absolute path from project root for reliability
    if directory is None:
        directory = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "raw", "api")
    """Parse all JSON files in the given directory and print the number of items in each."""
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            filepath = os.path.join(directory, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Save to MongoDB
                if isinstance(data, dict) and "articles" in data:
                    print(f"{filename}: {len(data['articles'])} articles")
                    save_to_mongo(data["articles"])
                else:
                    print(f"{filename}: {type(data)} loaded")
                    save_to_mongo(data)

def parse_csv_file(filepath):
    """Parse a CSV file and print the number of rows."""
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
        print(f"{os.path.basename(filepath)}: {len(rows)} data rows (excluding header)")
        if rows:
            save_to_mongo(rows)

def parse_xml_file(filepath):
    """Parse an XML file and print the number of top-level elements."""
    tree = ET.parse(filepath)
    root = tree.getroot()
    print(f"{os.path.basename(filepath)}: {len(root)} top-level elements")
    # Convert XML elements to dicts for MongoDB
    data = []
    for elem in root:
        entry = {child.tag: child.text for child in elem}
        data.append(entry)
    if data:
        save_to_mongo(data)

if __name__ == "__main__":
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    parse_json_files()
    parse_csv_file(os.path.join(project_root, "data", "raw", "csv", "sample.csv"))
    parse_xml_file(os.path.join(project_root, "data", "raw", "xml", "sample.xml"))
