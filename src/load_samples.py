
from io_utils import read_json, setup_logging, read_text

if __name__ == "__main__":
	setup_logging("pipeline.log")
	
	sample_json_path = "data/raw/apple/apple_insiderRu.json"
	sample_text_path = "data/raw/title/title.txt"

	brand_data = read_json(sample_json_path)
	title_text = read_text(sample_text_path)

	if brand_data:
		print("Authon:", brand_data["author"])
	if title_text:
		print("Title: ",title_text)
