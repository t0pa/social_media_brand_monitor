import sys
import os
import pytesseract
from PIL import Image
import pdfplumber
from pdf2image import convert_from_path

# Add the project root to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.utils.logger import get_logger

logger = get_logger(__name__)

# You may need to set the path to the Tesseract executable
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def preprocess_image(image_path):
    """
    Preprocesses an image for OCR by converting it to grayscale and applying a threshold.
    
    Args:
        image_path (str): The path to the image file.
        
    Returns:
        PIL.Image.Image: The preprocessed image.
    """
    try:
        image = Image.open(image_path)
        # Convert to grayscale
        image = image.convert('L')
        # Apply a threshold to get a binary image
        # image = image.point(lambda x: 0 if x < 128 else 255, '1')
        return image
    except FileNotFoundError:
        logger.error(f"Image file not found at {image_path}")
        return None

def extract_text_from_image(image_path):
    """
    Extracts text from an image using OCR.
    
    Args:
        image_path (str): The path to the image file.
        
    Returns:
        str: The extracted text, or an empty string if extraction fails.
    """
    preprocessed_image = preprocess_image(image_path)
    if preprocessed_image:
        try:
            text = pytesseract.image_to_string(preprocessed_image)
            return text.strip()
        except pytesseract.TesseractNotFoundError:
            logger.error("Tesseract is not installed or not in your PATH.")
            logger.error("Please install Tesseract and/or set the `tesseract_cmd` path.")
            return ""
        except Exception as e:
            logger.error(f"Error during OCR: {e}")
            return ""
    return ""

def extract_text_from_pdf(pdf_path):
    """
    Extracts text from a PDF file, handling both regular and scanned PDFs.
    
    Args:
        pdf_path (str): The path to the PDF file.
        
    Returns:
        dict: A dictionary where keys are page numbers and values are the extracted text.
    """
    # First, try to extract text with pdfplumber
    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_text = {}
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    all_text[i + 1] = text.strip()
            
            if all_text:
                # Check if all pages returned empty text
                if all(not v for v in all_text.values()):
                    logger.info("pdfplumber extracted no text, attempting OCR.")
                else:
                    logger.info(f"Successfully extracted text from {pdf_path} using pdfplumber.")
                    return all_text

    except Exception as e:
        logger.error(f"Error with pdfplumber for file {pdf_path}: {e}")

    # If pdfplumber fails or returns no text, treat as a scanned PDF
    logger.info(f"Treating {pdf_path} as a scanned PDF and applying OCR.")
    all_text = {}
    try:
        # Provide the path to the poppler installation
        poppler_path = r"C:\poppler\poppler-25.12.0\Library\bin"
        images = convert_from_path(pdf_path, poppler_path=poppler_path)
        for i, image in enumerate(images):
            try:
                # The preprocess_image function expects a path, so we need to save the image temporarily
                # Or modify preprocess_image to accept an image object.
                # For now, let's just pass the image object to pytesseract directly after converting to grayscale.
                grayscale_image = image.convert('L')
                text = pytesseract.image_to_string(grayscale_image)
                all_text[i + 1] = text.strip()
            except pytesseract.TesseractNotFoundError:
                logger.error("Tesseract is not installed or not in your PATH.")
                logger.error("Please install Tesseract and/or set the `tesseract_cmd` path.")
                return {}
            except Exception as e:
                logger.error(f"Error during OCR on page {i+1} of {pdf_path}: {e}")
                all_text[i + 1] = ""
        return all_text
    except Exception as e:
        logger.error(f"Error converting PDF to images for {pdf_path}: {e}")
        return {}

if __name__ == '__main__':
    # Example usage with an image
    image_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'images', 'test_scan.png'))
    if not os.path.exists(image_path):
        logger.error(f"Test image not found at {image_path}")
    else:
        text_from_image = extract_text_from_image(image_path)
        logger.info(f"Extracted text from image:\n---\n{text_from_image}\n---")

    # Example usage with a scanned PDF
    scanned_pdf_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw', 'scanned', 'pdf_scan.pdf'))
    if not os.path.exists(scanned_pdf_path):
        logger.error(f"Test scanned PDF not found at {scanned_pdf_path}")
    else:
        text_from_scanned_pdf = extract_text_from_pdf(scanned_pdf_path)
        logger.info(f"Extracted text from scanned PDF:\n---")
        for page, text in text_from_scanned_pdf.items():
            logger.info(f"Page {page}:\n{text}")
        logger.info("---")
