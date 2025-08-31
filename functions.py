import re
import os
import pandas as pd

def read_unique_links(txt_file: str):
    unique_links = set()
    with open(txt_file, "r") as f:
        for line in f:
            link = line.strip()
            # Skip empty lines and comments
            if not link or link.startswith("#"):
                continue
            unique_links.add(link)
    return list(unique_links)



def detect_file_type(file_path: str) -> str:
    """
    Detect whether the file is HTML or PDF.
    
    Args:
        file_path (str): Path to the file.
    
    Returns:
        str: "pdf", "html", or "unknown"
    """
    # Check extension first
    ext = os.path.splitext(file_path)[1].lower()
    if ext in [".pdf"]:
        return "pdf"
    elif ext in [".html", ".htm"]:
        return "html"

    # If extension is missing/misleading, check file signature
    try:
        with open(file_path, "rb") as f:
            header = f.read(8).lower()
            # PDF files start with "%PDF"
            if header.startswith(b"%pdf"):
                return "pdf"
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            start = f.read(500).lower()
            # HTML files usually have <html> or <!doctype
            if "<html" in start or "<!doctype" in start:
                return "html"
    except Exception:
        return "unknown"

    return "unknown"




def extract_metadata(file_path: str):
    """
    Extract ticker and filing_date (YYYY-MM-DD) from file name.
    Works for both HTML and PDF filenames like:
        CATX_20250813_PR.html
        CATX_20250813_QR.pdf
    """
    # Get the filename only (remove directories)
    filename = os.path.basename(file_path)
    
    # Regex to match: TICKER_YYYYMMDD_XX.ext
    match = re.match(r"([A-Z]+)_(\d{8})_", filename)
    if not match:
        raise ValueError(f"Filename does not match expected pattern: {filename}")
    
    ticker, date_str = match.groups()
    # Convert YYYYMMDD → YYYY-MM-DD
    filing_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    
    return ticker, filing_date



# 2. Extract structured period info
def parse_period(p):
    # Unaudited flag
    unaudited = "unaudited" in p.lower()
    
    # Period type
    if "Three Months" in p:
        period_type = "Three Months"
    elif "Six Months" in p:
        period_type = "Six Months"
    elif "Year Ended" in p:
        period_type = "Year Ended"
    else:
        period_type = "Point-in-Time"
    
    # Extract date
    match = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*\d{4}", p)
    if match:
        end_date = match.group(0)
        year = int(re.search(r"\d{4}", end_date).group(0))
    else:
        match = re.search(r"\d{4}", p)
        if match:
            year = int(match.group(0))
            end_date = str(year)
        else:
            year, end_date = None, None
    
    return pd.Series([period_type, end_date, unaudited, year])

