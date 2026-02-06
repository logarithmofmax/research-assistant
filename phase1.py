#!/usr/bin/env python3
"""
Phase 1: CSRD Report PDF Scraping Pipeline

Downloads annual report PDFs listed in SRN-CSRD_report_archive.xlsx,
extracts the CSRD section text and tables from each, and produces a
metadata summary CSV.

Usage:
    python phase1.py                    # process all valid reports
    python phase1.py --limit 5          # process only the first 5
    python phase1.py --start-from 10    # resume from index 10
    python phase1.py --limit 5 --start-from 10  # combine both
"""

import argparse
import csv
import json
import logging
import os
import re
import time
from pathlib import Path

import fitz  # pymupdf
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EXCEL_FILE = "SRN-CSRD_report_archive.xlsx"
SHEET_NAME = "csrd"
HEADER_ROW = 2  # 0-indexed; row 3 in the spreadsheet

PDF_DIR = "pdfs"
TEXT_DIR = "extracted_text"
TABLE_DIR = "extracted_tables"
DOWNLOAD_LOG = "download_log.csv"
SUMMARY_CSV = "extraction_summary.csv"

REQUEST_DELAY = 1.5  # seconds between downloads
REQUEST_TIMEOUT = 60  # seconds per request
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def clean_filename(name: str) -> str:
    """Remove special characters and replace spaces with underscores."""
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s]+", "_", name.strip())
    return name


def ensure_dirs() -> None:
    """Create output directories if they don't exist."""
    for d in (PDF_DIR, TEXT_DIR, TABLE_DIR):
        os.makedirs(d, exist_ok=True)


# ---------------------------------------------------------------------------
# Step 1 – Read and filter the Excel file
# ---------------------------------------------------------------------------


def load_report_list(excel_path: str) -> pd.DataFrame:
    """Read the csrd sheet, filter to valid downloadable entries."""
    logger.info("Reading %s …", excel_path)
    df = pd.read_excel(excel_path, sheet_name=SHEET_NAME, header=HEADER_ROW)

    # Normalise column names that contain newlines
    df.columns = [c.replace("\n", " ").strip() for c in df.columns]

    # Filters
    is_verified = df["verified"].astype(str).str.lower() == "yes"
    has_link = df["link"].astype(str).str.startswith("http")
    has_start = pd.to_numeric(df["start PDF"], errors="coerce").notna()
    has_end = pd.to_numeric(df["end PDF"], errors="coerce").notna()

    valid = df[is_verified & has_link & has_start & has_end].copy()
    valid["start PDF"] = valid["start PDF"].astype(int)
    valid["end PDF"] = valid["end PDF"].astype(int)

    logger.info(
        "Total rows: %d | Verified with valid link & pages: %d",
        len(df),
        len(valid),
    )
    return valid.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 2 – Download PDFs
# ---------------------------------------------------------------------------


def _pdf_path(company: str, isin: str) -> str:
    return os.path.join(PDF_DIR, f"{clean_filename(company)}_{isin}.pdf")


def download_pdf(url: str, dest: str) -> str:
    """
    Download a PDF from *url* to *dest*.

    Returns one of: 'success', 'skipped', 'not_pdf', 'error:<reason>'
    """
    if os.path.exists(dest):
        logger.info("  Already exists – skipping download: %s", dest)
        return "skipped"

    headers = {"User-Agent": USER_AGENT}
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, stream=True)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").lower()
        # Some servers return application/octet-stream for PDFs — allow that too.
        if "pdf" not in content_type and "octet-stream" not in content_type:
            # Check if the body starts with the PDF magic bytes
            first_bytes = next(resp.iter_content(chunk_size=8), b"")
            if not first_bytes.startswith(b"%PDF"):
                logger.warning(
                    "  URL does not point to a PDF (Content-Type: %s): %s",
                    content_type,
                    url,
                )
                return "not_pdf"
            # It *is* a PDF despite the header; write the bytes we already read.
            with open(dest, "wb") as f:
                f.write(first_bytes)
                for chunk in resp.iter_content(chunk_size=1 << 16):
                    f.write(chunk)
            return "success"

        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 16):
                f.write(chunk)
        return "success"

    except requests.RequestException as exc:
        logger.error("  Download failed for %s: %s", url, exc)
        return f"error:{exc}"


def download_all(df: pd.DataFrame) -> dict[int, str]:
    """Download PDFs for every row in *df*. Returns {index: status}."""
    statuses: dict[int, str] = {}

    # Prepare / append to download log CSV
    log_exists = os.path.exists(DOWNLOAD_LOG)
    log_file = open(DOWNLOAD_LOG, "a", newline="", encoding="utf-8")
    log_writer = csv.writer(log_file)
    if not log_exists:
        log_writer.writerow(["index", "company", "isin", "url", "status", "dest"])

    for idx, row in df.iterrows():
        company = str(row["company"])
        isin = str(row["isin"])
        url = str(row["link"])
        dest = _pdf_path(company, isin)

        logger.info("[%d/%d] Downloading %s …", idx + 1, len(df), company)
        status = download_pdf(url, dest)
        statuses[idx] = status
        log_writer.writerow([idx, company, isin, url, status, dest])
        log_file.flush()

        if status == "success":
            logger.info("  ✓ saved to %s", dest)

        # Rate-limit only when we actually made a request
        if status not in ("skipped",):
            time.sleep(REQUEST_DELAY)

    log_file.close()
    return statuses


# ---------------------------------------------------------------------------
# Step 3 – Extract text and tables from CSRD pages
# ---------------------------------------------------------------------------


def extract_text(pdf_path: str, start_page: int, end_page: int) -> str | None:
    """
    Extract text from *pdf_path* between *start_page* and *end_page*
    (1-indexed, inclusive).  Returns the concatenated text or None on failure.
    """
    try:
        doc = fitz.open(pdf_path)
    except Exception as exc:
        logger.error("  Cannot open PDF %s: %s", pdf_path, exc)
        return None

    # Convert 1-indexed inclusive range to 0-indexed
    pages_text: list[str] = []
    for page_num in range(start_page - 1, min(end_page, len(doc))):
        page = doc[page_num]
        pages_text.append(page.get_text("text"))

    doc.close()
    return "\n\n".join(pages_text)


def extract_tables(pdf_path: str, start_page: int, end_page: int) -> list[list[list[str]]]:
    """
    Try to extract tables from the CSRD section using pymupdf's built-in
    table finder.  Returns a list of tables, where each table is a list of
    rows (list of cell strings).
    """
    tables_found: list[list[list[str]]] = []
    try:
        doc = fitz.open(pdf_path)
    except Exception:
        return tables_found

    for page_num in range(start_page - 1, min(end_page, len(doc))):
        page = doc[page_num]
        try:
            tab_finder = page.find_tables()
            for table in tab_finder.tables:
                rows = table.extract()
                if rows:
                    tables_found.append(rows)
        except Exception:
            # Some pages may not support table extraction
            continue

    doc.close()
    return tables_found


def process_pdf(
    pdf_path: str,
    company: str,
    isin: str,
    start_page: int,
    end_page: int,
) -> dict:
    """
    Extract text and tables from one PDF's CSRD section.

    Returns a dict with extraction metadata.
    """
    result: dict = {
        "text_file": None,
        "table_file": None,
        "word_count": 0,
        "pages_extracted": 0,
        "tables_found": 0,
        "extraction_status": "success",
    }

    stem = f"{clean_filename(company)}_{isin}"

    # --- Text extraction ---
    text = extract_text(pdf_path, start_page, end_page)
    if text is None:
        result["extraction_status"] = "failed"
        return result

    result["pages_extracted"] = min(end_page, fitz.open(pdf_path).page_count) - (start_page - 1)
    result["word_count"] = len(text.split())

    text_path = os.path.join(TEXT_DIR, f"{stem}.txt")
    with open(text_path, "w", encoding="utf-8") as f:
        f.write(text)
    result["text_file"] = text_path

    # --- Table extraction ---
    tables = extract_tables(pdf_path, start_page, end_page)
    result["tables_found"] = len(tables)

    if tables:
        table_path = os.path.join(TABLE_DIR, f"{stem}_tables.json")
        with open(table_path, "w", encoding="utf-8") as f:
            json.dump(tables, f, ensure_ascii=False, indent=2)
        result["table_file"] = table_path

    return result


# ---------------------------------------------------------------------------
# Step 4 – Build the extraction summary
# ---------------------------------------------------------------------------


def build_summary(df: pd.DataFrame, download_statuses: dict, extraction_results: dict) -> None:
    """Write extraction_summary.csv."""
    rows: list[dict] = []

    # Resolve column names that may have embedded newlines
    industry_col = [c for c in df.columns if "SASB industry" in c][0]
    sector_col = [c for c in df.columns if "SASB sector" in c][0]

    for idx, row in df.iterrows():
        company = str(row["company"])
        isin = str(row["isin"])

        dl_status = download_statuses.get(idx, "unknown")
        ext = extraction_results.get(idx, {})

        rows.append(
            {
                "company": company,
                "isin": isin,
                "country": row.get("country", ""),
                "industry": row.get(industry_col, ""),
                "sector": row.get(sector_col, ""),
                "download_status": dl_status,
                "extraction_status": ext.get("extraction_status", "not_attempted"),
                "pages_extracted": ext.get("pages_extracted", 0),
                "word_count": ext.get("word_count", 0),
                "tables_found": ext.get("tables_found", 0),
                "text_file": ext.get("text_file", ""),
                "table_file": ext.get("table_file", ""),
            }
        )

    summary_df = pd.DataFrame(rows)
    summary_df.to_csv(SUMMARY_CSV, index=False)
    logger.info("Summary written to %s (%d rows)", SUMMARY_CSV, len(summary_df))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 1: CSRD PDF scraping pipeline")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N valid reports",
    )
    parser.add_argument(
        "--start-from",
        type=int,
        default=0,
        help="Start processing from this 0-based index (for resuming)",
    )
    args = parser.parse_args()

    ensure_dirs()

    # Step 1 – load and filter
    df = load_report_list(EXCEL_FILE)

    # Apply start-from and limit
    if args.start_from > 0:
        df = df.iloc[args.start_from :].reset_index(drop=True)
        logger.info("Resuming from index %d (%d remaining)", args.start_from, len(df))

    if args.limit is not None:
        df = df.iloc[: args.limit]
        logger.info("Limiting to %d reports", len(df))

    if df.empty:
        logger.warning("No valid reports to process – exiting.")
        return

    # Step 2 – download PDFs
    logger.info("=" * 60)
    logger.info("STEP 2: Downloading PDFs")
    logger.info("=" * 60)
    download_statuses = download_all(df)

    # Step 3 – extract text and tables
    logger.info("=" * 60)
    logger.info("STEP 3: Extracting text and tables")
    logger.info("=" * 60)
    extraction_results: dict[int, dict] = {}

    for idx, row in df.iterrows():
        company = str(row["company"])
        isin = str(row["isin"])
        pdf_path = _pdf_path(company, isin)
        start_page = int(row["start PDF"])
        end_page = int(row["end PDF"])

        dl_status = download_statuses.get(idx, "unknown")
        if dl_status not in ("success", "skipped"):
            logger.info("[%d/%d] Skipping extraction for %s (download: %s)", idx + 1, len(df), company, dl_status)
            continue

        if not os.path.exists(pdf_path):
            logger.warning("[%d/%d] PDF file missing for %s", idx + 1, len(df), company)
            continue

        logger.info(
            "[%d/%d] Extracting %s (pages %d–%d) …",
            idx + 1,
            len(df),
            company,
            start_page,
            end_page,
        )
        extraction_results[idx] = process_pdf(pdf_path, company, isin, start_page, end_page)
        er = extraction_results[idx]
        logger.info(
            "  → %d pages, %d words, %d tables",
            er["pages_extracted"],
            er["word_count"],
            er["tables_found"],
        )

    # Step 4 – summary
    logger.info("=" * 60)
    logger.info("STEP 4: Building summary")
    logger.info("=" * 60)
    build_summary(df, download_statuses, extraction_results)

    # Final stats
    successes = sum(1 for s in download_statuses.values() if s == "success")
    skips = sum(1 for s in download_statuses.values() if s == "skipped")
    failures = len(download_statuses) - successes - skips
    logger.info(
        "Done. Downloads — success: %d, skipped: %d, failed: %d",
        successes,
        skips,
        failures,
    )


if __name__ == "__main__":
    main()
