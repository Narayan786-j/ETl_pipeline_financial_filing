import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import numpy as np
import os
import functions as F
import OLTP_load as DB
import OLAP as OL
import quality_check as QC
import logger as log


def run_etl_pipeline(file_path):
    # Read the HTML file
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        html_content = f.read()

    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, "lxml")

    # Extract all tables
    tables = soup.find_all("table")

    # print(f"Total tables found: {len(tables)}")
    log.logger.info(f"Total tables found: {len(tables)}")

    # Convert tables to DataFrames
    dfs = []
    for i, table in enumerate(tables):
        try:
            df = pd.read_html(StringIO(str(table)), flavor="lxml")[0]

            # --- Cleaning & Standardization ---
            # Drop all-empty rows/columns
            df = df.dropna(how="all").dropna(axis=1, how="all")

            # Forward fill multi-level headers if they exist
            df.columns = [str(col).strip() for col in df.columns]
            if df.columns.duplicated().any():
                df = df.loc[:, ~df.columns.duplicated()]

            # Normalize column names (remove extra spaces/newlines)
            df.columns = [c.replace("\n", " ").replace("  ", " ").strip() for c in df.columns]

            # --- Identify Balance Sheet / Income Statement ---
            keywords_balance = ["total assets", "total liabilities", "stockholders"]
            keywords_income = ["operating expenses","total operating expenses","income from operations","net loss","earnings per share","comprehensive loss"]

            table_text = " ".join(df.astype(str).values.flatten()).lower()

            if any(k in table_text for k in keywords_balance):
                log.logger.info(f"Found Balance Sheet candidate at table {i} with shape {df.shape}")
                dfs.append(("Balance Sheet", df))

            elif any(k in table_text for k in keywords_income):
                log.logger.info(f"Found Income Statement candidate at table {i} with shape {df.shape}")
                dfs.append(("Income Statement", df))


            # log.logger.info(f"Extracted table {i} with shape {df.shape}")
        except Exception as e:
            log.logger.warning(f"Skipped table {i} due to error: {e}")

    # # Example: Display the first cleaned table
    # if dfs:
    #     print(dfs[0])
    #     print(dfs[1])

    for df_type,dfs_raw in dfs:

        df_raw = dfs_raw.copy()   # your extracted DataFrame

        # 1. Drop fully empty columns
        df = df_raw.dropna(axis=1, how="all").reset_index(drop=True)

        # 2. Build header from first two rows
        header = df.iloc[0:2].fillna("").astype(str).agg(" ".join).str.strip()
        df.columns = ["Line Item"] + list(header[1:])

        # 3. Drop header rows
        df = df.iloc[3:].reset_index(drop=True)

        # 🚨 Ensure unique column names manually
        def make_unique(cols):
            seen = {}
            new_cols = []
            for c in cols:
                if c not in seen:
                    seen[c] = 0
                    new_cols.append(c)
                else:
                    seen[c] += 1
                    new_cols.append(f"{c}_{seen[c]}")
            return new_cols

        df.columns = make_unique(df.columns)

        # 4. Function to clean numbers
        def clean_number(x):
            if pd.isna(x):
                return np.nan
            x = str(x).replace(",", "").replace("$", "").strip()
            if x.startswith("(") and x.endswith(")"):   # e.g. (271,381)
                x = "-" + x[1:-1]
            try:
                return float(x)
            except:
                return np.nan

        # 5. Apply cleaning
        for col in df.columns[1:]:
            df[col] = df[col].apply(clean_number)

        # 6. Clean Line Item text
        df["Line Item"] = df["Line Item"].astype(str).str.strip()

        # Start from your cleaned df
        df_clean = df.copy()

        # 1. Keep only useful columns (drop NaN filler ones)
        # Heuristic: keep columns that actually contain numbers
        num_cols = [c for c in df_clean.columns if df_clean[c].notna().sum() > 0 and c != "Line Item"]

        df_clean = df_clean[["Line Item"] + num_cols]

        # 2. Rename columns (merge date + unaudited tags if needed)
        rename_map = {}
        for col in df_clean.columns:
            if "June 30, 2025" in col:
                rename_map[col] = "June 30, 2025 (unaudited)"
            elif "December 31, 2024" in col:
                rename_map[col] = "December 31, 2024"
        df_clean = df_clean.rename(columns=rename_map)

        # 3. Melt into long/tidy format
        df_tidy = df_clean.melt(id_vars="Line Item", 
                                var_name="Period", 
                                value_name="Value")

        # 4. Drop empty rows
        df_tidy = df_tidy.dropna(subset=["Value"]).reset_index(drop=True)

        # print(df_tidy)

        df_tidy_clean = df_tidy.copy()

        # 1. Clean up the period names (remove duplicate suffixes like "_1")
        df_tidy_clean["Period"] = df_tidy_clean["Period"].str.replace(r"_\d+$", "", regex=True)

        df_tidy_clean[["Period Type", "End Date", "Unaudited", "Fiscal Year"]] = df_tidy_clean["Period"].apply(F.parse_period)

        # 3. Add metadata columns
        df_tidy_clean["Ticker"] = F.extract_metadata(file_path)[0]
        df_tidy_clean["Filing Date"] = F.extract_metadata(file_path)[1]
        df_tidy_clean["Statement Type"] = df_type

        # 4. Reorder columns for clarity
        df_tidy_clean = df_tidy_clean[
        ["Ticker", "Filing Date", "Statement Type",
        "Line Item", "Period Type", "End Date", "Fiscal Year", "Unaudited", "Value"]
        ]


        # print(df_tidy_clean)
        log.logger.info(f"OLTP loading started.....")
        DB.load_dataframe_to_db(df_tidy_clean)
        log.logger.info(f"OLTP loading completed.")

    log.logger.info(f"OLAP loading started.....")
    OL.create_and_populate_olap_schema_from_oltp()
    log.logger.info(f"OLAP loading completed.")

    log.logger.info(f'running quality checks....')
    res=QC.run_quality_checks()
    log.logger.info(f"quality check result: {res}")


if __name__ == "__main__":
    # txt_file = r"C:\Users\NARAYAN JHA\Documents\ETL_python\input_file.txt"
    # Get the folder where main.py is located
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Join with your input file name
    txt_file = os.path.join(BASE_DIR, "input_file.txt")
    file_paths = F.read_unique_links(txt_file)

    for file_path in file_paths:
        if F.detect_file_type(file_path) == "html":
            log.logger.info(f"Processing file: {file_path}")
            run_etl_pipeline(file_path)
        else:
            log.logger.warning(f"Skipping unsupported file type: {file_path}")