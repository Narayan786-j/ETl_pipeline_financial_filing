# OLAP SCHEMA DESIGN

import sqlite3
import logger as log

def create_and_populate_olap_schema_from_oltp(olap_db="olap_db.sqlite", oltp_db="fin_db.sqlite"):
    """
    Create OLAP schema (star schema) and populate it from OLTP database.
    
    Parameters:
        olap_db (str): Path to OLAP SQLite DB file
        oltp_db (str): Path to OLTP SQLite DB file
    """
    # connect to OLAP DB
    conn = sqlite3.connect(olap_db)
    cur = conn.cursor()

    # Attach OLTP DB
    cur.execute(f"ATTACH DATABASE '{oltp_db}' AS oltp;")

    # 1. Create Schema
    cur.executescript("""
    DROP TABLE IF EXISTS fact_financials;
    DROP TABLE IF EXISTS company_dim;
    DROP TABLE IF EXISTS date_dim;
    DROP TABLE IF EXISTS statement_type_dim;

    -- Company Dimension
    CREATE TABLE company_dim (
        company_key     INTEGER PRIMARY KEY,
        cik             TEXT,
        ticker          TEXT,
        company_name    TEXT
    );

    -- Date Dimension
    CREATE TABLE date_dim (
        date_key        INTEGER PRIMARY KEY,
        date            TEXT,
        year            INT,
        quarter         INT,
        month           INT,
        day             INT
    );

    -- Statement Type Dimension
    CREATE TABLE statement_type_dim (
        statement_type_key INTEGER PRIMARY KEY,
        statement_type     TEXT
    );

    -- Fact Table
    CREATE TABLE fact_financials (
        fact_id             INTEGER PRIMARY KEY AUTOINCREMENT,
        company_key         INTEGER,
        date_key            INTEGER,
        statement_type_key  INTEGER,
        line_item_id        INTEGER,
        value               REAL,
        FOREIGN KEY (company_key) REFERENCES company_dim(company_key),
        FOREIGN KEY (date_key) REFERENCES date_dim(date_key),
        FOREIGN KEY (statement_type_key) REFERENCES statement_type_dim(statement_type_key)
    );
    """)
    conn.commit()

    # 2. Populate Dimensions

    # company_dim
    cur.execute("""
    INSERT OR REPLACE INTO company_dim (company_key, cik, ticker, company_name)
    SELECT company_id, company_id, ticker, ticker FROM oltp.company;
    """)
    conn.commit()

    # date_dim
    cur.execute("""
    INSERT OR REPLACE INTO date_dim (date_key, date, year, quarter, month, day)
    SELECT DISTINCT
        CAST(strftime('%Y%m%d', filing_date) AS INT) AS date_key,
        filing_date,
        CAST(strftime('%Y', filing_date) AS INT) AS year,
        ((CAST(strftime('%m', filing_date) AS INT)-1)/3)+1 AS quarter,
        CAST(strftime('%m', filing_date) AS INT) AS month,
        CAST(strftime('%d', filing_date) AS INT) AS day
    FROM oltp.filing;
    """)
    conn.commit()

    # statement_type_dim
    cur.execute("""
    INSERT OR REPLACE INTO statement_type_dim (statement_type_key, statement_type)
    SELECT statement_type_id, name FROM oltp.statement_type;
    """)
    conn.commit()

    # 3. Populate Fact Table
    cur.execute("""
    INSERT INTO fact_financials (company_key, date_key, statement_type_key, line_item_id, value)
    SELECT
        f.company_id AS company_key,
        CAST(strftime('%Y%m%d', fl.filing_date) AS INT) AS date_key,
        fl.filing_id AS statement_type_key,
        ff.line_item_id,
        ff.value
    FROM oltp.financial_fact ff
    JOIN oltp.filing fl ON ff.filing_id = fl.filing_id
    JOIN oltp.company f ON fl.company_id = f.company_id;
    """)
    conn.commit()

    conn.close()
    log.logger.info("OLAP schema created & populated successfully!")

