"""
ETL: load single dataframe of financial statements into a normalized OLTP schema.

"""

import pandas as pd
from sqlalchemy import (create_engine, Column, Integer, String, Date, Boolean, Numeric, ForeignKey, UniqueConstraint, MetaData)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import insert as pg_insert  # optional PG upsert
from datetime import datetime
import logger as log

# -------------------------
# CONFIG
# -------------------------
DATABASE_URL = "sqlite:///fin_db.sqlite" 
BATCH_SIZE = 1000  # for bulk inserts

# -------------------------
# SQLAlchemy models
# -------------------------
engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, future=True)
Base = declarative_base()


class Company(Base):
    __tablename__ = "company"
    company_id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(20), unique=True, nullable=False)


class Filing(Base):
    __tablename__ = "filing"
    filing_id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("company.company_id"), nullable=False)
    filing_date = Column(Date, nullable=False)
    fiscal_year = Column(Integer)
    is_audited = Column(Boolean)
    # uniqueness: company + filing_date + fiscal_year + is_audited
    __table_args__ = (UniqueConstraint('company_id', 'filing_date', 'fiscal_year', 'is_audited',
                                       name='uq_filing_company_date_year_audited'),)

    company = relationship("Company")


class StatementType(Base):
    __tablename__ = "statement_type"
    statement_type_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)


class LineItem(Base):
    __tablename__ = "line_item"
    line_item_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)


class FinancialFact(Base):
    __tablename__ = "financial_fact"
    fact_id = Column(Integer, primary_key=True, autoincrement=True)
    filing_id = Column(Integer, ForeignKey("filing.filing_id"), nullable=False)
    statement_type_id = Column(Integer, ForeignKey("statement_type.statement_type_id"), nullable=False)
    line_item_id = Column(Integer, ForeignKey("line_item.line_item_id"), nullable=False)
    period_type = Column(String(50))
    end_date = Column(Date)
    value = Column(Numeric(20, 2))


# -------------------------
# Create tables (if not exists)
# -------------------------
Base.metadata.create_all(bind=engine)


# -------------------------
# Helper functions
# -------------------------
def parse_date(d):
    if pd.isna(d):
        return None
    if isinstance(d, (datetime, pd.Timestamp)):
        return d.date()
    # Try common formats:
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(str(d), fmt).date()
        except Exception:
            pass
    # last resort: pandas
    try:
        return pd.to_datetime(d, errors="coerce").date()
    except Exception:
        return None


def to_bool(v):
    if pd.isna(v):
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "1", "t", "yes", "y"):
        return True
    if s in ("false", "0", "f", "no", "n"):
        return False
    return None


# -------------------------
# ETL function
# -------------------------
def load_dataframe_to_db(df: pd.DataFrame):
    """
    df must contain columns:
    ['Ticker','Filing Date','Statement Type','Line Item','Period Type','End Date','Fiscal Year','Unaudited','Value']
    """
    # Normalize column names to predictable names
    df = df.rename(columns={
        'Ticker': 'ticker',
        'Filing Date': 'filing_date',
        'Statement Type': 'statement_type',
        'Line Item': 'line_item',
        'Period Type': 'period_type',
        'End Date': 'end_date',
        'Fiscal Year': 'fiscal_year',
        'Unaudited': 'is_audited',
        'Value': 'value'
    })

    # Clean & parse
    df['filing_date_parsed'] = df['filing_date'].apply(parse_date)
    df['end_date_parsed'] = df['end_date'].apply(parse_date)
    df['is_audited_bool'] = df['is_audited'].apply(to_bool)
    # Ensure numeric values
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    session = SessionLocal()
    try:
        # -------------------------
        # 1) Upsert companies (tickers)
        # -------------------------
        tickers = df['ticker'].dropna().astype(str).str.upper().unique().tolist()

        # Fetch existing
        existing_companies = session.query(Company).filter(Company.ticker.in_(tickers)).all()
        existing_ticker_map = {c.ticker: c.company_id for c in existing_companies}

        new_company_objs = []
        for t in tickers:
            if t not in existing_ticker_map:
                new_company_objs.append({"ticker": t})

        if new_company_objs:
            session.bulk_insert_mappings(Company, new_company_objs)
            session.commit()
            # refresh existing map
            existing_companies = session.query(Company).filter(Company.ticker.in_(tickers)).all()
            existing_ticker_map = {c.ticker: c.company_id for c in existing_companies}

        # -------------------------
        # 2) Upsert statement types
        # -------------------------
        stmt_names = df['statement_type'].dropna().astype(str).unique().tolist()
        existing_stmt = session.query(StatementType).filter(StatementType.name.in_(stmt_names)).all()
        existing_stmt_map = {s.name: s.statement_type_id for s in existing_stmt}
        new_stmt_objs = [{"name": s} for s in stmt_names if s not in existing_stmt_map]
        if new_stmt_objs:
            session.bulk_insert_mappings(StatementType, new_stmt_objs)
            session.commit()
            existing_stmt = session.query(StatementType).filter(StatementType.name.in_(stmt_names)).all()
            existing_stmt_map = {s.name: s.statement_type_id for s in existing_stmt}

        # -------------------------
        # 3) Upsert line items
        # -------------------------
        li_names = df['line_item'].dropna().astype(str).unique().tolist()
        existing_li = session.query(LineItem).filter(LineItem.name.in_(li_names)).all()
        existing_li_map = {li.name: li.line_item_id for li in existing_li}
        new_li_objs = [{"name": li} for li in li_names if li not in existing_li_map]
        if new_li_objs:
            session.bulk_insert_mappings(LineItem, new_li_objs)
            session.commit()
            existing_li = session.query(LineItem).filter(LineItem.name.in_(li_names)).all()
            existing_li_map = {li.name: li.line_item_id for li in existing_li}

        # -------------------------
        # 4) Upsert filings (unique per company + filing_date + fiscal_year + is_audited)
        # -------------------------
        # Create canonical rows of filings from df
        filings_df = df[['ticker', 'filing_date_parsed', 'fiscal_year', 'is_audited_bool']].drop_duplicates()
        # map ticker -> company_id
        filings_df['ticker'] = filings_df['ticker'].astype(str).str.upper()
        filings_df['company_id'] = filings_df['ticker'].map(existing_ticker_map)
        filings_to_create = []
        for _, row in filings_df.iterrows():
            # skip rows without company or filed date
            if pd.isna(row['company_id']) or pd.isna(row['filing_date_parsed']):
                continue
            filings_to_create.append({
                "company_id": int(row['company_id']),
                "filing_date": row['filing_date_parsed'],
                "fiscal_year": int(row['fiscal_year']) if not pd.isna(row['fiscal_year']) else None,
                "is_audited": bool(row['is_audited_bool']) if row['is_audited_bool'] is not None else None
            })

        # Fetch existing filings to avoid duplicates
        # Build a set of keys to check
        existing_filing_map = {}
        if filings_to_create:
            # Query possible matching filings
            # Note: this query looks for filings for the companies involved.
            company_ids = list({f['company_id'] for f in filings_to_create})
            existing_filings = session.query(Filing).filter(Filing.company_id.in_(company_ids)).all()
            for f in existing_filings:
                key = (f.company_id, f.filing_date, f.fiscal_year, f.is_audited)
                existing_filing_map[key] = f.filing_id

        # Insert only missing filings
        new_filings = []
        for f in filings_to_create:
            key = (f['company_id'], f['filing_date'], f['fiscal_year'], f['is_audited'])
            if key not in existing_filing_map:
                new_filings.append(f)
        if new_filings:
            session.bulk_insert_mappings(Filing, new_filings)
            session.commit()
            # refresh filing map
            company_ids = list({f['company_id'] for f in filings_to_create})
            existing_filings = session.query(Filing).filter(Filing.company_id.in_(company_ids)).all()
            for f in existing_filings:
                key = (f.company_id, f.filing_date, f.fiscal_year, f.is_audited)
                existing_filing_map[key] = f.filing_id


        # 5) Prepare facts and bulk insert
        # Map helper lookups in memory
        ticker_to_company_id = existing_ticker_map
        stmtname_to_id = existing_stmt_map
        lineitem_to_id = existing_li_map
        # existing_filing_map maps (company_id, filing_date, fiscal_year, is_audited) -> filing_id

        # Build financial_fact dictionaries
        fact_rows = []
        for _, r in df.iterrows():
            t = str(r.get('ticker')) if pd.notna(r.get('ticker')) else None
            if t is None:
                continue
            t = t.upper()
            company_id = ticker_to_company_id.get(t)
            filing_date = r.get('filing_date_parsed')
            fy = int(r['fiscal_year']) if pd.notna(r.get('fiscal_year')) else None
            is_aud = bool(r['is_audited_bool']) if r['is_audited_bool'] is not None else None
            filing_key = (company_id, filing_date, fy, is_aud)
            filing_id = existing_filing_map.get(filing_key)
            # If filing_id missing (maybe because filing_date parsing failed), skip
            if filing_id is None:
                # try to find filing by company+filing_date only as fallback
                fallback_filings = [k for k in existing_filing_map.keys() if k[0] == company_id and k[1] == filing_date]
                if fallback_filings:
                    filing_id = existing_filing_map[fallback_filings[0]]
                else:
                    # can't map filing -> skip row (log/collect later)
                    continue

            stmt_name = r.get('statement_type')
            line_item_name = r.get('line_item')
            if pd.isna(stmt_name) or pd.isna(line_item_name):
                continue
            stmt_id = stmtname_to_id.get(stmt_name)
            li_id = lineitem_to_id.get(line_item_name)
            # If any id missing (shouldn't be), skip
            if stmt_id is None or li_id is None:
                continue

            end_date = r.get('end_date_parsed')
            val = r.get('value')
            # Accept null values too (store as NULL)
            fact_rows.append({
                "filing_id": filing_id,
                "statement_type_id": stmt_id,
                "line_item_id": li_id,
                "period_type": r.get('period_type'),
                "end_date": end_date,
                "value": float(val) if pd.notna(val) else None
            })

        # Bulk insert facts in chunks
        for i in range(0, len(fact_rows), BATCH_SIZE):
            chunk = fact_rows[i:i + BATCH_SIZE]
            if chunk:
                session.bulk_insert_mappings(FinancialFact, chunk)
                session.commit()

        log.logger.info(f"OLTP Inserted/Updated: companies={len(existing_ticker_map)}, "
              f"statement_types={len(existing_stmt_map)}, line_items={len(existing_li_map)}, "
              f"filings={len(existing_filing_map)}, facts={len(fact_rows)}")

    except Exception as exc:
        session.rollback()
        raise
    finally:
        session.close()

