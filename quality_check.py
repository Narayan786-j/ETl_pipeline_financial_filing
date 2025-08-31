import sqlite3

def run_quality_checks(olap_db="olap_db.sqlite", oltp_db="fin_db.sqlite"):
    conn = sqlite3.connect(olap_db)
    cur = conn.cursor()

    # Attach OLTP
    cur.execute(f"ATTACH DATABASE '{oltp_db}' AS oltp;")

    results = {}

    # (1) No future dates
    cur.execute("""
        SELECT COUNT(*) FROM date_dim WHERE date > DATE('now');
    """)
    results["future_dates"] = cur.fetchone()[0]

    # (2) No duplicate fact IDs
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT fact_id, COUNT(*) c
            FROM fact_financials
            GROUP BY fact_id
            HAVING c > 1
        );
    """)
    results["duplicate_fact_ids"] = cur.fetchone()[0]

    # (3) Revenue must be > 0
    cur.execute("""
        SELECT COUNT(*) 
        FROM fact_financials ff
        JOIN oltp.line_item li ON ff.line_item_id = li.line_item_id
        WHERE li.name = 'Revenue' AND ff.value <= 0;
    """)
    results["revenue_non_positive"] = cur.fetchone()[0]

    # (4) Required metrics must be present
    cur.execute("""
        SELECT COUNT(*) 
        FROM (
            SELECT company_key, date_key
            FROM fact_financials
            GROUP BY company_key, date_key
            HAVING SUM(CASE WHEN line_item_id IS NULL THEN 1 ELSE 0 END) > 0
        );
    """)
    results["missing_required_metrics"] = cur.fetchone()[0]

    # (5) No orphaned foreign keys
    cur.execute("""
        SELECT COUNT(*) 
        FROM fact_financials ff
        LEFT JOIN company_dim cd ON ff.company_key = cd.company_key
        WHERE cd.company_key IS NULL;
    """)
    results["orphaned_company_refs"] = cur.fetchone()[0]

    conn.close()
    return results
