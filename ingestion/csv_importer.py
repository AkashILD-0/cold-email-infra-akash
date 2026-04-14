import csv
import logging
from db import insert_lead

logger = logging.getLogger(__name__)

# Common CSV column name mappings -> our field names
COLUMN_MAPPINGS = {
    "business_name": ["business_name", "company", "company_name", "name", "store_name"],
    "business_domain": ["domain", "business_domain", "company_domain"],
    "website": ["website", "url", "website_url"],
    "email": ["email", "email_address"],
    "phone": ["phone", "phone_number"],
    "address": ["address", "street_address"],
    "city": ["city"],
    "state": ["state", "state_code"],
    "zip": ["zip", "zip_code", "postal_code"],
    "owner_name": ["owner", "owner_name", "contact_name", "first_name"],
    "industry": ["industry", "category"],
    "rating": ["rating", "google_rating"],
    "review_count": ["reviews", "review_count"],
}


def import_excel_sheet(file_path: str, sheet_name: str, campaign_id: str,
                       source_name: str = "client_csv") -> dict:
    """Import a single sheet from the HTAI Excel file into leads table.
    Row 1 = title (skipped), Row 2 = headers, data from Row 3.
    Columns: First Name, Last Name, Business Email, Physician Group Name,
    Main Specialty, Address, City, State, # of Physicians.
    Uses a single DB connection with bulk insert for speed.
    Returns {imported, skipped, errors}."""
    import openpyxl
    import json
    from db import get_connection
    stats = {"imported": 0, "skipped": 0, "errors": 0}

    wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    ws = wb[sheet_name]

    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if len(rows) < 2:
        logger.warning(f"Sheet '{sheet_name}' has no data.")
        return stats

    # Row 0 = sheet title, Row 1 = actual column headers
    raw_headers = [str(h).strip() if h is not None else "" for h in rows[1]]

    def col(name):
        name_lower = name.lower()
        for i, h in enumerate(raw_headers):
            if h.lower() == name_lower:
                return i
        return None

    idx = {
        "first_name": col("First Name"),
        "last_name":  col("Last Name"),
        "email":      col("Business Email"),
        "business":   col("Physician Group Name"),
        "specialty":  col("Main Specialty"),
        "address":    col("Address"),
        "city":       col("City"),
        "state":      col("State"),
        "num_phys":   col("# of Physicians"),
    }

    def get(row, key):
        i = idx.get(key)
        if i is None or i >= len(row):
            return None
        val = row[i]
        return str(val).strip() if val is not None else None

    # Parse all rows into lead records first (no DB yet)
    lead_records = []
    for row in rows[2:]:
        try:
            first = get(row, "first_name") or ""
            last  = get(row, "last_name") or ""
            owner = (first + " " + last).strip() or None
            email = get(row, "email")
            biz   = get(row, "business")

            if not biz and not email:
                stats["skipped"] += 1
                continue

            lead_records.append({
                "business_name": biz,
                "industry":      get(row, "specialty"),
                "address":       get(row, "address"),
                "city":          get(row, "city"),
                "state":         get(row, "state"),
                "company_size":  get(row, "num_phys"),
                "owner":         owner,
                "email":         email,
                "raw_data":      json.dumps({raw_headers[i]: str(row[i]) if row[i] is not None else None
                                             for i in range(len(raw_headers)) if i < len(row)}),
            })
        except Exception as e:
            logger.error(f"Excel parse error row {stats['skipped'] + stats['errors']}: {e}")
            stats["errors"] += 1

    if not lead_records:
        logger.info(f"Excel import [{sheet_name}]: no valid rows")
        return stats

    # Bulk insert using a single connection
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for rec in lead_records:
                try:
                    cur.execute("""
                        INSERT INTO leads (
                            campaign_id, business_name, industry,
                            address, city, state, company_size,
                            sources, raw_data,
                            owner_name, owner_status, owner_source,
                            email, email_source, enrichment_status,
                            country
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            'US'
                        )
                        ON CONFLICT DO NOTHING
                        RETURNING lead_id
                    """, (
                        campaign_id,
                        rec["business_name"],
                        rec["industry"],
                        rec["address"],
                        rec["city"],
                        rec["state"],
                        rec["company_size"],
                        [source_name],
                        rec["raw_data"],
                        rec["owner"] or None,
                        "found" if rec["owner"] else "pending",
                        "client_list" if rec["owner"] else None,
                        rec["email"] or None,
                        "client_list" if rec["email"] else None,
                        "partial" if rec["email"] else "raw",
                    ))
                    result = cur.fetchone()
                    if result:
                        stats["imported"] += 1
                    else:
                        stats["skipped"] += 1
                except Exception as e:
                    logger.error(f"Excel import error row {stats['imported'] + stats['errors']}: {e}")
                    stats["errors"] += 1
                    conn.rollback()

        conn.commit()
    finally:
        conn.close()

    logger.info(f"Excel import [{sheet_name}]: {stats}")
    return stats


def import_csv(file_path: str, campaign_id: str, source_name: str = "client_csv") -> dict:
    """Import a CSV file into leads table. Returns {imported, skipped, errors}."""
    stats = {"imported": 0, "skipped": 0, "errors": 0}

    with open(file_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        field_map = _build_field_map(reader.fieldnames)

        for row in reader:
            try:
                lead_data = _map_row(row, field_map, source_name)
                if not lead_data.get("business_name") and not lead_data.get("business_domain"):
                    stats["skipped"] += 1
                    continue
                insert_lead(lead_data, campaign_id)
                stats["imported"] += 1
            except Exception as e:
                logger.error(f"CSV import error row {stats['imported'] + stats['errors']}: {e}")
                stats["errors"] += 1

    logger.info(f"CSV import: {stats}")
    return stats


def _build_field_map(csv_headers: list) -> dict:
    """Map CSV headers to our field names."""
    field_map = {}
    normalized_headers = {h.strip().lower().replace(" ", "_"): h for h in csv_headers}

    for our_field, possible_names in COLUMN_MAPPINGS.items():
        for name in possible_names:
            if name in normalized_headers:
                field_map[our_field] = normalized_headers[name]
                break
    return field_map


def _map_row(row: dict, field_map: dict, source_name: str) -> dict:
    """Map a CSV row to a lead dict using the field map."""
    lead = {"sources": [source_name], "raw_data": dict(row)}
    for our_field, csv_header in field_map.items():
        value = row.get(csv_header, "").strip()
        if value:
            lead[our_field] = value
    if not lead.get("business_domain") and lead.get("website"):
        from ingestion.apify_client import _extract_domain
        lead["business_domain"] = _extract_domain(lead["website"])
    return lead