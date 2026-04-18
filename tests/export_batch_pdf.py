"""
Export OBGYN batch email preview to PDF.
Usage: python -m tests.export_batch_pdf --offset 0 --batch-size 5
Output: obgyn_batch_preview_offset0.pdf in the project root
"""

import argparse
import os
import sys
import logging
logging.basicConfig(level=logging.WARNING)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fpdf import FPDF
from fpdf.enums import XPos, YPos
from db import get_connection, get_campaign_brief
from psycopg2.extras import RealDictCursor
from generation.email_generator import generate_personalized_sequence

OBGYN_CAMPAIGN_ID = "b3fafa6f-623d-4c55-a475-0dc6ddfc5e6e"
FONT_PATH = r"C:\Windows\Fonts\calibri.ttf"
FONT_PATH_B = r"C:\Windows\Fonts\calibrib.ttf"
FONT_PATH_I = r"C:\Windows\Fonts\calibrii.ttf"
FONT_PATH_BI = r"C:\Windows\Fonts\calibriz.ttf"


def fetch_batch(campaign_id, offset, limit):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT l.lead_id, l.owner_name, l.business_name, l.email,
                       l.website, l.business_domain, l.city, l.state,
                       l.email_verdict, es.sequence_id
                FROM leads l
                LEFT JOIN email_sequences es ON l.lead_id = es.lead_id
                WHERE l.campaign_id = %s
                  AND l.email_verdict = 'SEND'
                ORDER BY l.ingested_at ASC
                LIMIT %s OFFSET %s
            """, (campaign_id, limit, offset))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


class BatchPDF(FPDF):
    def header(self):
        self.set_font("Calibri", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "HealthTalk AI \u2014 OBGYN Email Sequence Preview",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.ln(2)
        self.set_draw_color(200, 200, 200)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-12)
        self.set_font("Calibri", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 6, f"Page {self.page_no()}", align="C")


def add_lead_section(pdf, index, lead, sequences, website_insights):
    name = lead.get("owner_name") or lead.get("business_name") or lead.get("email", "Unknown")
    practice = lead.get("business_name", "")
    city = lead.get("city", "")
    state = lead.get("state", "")
    location = f"{city}, {state}".strip(", ") if city or state else "Unknown"
    website = lead.get("website") or (
        f"https://{lead['business_domain']}" if lead.get("business_domain")
        else (f"https://{lead['email'].split('@')[1]}" if lead.get("email") and "@" in lead["email"] else "N/A")
    )

    pdf.add_page()

    # Lead header bar
    pdf.set_fill_color(30, 80, 140)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Calibri", "B", 12)
    pdf.cell(0, 10, f"  Lead {index}: {name}",
             fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # Practice details
    pdf.set_fill_color(240, 245, 255)
    pdf.set_text_color(50, 50, 50)
    pdf.set_font("Calibri", "", 9)
    pdf.cell(0, 7, f"  Practice: {practice}   |   Location: {location}   |   Website: {website}",
             fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(4)

    # Website insights section
    pdf.set_font("Calibri", "B", 10)
    pdf.set_text_color(30, 80, 140)
    pdf.cell(0, 6, "Website Insights", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_draw_color(30, 80, 140)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(2)

    pdf.set_font("Calibri", "", 9)
    pdf.set_text_color(60, 60, 60)
    if website_insights and "No distinctive details" not in website_insights:
        pdf.multi_cell(0, 5, website_insights.strip())
    else:
        pdf.set_text_color(150, 100, 50)
        pdf.cell(0, 5, "No distinctive details found \u2014 generic OBGYN angle used.",
                 new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(5)

    # Emails
    email_labels = {
        1: "EMAIL 1 \u2014 Initial Outreach",
        2: "EMAIL 2 \u2014 Allegiance Follow-Up",
        3: "EMAIL 3 \u2014 Break-Up"
    }
    email_colors = {1: (20, 120, 60), 2: (120, 60, 20), 3: (100, 20, 80)}

    for i in range(1, 4):
        subject = sequences.get(f"email_{i}_subject", "") if sequences else ""
        body = sequences.get(f"email_{i}_body", "[Generation failed]") if sequences else "[Generation failed]"

        r, g, b = email_colors[i]
        pdf.set_font("Calibri", "B", 10)
        pdf.set_text_color(r, g, b)
        pdf.cell(0, 6, email_labels[i], new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_draw_color(r, g, b)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(1)

        pdf.set_font("Calibri", "BI", 9)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(0, 5, f'Subject: "{subject}"', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(1)

        pdf.set_font("Calibri", "", 9)
        pdf.set_text_color(40, 40, 40)
        pdf.multi_cell(0, 5, body.strip())
        pdf.ln(5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--campaign", default=OBGYN_CAMPAIGN_ID)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    print(f"Fetching {args.batch_size} leads (offset {args.offset})...")
    leads = fetch_batch(args.campaign, args.offset, args.batch_size)
    if not leads:
        print("No leads found.")
        return

    brief = get_campaign_brief(args.campaign)
    if not brief:
        print("No campaign brief found.")
        sys.exit(1)

    results = []
    for i, lead in enumerate(leads, 1):
        name = lead.get("owner_name") or lead.get("business_name") or lead.get("email")
        print(f"[{i}/{len(leads)}] {name}...")
        seqs, insights, _ = generate_personalized_sequence(
            lead=dict(lead), brief=dict(brief), campaign_id=args.campaign
        )
        results.append((lead, seqs, insights))

    pdf = BatchPDF(orientation="P", unit="mm", format="A4")
    pdf.add_font("Calibri", "", FONT_PATH)
    pdf.add_font("Calibri", "B", FONT_PATH_B)
    pdf.add_font("Calibri", "I", FONT_PATH_I)
    pdf.add_font("Calibri", "BI", FONT_PATH_BI)
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_margins(10, 15, 10)

    for i, (lead, seqs, insights) in enumerate(results, 1):
        add_lead_section(pdf, i, lead, seqs, insights)

    output_path = args.output or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        f"obgyn_batch_preview_offset{args.offset}.pdf"
    )
    pdf.output(output_path)
    print(f"\nPDF saved: {output_path}")


if __name__ == "__main__":
    main()