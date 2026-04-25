"""
GY6 Deal Pipeline — Layer D: Notifications + Seller Email Outreach
Triggers on A/B grade deals. Sends alert email to Travis + generates
AI seller outreach email via Claude API.

Free stack:
- Gmail SMTP (smtplib, no cost)
- Claude API for seller letter generation (claude-haiku-4-5 — lowest cost)
- Airtable webhook polling (free tier)

Env vars required:
  GMAIL_ADDRESS      — your Gmail address
  GMAIL_APP_PASSWORD — Gmail App Password (not your login password)
  ALERT_EMAIL        — where alerts go (can be same as GMAIL_ADDRESS)
  ANTHROPIC_API_KEY  — for seller letter generation
  AIRTABLE_API_KEY   — to poll for new deals
  AIRTABLE_BASE_ID
  AIRTABLE_TABLE     — default: Deals
"""

import smtplib
import os
import json
import time
import logging
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# ─── CONFIG ────────────────────────────────────────────────────────────────────

GMAIL_ADDRESS      = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
ALERT_EMAIL        = os.getenv("ALERT_EMAIL", GMAIL_ADDRESS)
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
AIRTABLE_API_KEY   = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID   = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE     = os.getenv("AIRTABLE_TABLE", "Deals")

# Sender identity for outreach emails
SENDER_NAME    = "Travis"
SENDER_COMPANY = ""  # intentionally blank — personal outreach, not GY6 branded

# ─── GMAIL SMTP SENDER ─────────────────────────────────────────────────────────

def send_email(to: str, subject: str, body_html: str, body_text: str = "") -> bool:
    """
    Send email via Gmail SMTP using App Password (free).
    Requires: Gmail account with 2FA enabled + App Password generated.
    Setup: myaccount.google.com → Security → App Passwords → Mail
    """
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        log.warning("Gmail not configured — skipping email send")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"GY6 Deal Pipeline <{GMAIL_ADDRESS}>"
    msg["To"]      = to

    if body_text:
        msg.attach(MIMEText(body_text, "plain"))
    msg.attach(MIMEText(body_html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_ADDRESS, to, msg.as_string())
        log.info(f"Email sent → {to} | {subject}")
        return True
    except Exception as e:
        log.error(f"Email send failed: {e}")
        return False


# ─── ALERT EMAIL ───────────────────────────────────────────────────────────────

def build_alert_html(deals: list[dict]) -> str:
    """Build HTML alert email for A/B grade deals."""
    grade_colors = {"A": "#4A5240", "B": "#8B4513", "C": "#888780"}
    rows = ""
    for d in deals:
        color = grade_colors.get(d.get("grade", "C"), "#888")
        rows += f"""
        <tr style="border-bottom: 1px solid #eee;">
          <td style="padding:10px 12px; font-weight:bold; color:{color}; font-size:16px;">{d.get('grade','?')}</td>
          <td style="padding:10px 12px;">{d.get('Address') or d.get('SITUS_ADDR','—')}</td>
          <td style="padding:10px 12px;">{d.get('County','—')}</td>
          <td style="padding:10px 12px;">{d.get('Acres') or d.get('ACRES','—')} ac</td>
          <td style="padding:10px 12px;">${int(d.get('Price Est') or d.get('price_est') or 0):,}</td>
          <td style="padding:10px 12px; font-size:12px; color:#666;">{d.get('Zoning') or d.get('effective_zoning','—')}</td>
          <td style="padding:10px 12px; font-size:11px; color:#888;">{d.get('Flags') or d.get('flags','')}</td>
        </tr>"""

    return f"""
    <html><body style="font-family:Arial,sans-serif; color:#2C2C2A; max-width:900px; margin:0 auto;">
      <div style="background:#4A5240; padding:20px 24px; border-radius:8px 8px 0 0;">
        <h2 style="color:#FAF8F4; margin:0; font-size:20px;">GY6 Deal Alert</h2>
        <p style="color:#C8B89A; margin:4px 0 0; font-size:13px;">
          {len(deals)} deal{'s' if len(deals) > 1 else ''} passed filters · {datetime.now().strftime('%A %B %d, %Y')}
        </p>
      </div>
      <div style="border:1px solid #ddd; border-top:none; border-radius:0 0 8px 8px; overflow:hidden;">
        <table style="width:100%; border-collapse:collapse; font-size:13px;">
          <thead style="background:#F5F2EC;">
            <tr>
              <th style="padding:10px 12px; text-align:left;">Grade</th>
              <th style="padding:10px 12px; text-align:left;">Address</th>
              <th style="padding:10px 12px; text-align:left;">County</th>
              <th style="padding:10px 12px; text-align:left;">Acres</th>
              <th style="padding:10px 12px; text-align:left;">Price Est</th>
              <th style="padding:10px 12px; text-align:left;">Zoning</th>
              <th style="padding:10px 12px; text-align:left;">Flags</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
      <p style="font-size:12px; color:#888; margin-top:16px;">
        GY6 Services · Automated Deal Pipeline · Reply to this email to unsubscribe from alerts
      </p>
    </body></html>"""


def send_deal_alert(deals: list[dict]) -> bool:
    """Send deal alert email to Travis for all A/B grade deals."""
    if not deals:
        return False

    a_count = sum(1 for d in deals if d.get("grade") == "A")
    b_count = sum(1 for d in deals if d.get("grade") == "B")
    subject  = f"🟢 GY6 Deal Alert — {a_count}A / {b_count}B deals · {datetime.now().strftime('%b %d')}"

    html = build_alert_html(deals)
    text = f"GY6 Deal Alert — {len(deals)} deals passed filters.\n\n"
    for d in deals:
        text += f"[{d.get('grade')}] {d.get('Address') or d.get('SITUS_ADDR','?')} — {d.get('County','?')} — {d.get('Acres') or d.get('ACRES','?')}ac — ${int(d.get('price_est') or 0):,}\n"

    return send_email(ALERT_EMAIL, subject, html, text)


# ─── SELLER LETTER GENERATION (CLAUDE API) ─────────────────────────────────────

def generate_seller_letter(deal: dict) -> str:
    """
    Generate a personalized seller outreach email via Claude API.
    Uses claude-haiku for cost efficiency (~$0.001 per letter).
    Returns plain text email body ready to send.
    """
    if not ANTHROPIC_API_KEY:
        log.warning("Anthropic API key not set — using template fallback")
        return _fallback_seller_letter(deal)

    address  = deal.get("Address") or deal.get("SITUS_ADDR") or "your property"
    county   = deal.get("County", "Maryland")
    acres    = deal.get("Acres") or deal.get("ACRES", "approximately 1")
    owner    = deal.get("Owner") or deal.get("OWNER_NAME") or ""
    # Personalize greeting if we have owner name
    owner_first = owner.split()[0].title() if owner and len(owner.split()) > 0 else ""

    prompt = f"""You are writing a brief, direct, personal real estate inquiry letter from a buyer to a landowner.

Property details:
- Address: {address}
- County: {county}, Maryland  
- Size: {acres} acres
- Owner name: {owner or 'Unknown'}

Write a short, genuine outreach email (150-200 words max) from a local buyer named Travis who is interested in purchasing this land. 

Rules:
- Direct and warm, not salesy
- No corporate language, no buzzwords
- Do not mention real estate investing, flipping, or development
- Simply express genuine interest in purchasing, flexibility on terms, ability to close quickly
- Ask if they'd be open to a conversation
- No subject line — body only
- End with: Travis

{'Start with: Hi ' + owner_first + ',' if owner_first else 'Start with: Hi,'}

Write the email body only:"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 400,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        data = r.json()
        return data["content"][0]["text"].strip()
    except Exception as e:
        log.warning(f"Claude API letter generation failed: {e}")
        return _fallback_seller_letter(deal)


def _fallback_seller_letter(deal: dict) -> str:
    """Plain template fallback if API is unavailable."""
    address = deal.get("Address") or deal.get("SITUS_ADDR") or "your property"
    owner   = deal.get("Owner") or deal.get("OWNER_NAME") or ""
    greeting = f"Hi {owner.split()[0].title()}," if owner else "Hi,"
    return f"""{greeting}

My name is Travis and I came across your property at {address} while looking for land in the area.

I'm a local buyer and I'm genuinely interested in making an offer. I can move quickly, I'm flexible on terms, and I'm easy to work with.

If you'd be open to a brief conversation, I'd love to connect. No pressure at all — just want to see if there's a fit.

Feel free to reply here or call me at your convenience.

Travis"""


def build_seller_email_html(letter_body: str, deal: dict) -> str:
    """Wrap plain text letter in minimal HTML for email send."""
    paragraphs = "".join(
        f'<p style="margin:0 0 14px; line-height:1.6;">{p.strip()}</p>'
        for p in letter_body.split("\n") if p.strip()
    )
    return f"""
    <html><body style="font-family:Arial,sans-serif; font-size:14px; color:#2C2C2A; max-width:560px; margin:0 auto; padding:24px;">
      {paragraphs}
    </body></html>"""


def send_seller_outreach(deal: dict) -> bool:
    """
    Generate and send a seller outreach email for a qualifying deal.
    Only sends if owner email is available (from enrichment or manual entry).
    If no email available, saves draft to Airtable for manual follow-up.
    """
    owner_email = deal.get("Owner Email") or deal.get("owner_email")

    letter = generate_seller_letter(deal)
    address = deal.get("Address") or deal.get("SITUS_ADDR") or "property"

    if owner_email:
        subject = f"Interested in your land at {address}"
        html    = build_seller_email_html(letter, deal)
        success = send_email(owner_email, subject, html, letter)
        if success:
            _update_airtable_outreach(deal, letter, "Sent")
        return success
    else:
        # No email — save draft to Airtable for manual outreach
        log.info(f"No owner email for {address} — saving draft to Airtable")
        _update_airtable_outreach(deal, letter, "Draft — No Email")
        # Also forward draft to Travis so he can find contact info manually
        _send_draft_to_travis(deal, letter)
        return False


def _send_draft_to_travis(deal: dict, letter: str):
    """Send unsent seller letter drafts to Travis for manual follow-up."""
    address = deal.get("Address") or deal.get("SITUS_ADDR") or "unknown"
    owner   = deal.get("Owner") or deal.get("OWNER_NAME") or "unknown"
    county  = deal.get("County", "")

    html = f"""
    <html><body style="font-family:Arial,sans-serif; color:#2C2C2A; max-width:700px; margin:0 auto;">
      <div style="background:#4A5240; padding:16px 20px; border-radius:8px 8px 0 0;">
        <h3 style="color:#FAF8F4; margin:0;">Seller Letter Draft — Manual Follow-Up Needed</h3>
      </div>
      <div style="border:1px solid #ddd; border-top:none; padding:20px; border-radius:0 0 8px 8px;">
        <p><strong>Property:</strong> {address}</p>
        <p><strong>Owner:</strong> {owner}</p>
        <p><strong>County:</strong> {county}</p>
        <p><strong>Grade:</strong> {deal.get('grade','?')} (Score: {deal.get('score','?')})</p>
        <hr style="border:none; border-top:1px solid #eee; margin:16px 0;">
        <p style="color:#666; font-size:12px; margin-bottom:8px;">GENERATED LETTER — find owner contact info, then send:</p>
        <div style="background:#F5F2EC; padding:16px; border-radius:6px; font-size:13px; line-height:1.7;">
          {''.join(f'<p style="margin:0 0 10px">{p}</p>' for p in letter.split(chr(10)) if p.strip())}
        </div>
        <p style="font-size:12px; color:#888; margin-top:16px;">
          Find owner contact: county property records, Whitepages, BeenVerified (free tier), or Spokeo
        </p>
      </div>
    </body></html>"""

    send_email(
        ALERT_EMAIL,
        f"📋 Seller Draft — {address} [{deal.get('grade','?')} grade]",
        html,
        letter
    )


def _update_airtable_outreach(deal: dict, letter: str, status: str):
    """Update Airtable record with outreach letter and status."""
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        return
    parcel_id = deal.get("Parcel ID") or deal.get("PARCEL_ID")
    if not parcel_id:
        return

    # Find record by Parcel ID
    url     = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}
    try:
        r = requests.get(url, headers=headers,
                         params={"filterByFormula": f"{{Parcel ID}}='{parcel_id}'"}, timeout=15)
        records = r.json().get("records", [])
        if records:
            record_id = records[0]["id"]
            requests.patch(
                f"{url}/{record_id}", headers=headers,
                json={"fields": {"Outreach Letter": letter, "Outreach Status": status,
                                 "Outreach Date": datetime.today().strftime("%Y-%m-%d")}},
                timeout=15
            )
    except Exception as e:
        log.debug(f"Airtable outreach update failed: {e}")


# ─── AIRTABLE POLL — NEW DEAL TRIGGER ──────────────────────────────────────────

def poll_airtable_new_deals(since_hours: int = 25) -> list[dict]:
    """
    Poll Airtable for new A/B grade deals added in the last N hours.
    Used when running Layer D standalone (not called directly from pipeline).
    """
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        log.warning("Airtable not configured")
        return []

    url     = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    cutoff  = (datetime.now() - timedelta(hours=since_hours)).strftime("%Y-%m-%d")

    try:
        r = requests.get(url, headers=headers, params={
            "filterByFormula": f"AND(OR({{Grade}}='A',{{Grade}}='B'), {{Pulled Date}}>='{cutoff}', {{Outreach Status}}='')",
            "fields[]": ["Parcel ID","Address","County","Acres","Price Est","Zoning",
                         "Flood Zone","Score","Grade","Flags","Owner","Pulled Date","Owner Email"],
        }, timeout=20)
        records = r.json().get("records", [])
        return [rec["fields"] for rec in records]
    except Exception as e:
        log.error(f"Airtable poll failed: {e}")
        return []


# ─── PROXIMITY SCORING UPGRADE ─────────────────────────────────────────────────
# Injected into pipeline.score_deal — tenant pool anchors for MD market

# Key employment/transit anchors in the 4-county footprint
# (lat, lon, name, radius_miles, score_bonus)
TENANT_ANCHORS = [
    (39.1051, -76.7784, "Fort Meade / NSA",          10, 25),
    (38.8108, -76.8680, "Joint Base Andrews",          8, 20),
    (39.0899, -76.8527, "Capitol Technology Univ",     5, 15),
    (39.1437, -76.7290, "BWI / Airport corridor",      6, 15),
    (38.9784, -76.9442, "PG County Metro Green Line",  4, 20),
    (39.0458, -76.9413, "Greenbelt Metro",              4, 18),
    (39.1115, -76.9319, "College Park Metro",           4, 18),
    (39.1774, -76.6684, "MARC Penn Line — Odenton",    3, 15),
    (39.1579, -76.7301, "MARC Penn Line — Jessup",     3, 12),
    (39.2895, -76.7271, "MARC Camden — Dorsey",        3, 12),
    (39.2148, -76.8624, "Columbia Employment Hub",     6, 15),
]


def _haversine_miles(lat1, lon1, lat2, lon2) -> float:
    """Calculate distance in miles between two lat/lon points."""
    import math
    R = 3958.8  # Earth radius miles
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def score_proximity(lat: float, lon: float) -> tuple[int, list[str]]:
    """
    Score a parcel's proximity to tenant pool anchors.
    Returns (bonus_score, list_of_nearby_anchors).
    Only the highest single bonus applies to prevent double-counting.
    """
    hits   = []
    bonuses = []
    for alat, alon, name, radius, bonus in TENANT_ANCHORS:
        dist = _haversine_miles(lat, lon, alat, alon)
        if dist <= radius:
            hits.append(f"{name} ({dist:.1f}mi)")
            bonuses.append(bonus)

    if not hits:
        return -10, ["no major employment/transit anchor within range"]

    # Take the top bonus only (avoid stacking), but note all anchors
    return max(bonuses), hits


# ─── MAIN — STANDALONE LAYER D RUN ────────────────────────────────────────────

def run_layer_d(deals: list[dict] = None):
    """
    Run Layer D on a list of deals (passed from pipeline) or poll Airtable.
    1. Send alert email to Travis for all A/B deals
    2. Generate + send/draft seller outreach for each A/B deal
    """
    if deals is None:
        log.info("Polling Airtable for new A/B deals...")
        deals = poll_airtable_new_deals()

    ab_deals = [d for d in deals if d.get("grade") in ("A", "B")]

    if not ab_deals:
        log.info("Layer D: no A/B deals to action")
        return

    log.info(f"Layer D: actioning {len(ab_deals)} A/B deals")

    # 1. Send consolidated alert
    send_deal_alert(ab_deals)

    # 2. Generate seller outreach for each
    for deal in ab_deals:
        log.info(f"  Generating outreach: {deal.get('Address') or deal.get('SITUS_ADDR','?')}")
        send_seller_outreach(deal)
        time.sleep(1)  # gentle rate limit on Claude API calls

    log.info("Layer D complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_layer_d()
