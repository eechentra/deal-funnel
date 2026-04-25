"""
GY6 Deal Pipeline v2 — Single file, no external dependencies beyond requests.
Data: Redfin public CSV (active land listings, no IP blocking)
Markets: Anne Arundel, Prince George's, Howard, Charles — Maryland
Buy box: ~1 acre, <$100K, density-permissive zoning, no flood, road+util, near tenant anchors
"""

import requests, csv, io, time, logging, os, math
from datetime import datetime
from urllib.parse import urlencode
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── ENV ────────────────────────────────────────────────────────────────────────
AIRTABLE_API_KEY   = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID   = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE     = os.getenv("AIRTABLE_TABLE", "Deals")
GMAIL_ADDRESS      = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
ALERT_EMAIL        = os.getenv("ALERT_EMAIL", "")
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")

# ── BUY BOX ────────────────────────────────────────────────────────────────────
BUY_BOX = {
    "min_acres": 0.5,
    "max_acres": 2.5,
    "max_price": 100_000,
    "allowed_zoning": ["R-10","R-15","R-22","R10","R15","R22","R-MF","MF","RM","R-2"],
    "exclude_flood_zones": ["AE","AO","AH","VE","V","A"],
}

# ── REDFIN COUNTY IDs ─────────────────────────────────────────────────────────
COUNTIES = {
    "anne_arundel":   {"name": "Anne Arundel",    "region_id": "1861", "region_type": "5"},
    "prince_georges": {"name": "Prince George's",  "region_id": "2470", "region_type": "5"},
    "howard":         {"name": "Howard",            "region_id": "2155", "region_type": "5"},
    "charles":        {"name": "Charles",           "region_id": "1897", "region_type": "5"},
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Referer": "https://www.redfin.com/",
}

# ── TENANT ANCHORS ─────────────────────────────────────────────────────────────
TENANT_ANCHORS = [
    (39.1051,-76.7784,"Fort Meade / NSA",         10, 25),
    (38.8108,-76.8680,"Joint Base Andrews",          8, 20),
    (39.0899,-76.8527,"Capitol Technology Univ",     5, 15),
    (39.1437,-76.7290,"BWI corridor",                6, 15),
    (38.9784,-76.9442,"PG Metro Green Line",         4, 20),
    (39.0458,-76.9413,"Greenbelt Metro",              4, 18),
    (39.1115,-76.9319,"College Park Metro",           4, 18),
    (39.1774,-76.6684,"MARC Penn — Odenton",         3, 15),
    (39.1579,-76.7301,"MARC Penn — Jessup",          3, 12),
    (39.2148,-76.8624,"Columbia Hub",                 6, 15),
]

# ── LAYER A: REDFIN DATA PULL ─────────────────────────────────────────────────
def pull_parcels(county_key: str) -> list[dict]:
    county = COUNTIES[county_key]
    log.info(f"Pulling Redfin listings: {county['name']}...")

    params = {
        "al": 1, "market": "dc",
        "max_price": int(BUY_BOX["max_price"] * 1.5),
        "num_homes": 350, "ord": "redfin-recommended-asc",
        "page_number": 1, "property_type": 6,
        "region_id": county["region_id"],
        "region_type": county["region_type"],
        "status": 9, "uipt": 6, "v": 8,
    }
    url = "https://www.redfin.com/stingray/api/gis-csv?" + urlencode(params)

    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            log.warning(f"  Redfin {r.status_code} for {county['name']}")
            return []

        content = r.text
        if not content or len(content) < 50:
            log.warning(f"  Redfin empty response for {county['name']}")
            return []

        # Skip Redfin disclaimer lines, find CSV header
        lines = content.split("\n")
        csv_start = 0
        for i, line in enumerate(lines):
            if "ADDRESS" in line.upper() or "PRICE" in line.upper():
                csv_start = i
                break

        reader = csv.DictReader(io.StringIO("\n".join(lines[csv_start:])))
        parcels = []
        for row in reader:
            price_str = row.get("PRICE","0").replace("$","").replace(",","").strip()
            sqft_str  = row.get("SQUARE FEET","0").replace(",","").strip()
            lot_str   = row.get("LOT SIZE","0").replace(",","").strip()

            try:
                price = float(price_str) if price_str not in ("","—") else 0
            except ValueError:
                price = 0

            acres = 0
            for val in [lot_str, sqft_str]:
                if val and val not in ("","—"):
                    try:
                        n = float(val)
                        acres = n / 43560 if n > 100 else n
                        if 0.1 < acres < 100:
                            break
                    except ValueError:
                        continue

            try:
                lat = float(row.get("LATITUDE","") or 0) or None
                lon = float(row.get("LONGITUDE","") or 0) or None
            except ValueError:
                lat = lon = None

            if price == 0 or acres < BUY_BOX["min_acres"]:
                continue

            parcels.append({
                "PARCEL_ID":      row.get("MLS#", f"RF-{len(parcels)}"),
                "SITUS_ADDR":     row.get("ADDRESS",""),
                "ACRES":          round(acres, 3),
                "LAND_VALUE":     price,
                "ASSESSED_VALUE": price,
                "OWNER_NAME":     "",
                "ZONING":         "",
                "LATITUDE":       lat,
                "LONGITUDE":      lon,
                "county":         county["name"],
                "data_source":    "Redfin",
                "days_on_market": row.get("DAYS ON MARKET",""),
                "listing_url":    row.get("URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)",""),
            })

        log.info(f"  {len(parcels)} listings returned")
        return parcels

    except Exception as e:
        log.warning(f"  Redfin failed for {county['name']}: {e}")
        return []

# ── LAYER B: FILTER ENGINE ────────────────────────────────────────────────────
def _haversine_miles(lat1, lon1, lat2, lon2) -> float:
    R = 3958.8
    lat1,lon1,lat2,lon2 = map(math.radians,[lat1,lon1,lat2,lon2])
    a = math.sin((lat2-lat1)/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin((lon2-lon1)/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def score_proximity(lat, lon) -> tuple[int, list[str]]:
    hits, bonuses = [], []
    for alat, alon, name, radius, bonus in TENANT_ANCHORS:
        if _haversine_miles(lat, lon, alat, alon) <= radius:
            hits.append(f"{name} ({_haversine_miles(lat, lon, alat, alon):.1f}mi)")
            bonuses.append(bonus)
    return (max(bonuses), hits) if hits else (-10, ["no anchor proximity"])

def check_flood_zone(lat, lon) -> str:
    url = "https://msc.fema.gov/arcgis/rest/services/NFHL_National/MapServer/28/query"
    params = {"geometry": f"{lon},{lat}", "geometryType": "esriGeometryPoint",
              "inSR": "4326", "spatialRel": "esriSpatialRelIntersects",
              "outFields": "FLD_ZONE", "returnGeometry": "false", "f": "json"}
    try:
        r = requests.get(url, params=params, timeout=15)
        features = r.json().get("features", [])
        return features[0]["attributes"].get("FLD_ZONE","X") if features else "X"
    except:
        return "UNKNOWN"

def check_road_access(lat, lon) -> bool:
    query = f"[out:json][timeout:10];way(around:100,{lat},{lon})[\"highway\"];out count;"
    try:
        r = requests.post("https://overpass-api.de/api/interpreter", data={"data": query}, timeout=20)
        count = r.json().get("elements",[{}])[0].get("tags",{}).get("total",0)
        return int(count) > 0
    except:
        return True

def score_deal(parcel: dict, flood_zone: str, has_road: bool) -> dict:
    score = 100
    flags = []
    price = parcel.get("LAND_VALUE", 0)
    acres = parcel.get("ACRES", 0)

    if price == 0:
        flags.append("no price data")
    elif price <= 50_000:
        score += 20; flags.append(f"strong price ${price:,.0f}")
    elif price <= 75_000:
        score += 10; flags.append(f"good price ${price:,.0f}")
    elif price <= 100_000:
        flags.append(f"at ceiling ${price:,.0f}")
    else:
        score -= 40; flags.append(f"over budget ${price:,.0f}")

    if 0.75 <= acres <= 1.5:
        score += 20; flags.append(f"ideal size {acres:.2f}ac")
    elif 0.5 <= acres <= 2.5:
        score += 5;  flags.append(f"acceptable size {acres:.2f}ac")

    if flood_zone in BUY_BOX["exclude_flood_zones"]:
        score -= 100; flags.append(f"FLOOD ZONE {flood_zone} — EXCLUDE")
    elif flood_zone == "X":
        score += 15; flags.append("flood zone X (safe)")
    else:
        flags.append(f"flood zone {flood_zone} — verify")

    if not has_road:
        score -= 30; flags.append("no road access")
    else:
        flags.append("road access confirmed")

    # Zoning — density-permissive only, exclude single-family
    zoning = parcel.get("ZONING","")
    sf_zones = ["R-1","R-2","R1","R2","RS","RR","R-A","RA"]
    sf_hit = any(z.upper() == zoning.upper().strip() for z in sf_zones)
    zone_match = any(z.upper() in zoning.upper() for z in BUY_BOX["allowed_zoning"])
    if sf_hit:
        score -= 50; flags.append(f"SINGLE FAMILY — EXCLUDE: {zoning}")
    elif zone_match:
        score += 15; flags.append(f"zoning match: {zoning}")
    elif zoning:
        score -= 20; flags.append(f"zoning mismatch: {zoning}")
    else:
        flags.append("zoning unknown — verify on site")

    # Proximity
    lat = parcel.get("LATITUDE")
    lon = parcel.get("LONGITUDE")
    if lat and lon:
        bonus, hits = score_proximity(lat, lon)
        score += bonus
        flags.append(f"anchors: {', '.join(hits[:2])}" if bonus > 0 else hits[0])

    grade = "A" if score >= 140 else "B" if score >= 100 else "C" if score >= 60 else "FAIL"

    return {**parcel, "score": score, "grade": grade, "flood_zone": flood_zone,
            "road_access": has_road, "effective_zoning": zoning,
            "flags": " | ".join(flags), "price_est": price,
            "pulled_date": datetime.today().strftime("%Y-%m-%d")}

def run_filter(parcels: list[dict]) -> list[dict]:
    results = []
    for i, p in enumerate(parcels):
        lat = p.get("LATITUDE")
        lon = p.get("LONGITUDE")
        price = p.get("LAND_VALUE", 0)

        if price > BUY_BOX["max_price"] * 1.5:
            continue
        if not lat or not lon:
            # Still score it, just skip geo checks
            scored = score_deal(p, "UNKNOWN", True)
            if scored["grade"] != "FAIL":
                results.append(scored)
            continue

        log.info(f"  [{i+1}/{len(parcels)}] {p.get('SITUS_ADDR','?')}")
        flood  = check_flood_zone(lat, lon)
        road   = check_road_access(lat, lon)
        scored = score_deal(p, flood, road)

        if scored["grade"] != "FAIL":
            results.append(scored)
            log.info(f"    Grade {scored['grade']} — {scored['flags']}")
        time.sleep(0.3)
    return results

# ── LAYER C: AIRTABLE ─────────────────────────────────────────────────────────
def push_to_airtable(deals: list[dict]) -> int:
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        log.info("Airtable not configured — skipping")
        return 0
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}
    pushed = 0
    for deal in deals:
        record = {"fields": {
            "Parcel ID":      str(deal.get("PARCEL_ID","")),
            "Address":        deal.get("SITUS_ADDR",""),
            "County":         deal.get("county",""),
            "Acres":          deal.get("ACRES", 0),
            "Price Est":      deal.get("price_est", 0),
            "Zoning":         deal.get("effective_zoning",""),
            "Flood Zone":     deal.get("flood_zone",""),
            "Road Access":    bool(deal.get("road_access", False)),
            "Score":          deal.get("score", 0),
            "Grade":          deal.get("grade",""),
            "Flags":          deal.get("flags",""),
            "Owner":          deal.get("OWNER_NAME",""),
            "Pulled Date":    deal.get("pulled_date",""),
            "Status":         "New",
        }}
        try:
            r = requests.post(url, headers=headers, json=record, timeout=15)
            r.raise_for_status()
            pushed += 1
            time.sleep(0.2)
        except Exception as e:
            log.warning(f"Airtable push failed: {e}")
    return pushed

# ── LAYER D: ALERTS ───────────────────────────────────────────────────────────
def send_alert(deals: list[dict]):
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD or not ALERT_EMAIL:
        log.info("Gmail not configured — skipping alert")
        return
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    a = sum(1 for d in deals if d.get("grade")=="A")
    b = sum(1 for d in deals if d.get("grade")=="B")
    subject = f"GY6 Deal Alert — {a}A / {b}B deals · {datetime.now().strftime('%b %d')}"

    rows = "".join(f"""<tr>
        <td style="padding:8px;font-weight:bold;color:{'#4A5240' if d.get('grade')=='A' else '#8B4513'}">{d.get('grade')}</td>
        <td style="padding:8px">{d.get('SITUS_ADDR','')}</td>
        <td style="padding:8px">{d.get('county','')}</td>
        <td style="padding:8px">{d.get('ACRES','')}ac</td>
        <td style="padding:8px">${int(d.get('price_est',0)):,}</td>
        <td style="padding:8px;font-size:11px">{d.get('flags','')}</td>
    </tr>""" for d in deals)

    html = f"""<html><body style="font-family:Arial;color:#2C2C2A">
    <div style="background:#4A5240;padding:16px;border-radius:8px 8px 0 0">
        <h2 style="color:#FAF8F4;margin:0">GY6 Deal Alert — {datetime.now().strftime('%b %d, %Y')}</h2>
    </div>
    <table style="width:100%;border-collapse:collapse;border:1px solid #ddd;font-size:13px">
        <tr style="background:#F5F2EC"><th style="padding:8px;text-align:left">Grade</th>
        <th style="padding:8px;text-align:left">Address</th><th style="padding:8px;text-align:left">County</th>
        <th style="padding:8px;text-align:left">Acres</th><th style="padding:8px;text-align:left">Price</th>
        <th style="padding:8px;text-align:left">Flags</th></tr>
        {rows}
    </table>
    </body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = ALERT_EMAIL
    msg.attach(MIMEText(html, "html"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            s.sendmail(GMAIL_ADDRESS, ALERT_EMAIL, msg.as_string())
        log.info(f"Alert sent → {ALERT_EMAIL}")
    except Exception as e:
        log.warning(f"Alert email failed: {e}")

# ── CSV SAVE ──────────────────────────────────────────────────────────────────
def save_csv(deals, path):
    if not deals:
        return
    import csv as _csv
    with open(path, "w", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=list(deals[0].keys()))
        w.writeheader(); w.writerows(deals)
    log.info(f"CSV saved → {path}")

# ── MAIN ──────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info(f"GY6 Deal Pipeline — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    import os
    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    all_deals = []

    for county_key, county in COUNTIES.items():
        log.info(f"\n--- {county['name']} County ---")
        parcels = pull_parcels(county_key)
        if not parcels:
            log.warning(f"  No listings returned for {county['name']}")
            continue
        deals = run_filter(parcels)
        log.info(f"  {len(deals)} deals passed filters")
        all_deals.extend(deals)
        time.sleep(1)

    all_deals.sort(key=lambda x: (x["grade"], -x["score"]))
    date_str = datetime.today().strftime("%Y%m%d")
    save_csv(all_deals, f"output/deals_{date_str}.csv")

    pushed = push_to_airtable(all_deals)

    grades = {g: sum(1 for d in all_deals if d.get("grade")==g) for g in "ABC"}
    log.info("\n" + "=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info(f"  Total: {len(all_deals)}  A:{grades['A']}  B:{grades['B']}  C:{grades['C']}")
    log.info(f"  Airtable: {pushed} pushed")
    log.info("=" * 60)

    ab = [d for d in all_deals if d.get("grade") in ("A","B")]
    if ab:
        send_alert(ab)

if __name__ == "__main__":
    run()
