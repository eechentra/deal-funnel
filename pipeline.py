"""
GY6 Deal Pipeline v3
Data: Redfin Base US via RapidAPI (works from GitHub Actions, no bot blocking)
Markets: Anne Arundel, Prince George's, Howard, Charles — Maryland
"""

import requests, csv, io, time, logging, os, math, json
from datetime import datetime
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
RAPIDAPI_KEY       = os.getenv("RAPIDAPI_KEY", "")

# ── BUY BOX ────────────────────────────────────────────────────────────────────
BUY_BOX = {
    "min_acres": 0.5,
    "max_acres": 2.5,
    "max_price": 100_000,
    "allowed_zoning": ["R-10","R-15","R-22","R10","R15","R22","R-MF","MF","RM","R-2"],
    "exclude_flood_zones": ["AE","AO","AH","VE","V","A"],
}

# ── COUNTY SEARCH TERMS ────────────────────────────────────────────────────────
COUNTIES = {
    "anne_arundel":   {"name": "Anne Arundel",   "region_id": "1311", "search": "Anne Arundel County, MD"},
    "prince_georges": {"name": "Prince George's", "region_id": "1325", "search": "Prince George's County, MD"},
    "howard":         {"name": "Howard",           "region_id": "1322", "search": "Howard County, MD"},
    "charles":        {"name": "Charles",          "region_id": "1317", "search": "Charles County, MD"},
}

# ── TENANT ANCHORS ─────────────────────────────────────────────────────────────
TENANT_ANCHORS = [
    (39.1051,-76.7784,"Fort Meade / NSA",        10, 25),
    (38.8108,-76.8680,"Joint Base Andrews",         8, 20),
    (39.0899,-76.8527,"Capitol Technology Univ",    5, 15),
    (39.1437,-76.7290,"BWI corridor",               6, 15),
    (38.9784,-76.9442,"PG Metro Green Line",        4, 20),
    (39.0458,-76.9413,"Greenbelt Metro",             4, 18),
    (39.1115,-76.9319,"College Park Metro",          4, 18),
    (39.1774,-76.6684,"MARC Penn — Odenton",        3, 15),
    (39.1579,-76.7301,"MARC Penn — Jessup",         3, 12),
    (39.2148,-76.8624,"Columbia Hub",                6, 15),
]

RAPIDAPI_HEADERS = {
    "x-rapidapi-key":  RAPIDAPI_KEY,
    "x-rapidapi-host": "redfin-base-us.p.rapidapi.com",
    "Content-Type":    "application/json",
}

# ── LAYER A: DATA PULL ─────────────────────────────────────────────────────────
# ── LAYER A: DATA PULL ─────────────────────────────────────────────────────────
def pull_parcels(county_key: str) -> list[dict]:
    import urllib.parse
    county = COUNTIES[county_key]
    log.info(f"Pulling listings: {county['name']}...")

    if not RAPIDAPI_KEY:
        log.warning("  RAPIDAPI_KEY not set")
        return []

    base = "https://redfin-base-us.p.rapidapi.com"

    # Endpoint 1: /search/location/for-sale
    try:
        loc = urllib.parse.quote(county["search"])
        url = f"{base}/search/location/for-sale?location={loc}&propertyType=land&maxPrice={int(BUY_BOX['max_price'] * 1.5)}"
        r = requests.get(url, headers=RAPIDAPI_HEADERS, timeout=30)
        log.info(f"  /search/location/for-sale: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            parcels = parse_rapidapi_response(data, county["name"])
            if parcels:
                log.info(f"  {len(parcels)} listings")
                return parcels
            log.info(f"  Empty — keys: {list(data.keys()) if isinstance(data,dict) else type(data)}")
    except Exception as e:
        log.warning(f"  endpoint 1 failed: {e}")

    time.sleep(1)

    # Endpoint 2: /search-by-url
    try:
        county_slug = county["name"].replace(" ", "-").replace("'", "-")
        redfin_url = "https://www.redfin.com/county/{}/MD/{}-County/filter/property-type=land,max-price={}".format(
            county["region_id"], county_slug, int(BUY_BOX["max_price"] * 1.5)
        )
        encoded = urllib.parse.quote(redfin_url, safe="")
        url = f"{base}/search-by-url?url={encoded}"
        r = requests.get(url, headers=RAPIDAPI_HEADERS, timeout=30)
        log.info(f"  /search-by-url: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            parcels = parse_rapidapi_response(data, county["name"])
            if parcels:
                log.info(f"  {len(parcels)} listings")
                return parcels
            log.info(f"  Empty — keys: {list(data.keys()) if isinstance(data,dict) else type(data)}")
    except Exception as e:
        log.warning(f"  endpoint 2 failed: {e}")

    time.sleep(1)

    # Endpoint 3: /search/location
    try:
        loc = urllib.parse.quote(county["search"])
        url = f"{base}/search/location?channel=sale&location={loc}"
        r = requests.get(url, headers=RAPIDAPI_HEADERS, timeout=30)
        log.info(f"  /search/location: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            parcels = parse_rapidapi_response(data, county["name"])
            log.info(f"  {len(parcels)} listings")
            return parcels
    except Exception as e:
        log.warning(f"  endpoint 3 failed: {e}")

    return []


def parse_rapidapi_response(data: dict, county_name: str) -> list[dict]:
    """Parse RapidAPI response into normalized parcel dicts."""
    parcels = []

    # Handle various response structures
    homes = []
    if isinstance(data, list):
        homes = data
    elif isinstance(data, dict):
        homes = (data.get("homes") or data.get("results") or
                 data.get("listings") or data.get("data") or
                 data.get("properties") or [])
        if isinstance(homes, dict):
            homes = homes.get("homes") or homes.get("results") or []

    for item in homes:
        if not isinstance(item, dict):
            continue

        # Extract price
        price = (item.get("price") or item.get("listPrice") or
                 item.get("list_price") or item.get("priceInfo", {}).get("amount") or 0)
        try:
            price = float(str(price).replace("$","").replace(",","")) if price else 0
        except (ValueError, TypeError):
            price = 0

        # Extract acreage
        sqft = (item.get("sqft") or item.get("lotSize") or item.get("lot_size") or
                item.get("lotSqft") or item.get("lotSizeValue") or 0)
        try:
            sqft = float(str(sqft).replace(",","")) if sqft else 0
        except (ValueError, TypeError):
            sqft = 0
        acres = round(sqft / 43560, 3) if sqft > 100 else round(sqft, 3) if sqft else 0

        # Extract coordinates
        lat = (item.get("lat") or item.get("latitude") or
               item.get("latLong", {}).get("latitude") or
               item.get("location", {}).get("lat") or None)
        lon = (item.get("lng") or item.get("longitude") or
               item.get("latLong", {}).get("longitude") or
               item.get("location", {}).get("lng") or None)
        try:
            lat = float(lat) if lat else None
            lon = float(lon) if lon else None
        except (ValueError, TypeError):
            lat = lon = None

        # Extract address
        addr = (item.get("address") or item.get("streetLine") or
                item.get("fullAddress") or "")
        if isinstance(addr, dict):
            addr = addr.get("line1") or addr.get("street") or str(addr)

        if price == 0 and acres == 0:
            continue

        parcels.append({
            "PARCEL_ID":      str(item.get("mlsId") or item.get("id") or item.get("propertyId") or f"RP-{len(parcels)}"),
            "SITUS_ADDR":     str(addr),
            "ACRES":          acres,
            "LAND_VALUE":     price,
            "ASSESSED_VALUE": price,
            "OWNER_NAME":     "",
            "ZONING":         str(item.get("zoning") or ""),
            "LATITUDE":       lat,
            "LONGITUDE":      lon,
            "county":         county_name,
            "data_source":    "Redfin/RapidAPI",
            "days_on_market": str(item.get("dom") or item.get("daysOnMarket") or ""),
            "listing_url":    str(item.get("url") or item.get("listingUrl") or ""),
        })

    return parcels


# ── LAYER B: FILTER ────────────────────────────────────────────────────────────
def _haversine_miles(lat1, lon1, lat2, lon2) -> float:
    R = 3958.8
    lat1,lon1,lat2,lon2 = map(math.radians,[lat1,lon1,lat2,lon2])
    a = math.sin((lat2-lat1)/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin((lon2-lon1)/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def score_proximity(lat, lon) -> tuple[int, list[str]]:
    hits, bonuses = [], []
    for alat, alon, name, radius, bonus in TENANT_ANCHORS:
        dist = _haversine_miles(lat, lon, alat, alon)
        if dist <= radius:
            hits.append(f"{name} ({dist:.1f}mi)")
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
        r = requests.post("https://overpass-api.de/api/interpreter",
                          data={"data": query}, timeout=20)
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
    elif acres > 0:
        flags.append(f"outside size range {acres:.2f}ac")

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

    zoning = str(parcel.get("ZONING",""))
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
        price = p.get("LAND_VALUE", 0)
        acres = p.get("ACRES", 0)
        if price > BUY_BOX["max_price"] * 1.5:
            continue
        if acres > 0 and (acres < BUY_BOX["min_acres"] or acres > BUY_BOX["max_acres"]):
            continue

        lat = p.get("LATITUDE")
        lon = p.get("LONGITUDE")
        log.info(f"  [{i+1}/{len(parcels)}] {p.get('SITUS_ADDR','?')}")

        if lat and lon:
            flood = check_flood_zone(lat, lon)
            road  = check_road_access(lat, lon)
        else:
            flood, road = "UNKNOWN", True

        scored = score_deal(p, flood, road)
        if scored["grade"] != "FAIL":
            results.append(scored)
            log.info(f"    Grade {scored['grade']} — {scored['flags']}")
        time.sleep(0.3)
    return results

# ── LAYER C: AIRTABLE ──────────────────────────────────────────────────────────
def push_to_airtable(deals: list[dict]) -> int:
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        log.info("Airtable not configured")
        return 0
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}
    pushed = 0
    for deal in deals:
        record = {"fields": {
            "Parcel ID":   str(deal.get("PARCEL_ID","")),
            "Address":     deal.get("SITUS_ADDR",""),
            "County":      deal.get("county",""),
            "Acres":       deal.get("ACRES", 0),
            "Price Est":   deal.get("price_est", 0),
            "Zoning":      deal.get("effective_zoning",""),
            "Flood Zone":  deal.get("flood_zone",""),
            "Road Access": bool(deal.get("road_access", False)),
            "Score":       deal.get("score", 0),
            "Grade":       deal.get("grade",""),
            "Flags":       deal.get("flags",""),
            "Owner":       deal.get("OWNER_NAME",""),
            "Pulled Date": deal.get("pulled_date",""),
            "Status":      "New",
        }}
        try:
            r = requests.post(url, headers=headers, json=record, timeout=15)
            r.raise_for_status()
            pushed += 1
            time.sleep(0.2)
        except Exception as e:
            log.warning(f"Airtable push failed: {e}")
    return pushed

# ── LAYER D: ALERT EMAIL ───────────────────────────────────────────────────────
def send_alert(deals: list[dict]):
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD or not ALERT_EMAIL:
        log.info("Gmail not configured — skipping alert")
        return
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    a = sum(1 for d in deals if d.get("grade")=="A")
    b = sum(1 for d in deals if d.get("grade")=="B")
    subject = f"GY6 Deal Alert — {a}A / {b}B · {datetime.now().strftime('%b %d')}"

    rows = "".join(f"""<tr>
        <td style="padding:8px;font-weight:bold;color:{'#4A5240' if d.get('grade')=='A' else '#8B4513'}">{d.get('grade')}</td>
        <td style="padding:8px">{d.get('SITUS_ADDR','')}</td>
        <td style="padding:8px">{d.get('county','')}</td>
        <td style="padding:8px">{d.get('ACRES','')}ac</td>
        <td style="padding:8px">${int(d.get('price_est',0)):,}</td>
        <td style="padding:8px;font-size:11px">{d.get('flags','')[:80]}</td>
    </tr>""" for d in deals)

    html = f"""<html><body style="font-family:Arial;color:#2C2C2A;max-width:900px">
    <div style="background:#4A5240;padding:16px;border-radius:8px 8px 0 0">
        <h2 style="color:#FAF8F4;margin:0">GY6 Deal Alert — {datetime.now().strftime('%b %d, %Y')}</h2>
    </div>
    <table style="width:100%;border-collapse:collapse;border:1px solid #ddd;font-size:13px">
        <tr style="background:#F5F2EC">
            <th style="padding:8px;text-align:left">Grade</th>
            <th style="padding:8px;text-align:left">Address</th>
            <th style="padding:8px;text-align:left">County</th>
            <th style="padding:8px;text-align:left">Acres</th>
            <th style="padding:8px;text-align:left">Price</th>
            <th style="padding:8px;text-align:left">Flags</th>
        </tr>{rows}
    </table></body></html>"""

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
        log.warning(f"Alert failed: {e}")

# ── CSV ────────────────────────────────────────────────────────────────────────
def save_csv(deals, path):
    if not deals: return
    import csv as _csv
    with open(path, "w", newline="") as f:
        w = _csv.DictWriter(f, fieldnames=list(deals[0].keys()))
        w.writeheader(); w.writerows(deals)
    log.info(f"CSV → {path}")

# ── MAIN ───────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info(f"GY6 Deal Pipeline v3 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)
    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    all_deals = []
    for county_key in COUNTIES:
        log.info(f"\n--- {COUNTIES[county_key]['name']} County ---")
        parcels = pull_parcels(county_key)
        if not parcels:
            log.warning(f"  No listings returned")
            continue
        deals = run_filter(parcels)
        log.info(f"  {len(deals)} deals passed filters")
        all_deals.extend(deals)
        time.sleep(2)

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
