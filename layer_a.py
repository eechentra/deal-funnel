"""
GY6 Deal Pipeline — Layer A v3
Uses Redfin public CSV download — no auth, works from any IP including GitHub Actions.
Returns active FOR SALE land listings with price, acreage, coordinates.
Fallback: Realtor.com public JSON API.
"""

import requests
import csv
import io
import time
import logging
from urllib.parse import urlencode

log = logging.getLogger(__name__)

REDFIN_COUNTIES = {
    "anne_arundel":   {"name": "Anne Arundel",   "region_id": "1861", "region_type": "5"},
    "prince_georges": {"name": "Prince George's", "region_id": "2470", "region_type": "5"},
    "howard":         {"name": "Howard",           "region_id": "2155", "region_type": "5"},
    "charles":        {"name": "Charles",          "region_id": "1897", "region_type": "5"},
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.redfin.com/",
}


def pull_redfin_land(county_key: str, buy_box: dict) -> list[dict]:
    county = REDFIN_COUNTIES[county_key]
    log.info(f"  Pulling Redfin listings: {county['name']}...")

    params = {
        "al":          1,
        "market":      "dc",
        "max_price":   int(buy_box["max_price"] * 1.5),
        "num_homes":   350,
        "ord":         "redfin-recommended-asc",
        "page_number": 1,
        "property_type": 6,
        "region_id":   county["region_id"],
        "region_type": county["region_type"],
        "status":      9,
        "uipt":        6,
        "v":           8,
    }

    url = "https://www.redfin.com/stingray/api/gis-csv?" + urlencode(params)

    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            log.warning(f"  Redfin {r.status_code} for {county['name']}")
            return []

        content = r.text
        if not content or len(content) < 100:
            log.warning(f"  Redfin empty response for {county['name']}")
            return []

        # Find CSV start — Redfin prepends a disclaimer line
        lines = content.split("\n")
        csv_start = 0
        for i, line in enumerate(lines):
            if "ADDRESS" in line.upper() or "PRICE" in line.upper():
                csv_start = i
                break

        csv_content = "\n".join(lines[csv_start:])
        reader = csv.DictReader(io.StringIO(csv_content))
        parcels = []

        for row in reader:
            price_str = row.get("PRICE", "0").replace("$", "").replace(",", "").strip()
            sqft_str  = row.get("SQUARE FEET", "0").replace(",", "").strip()
            lot_str   = row.get("LOT SIZE", "0").replace(",", "").strip()

            try:
                price = float(price_str) if price_str and price_str not in ("—", "") else 0
            except ValueError:
                price = 0

            # Acreage: try lot size first, then square feet
            acres = 0
            for val in [lot_str, sqft_str]:
                if val and val not in ("—", ""):
                    try:
                        n = float(val)
                        acres = n / 43560 if n > 100 else n  # >100 = sqft, else acres
                        if 0.1 < acres < 100:
                            break
                    except ValueError:
                        continue

            try:
                lat = float(row.get("LATITUDE", "") or 0) or None
                lon = float(row.get("LONGITUDE", "") or 0) or None
            except ValueError:
                lat = lon = None

            if price == 0 or acres < buy_box["min_acres"]:
                continue

            parcels.append({
                "PARCEL_ID":      row.get("MLS#", row.get("LISTING ID", f"RF-{len(parcels)}")),
                "SITUS_ADDR":     row.get("ADDRESS", ""),
                "ACRES":          round(acres, 3),
                "LAND_VALUE":     price,
                "ASSESSED_VALUE": price,
                "OWNER_NAME":     "",
                "ZONING":         "",
                "LATITUDE":       lat,
                "LONGITUDE":      lon,
                "county":         county["name"],
                "data_source":    "Redfin",
                "listing_url":    row.get("URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)", ""),
                "days_on_market": row.get("DAYS ON MARKET", ""),
            })

        log.info(f"  Redfin: {len(parcels)} land listings for {county['name']}")
        return parcels

    except Exception as e:
        log.warning(f"  Redfin failed for {county['name']}: {e}")
        return []


def pull_realtor_land(county_key: str, buy_box: dict) -> list[dict]:
    county_name = REDFIN_COUNTIES[county_key]["name"]
    log.info(f"  Trying Realtor.com: {county_name}...")

    slug = {
        "anne_arundel":   "anne-arundel-county_md",
        "prince_georges": "prince-georges-county_md",
        "howard":         "howard-county_md",
        "charles":        "charles-county_md",
    }.get(county_key, "")

    url = "https://www.realtor.com/api/v1/hulk_main_srp/list"
    params = {
        "client_id":  "rdc-x",
        "schema":     "vesta",
        "prop_type":  "land",
        "list_price": f"0-{int(buy_box['max_price'] * 1.5)}",
        "state_code": "MD",
        "county":     f"{county_name} County",
        "offset":     0,
        "limit":      200,
    }

    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        data = r.json()
        listings = data.get("data", {}).get("results", [])
        parcels = []
        for item in listings:
            loc   = item.get("location", {}).get("address", {})
            desc  = item.get("description", {})
            coord = item.get("location", {}).get("coordinate", {})
            price = item.get("list_price", 0) or 0
            sqft  = desc.get("lot_sqft", 0) or 0
            acres = round(sqft / 43560, 3) if sqft else 0

            if acres < buy_box["min_acres"]:
                continue

            parcels.append({
                "PARCEL_ID":      item.get("property_id", ""),
                "SITUS_ADDR":     f"{loc.get('line','')} {loc.get('city','')} MD".strip(),
                "ACRES":          acres,
                "LAND_VALUE":     price,
                "ASSESSED_VALUE": price,
                "OWNER_NAME":     "",
                "ZONING":         "",
                "LATITUDE":       coord.get("lat"),
                "LONGITUDE":      coord.get("lon"),
                "county":         county_name,
                "data_source":    "Realtor.com",
                "listing_url":    f"https://www.realtor.com{item.get('permalink','')}",
                "days_on_market": item.get("list_date", ""),
            })

        log.info(f"  Realtor.com: {len(parcels)} listings for {county_name}")
        return parcels
    except Exception as e:
        log.warning(f"  Realtor.com failed for {county_name}: {e}")
        return []


def pull_parcels(county_key: str, buy_box: dict, max_records: int = 350) -> list[dict]:
    """Primary: Redfin CSV. Fallback: Realtor.com."""
    parcels = pull_redfin_land(county_key, buy_box)
    if parcels:
        return parcels
    time.sleep(2)
    return pull_realtor_land(county_key, buy_box)
