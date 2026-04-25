
"""
GY6 / Land Deal Sourcing Pipeline
Markets: Anne Arundel, Prince George's, Howard, Charles — Maryland
Buy box: ~1 acre, <$100K, R10/R15/R22/MF zoning, no AE/VE flood, road+util access
All data sources: FREE (Maryland MDP, FEMA NFHL, county GIS, OpenStreetMap)
"""

import requests
import json
import csv
import time
import logging
import os
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── CONFIG ────────────────────────────────────────────────────────────────────

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE   = os.getenv("AIRTABLE_TABLE", "Deals")

BUY_BOX = {
    "min_acres": 0.5,
    "max_acres": 2.5,
    "max_price": 100_000,
    "allowed_zoning": ["R-10", "R-15", "R-22", "R10", "R15", "R22", "R-2", "R-MF", "MF", "RM"],
    "exclude_flood_zones": ["AE", "AO", "AH", "VE", "V", "A"],
}

# Maryland counties — FIPS codes + ArcGIS REST endpoints (all free, no key)
COUNTIES = {
    "anne_arundel": {
        "fips": "24003",
        "name": "Anne Arundel",
        "parcels_url": "https://geodata.md.gov/imap/rest/services/PlanningCadastre/MD_ParcelBoundaries/MapServer/0/query",
        "zoning_url":  "https://gis.aacounty.org/arcgis/rest/services/Planning/Zoning/MapServer/0/query",
    },
    "prince_georges": {
        "fips": "24033",
        "name": "Prince George's",
        "parcels_url": "https://geodata.md.gov/imap/rest/services/PlanningCadastre/MD_ParcelBoundaries/MapServer/0/query",
        "zoning_url":  "https://gisapps.pgplanning.org/arcgis/rest/services/OpenData/Zoning/MapServer/0/query",
    },
    "howard": {
        "fips": "24027",
        "name": "Howard",
        "parcels_url": "https://geodata.md.gov/imap/rest/services/PlanningCadastre/MD_ParcelBoundaries/MapServer/0/query",
        "zoning_url":  "https://gis.howardcountymd.gov/arcgis/rest/services/Planning/Zoning/MapServer/0/query",
    },
    "charles": {
        "fips": "24017",
        "name": "Charles",
        "parcels_url": "https://geodata.md.gov/imap/rest/services/PlanningCadastre/MD_ParcelBoundaries/MapServer/0/query",
        "zoning_url":  "https://gis.charlescountymd.gov/arcgis/rest/services/Planning/Zoning/MapServer/0/query",
    },
}

# ─── LAYER A: DATA PULL ────────────────────────────────────────────────────────

def pull_parcels(county_key: str, max_records: int = 2000) -> list[dict]:
    """Pull raw parcel records from Maryland MDP statewide GIS (free, no key)."""
    county = COUNTIES[county_key]
    log.info(f"Pulling parcels: {county['name']} (FIPS {county['fips']})")

    params = {
        "where": f"COUNTY_FIPS='{county['fips']}' AND ACRES >= {BUY_BOX['min_acres']} AND ACRES <= {BUY_BOX['max_acres']}",
        "outFields": "OBJECTID,PARCEL_ID,OWNER_NAME,SITUS_ADDR,ACRES,ASSESSED_VALUE,LAND_VALUE,LATITUDE,LONGITUDE,ZONING",
        "returnGeometry": "true",
        "geometryType": "esriGeometryEnvelope",
        "f": "json",
        "resultRecordCount": max_records,
    }

    try:
        r = requests.get(county["parcels_url"], params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        features = data.get("features", [])
        parcels = []
        for f in features:
            attrs = f.get("attributes", {})
            geo   = f.get("geometry", {})
            # Attempt centroid from geometry rings if lat/lon not in attributes
            if not attrs.get("LATITUDE") and geo.get("rings"):
                pts = geo["rings"][0]
                attrs["LONGITUDE"] = sum(p[0] for p in pts) / len(pts)
                attrs["LATITUDE"]  = sum(p[1] for p in pts) / len(pts)
            attrs["county"] = county["name"]
            parcels.append(attrs)
        log.info(f"  {len(parcels)} parcels in acreage range")
        return parcels
    except Exception as e:
        log.warning(f"  Parcel pull failed for {county['name']}: {e}")
        return []


def pull_zoning(lat: float, lon: float, county_key: str) -> Optional[str]:
    """Query county ArcGIS zoning layer by lat/lon point (free)."""
    county = COUNTIES[county_key]
    params = {
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "ZONING,ZONE_DESC,ZONE_CLASS",
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        r = requests.get(county["zoning_url"], params=params, timeout=15)
        data = r.json()
        features = data.get("features", [])
        if features:
            attrs = features[0].get("attributes", {})
            return attrs.get("ZONING") or attrs.get("ZONE_CLASS") or attrs.get("ZONE_DESC")
    except Exception as e:
        log.debug(f"Zoning lookup failed ({lat},{lon}): {e}")
    return None


# ─── LAYER B: FILTER ENGINE ────────────────────────────────────────────────────

def check_flood_zone(lat: float, lon: float) -> str:
    """
    Check FEMA NFHL flood zone via free API (no key required).
    Returns zone designation string e.g. 'X', 'AE', 'VE'
    """
    url = "https://msc.fema.gov/arcgis/rest/services/NFHL_National/MapServer/28/query"
    params = {
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "FLD_ZONE,ZONE_SUBTY",
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()
        features = data.get("features", [])
        if features:
            return features[0]["attributes"].get("FLD_ZONE", "X")
        return "X"  # no data = outside mapped flood area = safe
    except Exception as e:
        log.debug(f"FEMA lookup failed ({lat},{lon}): {e}")
        return "UNKNOWN"


def check_road_access(lat: float, lon: float, radius_m: int = 100) -> bool:
    """
    Check road proximity via OpenStreetMap Overpass API (completely free).
    Returns True if a road exists within radius_m meters of the parcel.
    """
    query = f"""
    [out:json][timeout:10];
    way(around:{radius_m},{lat},{lon})["highway"];
    out count;
    """
    try:
        r = requests.post(
            "https://overpass-api.de/api/interpreter",
            data={"data": query},
            timeout=20
        )
        data = r.json()
        count = data.get("elements", [{}])[0].get("tags", {}).get("total", 0)
        return int(count) > 0
    except Exception as e:
        log.debug(f"Road check failed ({lat},{lon}): {e}")
        return True  # default pass if API unreachable


def score_deal(parcel: dict, flood_zone: str, has_road: bool, zoning: str) -> dict:
    """
    Score a deal A/B/C based on buy box fit.
    Returns enriched parcel dict with score and reason.
    """
    score = 100
    flags = []

    price = parcel.get("LAND_VALUE") or parcel.get("ASSESSED_VALUE") or 0
    acres = parcel.get("ACRES", 0)

    # Price scoring
    if price == 0:
        flags.append("no price data")
    elif price <= 50_000:
        score += 20
        flags.append(f"strong price ${price:,}")
    elif price <= 75_000:
        score += 10
        flags.append(f"good price ${price:,}")
    elif price <= 100_000:
        flags.append(f"at ceiling ${price:,}")
    else:
        score -= 40
        flags.append(f"over budget ${price:,}")

    # Acreage scoring
    if 0.75 <= acres <= 1.5:
        score += 20
        flags.append(f"ideal size {acres:.2f}ac")
    elif 0.5 <= acres <= 2.5:
        score += 5
        flags.append(f"acceptable size {acres:.2f}ac")

    # Flood zone
    if flood_zone in BUY_BOX["exclude_flood_zones"]:
        score -= 100
        flags.append(f"FLOOD ZONE {flood_zone} — EXCLUDE")
    elif flood_zone == "X":
        score += 15
        flags.append("flood zone X (safe)")
    elif flood_zone == "UNKNOWN":
        flags.append("flood zone unknown — verify")

    # Road access
    if not has_road:
        score -= 30
        flags.append("no road access detected")
    else:
        flags.append("road access confirmed")

    # Zoning — must be density-permissive (R10/R15/R22/MF), NOT single-family R zones
    effective_zoning = zoning or parcel.get("ZONING", "")
    zoning_match = any(z.upper() in (effective_zoning or "").upper() for z in BUY_BOX["allowed_zoning"])
    # Explicit single-family exclusion
    sf_zones = ["R-1", "R-2", "R1", "R2", "RS", "RR", "R-A", "RA"]
    sf_flag  = any(z.upper() == (effective_zoning or "").upper().strip() for z in sf_zones)
    if sf_flag:
        score -= 50
        flags.append(f"SINGLE FAMILY ZONE — EXCLUDE: {effective_zoning}")
    elif zoning_match:
        score += 15
        flags.append(f"zoning match: {effective_zoning}")
    elif effective_zoning:
        score -= 20
        flags.append(f"zoning mismatch: {effective_zoning}")
    else:
        flags.append("zoning unknown — verify")

    # Proximity scoring — tenant pool anchors (Ft Meade, Andrews, Metro, MARC)
    lat = parcel.get("LATITUDE")
    lon = parcel.get("LONGITUDE")
    if lat and lon:
        try:
            from layer_d import score_proximity
            prox_bonus, prox_hits = score_proximity(lat, lon)
            score += prox_bonus
            if prox_bonus > 0:
                flags.append(f"tenant anchors: {', '.join(prox_hits[:2])}")
            else:
                flags.append(prox_hits[0] if prox_hits else "no anchor proximity")
        except Exception:
            pass

    # Grade
    if score >= 140:
        grade = "A"
    elif score >= 100:
        grade = "B"
    elif score >= 60:
        grade = "C"
    else:
        grade = "FAIL"

    return {
        **parcel,
        "score": score,
        "grade": grade,
        "flood_zone": flood_zone,
        "road_access": has_road,
        "effective_zoning": effective_zoning,
        "flags": " | ".join(flags),
        "price_est": price,
        "pulled_date": datetime.today().strftime("%Y-%m-%d"),
    }


def run_filter(parcels: list[dict], county_key: str) -> list[dict]:
    """Run all filters on a list of raw parcels. Returns only passing deals."""
    results = []
    for i, p in enumerate(parcels):
        lat = p.get("LATITUDE")
        lon = p.get("LONGITUDE")

        if not lat or not lon:
            continue

        # Price pre-filter (skip obvious misses before API calls)
        price = p.get("LAND_VALUE") or p.get("ASSESSED_VALUE") or 0
        if price > BUY_BOX["max_price"] * 1.5:
            continue

        log.info(f"  [{i+1}/{len(parcels)}] Checking parcel {p.get('PARCEL_ID','?')}...")

        flood   = check_flood_zone(lat, lon)
        road    = check_road_access(lat, lon)
        zoning  = pull_zoning(lat, lon, county_key)
        scored  = score_deal(p, flood, road, zoning)

        if scored["grade"] != "FAIL":
            results.append(scored)
            log.info(f"    Grade {scored['grade']} — {scored['flags']}")

        time.sleep(0.3)  # rate limit courtesy

    return results


# ─── LAYER C: AIRTABLE PUSH ────────────────────────────────────────────────────

def push_to_airtable(deals: list[dict]) -> int:
    """Push scored deals to Airtable. Skips if no API key configured."""
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        log.info("Airtable not configured — skipping push (set env vars to enable)")
        return 0

    url     = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"}
    pushed  = 0

    for deal in deals:
        record = {
            "fields": {
                "Parcel ID":      str(deal.get("PARCEL_ID", "")),
                "Address":        deal.get("SITUS_ADDR", ""),
                "County":         deal.get("county", ""),
                "Acres":          deal.get("ACRES", 0),
                "Price Est":      deal.get("price_est", 0),
                "Zoning":         deal.get("effective_zoning", ""),
                "Flood Zone":     deal.get("flood_zone", ""),
                "Road Access":    deal.get("road_access", False),
                "Score":          deal.get("score", 0),
                "Grade":          deal.get("grade", ""),
                "Flags":          deal.get("flags", ""),
                "Owner":          deal.get("OWNER_NAME", ""),
                "Pulled Date":    deal.get("pulled_date", ""),
                "Status":         "New",
            }
        }
        try:
            r = requests.post(url, headers=headers, json=record, timeout=15)
            r.raise_for_status()
            pushed += 1
            time.sleep(0.2)
        except Exception as e:
            log.warning(f"Airtable push failed for {deal.get('PARCEL_ID')}: {e}")

    return pushed


# ─── OUTPUT: CSV BACKUP ────────────────────────────────────────────────────────

def save_csv(deals: list[dict], path: str):
    if not deals:
        return
    keys = list(deals[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(deals)
    log.info(f"Saved {len(deals)} deals → {path}")


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info(f"Deal pipeline starting — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 60)

    # Layer A v2 — ArcGIS public portal + SODA fallback
    try:
        from layer_a import pull_parcels as pull_v2
        use_v2 = True
        log.info("Layer A v2 active (ArcGIS + SODA fallback)")
    except ImportError:
        use_v2 = False

    all_deals = []


    for county_key in COUNTIES:
        log.info(f"\n--- {COUNTIES[county_key]['name']} County ---")
        parcels = pull_v2(county_key, BUY_BOX) if use_v2 else pull_parcels(county_key)
        if not parcels:
            log.warning(f"  No parcels returned — check GIS endpoint")
            continue
        deals = run_filter(parcels, county_key)
        log.info(f"  {len(deals)} deals passed filters")
        all_deals.extend(deals)

    # Sort by grade then score
    all_deals.sort(key=lambda x: (x["grade"], -x["score"]))

    # Save CSV backup
    date_str = datetime.today().strftime("%Y%m%d")
    save_csv(all_deals, f"output/deals_{date_str}.csv")

    # Push to Airtable
    pushed = push_to_airtable(all_deals)

    # Summary
    grades = {"A": 0, "B": 0, "C": 0}
    for d in all_deals:
        if d["grade"] in grades:
            grades[d["grade"]] += 1

    log.info("\n" + "=" * 60)
    log.info(f"PIPELINE COMPLETE")
    log.info(f"  Total deals passing filters: {len(all_deals)}")
    log.info(f"  A-grade: {grades['A']}  B-grade: {grades['B']}  C-grade: {grades['C']}")
    log.info(f"  Pushed to Airtable: {pushed}")
    log.info(f"  CSV saved: output/deals_{date_str}.csv")
    log.info("=" * 60)

    # Layer D — alerts + seller outreach for A/B deals
    ab_deals = [d for d in all_deals if d.get("grade") in ("A", "B")]
    if ab_deals:
        log.info(f"\nRunning Layer D on {len(ab_deals)} A/B deals...")
        try:
            from layer_d import run_layer_d
            run_layer_d(ab_deals)
        except Exception as e:
            log.error(f"Layer D failed: {e}")


if __name__ == "__main__":
    run()
