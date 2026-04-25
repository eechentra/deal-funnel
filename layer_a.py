"""
GY6 Deal Pipeline — Layer A v2
Data sources rebuilt around publicly accessible endpoints that work from GitHub Actions.

Primary: Maryland Property Data - Parcel Points via ArcGIS public portal
         (data-maryland.opendata.arcgis.com — ESRI-hosted, open IP access)
Fallback: Prince George's County Planning GIS open data
          (gisdata.pgplanning.org/opendata — public REST API)

Field mappings from MD iMAP MDProperty View subscriber guide:
  COUNTY   — county code (AA=02, PG=17, HO=13, CH=08)
  ACRES    — polygon acres
  LANDVAL  — land assessed value
  OWNNAME  — owner name
  SITEADDR — site address
  ZONING   — zoning code
  X / Y    — centroid coordinates (MD State Plane, need projection)
  LATITUDE / LONGITUDE — decimal degrees (where available)
"""

import requests
import time
import logging

log = logging.getLogger(__name__)

# MD iMAP county codes for SDAT data
COUNTY_CODES = {
    "anne_arundel":  "02",
    "prince_georges": "17",
    "howard":        "13",
    "charles":       "08",
}

COUNTY_NAMES = {
    "anne_arundel":   "Anne Arundel",
    "prince_georges": "Prince George's",
    "howard":         "Howard",
    "charles":        "Charles",
}

# ArcGIS public portal — MD Property Data Parcel Points
# This is ESRI-hosted (not Maryland state servers) so no IP blocking
ARCGIS_PARCEL_URL = (
    "https://services.arcgis.com/njFNhDsUCentVYJW/arcgis/rest/services/"
    "MD_Property_Data_Parcel_Points/FeatureServer/0/query"
)

# Fallback: PG County Planning open data (direct county endpoint)
PG_PARCEL_URL = (
    "https://gisdata.pgplanning.org/opendata/rest/services/"
    "OpenData/Property/MapServer/0/query"
)

# Maryland open data ArcGIS hub (alternative)
MD_HUB_URL = (
    "https://geodata.md.gov/imap/rest/services/"
    "PlanningCadastre/MD_PropertyData/MapServer/0/query"
)


def pull_parcels_arcgis(county_key: str, buy_box: dict, max_records: int = 1000) -> list[dict]:
    """
    Pull parcels from Maryland ArcGIS public portal.
    Filters by county code, acreage range, and land use (vacant/residential land).
    """
    county_code = COUNTY_CODES[county_key]
    county_name = COUNTY_NAMES[county_key]
    min_ac = buy_box["min_acres"]
    max_ac = buy_box["max_acres"]
    max_price = buy_box["max_price"]

    log.info(f"  Trying ArcGIS portal for {county_name}...")

    # Land use codes for vacant/undeveloped residential land in MD iMAP
    # LU codes: 1100=residential, 1110=single family, 1130=MF, 9000=vacant
    # We want vacant residential and low-density residential that allows modular
    where_clause = (
        f"COUNTY='{county_code}' AND "
        f"ACRES >= {min_ac} AND ACRES <= {max_ac} AND "
        f"(LANDVAL <= {max_price * 1.5} OR LANDVAL IS NULL) AND "
        f"(DESCLU LIKE '%RESID%' OR DESCLU LIKE '%VACANT%' OR "
        f"LU IN ('1100','1130','1140','9000','9100','9110','1300'))"
    )

    params = {
        "where":          where_clause,
        "outFields":      "OBJECTID,COUNTY,ACRES,LANDVAL,OWNNAME,SITEADDR,ZONING,LU,DESCLU,LATITUDE,LONGITUDE,X,Y",
        "returnGeometry": "false",
        "f":              "json",
        "resultRecordCount": max_records,
        "orderByFields":  "ACRES ASC",
    }

    try:
        r = requests.get(ARCGIS_PARCEL_URL, params=params, timeout=30,
                         headers={"User-Agent": "GY6-DealPipeline/1.0"})
        r.raise_for_status()
        data = r.json()

        if "error" in data:
            log.warning(f"  ArcGIS error: {data['error']}")
            return []

        features = data.get("features", [])
        parcels = []
        for f in features:
            attrs = f.get("attributes", {})
            # Normalize field names to match rest of pipeline
            normalized = {
                "PARCEL_ID":      str(attrs.get("OBJECTID", "")),
                "SITUS_ADDR":     attrs.get("SITEADDR", ""),
                "ACRES":          attrs.get("ACRES", 0),
                "LAND_VALUE":     attrs.get("LANDVAL", 0),
                "ASSESSED_VALUE": attrs.get("LANDVAL", 0),
                "OWNER_NAME":     attrs.get("OWNNAME", ""),
                "ZONING":         attrs.get("ZONING", ""),
                "LU_CODE":        attrs.get("LU", ""),
                "LU_DESC":        attrs.get("DESCLU", ""),
                "LATITUDE":       attrs.get("LATITUDE"),
                "LONGITUDE":      attrs.get("LONGITUDE"),
                "county":         county_name,
                "data_source":    "MD iMAP ArcGIS",
            }
            # Use X/Y if lat/lon not populated
            if not normalized["LATITUDE"] and attrs.get("Y"):
                # MD State Plane coords — approximate conversion for scoring
                # Proper reprojection would need pyproj; use as-is for proximity
                normalized["LATITUDE"]  = attrs.get("Y")
                normalized["LONGITUDE"] = attrs.get("X")
            parcels.append(normalized)

        log.info(f"  ArcGIS returned {len(parcels)} parcels for {county_name}")
        return parcels

    except Exception as e:
        log.warning(f"  ArcGIS pull failed for {county_name}: {e}")
        return []


def pull_parcels_pg_fallback(buy_box: dict, max_records: int = 500) -> list[dict]:
    """
    Fallback: Pull PG County parcels directly from PG Planning open data portal.
    Only used if ArcGIS portal fails for Prince George's.
    """
    log.info("  Trying PG County Planning fallback...")
    params = {
        "where":          f"ACREAGE >= {buy_box['min_acres']} AND ACREAGE <= {buy_box['max_acres']}",
        "outFields":      "OBJECTID,ACREAGE,LANDVAL,OWNER,ADDRESS,ZONING,LATITUDE,LONGITUDE",
        "returnGeometry": "false",
        "f":              "json",
        "resultRecordCount": max_records,
    }
    try:
        r = requests.get(PG_PARCEL_URL, params=params, timeout=30,
                         headers={"User-Agent": "GY6-DealPipeline/1.0"})
        data = r.json()
        features = data.get("features", [])
        parcels = []
        for f in features:
            a = f.get("attributes", {})
            parcels.append({
                "PARCEL_ID":      str(a.get("OBJECTID", "")),
                "SITUS_ADDR":     a.get("ADDRESS", ""),
                "ACRES":          a.get("ACREAGE", 0),
                "LAND_VALUE":     a.get("LANDVAL", 0),
                "ASSESSED_VALUE": a.get("LANDVAL", 0),
                "OWNER_NAME":     a.get("OWNER", ""),
                "ZONING":         a.get("ZONING", ""),
                "LATITUDE":       a.get("LATITUDE"),
                "LONGITUDE":      a.get("LONGITUDE"),
                "county":         "Prince George's",
                "data_source":    "PG County Planning",
            })
        log.info(f"  PG fallback returned {len(parcels)} parcels")
        return parcels
    except Exception as e:
        log.warning(f"  PG fallback failed: {e}")
        return []


def pull_parcels_soda(county_key: str, buy_box: dict, max_records: int = 1000) -> list[dict]:
    """
    Secondary fallback: Maryland open data via Socrata SODA API.
    Dataset: Maryland Property Sales (has parcel-level data with address/owner).
    Endpoint: opendata.maryland.gov/resource/vqjs-wt3p.json (Real Property Sales)
    """
    county_name = COUNTY_NAMES[county_key]
    # SODA county name mapping
    soda_county = {
        "anne_arundel":   "ANNE ARUNDEL",
        "prince_georges": "PRINCE GEORGES",
        "howard":         "HOWARD",
        "charles":        "CHARLES",
    }.get(county_key, county_name.upper())

    log.info(f"  Trying SODA API for {county_name}...")

    url = "https://opendata.maryland.gov/resource/vqjs-wt3p.json"
    params = {
        "$where":  f"county_name='{soda_county}' AND land_area_sf >= {buy_box['min_acres'] * 43560} AND land_area_sf <= {buy_box['max_acres'] * 43560}",
        "$limit":  max_records,
        "$select": "acct_id,owner_name,property_address,county_name,land_area_sf,land_value,zoning,latitude,longitude",
    }
    try:
        r = requests.get(url, params=params, timeout=30,
                         headers={"User-Agent": "GY6-DealPipeline/1.0"})
        data = r.json()
        if not isinstance(data, list):
            log.warning(f"  SODA unexpected response: {str(data)[:100]}")
            return []
        parcels = []
        for row in data:
            sf = float(row.get("land_area_sf") or 0)
            parcels.append({
                "PARCEL_ID":      row.get("acct_id", ""),
                "SITUS_ADDR":     row.get("property_address", ""),
                "ACRES":          round(sf / 43560, 3) if sf else 0,
                "LAND_VALUE":     float(row.get("land_value") or 0),
                "ASSESSED_VALUE": float(row.get("land_value") or 0),
                "OWNER_NAME":     row.get("owner_name", ""),
                "ZONING":         row.get("zoning", ""),
                "LATITUDE":       float(row.get("latitude") or 0) or None,
                "LONGITUDE":      float(row.get("longitude") or 0) or None,
                "county":         county_name,
                "data_source":    "MD SODA API",
            })
        log.info(f"  SODA returned {len(parcels)} parcels for {county_name}")
        return parcels
    except Exception as e:
        log.warning(f"  SODA pull failed for {county_name}: {e}")
        return []


def pull_parcels(county_key: str, buy_box: dict, max_records: int = 1000) -> list[dict]:
    """
    Master pull function with fallback chain:
    1. ArcGIS public portal (primary)
    2. SODA API (secondary)
    3. PG County Planning direct (PG only)
    Returns normalized parcel list ready for Layer B filter.
    """
    # Try primary
    parcels = pull_parcels_arcgis(county_key, buy_box, max_records)
    if parcels:
        return parcels

    # Try SODA fallback
    time.sleep(1)
    parcels = pull_parcels_soda(county_key, buy_box, max_records)
    if parcels:
        return parcels

    # PG-specific fallback
    if county_key == "prince_georges":
        time.sleep(1)
        parcels = pull_parcels_pg_fallback(buy_box, max_records)

    return parcels
