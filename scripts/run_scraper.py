#!/usr/bin/env python3
"""Willhaben Mietwohnungen Scraper - Wien
Scrapes rental listings from Willhaben for Vienna,
extracts price and location (Bezirk), and stores them in MongoDB.
"""

import json
import os
import re
import time

import requests
from bs4 import BeautifulSoup
from pymongo import ASCENDING, MongoClient
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timezone


WILLHABEN_BASE_URL = "https://www.willhaben.at/iad/immobilien/mietwohnungen/wien/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
}
MAX_PAGES = 1000
REQUEST_DELAY = 5  # seconds between requests


def connect_mongo():
    """Connect to MongoDB and return the collection."""
    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", 27017))
    user = os.environ.get("MONGO_USER", "admin")
    password = os.environ.get("MONGO_PASSWORD", "secret")

    client = MongoClient(
        host=host,
        port=port,
        username=user,
        password=password,
        authSource="admin",
    )
    db = client["bigdata"]
    return db["willhaben_mietwohnungen"]


def extract_listings_from_next_data(soup):
    """Extract listing data from __NEXT_DATA__ JSON embedded in the page."""
    script_tag = soup.find("script", id="__NEXT_DATA__")
    if not script_tag:
        return []

    data = json.loads(script_tag.string)
    listings = []

    try:
        search_result = data["props"]["pageProps"]["searchResult"]
        ad_rows = search_result.get("advertSummaryList", {}).get("advertSummary", [])
    except (KeyError, TypeError):
        return []

    for ad in ad_rows:
        listing = parse_ad(ad)
        if listing:
            listings.append(listing)

    return listings


def parse_ad(ad):
    """Parse a single ad entry from the JSON data."""
    attributes = {}
    for attr in ad.get("attributes", {}).get("attribute", []):
        name = attr.get("name", "")
        values = attr.get("values", [])
        if values:
            attributes[name] = values[0]

    price_str = attributes.get("PRICE", "")
    price = parse_price(price_str)

    location = attributes.get("LOCATION", "")
    postcode = attributes.get("POSTCODE", "")
    district = extract_district(location, postcode)

    title = attributes.get("HEADING", ad.get("description", ""))
    area = parse_float(attributes.get("ESTATE_SIZE/LIVING_AREA", ""))
    rooms = parse_float(attributes.get("NUMBER_OF_ROOMS", ""))
    property_type = attributes.get("PROPERTY_TYPE", "")
    published = attributes.get("PUBLISHED_String", "")

    # Parse coordinates from "lat,lng" string
    latitude = None
    longitude = None
    coord_str = attributes.get("COORDINATES", "")
    if coord_str and "," in coord_str:
        parts = coord_str.split(",")
        try:
            latitude = float(parts[0])
            longitude = float(parts[1])
        except (ValueError, IndexError):
            pass

    willhaben_id = str(ad.get("id", "")).strip()

    # Without a stable ID we can't deduplicate — skip entirely.
    if not willhaben_id:
        return None

    if not price and not location:
        return None

    result = {
        "willhaben_id": willhaben_id,
        "title": title,
        "price": price,
        "price_raw": price_str,
        "location": location,
        "postcode": postcode,
        "district": district,
        "area_m2": area,
        "rooms": rooms,
        "property_type": property_type,
        "published": published,
        "latitude": latitude,
        "longitude": longitude,
        "scraped_at": datetime.now(timezone.utc),
    }

    # Add GeoJSON point for MongoDB geospatial queries
    if latitude is not None and longitude is not None:
        result["geo"] = {
            "type": "Point",
            "coordinates": [longitude, latitude],  # GeoJSON: [lng, lat]
        }

    return result


def parse_price(price_str):
    """Parse price string to float.
    Handles both API format ('1714.01') and display format ('€ 1.200,50').
    """
    if not price_str:
        return None
    cleaned = re.sub(r"[^\d,.]", "", price_str)
    # If the string contains both dots and commas, it's Austrian display format
    # (dots as thousand separators, comma as decimal): "1.200,50" -> 1200.50
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        # Comma-only: treat as decimal separator ("850,00" -> 850.00)
        cleaned = cleaned.replace(",", ".")
    # Dot-only or no separator: already valid float ("1714.01" or "2100")
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_float(val):
    """Parse a numeric string to float."""
    if not val:
        return None
    try:
        return float(val.replace(",", "."))
    except ValueError:
        return None


def extract_district(location, postcode):
    """Extract Vienna district number from postcode or location string.
    Vienna postcodes follow the pattern 1XXO where XX is the district number.
    """
    if postcode and postcode.startswith("1") and len(postcode) == 4:
        try:
            return int(postcode[1:3])
        except ValueError:
            pass

    match = re.search(r"1(\d{2})0\s+Wien", location)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass

    return None


def dedupe_existing(collection):
    """Collapse any pre-existing duplicates on willhaben_id so the unique
    index can be built. Keeps the most recently scraped document per ID.
    """
    pipeline = [
        {"$match": {"willhaben_id": {"$ne": None, "$ne": ""}}},
        {"$sort": {"scraped_at": -1}},
        {
            "$group": {
                "_id": "$willhaben_id",
                "keep": {"$first": "$_id"},
                "all": {"$push": "$_id"},
            }
        },
        {"$match": {"$expr": {"$gt": [{"$size": "$all"}, 1]}}},
    ]
    removed = 0
    for group in collection.aggregate(pipeline, allowDiskUse=True):
        to_delete = [oid for oid in group["all"] if oid != group["keep"]]
        if to_delete:
            removed += collection.delete_many({"_id": {"$in": to_delete}}).deleted_count

    # Drop any legacy rows without a willhaben_id — they can't be deduplicated.
    removed += collection.delete_many(
        {"$or": [{"willhaben_id": {"$exists": False}}, {"willhaben_id": ""}]}
    ).deleted_count
    return removed


def ensure_indexes(collection):
    """Create indexes before any writes. The unique index on willhaben_id is
    the hard guarantee against duplicates.
    """
    removed = dedupe_existing(collection)
    if removed:
        print(f"Dedup: {removed} Altbestand-Duplikate entfernt.")

    collection.create_index(
        [("willhaben_id", ASCENDING)], unique=True, name="willhaben_id_unique"
    )
    collection.create_index([("geo", "2dsphere")])
    collection.create_index("district")


def scrape_page(page_num):
    """Scrape a single page of Willhaben search results."""
    params = {"page": page_num, "rows": 25}
    response = requests.get(WILLHABEN_BASE_URL, headers=HEADERS, params=params, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    return extract_listings_from_next_data(soup)


def main():
    print("=== Willhaben Mietwohnungen Scraper - Wien ===")
    print(f"Scraping up to {MAX_PAGES} pages...\n")

    collection = connect_mongo()
    ensure_indexes(collection)

    total_new = 0
    total_updated = 0
    total_duplicates_skipped = 0

    for page in range(1, MAX_PAGES + 1):
        print(f"Seite {page}/{MAX_PAGES}...", end=" ")
        try:
            listings = scrape_page(page)
        except requests.RequestException as e:
            print(f"Fehler: {e}")
            continue

        if not listings:
            print("Keine Inserate gefunden, Abbruch.")
            break

        for listing in listings:
            try:
                result = collection.update_one(
                    {"willhaben_id": listing["willhaben_id"]},
                    {"$set": listing},
                    upsert=True,
                )
            except DuplicateKeyError:
                # Race with a concurrent writer that inserted the same ID
                # between our match and upsert — unique index blocked it.
                total_duplicates_skipped += 1
                continue

            if result.upserted_id:
                total_new += 1
            elif result.modified_count > 0:
                total_updated += 1

        print(f"{len(listings)} Inserate verarbeitet.")

        if page < MAX_PAGES:
            time.sleep(REQUEST_DELAY)

    print(f"\nFertig! {total_new} neue, {total_updated} aktualisierte Inserate.")
    if total_duplicates_skipped:
        print(f"{total_duplicates_skipped} Duplikat-Kollisionen abgefangen.")
    print(f"Gesamt in DB: {collection.count_documents({})}")


if __name__ == "__main__":
    main()
