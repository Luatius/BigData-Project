#!/usr/bin/env python3
"""Fetch Vienna U-Bahn station data from Wiener Linien Open Data.
Downloads stop, line, and platform CSVs, joins them to find
subway stations, and stores the result in MongoDB.
"""

import csv
import io
import os
from collections import defaultdict
from datetime import datetime, timezone

import requests
from pymongo import MongoClient

HALTESTELLEN_URL = "https://data.wien.gv.at/csv/wienerlinien-ogd-haltestellen.csv"
LINIEN_URL = "https://data.wien.gv.at/csv/wienerlinien-ogd-linien.csv"
STEIGE_URL = "https://data.wien.gv.at/csv/wienerlinien-ogd-steige.csv"


def connect_mongo():
    """Connect to MongoDB and return the ubahn_stations collection."""
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
    return db["ubahn_stations"]


def download_csv(url):
    """Download a semicolon-delimited CSV and return a list of dicts."""
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text), delimiter=";")
    return list(reader)


def parse_coordinate(value):
    """Parse a coordinate string, handling both dot and comma decimals."""
    if not value:
        return None
    try:
        return float(value.replace(",", "."))
    except (ValueError, TypeError):
        return None


def main():
    print("=== Wiener Linien U-Bahn Stations Fetcher ===\n")

    print("Downloading Haltestellen...")
    haltestellen = download_csv(HALTESTELLEN_URL)
    print(f"  {len(haltestellen)} stops loaded")

    print("Downloading Linien...")
    linien = download_csv(LINIEN_URL)
    print(f"  {len(linien)} lines loaded")

    print("Downloading Steige...")
    steige = download_csv(STEIGE_URL)
    print(f"  {len(steige)} platforms loaded")

    # Find U-Bahn line IDs (VERKEHRSMITTEL == "ptMetro")
    ubahn_line_ids = {}
    for line in linien:
        if line.get("VERKEHRSMITTEL") == "ptMetro":
            ubahn_line_ids[line["LINIEN_ID"]] = line["BEZEICHNUNG"]

    print(f"\nU-Bahn lines: {sorted(ubahn_line_ids.values())}")

    # Find which stops are served by U-Bahn lines (via steige join table)
    stop_lines = defaultdict(set)
    for platform in steige:
        line_id = platform.get("FK_LINIEN_ID")
        stop_id = platform.get("FK_HALTESTELLEN_ID")
        if line_id in ubahn_line_ids:
            stop_lines[stop_id].add(ubahn_line_ids[line_id])

    print(f"U-Bahn stop IDs found: {len(stop_lines)}")

    # Build station documents with coordinates
    halt_lookup = {h["HALTESTELLEN_ID"]: h for h in haltestellen}

    stations = []
    for stop_id, lines in stop_lines.items():
        halt = halt_lookup.get(stop_id)
        if not halt:
            continue

        lat = parse_coordinate(halt.get("WGS84_LAT", ""))
        lon = parse_coordinate(halt.get("WGS84_LON", ""))
        if lat is None or lon is None:
            continue

        stations.append({
            "station_name": halt.get("NAME", ""),
            "lines": sorted(lines),
            "latitude": lat,
            "longitude": lon,
            "geo": {
                "type": "Point",
                "coordinates": [lon, lat],  # GeoJSON: [lng, lat]
            },
            "haltestellen_id": stop_id,
            "fetched_at": datetime.now(timezone.utc),
        })

    print(f"Stations with coordinates: {len(stations)}")

    # Store in MongoDB (fresh load each time)
    collection = connect_mongo()
    collection.delete_many({})
    if stations:
        collection.insert_many(stations)

    # Create geospatial index
    collection.create_index([("geo", "2dsphere")])

    print(f"\nStored {len(stations)} U-Bahn stations in MongoDB.")


if __name__ == "__main__":
    main()
