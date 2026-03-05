#!/usr/bin/env python3
"""Market Scanner — fetches active Kalshi weather markets."""

import json
import urllib.request
import urllib.error
import sys
import time
from datetime import datetime, timezone

API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES = ["KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA", "KXHIGHDEN", "KXHIGHLAX", "KXHIGHAUS"]
OUTPUT = "/home/ubuntu/.openclaw/workspace/weather-trading/active-markets.json"

SERIES_META = {
    "KXHIGHNY":  {"city": "New York",  "lat": 40.7128, "lon": -73.9352},
    "KXHIGHCHI": {"city": "Chicago",   "lat": 41.8781, "lon": -87.6298},
    "KXHIGHMIA": {"city": "Miami",     "lat": 25.7617, "lon": -80.1918},
    "KXHIGHDEN": {"city": "Denver",    "lat": 39.7392, "lon": -104.9903},
    "KXHIGHLAX": {"city": "Los Angeles","lat": 34.0522, "lon": -118.2437},
    "KXHIGHAUS": {"city": "Austin",    "lat": 30.2672, "lon": -97.7431},
}


def api_get(path, params=None, retries=3):
    url = f"{API_BASE}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url += f"?{qs}"
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "User-Agent": "KingClaw-Weather/1.0"
    })
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} for {url}" + (f" (attempt {attempt+1}/{retries})" if retries > 1 else ""))
            if e.code == 429:  # rate limited
                wait = min(10 * (attempt + 1), 30)
                print(f"    Rate limited — waiting {wait}s...")
                time.sleep(wait)
                continue
            return None
        except Exception as e:
            print(f"  Error fetching {url}: {e}" + (f" (attempt {attempt+1}/{retries})" if retries > 1 else ""))
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            return None
    return None


def scan_series(series_ticker):
    """Fetch events for a series, return list of event dicts with nested markets."""
    print(f"  Scanning {series_ticker}...")
    # Try fetching events for this series
    data = api_get("/events", {
        "series_ticker": series_ticker,
        "with_nested_markets": "true",
        "status": "open",
        "limit": "10"
    })
    if not data:
        return []
    events = data.get("events", [])
    print(f"    Found {len(events)} events")
    return events


def run():
    print("=== KingClaw Market Scanner ===")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    results = {
        "scan_time": datetime.now(timezone.utc).isoformat(),
        "series": {}
    }

    total_markets = 0
    for i, series in enumerate(SERIES):
        if i > 0:
            time.sleep(4)  # Rate limit: 4s between Kalshi API calls
        meta = SERIES_META[series]
        events = scan_series(series)
        series_data = {
            "city": meta["city"],
            "lat": meta["lat"],
            "lon": meta["lon"],
            "events": []
        }
        for ev in events:
            markets = ev.get("markets", [])
            total_markets += len(markets)
            parsed_markets = []
            for m in markets:
                parsed_markets.append({
                    "ticker": m.get("ticker", ""),
                    "subtitle": m.get("subtitle", m.get("title", "")),
                    "yes_bid": m.get("yes_bid", 0),
                    "yes_ask": m.get("yes_ask", 0),
                    "no_bid": m.get("no_bid", 0),
                    "no_ask": m.get("no_ask", 0),
                    "last_price": m.get("last_price", 0),
                    "volume": m.get("volume", 0),
                    "open_interest": m.get("open_interest", 0),
                    "result": m.get("result", ""),
                    "status": m.get("status", ""),
                    "floor_strike": m.get("floor_strike"),
                    "cap_strike": m.get("cap_strike"),
                })
            series_data["events"].append({
                "event_ticker": ev.get("event_ticker", ""),
                "title": ev.get("title", ""),
                "category": ev.get("category", ""),
                "end_date": ev.get("end_date", ev.get("close_time", "")),
                "markets": parsed_markets,
            })
        results["series"][series] = series_data

    with open(OUTPUT, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nScan complete: {total_markets} markets across {len(SERIES)} series")
    print(f"Saved to {OUTPUT}")
    return results


if __name__ == "__main__":
    run()
