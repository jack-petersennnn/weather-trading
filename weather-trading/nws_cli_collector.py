#!/usr/bin/env python3
"""
NWS CLI Historical Data Collector
Pulls NWS Climatological Reports (CLI) for all 6 cities and extracts MAXIMUM temps.
These are the same reports Kalshi uses for settlement.
"""
import json, os, re, time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request

CACHE_FILE = os.path.join(os.path.dirname(__file__), "nws_cli_actuals.json")
DELAY = 0.6  # seconds between API calls

NWS_STATIONS = {
    "New York":    "NYC",
    "Chicago":     "ORD",
    "Miami":       "MIA",
    "Denver":      "DEN",
    "Los Angeles": "LAX",
    "Austin":      "AUS",
}

MONTHS = {
    "JANUARY":1,"FEBRUARY":2,"MARCH":3,"APRIL":4,"MAY":5,"JUNE":6,
    "JULY":7,"AUGUST":8,"SEPTEMBER":9,"OCTOBER":10,"NOVEMBER":11,"DECEMBER":12
}

def fetch_json(url):
    req = Request(url, headers={"User-Agent": "KingClaw-Weather/3.1", "Accept": "application/json"})
    try:
        with urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"    ⚠ Fetch error: {e}")
        return None

def parse_cli_report(text):
    """Parse a CLI report text and extract date and max temp."""
    # Extract date from report
    # Pattern: "...DATED MONTH DD YYYY..." or similar
    report_date = None
    
    # Try various date patterns in CLI reports
    # Pattern 1: CLIMATE REPORT ... DATED MONTH DD YYYY
    m = re.search(r'(?:DATED|FOR THE PERIOD)\s+(\w+)\s+(\d{1,2})\s+(\d{4})', text, re.IGNORECASE)
    if m:
        month_name = m.group(1).upper()
        day = int(m.group(2))
        year = int(m.group(3))
        month = MONTHS.get(month_name)
        if month:
            try:
                report_date = datetime(year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                pass
    
    if not report_date:
        # Pattern 2: ...REPORT FOR MONTH DD YYYY...
        m = re.search(r'REPORT\s+FOR\s+(\w+)\s+(\d{1,2})\s+(\d{4})', text, re.IGNORECASE)
        if m:
            month_name = m.group(1).upper()
            day = int(m.group(2))
            year = int(m.group(3))
            month = MONTHS.get(month_name)
            if month:
                try:
                    report_date = datetime(year, month, day).strftime("%Y-%m-%d")
                except ValueError:
                    pass

    if not report_date:
        # Pattern 3: Look for "CLIMATE REPORT FOR <STATION>\n...DATE: MM/DD/YYYY"
        m = re.search(r'DATE:\s*(\d{1,2})/(\d{1,2})/(\d{4})', text)
        if m:
            month = int(m.group(1))
            day = int(m.group(2))
            year = int(m.group(3))
            try:
                report_date = datetime(year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                pass
    
    # Extract max temp
    max_temp = None
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped.startswith("MAXIMUM") and "TEMPERATURE" not in stripped:
            parts = stripped.split()
            if len(parts) >= 2:
                try:
                    val = parts[1]
                    # Handle "MM" (missing) or "M" 
                    if val in ("MM", "M", "T"):
                        continue
                    max_temp = int(val)
                    break
                except ValueError:
                    continue
    
    return report_date, max_temp

def collect_station(city, cli_code, existing_dates=None):
    """Collect all available CLI reports for a station."""
    if existing_dates is None:
        existing_dates = set()
    
    print(f"\n  📡 {city} ({cli_code})...")
    
    # Get list of CLI reports
    url = f"https://api.weather.gov/products/types/CLI/locations/{cli_code}"
    data = fetch_json(url)
    time.sleep(DELAY)
    
    if not data or not data.get("@graph"):
        print(f"    ⚠ No reports found")
        return {}
    
    reports = data["@graph"]
    print(f"    Found {len(reports)} report references")
    
    results = {}
    fetched = 0
    errors = 0
    
    for report_meta in reports:
        report_id = report_meta.get("id", "")
        issue_time = report_meta.get("issuanceTime", "")
        
        # Quick date check from issuance time to skip already-cached dates
        if issue_time:
            try:
                issue_dt = datetime.fromisoformat(issue_time.replace("Z", "+00:00"))
                # CLI reports are typically issued early morning for previous day
                approx_date = (issue_dt - timedelta(hours=12)).strftime("%Y-%m-%d")
                if approx_date in existing_dates:
                    continue
            except:
                pass
        
        # Fetch full report
        report_url = f"https://api.weather.gov/products/{report_id}"
        report = fetch_json(report_url)
        time.sleep(DELAY)
        fetched += 1
        
        if not report:
            errors += 1
            continue
        
        text = report.get("productText", "")
        if not text:
            errors += 1
            continue
        
        report_date, max_temp = parse_cli_report(text)
        
        # Fallback date from issuance time
        if not report_date and issue_time:
            try:
                issue_dt = datetime.fromisoformat(issue_time.replace("Z", "+00:00"))
                # Morning reports are for previous day
                if issue_dt.hour < 14:
                    report_date = (issue_dt - timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    report_date = issue_dt.strftime("%Y-%m-%d")
            except:
                pass
        
        if report_date and max_temp is not None:
            results[report_date] = max_temp
        elif report_date:
            print(f"    ⚠ No max temp found for {report_date}")
        
        # Progress indicator every 20 reports
        if fetched % 20 == 0:
            print(f"    ... fetched {fetched}/{len(reports)}, found {len(results)} temps so far")
    
    print(f"    ✅ {len(results)} dates with max temps (fetched {fetched}, {errors} errors)")
    return results

def collect_all(force=False):
    """Collect NWS CLI data for all stations."""
    print("🌡️  NWS CLI Historical Data Collector")
    print("=" * 50)
    
    # Load existing cache
    existing = {}
    if os.path.exists(CACHE_FILE) and not force:
        with open(CACHE_FILE) as f:
            existing = json.load(f)
        print(f"Loaded existing cache: {sum(len(v.get('data',{})) for v in existing.get('cities',{}).values())} total data points")
    
    if "cities" not in existing:
        existing = {"cities": {}, "metadata": {}}
    
    for city, cli_code in NWS_STATIONS.items():
        city_data = existing.get("cities", {}).get(city, {}).get("data", {})
        existing_dates = set(city_data.keys())
        
        new_data = collect_station(city, cli_code, existing_dates)
        
        # Merge
        if city not in existing["cities"]:
            existing["cities"][city] = {"cli_code": cli_code, "data": {}}
        existing["cities"][city]["data"].update(new_data)
        
        dates = sorted(existing["cities"][city]["data"].keys())
        if dates:
            print(f"    📅 Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")
    
    # Update metadata
    existing["metadata"] = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "source": "NWS CLI (api.weather.gov)",
        "summary": {}
    }
    for city in NWS_STATIONS:
        data = existing["cities"].get(city, {}).get("data", {})
        dates = sorted(data.keys())
        existing["metadata"]["summary"][city] = {
            "count": len(dates),
            "first_date": dates[0] if dates else None,
            "last_date": dates[-1] if dates else None,
        }
    
    with open(CACHE_FILE, "w") as f:
        json.dump(existing, f, indent=2)
    
    print(f"\n💾 Saved to {CACHE_FILE}")
    
    # Print summary
    print("\n📊 Summary:")
    total = 0
    for city in NWS_STATIONS:
        s = existing["metadata"]["summary"][city]
        total += s["count"]
        print(f"  {city:15s}: {s['count']:3d} days  ({s['first_date']} to {s['last_date']})")
    print(f"  {'TOTAL':15s}: {total:3d} data points")
    
    return existing

if __name__ == "__main__":
    collect_all()
