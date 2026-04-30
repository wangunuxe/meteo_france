#按这个顺序走完，DE 的核心工作流你就全跑通了。下一步自然是把 transform.py 换成 dbt，把 PostgreSQL 换成 BigQuery，逻辑完全一样，只是工具升级。

def categorize_weather(precip_mm: float, wind_max: float) -> str:
    """
    Helper Function:
    This function takes rainfall and wind speed for a given day and returns a human-readable weather label. This is what's called ***business logic*** — a set of rules that converts raw numbers into something meaningful.
    
    """
    # Before applying any rules, it handles None values by replacing them with 0. This is a safety check in case the API didn't return a value for those fields.
    if precip_mm is None: precip_mm = 0
    if wind_max is None:  wind_max = 0

    if wind_max > 60:          return "stormy"
    elif precip_mm > 10:       return "rainy"   
    elif precip_mm > 0.5:      return "drizzle"
    else:                      return "clear"


def transform(raw_records: list[dict]) -> list[dict]:
    """
    Main cleaning function:
    Takes the 24 raw records from extract.py and returns a cleaned version of them.

    """
    # two couters
    clean = [] # collects the valid records
    skipped = 0 # counts how many were dropped

    for r in raw_records:
        if r["temp_max"] is None or r["temp_min"] is None:
            skipped += 1
            continue  # If temp_max or temp_min is missing, the record is dropped entirely — temperature is the core field of this dataset, and without it the record has no analytical value.

        clean.append({
            "city":             r["city"],
            "date":             r["date"],
            "temp_max_c":       round(r["temp_max"], 1),
            "temp_min_c":       round(r["temp_min"], 1),
            "temp_range_c":     round(r["temp_max"] - r["temp_min"], 1),
            "precip_mm":        r["precip_mm"] or 0.0,
            "wind_max_kmh":     r["wind_max"],
            "weather_category": categorize_weather(r["precip_mm"], r["wind_max"]),
        })

    print(f"✅ Transform complete: {len(clean)} valid records, {skipped} dropped.")
    return clean

if __name__ == "__main__":
    import sys
    sys.path.insert(0, "/home/lenovo/projects/DE/meteo_france/scripts")

    from extract import extract_all_cities
    raw = extract_all_cities()
    result = transform(raw)

    import json
    print(json.dumps(result[:2], indent=2, ensure_ascii=False))
