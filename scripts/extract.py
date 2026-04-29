import requests
import json
from datetime import datetime, timezone

CITIES = {
    "Paris":     {"lat": 48.8566, "lon": 2.3522},
    "Lyon":      {"lat": 45.7640, "lon": 4.8357},
    "Marseille": {"lat": 43.2965, "lon": 5.3698},
}

def fetch_weather(city_name: str, lat: float, lon: float) -> dict:
    """
    Call the Open-Meteo API to fetch the last 7 days of daily weather data.
    Completely free — no account or API key required.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":  lat,
        "longitude": lon,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "windspeed_10m_max",
        ],
        "timezone":   "Europe/Paris",
        "past_days":  7,
        "forecast_days": 1,
    }
    # The Response class
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()  # If the status code is not 200, raise a execption immdiately and Airflow will catch it and retry
    return response.json() # Converts the raw response (just a string in format JSON) text into a python dictionary


def extract_all_cities() -> list[dict]:
    """
    extract_all_cities() loops through all three cities and calls fetch_weather() for each one, which returns a dictionary containing the raw API response. -> "raw" is the full dictionary, which contains several keys

    raw["daily"] picks out the value associated with the key "daily" — which is the nested dictionary containing all the weather data. -> "daily = raw["daily"] " is also a dictionary

    The API response is column-oriented — meaning all the dates are stored in one list, all the max temperatures in another list, and so on. However, a database expects row-oriented data — one complete record per day. So the function uses enumerate() to loop through the dates by index, and uses that index i to pick the matching value from each of the other lists, building one dictionary per day.

    Each dictionary is then appended to the records list. After all three cities are processed, records contains 24 dictionaries in total (8 days * 3 cities), each representing one city's weather on one specific day
    """
    records = []
    for city_name, coords in CITIES.items():
        raw = fetch_weather(city_name, coords["lat"], coords["lon"])
        daily = raw["daily"]

        for i, date_str in enumerate(daily["time"]):
            records.append({
                "city":        city_name,
                "date":        date_str,
                "temp_max":    daily["temperature_2m_max"][i],
                "temp_min":    daily["temperature_2m_min"][i],
                "precip_mm":   daily["precipitation_sum"][i],
                "wind_max":    daily["windspeed_10m_max"][i],
                "fetched_at":  datetime.now(timezone.utc).isoformat(),  # Record fetch time to help identify duplicates
            })

    print(f"✅ Extraction complete — {len(records)} records fetched.")
    return records


if __name__ == "__main__":
    data = extract_all_cities()
    #json.dumps() converts the Python dictionary into JSON string
    print(json.dumps(data[:2], indent=2, ensure_ascii=False))