import requests
import json
from datetime import datetime, timedelta

CITIES = {
    "Paris":     {"lat": 48.8566, "lon": 2.3522},
    "Lyon":      {"lat": 45.7640, "lon": 4.8357},
    "Marseille": {"lat": 43.2965, "lon": 5.3698},
}

def fetch_weather(city_name: str, lat: float, lon: float) -> dict:
    """
    调用 Open-Meteo API，抓最近7天的每日数据。
    API 完全免费，无需注册，无需 API key。
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
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()  # 非200状态码直接抛异常，Airflow 会捕获并重试
    return response.json()


def extract_all_cities() -> list[dict]:
    """
    抓所有城市，返回扁平化的记录列表，每行=一个城市某一天的数据。
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
                "fetched_at":  datetime.utcnow().isoformat(),  # 记录抓取时间，方便排查重复
            })

    print(f"✅ 抓取完成，共 {len(records)} 条记录")
    return records


if __name__ == "__main__":
    data = extract_all_cities()
    print(json.dumps(data[:2], indent=2, ensure_ascii=False))  # 预览前两条