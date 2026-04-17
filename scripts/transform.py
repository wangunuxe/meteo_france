#按这个顺序走完，DE 的核心工作流你就全跑通了。下一步自然是把 transform.py 换成 dbt，把 PostgreSQL 换成 BigQuery，逻辑完全一样，只是工具升级。

def categorize_weather(precip_mm: float, wind_max: float) -> str:
    """简单的天气分类规则，这就是'业务逻辑'"""
    if precip_mm is None: precip_mm = 0
    if wind_max is None:  wind_max = 0

    if wind_max > 60:          return "stormy"   # 大风/暴风
    elif precip_mm > 10:       return "rainy"    # 大雨
    elif precip_mm > 0.5:      return "drizzle"  # 小雨
    else:                      return "clear"    # 晴天


def transform(raw_records: list[dict]) -> list[dict]:
    """
    原始数据 → 清洗数据
    规则：
      1. 过滤掉 temp_max 或 temp_min 是 None 的记录（传感器故障）
      2. 计算温差
      3. NULL 降雨替换为 0
      4. 添加天气分类
    """
    clean = []
    skipped = 0

    for r in raw_records:
        if r["temp_max"] is None or r["temp_min"] is None:
            skipped += 1
            continue  # 缺核心字段，丢弃这条记录

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

    print(f"✅ 清洗完成：{len(clean)} 条有效，丢弃 {skipped} 条")
    return clean