import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DB_CONFIG = {
    "host": "localhost", "port": 5433,
    "dbname": "weather", "user": "weather", "password": "weather123",
}

def load_data() -> pd.DataFrame:
    sql = """
        SELECT city, date, temp_max_c, temp_min_c, precip_mm, weather_category
        FROM clean_weather
        ORDER BY date, city
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        return pd.read_sql(sql, conn, parse_dates=["date"])

def plot_temperature(df: pd.DataFrame):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    fig.suptitle("法国三城市天气趋势", fontsize=14, fontweight="bold")

    colors = {"Paris": "#2196F3", "Lyon": "#FF5722", "Marseille": "#4CAF50"}

    # 上图：温度趋势
    for city, color in colors.items():
        city_df = df[df["city"] == city]
        ax1.fill_between(city_df["date"], city_df["temp_min_c"], city_df["temp_max_c"],
                         alpha=0.15, color=color)
        ax1.plot(city_df["date"], city_df["temp_max_c"], color=color,
                 linewidth=2, label=f"{city} (最高)")
        ax1.plot(city_df["date"], city_df["temp_min_c"], color=color,
                 linewidth=1, linestyle="--", alpha=0.7)

    ax1.set_ylabel("温度 (°C)")
    ax1.legend(loc="upper left")
    ax1.grid(alpha=0.3)

    # 下图：降雨量柱状图
    cities = df["city"].unique()
    width = 0.25
    dates = df["date"].unique()

    for i, city in enumerate(cities):
        city_df = df[df["city"] == city].set_index("date")
        offsets = [d + pd.Timedelta(days=i * width - width) for d in dates]
        ax2.bar(offsets, [city_df.loc[d, "precip_mm"] if d in city_df.index else 0
                          for d in dates],
                width=0.2, label=city, color=list(colors.values())[i], alpha=0.8)

    ax2.set_ylabel("降雨量 (mm)")
    ax2.legend(loc="upper right")
    ax2.grid(alpha=0.3, axis="y")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))

    plt.tight_layout()
    plt.savefig("weather_trend.png", dpi=150, bbox_inches="tight")
    print("✅ 图表已保存到 weather_trend.png")
    plt.show()

if __name__ == "__main__":
    df = load_data()
    print(f"数据量：{len(df)} 条，日期范围：{df['date'].min()} ~ {df['date'].max()}")
    plot_temperature(df)