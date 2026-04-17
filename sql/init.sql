-- 原始层：追加写入，永不更新，保留完整历史
CREATE TABLE IF NOT EXISTS raw_weather (
    id          SERIAL PRIMARY KEY,
    city        VARCHAR(50)   NOT NULL,
    date        DATE          NOT NULL,
    temp_max    FLOAT,
    temp_min    FLOAT,
    precip_mm   FLOAT,
    wind_max    FLOAT,
    fetched_at  TIMESTAMP     NOT NULL DEFAULT NOW(),
    -- 同一城市同一天可能被抓多次（每次调度都追加），这是故意的
    -- raw 层的职责是"忠实记录"，不做去重
    CONSTRAINT raw_weather_natural_key UNIQUE (city, date, fetched_at)
);

-- 清洗层：幂等写入，每次调度覆盖当天数据
CREATE TABLE IF NOT EXISTS clean_weather (
    city             VARCHAR(50)   NOT NULL,
    date             DATE          NOT NULL,
    temp_max_c       FLOAT,        -- 保留摄氏度，去掉NULL
    temp_min_c       FLOAT,
    temp_range_c     FLOAT,        -- 新增衍生字段：温差 = max - min
    precip_mm        FLOAT         NOT NULL DEFAULT 0,  -- NULL替换成0（无降雨）
    wind_max_kmh     FLOAT,
    weather_category VARCHAR(20),  -- 衍生字段：晴/小雨/大雨/大风
    updated_at       TIMESTAMP     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city, date)       -- 主键保证幂等：同一城市同一天只有一条
);

-- 供可视化用的索引（按城市+日期查询是最常见模式）
CREATE INDEX IF NOT EXISTS idx_clean_weather_city_date
    ON clean_weather (city, date DESC);