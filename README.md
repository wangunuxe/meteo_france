# 🌤️ France Weather Pipeline

一个完整的数据工程入门项目，用真实天气数据跑通从 API 抓取到自动调度的完整 DE 链路。

## 项目概览

```
Open-Meteo API（免费·无需注册）
        ↓
extract.py（抓取巴黎/里昂/马赛天气数据）
        ↓
transform.py（清洗·去重·衍生字段）
        ↓
PostgreSQL raw_weather（原始数据层）
        ↓
PostgreSQL clean_weather（清洗数据层）
        ↓
Airflow DAG（每天 06:00 自动调度）
        ↓
Metabase / matplotlib（可视化）
```

## 技术栈

| 组件 | 用途 |
|------|------|
| Python 3.12 | 数据抓取与清洗脚本 |
| PostgreSQL 15 | 数据存储（双层架构） |
| Apache Airflow 2.9 | 任务调度与编排 |
| Docker Compose | 本地环境一键启动 |
| Open-Meteo API | 天气数据源（免费） |

## 项目结构

```
france-weather-pipeline/
├── docker-compose.yml      # Airflow + PostgreSQL 全家桶
├── .env                    # 环境变量（不提交到 Git）
├── dags/
│   └── weather_dag.py      # Airflow DAG 定义
├── scripts/
│   ├── extract.py          # Step 1：调用 Open-Meteo API
│   ├── transform.py        # Step 2：数据清洗与分类
│   └── load.py             # Step 3：写入 PostgreSQL
├── sql/
│   └── init.sql            # 建表语句（容器启动时自动执行）
└── viz/
    └── plot_weather.py     # matplotlib 温度趋势可视化
```

## 快速开始

### 前置要求

- Docker（已安装并运行）
- Python 3.10+（用于本地可视化）

### 1. 启动所有服务

```bash
# 初始化 Airflow 数据库和管理员账号（只需运行一次）
docker compose up airflow-init

# 后台启动所有服务
docker compose up -d

# 确认服务状态
docker compose ps
```

### 2. 访问 Airflow UI

打开浏览器访问 `http://localhost:8080`

- 用户名：`airflow`
- 密码：`airflow`

### 3. 触发 DAG

在 Airflow UI 中：

1. 找到 `france_weather_pipeline`
2. 打开左侧开关（Unpause）
3. 点击右侧 ▶ 按钮手动触发

### 4. 验证数据

```bash
# 连接业务数据库（密码：weather123）
psql -h localhost -p 5433 -U weather -d weather

# 查看清洗后数据
SELECT city, date, temp_max_c, temp_min_c, weather_category
FROM clean_weather
ORDER BY date DESC, city;

# 查看原始数据
SELECT city, date, temp_max, fetched_at
FROM raw_weather
ORDER BY fetched_at DESC
LIMIT 10;
```

### 5. 本地可视化

```bash
pip install psycopg2-binary pandas matplotlib
python viz/plot_weather.py
```

## 数据库设计

### raw_weather（原始层）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | SERIAL | 主键 |
| city | VARCHAR | 城市名 |
| date | DATE | 日期 |
| temp_max | FLOAT | 最高温度 |
| temp_min | FLOAT | 最低温度 |
| precip_mm | FLOAT | 降雨量 (mm) |
| wind_max | FLOAT | 最大风速 (km/h) |
| fetched_at | TIMESTAMP | 抓取时间 |

> 原始层**只追加、不更新**，完整保留历史原始数据。

### clean_weather（清洗层）

| 字段 | 类型 | 说明 |
|------|------|------|
| city | VARCHAR | 城市名（联合主键） |
| date | DATE | 日期（联合主键） |
| temp_max_c | FLOAT | 最高温度（已清洗） |
| temp_min_c | FLOAT | 最低温度（已清洗） |
| temp_range_c | FLOAT | 温差（衍生字段） |
| precip_mm | FLOAT | 降雨量（NULL 替换为 0） |
| wind_max_kmh | FLOAT | 最大风速 |
| weather_category | VARCHAR | 天气分类（clear/drizzle/rainy/stormy） |
| updated_at | TIMESTAMP | 最后更新时间 |

> 清洗层使用 `UPSERT` **幂等写入**，重复调度不产生重复数据。

## DAG 说明

```
extract → transform → load
```

| 配置项 | 值 |
|--------|-----|
| 调度时间 | 每天 06:00（巴黎时间） |
| 失败重试 | 最多 2 次，间隔 5 分钟 |
| 回填历史 | 关闭（catchup=False） |
| Task 间传值 | XCom |

## 天气分类规则

| 分类 | 条件 |
|------|------|
| `stormy` | 风速 > 60 km/h |
| `rainy` | 降雨量 > 10 mm |
| `drizzle` | 降雨量 > 0.5 mm |
| `clear` | 其他 |

## 停止与清理

```bash
# 停止所有容器（保留数据）
docker compose down

# 停止并删除所有数据（谨慎）
docker compose down -v
```

## 后续扩展方向

```
当前项目（基础）
      ↓
接入 dbt（替换 transform.py，SQL 化清洗逻辑）
      ↓
迁移到云平台（AWS S3 + RDS / GCP BigQuery）
      ↓
加入数据质量检测（Great Expectations）
      ↓
综合项目（Reddit 评论情感分析管道）
```

## 数据源

天气数据来自 [Open-Meteo](https://open-meteo.com/) —— 完全免费，无需注册，无需 API Key。

覆盖城市：巴黎（Paris）、里昂（Lyon）、马赛（Marseille）
