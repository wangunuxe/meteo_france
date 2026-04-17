# Airflow DAG（第三步的核心）

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 把 scripts/ 加到 Python 路径
import sys
sys.path.insert(0, "/opt/airflow/scripts")

from extract import extract_all_cities
from transform import transform
from load import load_raw, load_clean

# ── DAG 默认参数 ─────────────────────────────────────────────
default_args = {
    "owner": "you",
    "retries": 2,                         # 失败后最多重试2次
    "retry_delay": timedelta(minutes=5),  # 每次重试间隔5分钟
    "email_on_failure": True,             # 失败发邮件（需要配置 SMTP）
    "email": ["your@email.com"],
}

# ── DAG 定义 ─────────────────────────────────────────────────
with DAG(
    dag_id="france_weather_pipeline",
    default_args=default_args,
    description="每日抓取法国三城市天气数据",
    schedule="0 6 * * *",    # 每天早上6点（巴黎时间）运行
    start_date=datetime(2024, 1, 1),
    catchup=False,            # 不补跑历史数据（入门阶段先关掉）
    tags=["weather", "etl"],
) as dag:

    # ── Task 1：Extract ──────────────────────────────────────
    # XCom：Airflow 在 task 之间传数据的机制
    # return 的值会自动推送到 XCom，下一个 task 用 ti.xcom_pull() 取
    def task_extract(**context):
        records = extract_all_cities()
        # 数据量小时可以直接用 XCom 传；数据大时应该写临时文件或 S3
        return records

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    # ── Task 2：Transform ────────────────────────────────────
    def task_transform(**context):
        ti = context["ti"]
        raw_records = ti.xcom_pull(task_ids="extract")  # 从 extract task 取数据
        clean_records = transform(raw_records)
        return {"raw": raw_records, "clean": clean_records}

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    # ── Task 3：Load ─────────────────────────────────────────
    def task_load(**context):
        ti = context["ti"]
        data = ti.xcom_pull(task_ids="transform")
        load_raw(data["raw"])
        load_clean(data["clean"])

    load_task = PythonOperator(
        task_id="load",
        python_callable=task_load,
    )

    # ── Task 依赖关系（就是流程图的箭头）──────────────────────
    extract_task >> transform_task >> load_task