from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

from project.alert.telegram import send_telegram_message, telegram_failure_callback
from project.operators.spark_health_check import SparkHealthCheckOperator

YARN_CONN_ID = 'yarn_resourcemanager'
POSTGRES_CONN_ID = 'project_postgres'
OUTPUT_TABLE = 'fact_page_view'
# Job streaming của spark/99-project (spark.app.name trong spark.conf)
REQUIRED_SPARK_APPS = ['Project99PageViewStreaming']

INTERVAL_MINUTES = 5

# Ngưỡng cảnh báo
MIN_RUNNING_WORKERS = 1
MAX_CPU_USAGE_PCT = 90
MAX_MEMORY_USAGE_PCT = 90
MIN_ROWS_PER_INTERVAL = 1          # không có dòng mới nào trong 5 phút => job không ghi được dữ liệu


@dag(
    dag_id='spark_monitoring',
    schedule=f'*/{INTERVAL_MINUTES} * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={'send_report': Param(False, type='boolean', description='Gửi báo cáo lên Telegram kể cả khi không có cảnh báo')},
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': telegram_failure_callback,
    },
    tags=['project', 'monitoring', 'spark'],
)
def spark_monitoring():
    health_check = SparkHealthCheckOperator(
        task_id='health_check',
        yarn_conn_id=YARN_CONN_ID,
        min_running_workers=MIN_RUNNING_WORKERS,
        max_cpu_usage_pct=MAX_CPU_USAGE_PCT,
        max_memory_usage_pct=MAX_MEMORY_USAGE_PCT,
        required_apps=REQUIRED_SPARK_APPS,
    )

    @task
    def check_data_output():
        """Kiểm tra job Spark có insert dữ liệu mới vào db trong 5 phút gần nhất không."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        new_rows, total_rows, last_ingested_at = hook.get_first(f"""
            SELECT count(*) FILTER (WHERE ingested_at >= now() - interval '{INTERVAL_MINUTES} minutes'),
                   count(*),
                   max(ingested_at)
            FROM {OUTPUT_TABLE}
        """)
        print(f"{OUTPUT_TABLE}: +{new_rows} dòng trong {INTERVAL_MINUTES} phút, tổng {total_rows}, "
              f"lần ghi cuối {last_ingested_at}")

        issues = []
        if new_rows < MIN_ROWS_PER_INTERVAL:
            issues.append(f"Không có dữ liệu mới trong bảng {OUTPUT_TABLE} {INTERVAL_MINUTES} phút qua "
                          f"(lần ghi cuối: {last_ingested_at})")
        return {
            'issues': issues,
            'line': f"Bảng {OUTPUT_TABLE}: +{new_rows} dòng / {INTERVAL_MINUTES} phút, tổng {total_rows} dòng",
        }

    @task(trigger_rule='all_done')
    def send_alert(health, output, params=None):
        # Task health_check fail đã được báo qua on_failure_callback => ở đây chỉ gửi phần còn lại
        health = health or {'issues': [], 'workers': [], 'running_apps': [], 'master': None}
        output = output or {'issues': [], 'line': 'Không kiểm tra được dữ liệu output'}

        issues = health['issues'] + output['issues']
        if not issues and not params['send_report']:
            print("Không có cảnh báo")
            return

        header = f"⚠️ SPARK ALERT ({len(issues)} vấn đề)" if issues else "✅ SPARK REPORT"
        workers = [f"  {w['host']} {w['state']}: CPU {w['cores_used']}/{w['cores_total']} cores "
                   f"({w['cpu_usage_pct']}%), RAM {w['memory_used_mb']}/{w['memory_total_mb']} MB "
                   f"({w['memory_usage_pct']}%)" for w in health['workers']]
        master_state = health['master']['state'] if health['master'] else 'UNKNOWN'
        message = '\n'.join([
            header,
            *[f"- {issue}" for issue in issues],
            '',
            f"Master: {master_state}",
            f"Workers ({len(health['workers'])}):",
            *workers,
            f"Running apps: {', '.join(health['running_apps']) or 'không có'}",
            output['line'],
        ])
        print(message)
        send_telegram_message(message)

    send_alert(health_check.output, check_data_output())


spark_monitoring()
