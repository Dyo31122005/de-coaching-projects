import requests
from airflow.hooks.base import BaseHook

# Telegram giới hạn 4096 ký tự / tin nhắn
MAX_MESSAGE_LENGTH = 4000


def send_telegram_message(text, conn_id='telegram_alert'):
    """Gửi tin nhắn tới group Telegram.

    Connection `telegram_alert` (HTTP):
      - host:     https://api.telegram.org
      - password: bot token (lấy từ @BotFather)
      - extra:    {"chat_id": "<id của group>"}
    """
    conn = BaseHook.get_connection(conn_id)
    token = conn.password
    chat_id = conn.extra_dejson['chat_id']
    url = f"{conn.host.rstrip('/')}/bot{token}/sendMessage"

    response = requests.post(url, json={
        'chat_id': chat_id,
        'text': text[:MAX_MESSAGE_LENGTH],
        'disable_web_page_preview': True,
    }, timeout=10)
    # Telegram trả về 4xx khi token/chat_id sai => raise để task bị fail thay vì "success" giả
    response.raise_for_status()


def telegram_failure_callback(context):
    """Dùng cho `on_failure_callback`: báo Telegram khi 1 task bị fail."""
    ti = context['task_instance']
    exception = context.get('exception')
    send_telegram_message(
        f"🔴 TASK FAILED\n"
        f"DAG: {ti.dag_id}\n"
        f"Task: {ti.task_id}\n"
        f"Run: {ti.run_id}\n"
        f"Try: {ti.try_number}\n"
        f"Error: {str(exception)[:1000]}\n"
        f"Log: {ti.log_url}"
    )
