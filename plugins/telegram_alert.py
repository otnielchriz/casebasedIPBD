import requests

TELEGRAM_TOKEN = "8526540730:AAEIyUVGN_XOcDTQyLy-ANsFGi-iIT_G5Pw"
TELEGRAM_CHAT_ID = "5965992656"

def send_telegram_alert(context):
    """Callback function for task failures to send alerts to Telegram"""
    task_instance = context.get('task_instance')
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    
    # Safe resolution of logical/execution date for Airflow 3 compatibility
    dag_run = context.get('dag_run')
    execution_date = None
    if dag_run:
        execution_date = getattr(dag_run, 'logical_date', None) or getattr(dag_run, 'execution_date', None)
    if not execution_date:
        execution_date = context.get('logical_date') or context.get('execution_date') or "Unknown"
        
    # Simple formatting of error message
    message = (
        f"*Airflow Task Failed*\n\n"
        f"*DAG ID:* `{dag_id}`\n"
        f"*Task ID:* `{task_id}`\n"
        f"*Execution Date:* `{execution_date}`\n"
        f"*Status:* `FAILED`"
    )
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print(f"Failed to send Telegram alert: {response.text}")
    except Exception as e:
        print(f"Error sending Telegram alert: {e}")
