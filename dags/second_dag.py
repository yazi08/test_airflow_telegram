from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import requests

def send_telegram_message(bot_token, chat_id, message):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message
    }
    response = requests.post(url, json=payload)
    if response.status_code != 200:
        raise Exception(f"Failed to send message. Status code: {response.status_code}")
    else:
        print("Message sent successfully!")

def send_message_task():
    bot_token = '6787292037:AAG7LeOWCVcktfAW31BT0Lio8CBpOhni6bQ'
    chat_id = '830899933'
    message = 'Привет, мир! Это сообщение отправлено через Apache Airflow.'
    send_telegram_message(bot_token, chat_id, message)

# Определение параметров DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8),
    'retries': 1
}

# Определение DAG
dag = DAG('telegram_message_dag',
          default_args=default_args,
          schedule_interval='@daily')

# Создание задачи
send_message = PythonOperator(
    task_id='send_message_task',
    python_callable=send_message_task,
    dag=dag
)

# Определение зависимостей
send_message
