import sys
import os

import datetime as dt
import requests
from airflow.models import Variable
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.append(
    os.path.join(os.path.dirname(__file__), '..'),
)

from data_preparations.data_transformations import transform_deals_data, transform_users_data

# Базовые аргументы DAG
DEFAULT_ARGUMENTS = {
    'owner': 'ruslan_trifonov',
    # Время начала выполнения пайплайна
    'start_date': dt.datetime(2022, 6, 8),
}

# Доступ к GetCourse
secret_key_getcourse = Variable.get("GETCOURSE_KEY")


# ЗАКАЗЫ
def deals_data(date):
    """Получение данных о заказах на определенную дату"""

    # Запрос на получение данных
    deals_requests = requests.get(
        'https://kt-on-line.getcourse.ru/pl/api/account/deals?',
        params={
            'created_at[from]': date,
            'key': secret_key_getcourse,
        }
    )
    # Извлечение id запроса
    deals_export_id = deals_requests.json()['info']['export_id']

    # Получение данных
    get_DEALS = requests.get(
        f'https://kt-on-line.getcourse.ru/pl/api/account/exports/{deals_export_id}?',
        params={
            'key': secret_key_getcourse,
        }
    )

    return get_DEALS.json()


def save_deals(data):
    return data.reset_index().to_feather(f'../data/deals.file')


# ПОЛЬЗОВАТЕЛИ
def users_data(date):
    """Получение данных о пользователях на определенную дату"""

    # Запрос на получение данных
    users_requests = requests.get(
        'https://kt-on-line.getcourse.ru/pl/api/account/users?',
        params={
            'status': 'in_base',
            'created_at[from]': date,
            'key': secret_key_getcourse,
        }
    )
    # Извлечение id запроса
    users_export_id = users_requests.json()['info']['export_id']

    # Получение данных
    get_USERS = requests.get(
        f'https://kt-on-line.getcourse.ru/pl/api/account/exports/{users_export_id}?',
        params={
            'key': secret_key_getcourse,
        }
    )
    return get_USERS.json()


def save_users(data):
    return data.reset_index().to_feather(f'../data/users.file')


# Объединение данных
def users_deals_df(transform_deals_data, transform_users_data):
    users_deals_df = transform_users_data.merge(transform_deals_data,
                                                left_on=['id', 'Email'],
                                                right_on=['ID пользователя', 'Email'],
                                                how='outer')
    return users_deals_df


def save_users_deals_df(data):
    return data.reset_index().to_feather(f'../data/users_deals_df.file')


# Определение DAG
with DAG(
        dag_id='getting_data_from_getcourse_1',
        schedule_interval='@hourly',
        default_args=DEFAULT_ARGUMENTS,
        catchup=False
) as dag:
    start = EmptyOperator(task_id="start")

    extract_deals = PythonOperator(
        task_id='extract_deals',
        python_callable=deals_data,
        op_kwargs={
            'date': '{{execution_date}}'
        },
    )

    check_extraction_deals = PythonSensor(
        task_id="check_extraction_deals",
        python_callable=deals_data
    )

    extract_users = PythonOperator(
        task_id='extract_users',
        python_callable=users_data,
        op_kwargs={
            'date': '{{execution_date}}'
        },
    )
    check_extraction_users = PythonSensor(
        task_id="check_extraction_users",
        python_callable=deals_data
    )

    transform_deals = PythonOperator(
        task_id='transform_deals',
        python_callable=transform_deals_data
    )

    transform_users = PythonOperator(
        task_id='transform_users',
        python_callable=transform_users_data
    )
    save_deals = PythonOperator(
        task_id='save_deals',
        python_callable=save_deals
    )

    save_users = PythonOperator(
        task_id='save_users',
        python_callable=save_users
    )

    join_data = PythonOperator(
        task_id='join_users_deals',
        python_callable=users_deals_df
    )

    save_users_deals = PythonOperator(
        task_id='save_users_deals',
        python_callable=save_users_deals_df
    )

start >> extract_deals >> check_extraction_deals >> [extract_users, transform_deals]
transform_deals >> save_deals
extract_users >> check_extraction_users >> transform_users >> save_users
[transform_deals, transform_users] >> join_data >> save_users_deals
