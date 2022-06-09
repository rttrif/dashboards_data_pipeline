import sys
import os

import datetime as dt
import requests
from airflow.models import Variable
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
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
    # # Количество повторений в случае неудач
    # 'retries': 10,
    # # Пауза между повторами
    # 'retry_delay': dt.timedelta(minutes=15),
}

# Доступ к GetCourse
secret_key_getcourse = Variable.get("GETCOURSE_KEY")

# Определение DAG
with DAG(
        dag_id='getting_data_from_getcourse_2',
        schedule_interval='@hourly',
        default_args=DEFAULT_ARGUMENTS,
        catchup=False
) as dag:
    start = EmptyOperator(task_id="start")


    # ЗАКАЗЫ
    @task
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


    # ПОЛЬЗОВАТЕЛИ
    @task
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


    @task
    def save_deals(data):
        return data.reset_index().to_feather(f'../data/deals.file')


    @task
    def save_users(data):
        return data.reset_index().to_feather(f'../data/users.file')


    @task
    def users_deals_df(transform_deals_data, transform_users_data):
        users_deals_df = transform_users_data.merge(transform_deals_data,
                                                    left_on=['id', 'Email'],
                                                    right_on=['ID пользователя', 'Email'],
                                                    how='outer')
        return users_deals_df


    @task
    def save_users_deals_df(data):
        return data.reset_index().to_feather(f'../data/users_deals_df.file')


    extract_deals_data = deals_data('2022-6-9')
    extract_users_data = users_data('2022-6-9')

    transform_deals = transform_deals_data(extract_deals_data)
    transform_users = transform_users_data(extract_users_data)
    join_data = users_deals_df(transform_deals, transform_users)

    deals_df = save_deals(transform_deals)
    users_df = save_users(transform_users)
    users_deals_df = users_deals_df(join_data)

    start >> extract_deals_data >> [extract_users_data, transform_deals]
    transform_deals >> deals_df
    extract_users_data >> transform_users >> users_df
    [transform_deals, transform_users] >> join_data >> users_deals_df
