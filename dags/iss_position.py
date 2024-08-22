import requests
import json
import logging as log
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# проверяем данные (примитивно - на наличие координат)
def _check_data(data):

    try:
        lat = float(data['iss_position']['latitude'])
        lon = float(data['iss_position']['longitude'])
        if not (lat and lon):
            err = 'API response is incorrect: latitude = {}, longitude = {}'.format(lat, lon)
            log.error(err)
            raise ValueError(err)
    except Exception as e:
        log.error(e)
        raise ValueError(e)

    log.info(f'Get Data: latitude: {lat}, longitude: {lon}')

    return data


# получаем данные из API
def extract_data():

    # делаем запрос к API
    req = requests.get('http://api.open-notify.org/iss-now.json')

    # проверяем статус ответа
    if req.status_code != 200:
        log.error(f'API requests error, status={req.status_code}')

    # записываем ответ API в переменную resp
    resp = req.text

    # пробуем преобразовать ответ в json
    try:
        data = json.loads(resp)
    except json.JSONDecodeError:
        err = f'Failed to parse ISS position: {resp}'
        log.error(err)  # логируем
        raise ValueError(err)  # прерываем выполнение функции

    # ещё одна проверка на статус ответа
    if data.get('message', '') != 'success':
        err = 'API response message error'
        log.error(err)  # логируем
        raise ValueError(err)  # прерываем выполнение функции

    # ti.xcom_push(key='iis_position', value=data)
    return data


def load_data(data):

    data = _check_data(data)

    print(data)

    hook = PostgresHook(postgres_conn_id='DWH')
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_table_query = """
        create table if not exists iss_position (
            id serial primary key,
            created_ts timestamptz default (now() at time zone 'UTC'),
            lat double precision not null,
            lon double precision not null
        );
    """

    insert_data_query = """
        insert into iss_position (lat, lon) values (%s, %s)
        returning id;
    """

    try:
        lat = float(data['iss_position']['latitude'])
        lon = float(data['iss_position']['longitude'])
        cursor.execute(create_table_query)
        cursor.execute(insert_data_query, (lat, lon))
        inserted_id = cursor.fetchone()[0]
    except Exception as e:
        log.error(e)
        raise ValueError(e)
    finally:
        cursor.close()
        conn.commit()

    log.info(f'Data was successfully loaded (id={inserted_id})')


args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='iis_position',
    default_args=args,
    start_date=datetime(2024, 8, 1),
    schedule_interval='*/5 * * * *',  # каждый час в 15 минут
    catchup=False,
    tags=['api', 'epihovad'],
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={'data': extract_data.output},  # передаем данные напрямую
    )

extract_data >> load_data
