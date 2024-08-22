FROM apache/airflow:2.9.3-python3.11

USER airflow

COPY requirements.txt requirements.txt

# установка python пакетов
RUN python -m pip install --no-warn-script-location --no-cache-dir -r requirements.txt
