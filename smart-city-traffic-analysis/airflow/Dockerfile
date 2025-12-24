FROM python:3.11-slim

ENV AIRFLOW_HOME=/app/airflow
ENV PYTHONPATH="${PYTHONPATH}:/app/airflow/scripts"

WORKDIR $AIRFLOW_HOME

RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    curl \
    && apt-get clean

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY dags/ dags/
COPY scripts/ scripts/
COPY data/ data/

CMD ["airflow", "standalone"]
