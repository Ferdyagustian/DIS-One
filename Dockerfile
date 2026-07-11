# Dockerfile untuk IHSG Data Ingestion Pipeline
# Extends official Apache Airflow 2.9.2 dengan dependency yang sudah baked-in
# Menghilangkan kebutuhan _PIP_ADDITIONAL_REQUIREMENTS di runtime

FROM apache/airflow:2.9.2-python3.12

USER airflow

# Salin requirements.txt dan install dependency
# Re-install apache-airflow untuk mencegah konflik versi secara silent
COPY saham_ingestion/requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
