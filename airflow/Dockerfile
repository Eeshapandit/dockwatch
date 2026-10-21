FROM apache/airflow:2.10.5-python3.12

# Pipeline libraries, installed against Airflow's own version constraints
# so we never break Airflow's dependencies.
RUN pip install --no-cache-dir "apache-airflow==2.10.5" "psycopg[binary]==3.3.6" pyarrow requests \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

# dbt gets its OWN virtual environment. dbt and Airflow pin different versions
# of shared libraries; separate venvs is the standard way to keep both happy.
USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-postgres==1.11.0 \
 && chown -R airflow: /opt/dbt-venv
USER airflow
