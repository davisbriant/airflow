FROM apache/airflow:3.1.7
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r requirements.txt

# Install dbt and Cosmos into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --upgrade pip && pip install --no-cache-dir dbt-core dbt-athena-community astronomer-cosmos[dbt-athena-community] && deactivate

# Set the path to the dbt executable
ENV DBT_EXECUTABLE_PATH="/usr/local/airflow/dbt_venv/bin/dbt"
