FROM quay.io/astronomer/astro-runtime:12.7.1

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate