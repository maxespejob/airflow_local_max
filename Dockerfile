FROM quay.io/astronomer/astro-runtime:12.2.0

# Set environment variables
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled

# Install any additional dependencies
RUN pip install --no-cache-dir apache-airflow-providers-microsoft-mssql