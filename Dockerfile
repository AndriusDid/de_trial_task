FROM apache/airflow:2.11.0

USER root

# Install system dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Ensure setuptools is up to date (needed for pkg_resources on Python 3.12+)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy project package and make it importable
COPY mediatech_de_trial_task /opt/airflow/mediatech_de_trial_task
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
