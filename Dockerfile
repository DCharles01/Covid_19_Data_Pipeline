
# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.6.1

# add requirements file
ADD requirements.txt .
RUN pip install -r requirements.txt

# Set the working directory inside the container
WORKDIR /opt/airflow

# Copy the project files and directories into the container
COPY dags/ /opt/airflow/dags
COPY logs/ /opt/airflow/logs
COPY config/ /opt/airflow/config
COPY plugins/ /opt/airflow/plugins
COPY pipeline_utils/ /opt/airflow/pipeline_utils
COPY src/ /opt/airflow/src
COPY tests/ /opt/airflow/tests

# Set the environment variables
ENV AIRFLOW__CORE__EXECUTOR=CeleryExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
ENV AIRFLOW__CORE__FERNET_KEY=
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
ENV AIRFLOW__CORE__LOAD_EXAMPLES=true
ENV AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session
ENV AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true
ENV PYTHONPATH=/opt/airflow

# Set the user and group inside the container
USER 50000:0

# Build the image with `docker build .` to include additional PIP requirements
# Uncomment the following line if you want to build a custom image or extend the official image
# using a Dockerfile placed in the same directory as the docker-compose.yml file

# RUN pip install <additional-requirements>

