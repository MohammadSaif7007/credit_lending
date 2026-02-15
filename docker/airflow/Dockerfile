FROM apache/airflow:2.8.1

USER root

# Install OS certificates (SSL fix)
RUN apt-get update && apt-get install -y openjdk-17-jdk curl ca-certificates && \
    update-ca-certificates

USER airflow

# Set JAVA_HOME environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set trusted certificates path for pip
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV PATH=/home/airflow/.local/bin:$PATH

# Install Python packages ignoring SSL verification
RUN pip install --user --upgrade pip --trusted-host pypi.org --trusted-host files.pythonhosted.org \
    && pip install --user pyyaml jinja2 pyspark psycopg2-binary --trusted-host pypi.org --trusted-host files.pythonhosted.org

WORKDIR /opt/airflow/app