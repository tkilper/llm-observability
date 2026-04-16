FROM flink:1.18.1-scala_2.12-java11

# Install Python 3.11 and bootstrap pip directly for it.
# python3-pip (apt) installs pip for the system default Python, not python3.11,
# so we use get-pip.py instead.
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-dev \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && update-alternatives --install /usr/bin/python  python  /usr/bin/python3.11 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
    && curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
    && python3.11 /tmp/get-pip.py --break-system-packages \
    && rm /tmp/get-pip.py

ENV PYFLINK_PYTHON=/usr/bin/python3.11

COPY requirements.txt /tmp/requirements.txt
RUN python3.11 -m pip install --no-cache-dir --break-system-packages -r /tmp/requirements.txt

# ── Connector JARs (placed in /opt/flink/lib so Flink picks them up on start)
# Kafka SQL connector (fat jar — includes all Kafka client deps)
RUN wget -q \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar" \
    -O /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar

# JDBC connector
RUN wget -q \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar" \
    -O /opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar

# PostgreSQL JDBC driver
RUN wget -q \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" \
    -O /opt/flink/lib/postgresql-42.7.3.jar
