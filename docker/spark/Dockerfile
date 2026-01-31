# =============================================================================
# Custom Apache Spark Image with Iceberg + AWS (MinIO) Support
# =============================================================================

FROM apache/spark:3.5.0

USER root

# Install Python, curl and wget
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3 /usr/bin/python

# Install Python packages for PySpark
# Note: pyspark is already included in bitnami/spark image
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    pandas \
    pyarrow \
    boto3 \
    s3fs

# Download required JARs for Iceberg + S3/MinIO integration
ENV SPARK_HOME=/opt/spark
ENV JARS_DIR=$SPARK_HOME/jars

# Iceberg JARs
RUN curl -L -o $JARS_DIR/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# AWS SDK for S3/MinIO
RUN curl -L -o $JARS_DIR/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Hadoop AWS
RUN curl -L -o $JARS_DIR/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# URL Connection Client (required for Iceberg REST catalog)
RUN curl -L -o $JARS_DIR/iceberg-aws-1.4.3.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.3/iceberg-aws-1.4.3.jar

# Create directories
RUN mkdir -p /opt/spark-apps /opt/data

# Copy Spark configuration
COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf.template
RUN cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf

# Set environment variables
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Apache Spark image uses spark user
USER spark

WORKDIR /opt/spark-apps
