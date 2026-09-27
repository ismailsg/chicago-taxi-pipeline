FROM python:3.12-slim-bookworm

WORKDIR /opt/dagster/app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

# Résout dynamiquement le vrai chemin Java (le nom du dossier diffère
# selon l'architecture : amd64 vs arm64), et crée un symlink stable.
RUN JAVA_REAL_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    ln -sfn "$JAVA_REAL_HOME" /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME

WORKDIR /opt/dagster/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Jars hadoop-aws / aws-sdk pour que Spark parle au stockage S3 (S3A)
ENV SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark
RUN mkdir -p $SPARK_HOME/jars && \
    curl -sL -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sL -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY dagster.yaml $DAGSTER_HOME/dagster.yaml
COPY . .

EXPOSE 3000
