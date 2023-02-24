FROM apache/airflow:2.5.1

RUN pip install apache-airflow-providers-apache-spark

USER root

###############################
## JAVA installation
###############################
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-get install -y procps && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \ 
    apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    java -version && \
    javac -version

# Setup JAVA_HOME 
RUN export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

###############################
## SPARK files and variables
###############################
ENV SPARK_HOME /usr/local/spark
ENV SPARK_VERSION 3.3.2

# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
    curl -o "spark-${SPARK_VERSION}-bin-hadoop3.tgz" "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    mkdir -p "${SPARK_HOME}/bin" && \
    mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
    cp -a "spark-${SPARK_VERSION}-bin-hadoop3/bin/." "${SPARK_HOME}/bin/" && \
    cp -a "spark-${SPARK_VERSION}-bin-hadoop3/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop3.tgz" && \
    rm -rf "spark-${SPARK_VERSION}-bin-hadoop3/"

# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin

###############################
## Install MongoDB Tools
###############################
RUN cd "/tmp" && \
    curl -o "mongodb-database-tools-ubuntu1604-arm64-100.6.1.deb" "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu1604-arm64-100.6.1.deb" && \
    apt-get install "./mongodb-database-tools-ubuntu1604-arm64-100.6.1.deb" && \
    rm "./mongodb-database-tools-ubuntu1604-arm64-100.6.1.deb"

USER airflow