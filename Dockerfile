# Image de base Hadoop sur laquelle on installe Apache Pig
FROM --platform=linux/amd64 bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

USER root

# 1. Correction des dépôts Debian Stretch (Archive)
RUN sed -i 's/deb.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i '/stretch-updates/d' /etc/apt/sources.list

# 2. Installation de wget et tar (Java 8 est déjà présent dans l'image de base)
RUN apt-get update -o Acquire::Check-Valid-Until=false -q && \
    apt-get install -y --force-yes wget tar && \
    apt-get clean

# 3. Installation de Apache Pig 0.17.0
RUN wget https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz && \
    tar -xzf pig-0.17.0.tar.gz -C /opt/ && \
    ln -s /opt/pig-0.17.0 /opt/pig && \
    rm pig-0.17.0.tar.gz

# 4. Configuration des variables d'environnement
ENV PIG_HOME=/opt/pig
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV PATH=$PATH:$PIG_HOME/bin
