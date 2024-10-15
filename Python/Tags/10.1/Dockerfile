FROM python:3.6

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN apt-get update && apt-get install -y \
	libaio-dev \
	sudo

COPY instantclient_19_5 ./

COPY *.crt /usr/local/share/ca-certificates/

RUN apt-get --allow-releaseinfo-change update
RUN apt-get update                             \
    && apt-get install -y --no-install-recommends \
        ca-certificates wget 

RUN export ORACLE_HOME=$(pwd)/instantclient_19_5

RUN export ORACLE_HOME=/location/of/your/files/instantclient_19_5
RUN export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME

RUN sudo ldconfig

RUN apt-get update \
    && apt-get -y install \
        apt-transport-https \
        curl \
    && apt-get clean

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get -y install \
        msodbcsql17 \
        libgssapi-krb5-2 \
        unixodbc-dev \
    && apt-get clean  

COPY requirements.txt ./

RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt