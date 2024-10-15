FROM rocker/tidyverse:4.3

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

COPY *.crt /usr/local/share/ca-certificates/

RUN apt-get update && apt-get install -y \
    pandoc \
    pandoc-citeproc \
    libcairo2-dev \
    libxt-dev \
    libssl-dev \
    libssh2-1-dev \
    libmpfr-dev \
    curl \
    sudo \
    libcurl4-gnutls-dev \
    freetds-dev \
    freetds-bin \
    build-essential \
    ca-certificates \
    gcc \
    git \
    libpq-dev \
    make \
    ssh \
    putty \
    software-properties-common \
    apt-transport-https \
    && apt-get autoremove \
    && apt-get clean

# system libraries of general use
RUN apt-get update && apt-get install -y \
    unixodbc \
    && apt-get autoremove \
    && apt-get clean

# Rewrite odbcinst.ini file for connectors
COPY etc_odbcinst.ini /etc/odbcinst.ini

# Add Repo and install JDK 8
RUN apt-get update -qq && apt-get -y --no-install-recommends install \
  libxml2-dev \
  libcairo2-dev \
  libpq-dev \
  libicu-dev \
  libbz2-dev \
  liblzma-dev \
  default-jdk \
  && R CMD javareconf

# install dependencies of most cron jobs
RUN install2.r --error \
    rJava \
    odbc \
    DBI \
    RJDBC \
    httr \
    jsonlite \
    RCurl \
    readxl \
    zoo \
    rlang \
    httr \
    dotenv \
    snakecase \
    plotly \
    pander \
    kableExtra \
    treemap

# Copy of java connector drivers
# Oracle
COPY ojdbc6.jar /lib/ojdbc6.jar
# AS 400
COPY jt400-jdk9-10.1.jar /lib/jt400-jdk9-10.1.jar
COPY moveitAPI_0.1.0.tar.gz /

RUN  R -e "install.packages('moveitAPI_0.1.0.tar.gz', repos = NULL, type = 'source')"

RUN apt-get update && \
    DEBIAN_FRONTEND='noninteractive' apt-get install -y --no-install-recommends \
    realmd\
    libpam-sss\
    libnss-sss\
    sssd\
    sssd-tools\
    adcli\
    krb5-user\
    libpam-krb5

RUN apt-get install -y unixodbc-dev \
  && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && sudo apt-get update \
  && sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

RUN apt-get install -y unixodbc-dev \
  && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list \
  && sudo apt-get update \
  && sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
