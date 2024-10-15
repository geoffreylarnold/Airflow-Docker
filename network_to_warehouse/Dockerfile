FROM countystats/r-basic:latest

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

RUN install2.r --error \
    dplyr \
    janitor \
    openxlsx \
    data.table

COPY excel_to_staging.R /
COPY network_to_warehouse.sh /
COPY warehouse_to_network.R /
COPY warehouse_to_network.sh /
