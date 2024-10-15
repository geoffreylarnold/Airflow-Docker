FROM countystats/r-basic:4.2.1

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN install2.r --error \
    plyr

COPY alco_geocoder.R /

COPY warehouse_geocode.R /

CMD [ "Rscript", "warehouse_geocode.R" ]