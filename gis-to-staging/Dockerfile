from countystats/r-geo:4.3

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN install2.r --error \
    dplyr

ADD gis_load.R /
ADD gis_penndot.R /

CMD [ "Rscript", "gis_load.R" ]