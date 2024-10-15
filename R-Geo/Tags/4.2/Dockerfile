FROM countystats/r-basic:4.2

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

# system libraries of general use
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    lbzip2 \
    libfftw3-dev \
    libgdal-dev \
    libgeos-dev \
    libgsl0-dev \
    libgl1-mesa-dev \
    libglu1-mesa-dev \
    libhdf4-alt-dev \
    libhdf5-dev \
    libjq-dev \
    libpq-dev \
    libproj-dev \
    libprotobuf-dev \
    libnetcdf-dev \
    libsqlite3-dev \
    libudunits2-dev \
    libv8-dev \
    netcdf-bin \
    postgis \
    protobuf-compiler \
    sqlite3 \
    tk-dev 


# install dependencies of most cron jobs
RUN install2.r --error \
    maptools \
    rgdal \
    rgeos \
    sp \
    sf \
    lwgeom \
    raster \
    geoR \
    geojsonio \
    geosphere \
    tidycensus \
    ggmap \
    leaflet