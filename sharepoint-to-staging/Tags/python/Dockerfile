from countystats/python3:10

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN pip install oauthlib requests-oauthlib xlrd openpyxl

COPY sharepoint_to_staging.py /

CMD [ "python", "-u", "sharepoint_to_staging.py" ]