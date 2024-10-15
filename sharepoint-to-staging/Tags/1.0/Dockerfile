from countystats/python3:10

MAINTAINER Geoffrey Arnold "geoffrey.arnold@alleghenycounty.us"

RUN pip install oauthlib requests-oauthlib xlrd openpyxl

COPY sharepoint_to_staging.py /
COPY sharepoint_upload.py /

COPY SSL.fwd.proxy.2024.crt /usr/local/share/ca-certificates/SSL.fwd.proxy.2024.crt
#RUN chmod 644 /usr/local/share/ca-certificates/SSL.fwd.proxy.2024.crt
#RUN sudo openssl x509 -inform der -outform pem -in /usr/local/share/ca-certificates/SSL.fwd.proxy.2024.crt -out /usr/local/share/ca-certificates/SSL.fwd.proxy.2024.crt
COPY R3.der /usr/local/share/ca-certificates/R3.der
RUN chmod 644 /usr/local/share/ca-certificates/R3.der
RUN sudo openssl x509 -inform der -outform pem -in /usr/local/share/ca-certificates/R3.der -out /usr/local/share/ca-certificates/R3.crt
COPY GeoTrustRSACA2018.crt /usr/local/share/ca-certificates/GeoTrustRSACA2018.crt
RUN chmod 644 /usr/local/share/ca-certificates/GeoTrustRSACA2018.crt
#COPY DigiCertTLSRSASHA2562020CA1-1.crt /usr/local/share/ca-certificates/DigiCertTLSRSASHA2562020CA1-1.crt
RUN update-ca-certificates

RUN apt-get --allow-releaseinfo-change update
RUN apt-get update                             \
     && apt-get install -y --no-install-recommends \
        ca-certificates curl python3-certifi

ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt


CMD [ "python", "-u", "sharepoint_to_staging.py" ]