from countystats/r-basic:4.2

MAINTAINER Daniel Andrus "daniel.andrus@alleghenycounty.us"

RUN install2.r --error \
   	dplyr \
	DBI \
	writexl

RUN fc-cache -fv


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
        ca-certificates curl python3-certifi python3-pip

RUN pip3 install pandas \
    oauthlib \
    requests_oauthlib \
    SQLAlchemy \
    pantab \
    pyodbc

ADD DDW_SPARQL_Query.R /
ADD DDW_Upload.R /
ADD Send_DDW_Email.py /
ADD send_email.py /
ADD Unfilled_Metadata_Email /Unfilled_Metadata_Email
ADD DDW_SQL_Query.R /