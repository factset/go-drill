FROM openjdk:8u232-jdk

RUN mkdir /opt/drill

RUN wget -O - http://apache.mirrors.hoobly.com/drill/drill-1.17.0/apache-drill-1.17.0.tar.gz | tar -xzC /opt

RUN mv /opt/apache-drill-1.17.0/* /opt/drill/

WORKDIR /opt/drill

COPY ./setup-and-run.sh /opt/drill/setup-and-run.sh
COPY ./storage-plugins-override.conf /opt/drill/conf/storage-plugins-override.conf
RUN chmod +x /opt/drill/setup-and-run.sh

ENTRYPOINT /opt/drill/setup-and-run.sh
