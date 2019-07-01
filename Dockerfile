FROM billryan/kafka:2.2.1

LABEL maintainer="yuanbin2014@gmail.com"

WORKDIR /opt/kafka

COPY */target/kafka-connect*.jar /opt/connectors/
COPY connect-distributed.properties config/

ENV KAFKA_OPTS="-XX:InitialRAMPercentage=40.0 -XX:MaxRAMPercentage=70.0" KAFKA_HEAP_OPTS=" " APP_PROFILES_ACTIVE="dev" BOOTSTRAP_SERVERS="localhost:9092"

EXPOSE 8083

ENTRYPOINT ["/sbin/tini", "--"]

CMD sed -i "s/BOOTSTRAP_SERVERS/$BOOTSTRAP_SERVERS/g" config/connect-distributed.properties && bin/connect-distributed.sh config/connect-distributed.properties
