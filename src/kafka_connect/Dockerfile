FROM debezium/connect:2.7.3.Final

USER root

# Install unzip
RUN microdnf update -y && \
    microdnf install -y unzip && \
    microdnf clean all

# Copy the plugins directory
COPY ./confluentinc-kafka-connect-avro-converter-8.1.1.zip /tmp/confluentinc-kafka-connect-avro-converter-8.1.1.zip

# Unzip the plugin
RUN unzip /tmp/confluentinc-kafka-connect-avro-converter-8.1.1.zip -d /kafka/connect/ && \
    rm /tmp/confluentinc-kafka-connect-avro-converter-8.1.1.zip

USER kafka