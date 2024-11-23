FROM confluentinc/cp-kafka-connect:7.6.0

COPY ./target/WebhookSourceConnector-1.0-SNAPSHOT.jar /usr/share/java/kafka/WebhookSourceConnector-1.0.jar