# Introduction

Kafka Source Connector for producing webhook data into Kafka.

## Features

- Creates HTTP server based on netty for listening to webhook requests
- Can validate incoming webhook requests with custom validation logic
- Determine Kafka topic based on a configurable request header
- Determine Kafka key based on a configurable request header
- Configurable DLQ if kafka topic header is not found
- Sanitizes topic name from header by replacing illegal characters with underscores(_)

## Configuration

| configuration | description | type | default | example |
| --- | --- | --- | --- | --- |
| poll.interval | Poll interval for producing messages to Kafka | long | 5000 (5s) | 5000 |
| port | Port for starting the HTTP server for webhook requests | int | - (Required) | 8000 |
| topic.header | Header for determining the topic | string | - (Required) | X-Topic-Name |
| key.header | Header for determining the key | string | - (Required) | X-Key-Name |
| topic.default | Default topic to write to for DLQ | string | - (Required) | webhook_default |
| validator.class | Validator Class for webhook request validation. Should be present in the classpath. The default validator returns true for all requests. For using the default validator, set the value to an empty string("") | string | - (Required) | com.platformatory.kafka.connect.ShopifyRequestValidator |

### Request Validator

Any custom request validator should implement the `com.platformatory.kafka.connect.Validator` interface, which has an abstract method with the following signature - 
```java
public boolean validate(FullHttpRequest request);
```

## Development

- The repository contains a docker-compose file which can be used to bring up a Kafka container along with a connect worker.

```bash
docker-compose up -d
```

> This mounts the `WebhookSourceConnector-1.0-SNAPSHOT.jar` from the target directory to the plugin path of the connect worker

- Configure the connector 

```bash
curl --location --request POST 'localhost:8083/connectors/' \
--header 'Content-Type: application/json' \
--data-raw '{
  "name": "WebhookSourceConnector",
    "config": {
        "connector.class": "com.platformatory.kafka.connect.WebhookSourceConnector",
        "tasks.max":1,
        "topic.default":"webhook",
        "topic.header":"X-Topic-Name",
        "key.header": "X-Key-Name",
        "validator.class":"",
        "port":8000
         }
}'
```

- Test the connector

```bash
# Using the DLQ with no X-Kafka-Topic header or X-Key-Name
curl -X POST --header 'Content-Type: application/json' localhost:6987 --data-raw '{
 "root": {
  "child": "value0",
  "arr": ["val0", "val1", "val2"]
 }
}'

curl -X POST -H 'Content-Type: application/json' \
  -H 'X-Topic-Name: hello/world' \
  -H 'X-Key-Name: hello' \
  localhost:6987 --data-raw \
'{
 "hello": "world"
}'
```

- Verify kafka messages

```bash
docker exec -it kafka-broker bash
[appuser@kafka ~] kafka-topics --bootstrap-server localhost:9092 --list
[appuser@kafka ~] kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning --property print.key=true --topic webhook
[appuser@kafka ~] kafka-console-consumer --bootstrap-server localhost:9092 --from-beginning --property print.key=true --topic hello_world
```

> For development, any changes can be reloaded with
```bash
mvn clean package && \
sleep 5 && \
docker-compose up -d --force-recreate kafka-connect && \
docker logs kafka-connect -f
```
