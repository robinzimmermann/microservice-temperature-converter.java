# Temperature Converter Microservice

A microservice that receives temperature readings from a Kafka topic, converts them to the target temperature unit, and publishes the result to another Kafka topic.

The service also subscribes to a command topic to remote control the service and designate the target temperature unit (**C**elsius, **F**ahrenheit, or **K**elvin).

## Prerequisites

- Java 1.8 or higher

- A Kafka broker or Kafka broker cluster that the microservice can connect to, with the following topics pre-created:

  | topic                | cleanup.policy                |
  | -------------------- | ----------------------------- |
  | `sensors`            | `compact` or `compact,delete` |
  | `readings`           | `compact` or `compact,delete` |
  | `readings.requests`  | `delete`                      |
  | `readings.responses` | `delete`                      |
  | `state`              | `compact` or `compact,delete` |
  | `control`            | `delete`                      |

  Note that the `sensors`, `readings`, and `state` topics are log compacted.

## Build

```
$ ./mvnw clean install
```

## Run

The microservice will connect to your Kafka to publish and subscribe. To start the microservice, run the following command.

```
$ java -jar target/microservice-temperature-converter-develop-SNAPSHOT.jar \
    -b kafka.example.com:9092
```

If port `9093` or `9094` is specified, the connection will use SSL. All other ports will default to PLAINTEXT. This default behavior can be overridden with the `--protocol` parameter.

Additional Kafka consumer and producer properties can be specified if needed:

```
$ java -jar target/microservice-temperature-converter-develop-SNAPSHOT.jar \
    -b kafka.example.com:9092 \
    --kafka-consumer-property session.timeout.ms=5000 \
    --kafka-consumer-property ... \
    --kafka-producer-property batch.size=16384 \
    --kafka-producer-property ...
```

See all options:

```
$ java -jar target/microservice-temperature-converter-develop-SNAPSHOT.jar --help
```

## Topic details

This section describes the Kafka topic usage of the microservice.

### Receiving original sensor readings

The microservice receives sensor readings in order to convert the temperatures and re-publish them (see [Publishing converted sensor readings](publishing-converted-sensor-readings#))

- **topic:** `sensors` (specified with `--input-topic`)
- **headers:**
  | name | value |
  | ---- | ----: |
  | row | 9 |
- **payload:**
  ```json
  { "id": "4", "unit": "C", "value": 100 }
  ```

### Publishing converted sensor readings

After receiving sensor readings (see [Receiving original sensor readings](#receiving-original-sensor-readings)), the temperature is converted to the target temperature unit and re-published.

- **topic:** `readings` (specified with `--output-topic`)
- **headers:**
  | name | value |
  | ---- | ----: |
  | row | 9 |
- **payload:**
  ```json
  { "id": "4", "unit": "F", "value": 212 }
  ```

### Receiving control messages

The microservice can be controlled to changed the target temperature unit. Upon receiving a control message the microservice will change its behavior accordingly, and send a response as a confirmation. The response topic used will be that from the `$http.replyTo` header, and the response message will include a `$http.correlationId` header with the same value as that received.

- **topic:** `readings.requests` (specified with `--requests-topic`)
- **headers:**
  | name | value |
  | ---- | ----: |
  | $http.replyTo | readings.responses |
  | $http.correlationId | 123 |
- **payload:**
  ```json
  { "unit": "F" }
  ```
