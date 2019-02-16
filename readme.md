# Kafka Asynchronous Client Demo

This is an example program that:

- uses Kafka Producer to send data to a kafka instance and topic
- uses Kafka Consumer to read the data from the same instance and topic
- runs both asynchronously to simulate the producer and consumer has separate instances
running at the same time
- runs for about 10 seconds and sends/reads each second, 
to keep it running for a longer period of time, just change the constant ```LIFECYLCE_TIMEOUT``` in ```KafkaAdmin.kt```
- encapsulates the commit operation in a retry policy, this avoids the automatic commit,
which is important if you want to assure that a commit is only processed after you successfully 
processed the received data from a consumer.

## How to execute

    gradle clean build
    gradle run
