# Kafka Client Demo

This is an example program that:

- uses Kafka Producer to send data to a kafka instance and topic
- uses Kafka Consumer to read the data from the same instance and topic
- runs both asynchronously to simulate the producer and consumer has separate instances
- runs for about 10 seconds and sends/reads each second, 
to keep it running for a longer period of time, just change the constant ```LIFECYLCE_TIMEOUT``` in ```Main.java```

## How to execute

- this is a gradle project, just import it to your favourite IDE and run it