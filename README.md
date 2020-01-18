# Kafka Scheduler

Builds upon the kafka messaging system to create a scheduler.

Each scheduler is built using several internal topics with hold semantics for each message. For a message to be delivered
it must be routed through these internal topics to be finally delivered to the client topic after the timeout expires.

## Usage

To enqueue a message for delivery we can use
```java
Instant timeToDeliver = Instant.now().plusSeconds(60);
String payload = "payload";
scheduler.enqueue(timeToDeliver, new ClientMessage("string client", "final-topic", payload.getBytes()));
```

The message will be delivered never before the `timeToDeliver` instant, but there are no guarantees over the exact
timing. Actual delivery delays depend on factors like service pressure, network latency and the finer granularity
topic available. Because we can't loose messages, we also can't process them out of order and every message must
respect the hold time associated with every topic.

The client application can expect a message to be enqueued on `final-topic` topic with an internal id and the bytes
of the `payload` string.

### Create a scheduler

To create a scheduler the [SchedulerBuilder](/builder/src/main/java/pt/pak3nuh/messaging/kafka/scheduler/SchedulerBuilder.java)
should be used. Several schedulers can be active given that they have a different **appName** so that the topics don't
collide.

```java
Scheduler scheduler = new SchedulerBuilder(getBrokers())
    .addScheduleMinutes(1)
    .addScheduleHours(1)
    .build();
```

Once created it can be started, and all the background resources are assembled.

### Error handling

A custom error handler can be provided so that client applications can react to messaging failures. The default error
handler only logs failures. **Messages with error aren't retried**. 

More advanced error handling is planned, but not yet available. Will depend on what is requested. 

#### At least once semantics

I'm aiming at **at least once** semantics for the delivery system, ensuring every message is delivered.

Like every Kafka application, it is very difficult to ensure exactly one semantics. There are are some statefull
 objects (mainly kafka) and intricate moving parts to manage. I'll try to avoid this, but I can't guarantee it.
Nevertheless, if receiving duplicate messages is a problem, the final topic consumers should expect them.

## Roadmap

- Message cancellation
- Destination partition control
- Reuse the same thread for multiple internal topics
- Handle internal consumer errors
- Custom consumer and producer