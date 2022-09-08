# Apache Pulsar Support for Java Applications

Framework for integrating java applications with `Apache Pulsar`.

While this is still an experimental project, it's a fully functional, high-throughput, robust messaging framework.
Users are encouraged to try it out and provide feedback.

This project is not currently published to `Maven Central` - code can be checked out and built locally.

### Parallel Processing

While more `Consumers` can easily be added to a topic via the `concurrency` setting, this framework also allows parallel processing based on key in a single or multiple consumers. In this mode, messages from the same key are processed sequentially, while being processed in parallel in relation to other keys.

This framework also supports full fan-out of messages.

### High-Throughput / Fully Non-Blocking

Leaning on Apache Pulsar's `CompletableFuture` API, this framework is fully non-blocking and designed for high-throughput.

It supports both regular `blocking` and `non-blocking` components such as `MessageListener`, `ErrorHandler`, `MessageInterceptor` and `AcknowledgementResultCallback`.

All polling and acknowledgement actions are `non-blocking`, and blocking components are seamlessly adapted so no async complexity is required from the user (though it's encouraged at least for simple components).

This means application's resources are fully available for user logic, resulting in less costs and environmental impact.

### Features:
* `@PulsarListener` annotation with `SpEL` and property placeholder resolution
* Automatic `Schema` detection from listener methods
* Supports `blocking` and `async` components
* `Batch` message processing
* `ErrorHandler` with message recovery support
* `MessageInterceptor` with callbacks before and after processing
* Accepts flexible listener arguments such as `Acknowledgement`, `BatchAcknowledgement`, `Consumer`, `MessageId`, `@Header`, Spring's `Message`, original Pulsar's `Message`, and any user-provided resolvers
* `Header Mapping`
* Manual `Factory` and `Container` creation and `lifecycle` management to use without annotations
* Acknowledgement result callbacks
* MANUAL, ON_SUCCESS and ALWAYS acknowledgement modes
* `@EnablePulsar` for quick setup (autoconfiguration coming later)
* ... and much more!

### Example of `Spring Boot` application:

```java
@SpringBootApplication
public class ApachePulsarSpringApplication {

    private static final Logger logger = LoggerFactory.getLogger(ApachePulsarSpringApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KafkaParallelDemoApplication.class, args);
    }

    @PulsarListener(topics = "${my.topic}", id = "my-container",
            subscription = "my-subscription", concurrency = "5")
    void listen(MyPojo message) {
        logger.info("Received message {}", message);
    }

    @PulsarListener(topics = "${my.batch.topic}", id = "my-batch-container", factory = "parallelPulsarContainerFactory")
    CompletableFuture<Void> listen(List<Message<String>> messages) {
        return CompletableFuture
                .completedFuture(messages)
                .thenAccept(msgs -> logger.info("Received {} messages", msgs.size()));
    }

    @EnablePulsar
    @Configuration
    static class PulsarConfiguration {

        @Bean
        MessageListenerContainerFactory<Object> defaultPulsarContainerFactory(ConsumerFactory<Object> consumerFactory) {
            return PulsarMessageListenerContainerFactory
                    .createWith(consumerFactory);
        }

        @Bean
        PulsarMessageListenerContainerFactory<Object> parallelPulsarContainerFactory(ConsumerFactory<Object> consumerFactory) {
            return PulsarMessageListenerContainerFactory
                    .createWith(consumerFactory)
                    .configure(options -> options
                            .processingOrdering(ProcessingOrdering.PARALLEL_BY_KEY)
                            .subscriptionType(SubscriptionType.Key_Shared)
                            .concurrency(4)
                            .acknowledgementMode(AcknowledgementMode.MANUAL)
                            .acknowledgementResultCallback(getManualAckResultCallback()));
        }

        @Bean
        ConsumerFactory<Object> consumerFactory(PulsarClientFactory clientFactory) {
            return DefaultPulsarConsumerFactory
                    .createWith(clientFactory)
                    .configure(options -> options
                            .batchReceivePolicy(BatchReceivePolicy
                                    .builder()
                                    .maxNumMessages(10)
                                    .timeout(1, TimeUnit.SECONDS)
                                    .build())
                            .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS));
        }

        @Bean
        PulsarClientFactory clientFactory() {
            return DefaultPulsarClientFactory
                    .create()
                    .configure(options -> options
                            .serviceUrl("http://localhost:6650"));
        }

    }
}
```

Improvements coming soon include:
* `Template` class for sending messages
* `@SendTo`
* `@ReplyTo`
* `Project Reactor` support
