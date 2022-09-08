/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.tomazfernandes;

import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementResultCallback;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig
class PulsarIntegrationTests extends PulsarBaseIntegrationTests {

    private static final Logger logger = LoggerFactory.getLogger(PulsarIntegrationTests.class);

    private static final String RECEIVES_MESSAGE_TOPIC_NAME = "receives.message.topic";

    private static final String RECEIVES_MESSAGE_TWO_TOPICS_TOPIC_NAME_1 = "receives.message.two.topics.1.topic";

    private static final String RECEIVES_MESSAGE_TWO_TOPICS_TOPIC_NAME_2 = "receives.message.two.topics.2.topic";

    private static final String RECEIVES_MESSAGE_TWO_CONSUMERS_TOPIC_NAME = "receives.message.two.consumers.topic";

    private static final String RECEIVES_MESSAGE_BATCH_TOPIC_NAME = "receives.message.batch.topic";

    private static final String RETRIES_ON_ERROR_TOPIC_NAME = "retries.on.error.topic";

    private static final String PROCESSES_POJO_TOPIC_NAME = "processes.pojo.topic";
    
    private static final String ACKNOWLEDGES_MANUALLY_TOPIC_NAME = "acknowledges.manually.topic";

    @Autowired
    private PulsarClientFactory pulsarClientFactory;

    @Autowired
    private LatchContainer latchContainer;

    @Test
    void shouldReceiveMessage() throws Exception {
        this.pulsarClientFactory.createClient()
                .newProducer(Schema.STRING).topic(RECEIVES_MESSAGE_TOPIC_NAME).create().send("receivesMessage test payload");
        assertThat(latchContainer.receivesMessageLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(latchContainer.receivesMessageAcknowledgementLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldReceiveMessageFromTwoTopics() throws Exception {
        this.pulsarClientFactory.createClient()
                .newProducer(Schema.STRING).topic(RECEIVES_MESSAGE_TWO_TOPICS_TOPIC_NAME_1).create().send("shouldReceiveMessageFromTwoTopics test payload 1");
        this.pulsarClientFactory.createClient()
                .newProducer(Schema.STRING).topic(RECEIVES_MESSAGE_TWO_TOPICS_TOPIC_NAME_2).create().send("shouldReceiveMessageFromTwoTopics test payload 2");
        assertThat(latchContainer.receivesMessageTwoTopicsLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldAcknowledgeManually() throws Exception {
        this.pulsarClientFactory.createClient()
                .newProducer(Schema.STRING).topic(ACKNOWLEDGES_MANUALLY_TOPIC_NAME).create().send("shouldAcknowledgeManually test payload");
        assertThat(latchContainer.manualAckAcknowledgementLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldReceiveMessageInDifferentConsumers() throws Exception {
        Producer<String> producer = this.pulsarClientFactory.createClient()
                .newProducer(Schema.STRING).topic(RECEIVES_MESSAGE_TWO_CONSUMERS_TOPIC_NAME)
                .create();
        producer.newMessage().key("one").value("receivesMessageTwoConsumers test payload one").send();
        producer.newMessage().key("two").value("receivesMessageTwoConsumers test payload two").send();
        assertThat(latchContainer.receivesMessageLatchTwoConsumers.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldReceiveMessageBatch() throws Exception {
        Producer<String> producer = this.pulsarClientFactory.createClient()
                .newProducer(Schema.STRING).topic(RECEIVES_MESSAGE_BATCH_TOPIC_NAME).create();
        IntStream.range(0, 10).forEach(index -> trySend(producer, index));
        assertThat(latchContainer.receivesMessageBatchLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(latchContainer.batchManualAckAcknowledgementLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private MessageId trySend(Producer<String> producer, int index) {
        try {
            return producer.send("receivesMessageBatch test payload - " + index);
        }
        catch (Exception e) {
            throw new RuntimeException("Error sending message", e);
        }
    }

    @Test
    void shouldRetryMessage() throws Exception {
        this.pulsarClientFactory.createClient()
                .newProducer(Schema.STRING).topic(RETRIES_ON_ERROR_TOPIC_NAME).create().send("retriesOnError test payload");
        assertThat(latchContainer.retriesOnErrorLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldProcessPojo() throws Exception {
        this.pulsarClientFactory.createClient()
                .newProducer(Schema.JSON(MyPojo.class)).topic(PROCESSES_POJO_TOPIC_NAME).create().send(new MyPojo("shouldProcessPojoField", "otherValue"));
        assertThat(latchContainer.processesPojoLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void shouldCreateContainerManually() throws Exception {
        String topicName = "creates.container.manually.topic";
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerFactory<String> consumerFactory = DefaultPulsarConsumerFactory
                .<String>createWith(this.pulsarClientFactory)
                .configure(options -> options
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest));
        PulsarMessageListenerContainer<String> container =
                PulsarMessageListenerContainer
                        .createWith(consumerFactory)
                        .configure(options -> options
                                .id("creates-container-manually-container")
                                .topics(topicName)
                                .subscriptionName("creates-container-manually-subscription")
                                .schema(Schema.STRING)
                                .messageListener(msg -> latch.countDown()));
        this.pulsarClientFactory.createClient()
                .newProducer(Schema.STRING)
                .topic(topicName)
                .create()
                .send("retriesOnError test payload");
        container.start();
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        container.stop();
    }

    @Test
    void shouldCreateContainerFromFactory() throws Exception {
        String topicName = "creates.container.from.factory.topic";
        this.pulsarClientFactory.createClient()
                .newProducer(Schema.STRING).topic(topicName).create().send("retriesOnError test payload");
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerFactory<String> consumerFactory = DefaultPulsarConsumerFactory
                .<String>createWith(this.pulsarClientFactory)
                .configure(options -> options
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest));
        PulsarMessageListenerContainer<String> container = PulsarMessageListenerContainerFactory
                .createWith(consumerFactory)
                .configure(options -> options
                        .subscriptionName("my-subscription-name")
                        .subscriptionType(SubscriptionType.Shared)
                        .schemaFactory(clazz -> Schema.STRING)
                        .messageListener(msg -> latch.countDown()))
                .createContainer(topicName);
        container.start();
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        container.stop();
    }

    static class LatchContainer {

        CountDownLatch receivesMessageLatchTwoConsumers = new CountDownLatch(2);
        CountDownLatch receivesMessageLatch = new CountDownLatch(1);
        CountDownLatch receivesMessageTwoTopicsLatch = new CountDownLatch(2);
        CountDownLatch receivesMessageAcknowledgementLatch = new CountDownLatch(1);
        CountDownLatch manualAckAcknowledgementLatch = new CountDownLatch(1);
        CountDownLatch batchManualAckAcknowledgementLatch = new CountDownLatch(10);
        CountDownLatch receivesMessageBatchLatch = new CountDownLatch(10);
        CountDownLatch retriesOnErrorLatch = new CountDownLatch(1);
        CountDownLatch processesPojoLatch = new CountDownLatch(1);

    }

    static class ReceiveMessageListener {

        @Autowired
        private LatchContainer latchContainer;

        @PulsarListener(topics = RECEIVES_MESSAGE_TOPIC_NAME, subscriptionName = "receives-message-subscription", id = "receives-message-container")
        void listen(String message) {
            logger.debug("Received message {}", message);
            latchContainer.receivesMessageLatch.countDown();
        }

    }

    static class ReceiveMessageTwoTopicsListener {

        @Autowired
        private LatchContainer latchContainer;

        @PulsarListener(topics = {RECEIVES_MESSAGE_TWO_TOPICS_TOPIC_NAME_1, RECEIVES_MESSAGE_TWO_TOPICS_TOPIC_NAME_2},
                subscriptionName = "receives-message-two-topics-subscription",
                id = "receives-message-two-topics-container")
        void listen(Message<String> message) {
            logger.debug("Received message {}", message);
            latchContainer.receivesMessageTwoTopicsLatch.countDown();
        }

    }

    static class ReceiveMessageTwoConsumersListener {

        private final Set<Consumer<String>> consumers = Collections.synchronizedSet(new HashSet<>());

        @Autowired
        private LatchContainer latchContainer;

        @PulsarListener(topics = RECEIVES_MESSAGE_TWO_CONSUMERS_TOPIC_NAME,
                subscriptionName = "receives-message-two-consumers-subscription",
                id = "receives-message-two-consumers-container", concurrency = "2")
        void listen(org.apache.pulsar.client.api.Message<String> message, Consumer<String> consumer) {
            logger.debug("Received message {} in consumer {}", message, consumer.getConsumerName());
            if (!this.consumers.contains(consumer)) {
                this.consumers.add(consumer);
                latchContainer.receivesMessageLatchTwoConsumers.countDown();
            }
        }

    }

    static class ProcessesPojoListener {

        @Autowired
        private LatchContainer latchContainer;

        @PulsarListener(topics = PROCESSES_POJO_TOPIC_NAME, subscriptionName = "processes-pojo-subscription",
                id = "processes-pojo-container")
        void listen(Message<MyPojo> message) {
            logger.debug("Received message {}", message);
            latchContainer.processesPojoLatch.countDown();
        }

    }

    static class AcknowledgesManuallyListener {

        @PulsarListener(topics = ACKNOWLEDGES_MANUALLY_TOPIC_NAME, subscriptionName = "acknowledges-manually-subscription",
                id = "acknowledges-manually-container", factory = "manualAckPulsarContainerFactory")
        CompletableFuture<Void> listen(String message, Acknowledgement acknowledgement) {
            logger.debug("Received message {}", message);
            return acknowledgement.acknowledgeAsync();
        }

    }

    static class ReceiveMessageBatchListener {

        @Autowired
        private LatchContainer latchContainer;

        @PulsarListener(topics = RECEIVES_MESSAGE_BATCH_TOPIC_NAME,
                factory = "manualAckPulsarContainerFactory",
                subscriptionName = "receives-message-batch-subscription",
                id = "receives-message-batch-container")
        CompletableFuture<Void> listen(List<Message<String>> messages, BatchAcknowledgement<String> batchAcknowledgement) {
            logger.debug("Received {} messages", messages);
            messages.forEach(msg -> latchContainer.receivesMessageBatchLatch.countDown());
            return batchAcknowledgement.acknowledgeAsync();
        }

    }

    static class RetryOnErrorListener {

        @Autowired
        private LatchContainer latchContainer;

        private final AtomicBoolean hasThrown = new AtomicBoolean();

        @PulsarListener(topics = RETRIES_ON_ERROR_TOPIC_NAME, subscriptionName = "retry-on-error-subscription", id = "retries-on-error-container")
        void listen(String message) {
            logger.debug("Received message {}", message);
            if (hasThrown.compareAndSet(false, true)) {
                throw new RuntimeException("Expected exception from retryOnErrorListener");
            }
            else {
                latchContainer.retriesOnErrorLatch.countDown();
            }
        }

    }

    @EnablePulsar
    @Configuration
    static class PulsarConfiguration {

        @Bean
        ReceiveMessageListener receiveMessageListener() {
            return new ReceiveMessageListener();
        }

        @Bean
        ReceiveMessageTwoTopicsListener receiveMessageTwoTopicsListener() {
            return new ReceiveMessageTwoTopicsListener();
        }

        @Bean
        ReceiveMessageTwoConsumersListener receiveMessageTwoConsumersListener() {
            return new ReceiveMessageTwoConsumersListener();
        }

        @Bean
        ReceiveMessageBatchListener receiveMessageBatchListener() {
            return new ReceiveMessageBatchListener();
        }

        @Bean
        RetryOnErrorListener retryOnErrorListener() {
            return new RetryOnErrorListener();
        }

        @Bean
        ProcessesPojoListener processesPojoListener() {
            return new ProcessesPojoListener();
        }

        @Bean
        AcknowledgesManuallyListener acknowledgesManuallyListener() {
            return new AcknowledgesManuallyListener();
        }

        LatchContainer latchContainer = new LatchContainer();

        @Bean
        LatchContainer latchContainer() {
            return this.latchContainer;
        }

        @Bean
        PulsarMessageListenerContainerFactory<Object> defaultPulsarContainerFactory(ConsumerFactory<Object> consumerFactory) {
            return PulsarMessageListenerContainerFactory
                    .createWith(consumerFactory)
                    .configure(options -> options
                            .failureHandlingMode(FailureHandlingMode.NACK)
                            .acknowledgementResultCallback(getDefaultAcknowledgementResultCallback()));
        }

        @NotNull
        private AcknowledgementResultCallback<Object> getDefaultAcknowledgementResultCallback() {
            return new AcknowledgementResultCallback<Object>() {
                @Override
                public void onSuccess(Collection<Message<Object>> messages) {
                    latchContainer.receivesMessageAcknowledgementLatch.countDown();
                }
            };
        }

        @Bean
        PulsarMessageListenerContainerFactory<Object> manualAckPulsarContainerFactory(ConsumerFactory<Object> consumerFactory) {
            return PulsarMessageListenerContainerFactory
                    .createWith(consumerFactory)
                    .configure(options -> options
                            .processingOrdering(ProcessingOrdering.PARALLEL_BY_KEY)
                            .subscriptionType(SubscriptionType.Key_Shared)
                            .concurrency(4)
                            .acknowledgementMode(AcknowledgementMode.MANUAL)
                            .acknowledgementResultCallback(getManualAckResultCallback()));
        }

        private AcknowledgementResultCallback<Object> getManualAckResultCallback() {
            return new AcknowledgementResultCallback<Object>() {
                @Override
                public void onSuccess(Collection<Message<Object>> messages) {
                    if (MessageHeaderUtils.getHeaderAsString(messages.iterator().next(),
                            PulsarHeaders.PULSAR_TOPIC_NAME_HEADER).contains(RECEIVES_MESSAGE_BATCH_TOPIC_NAME)) {
                        messages.forEach(msg -> latchContainer.batchManualAckAcknowledgementLatch.countDown());
                    } else {
                        latchContainer.manualAckAcknowledgementLatch.countDown();
                    }
                }
            };
        }

        @Bean
        PulsarClientFactory clientFactory() {
            return DefaultPulsarClientFactory
                    .create()
                    .configure(options -> options
                            .serviceUrl(PulsarIntegrationTests.pulsar.getPulsarBrokerUrl()));
        }

        @Bean
        ConsumerFactory<Object> consumerFactory(PulsarClientFactory clientFactory) {
            return DefaultPulsarConsumerFactory
                    .createWith(clientFactory)
                    .configure(options -> options
                            .batchReceivePolicy(BatchReceivePolicy.builder()
                                    .maxNumMessages(10)
                                    .timeout(1, TimeUnit.SECONDS).build())
                            .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS));
        }

    }

    static class MyPojo {

        private String myField;

        private String myOtherField;

        public MyPojo(String myField, String myOtherField) {
            this.myField = myField;
            this.myOtherField = myOtherField;
        }

        public MyPojo(){}

        public String getMyField() {
            return myField;
        }

        public void setMyField(String myField) {
            this.myField = myField;
        }

        public String getMyOtherField() {
            return myOtherField;
        }

        public void setMyOtherField(String myOtherField) {
            this.myOtherField = myOtherField;
        }

        @Override
        public String toString() {
            return "MyPojo{" +
                    "myField='" + myField + '\'' +
                    ", myOtherField='" + myOtherField + '\'' +
                    '}';
        }
    }
}
