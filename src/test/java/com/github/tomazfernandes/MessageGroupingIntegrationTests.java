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
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
@SpringJUnitConfig
public class MessageGroupingIntegrationTests extends PulsarBaseIntegrationTests {

    private final static Logger logger = LoggerFactory.getLogger(MessageGroupingIntegrationTests.class);

    @Autowired
    private PulsarClientFactory clientFactory;

    @Test
    void receivesMessagesInOrderWithSharedSubscription() throws Exception {
        receivesMessagesInOrderFromManyMessageKeys(SubscriptionType.Shared, ProcessingOrdering.SEQUENTIAL, 1);
    }

    @Test
    void receivesMessagesInOrderWithKeySharedSubscription() throws Exception {
        receivesMessagesInOrderFromManyMessageKeys(SubscriptionType.Key_Shared, ProcessingOrdering.SEQUENTIAL, 4);
    }

    @Test
    void receivesMessagesInOrderWithExclusiveSubscription() throws Exception {
        receivesMessagesInOrderFromManyMessageKeys(SubscriptionType.Exclusive, ProcessingOrdering.SEQUENTIAL, 1);
    }

    @Test
    void receivesMessagesInOrderWithFailOverSubscription() throws Exception {
        receivesMessagesInOrderFromManyMessageKeys(SubscriptionType.Failover, ProcessingOrdering.SEQUENTIAL, 4);
    }

    @Test
    void receivesMessagesInOrderFromManyMessageKeysWithSharedSubscription() throws Exception {
        receivesMessagesInOrderFromManyMessageKeys(SubscriptionType.Shared, ProcessingOrdering.PARALLEL_BY_KEY, 1);
    }

    @Test
    void receivesMessagesInOrderFromManyMessageKeysWithKeySharedSubscription() throws Exception {
        receivesMessagesInOrderFromManyMessageKeys(SubscriptionType.Key_Shared, ProcessingOrdering.PARALLEL_BY_KEY, 4);
    }

    @Test
    void receivesMessagesInParallelFromManyMessageKeysWithSharedSubscription() throws Exception {
        receivesMessagesInOrderFromManyMessageKeys(SubscriptionType.Shared, ProcessingOrdering.PARALLEL, 1);
    }

    @Test
    void receivesMessagesInParallelFromManyMessageKeysWithKeySharedSubscription() throws Exception {
        receivesMessagesInOrderFromManyMessageKeys(SubscriptionType.Key_Shared, ProcessingOrdering.PARALLEL, 4);
    }

    @SuppressWarnings("unchecked")
    private void receivesMessagesInOrderFromManyMessageKeys(SubscriptionType subscriptionType, ProcessingOrdering ordering, int concurrency) throws Exception {
        UUID uuid = UUID.randomUUID();
        String topic = "receives.messages.in.order.topic." + uuid;
        String subscriptionName = "receives.messages.in.order.subscription-" + uuid;
        int numberOfKeys = 10;
        int numberOfMessagesPerKey = 10;
        List<String> keys = IntStream.range(0, numberOfKeys).mapToObj(String::valueOf).collect(toList());
        List<String> payloads = IntStream.range(0, numberOfMessagesPerKey).mapToObj(index -> "messagePayload - " + index).collect(toList());
        Producer<String> producer = clientFactory.createClient().newProducer(Schema.STRING).topic(topic).create();
        keys.forEach(key -> payloads.forEach(payload -> trySend(producer, key, payload)));
        Map<String, List<String>> keyPayloadMap = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(numberOfKeys * numberOfMessagesPerKey);
        ConsumerFactory<String> consumerFactory = DefaultPulsarConsumerFactory
                .<String>createWith(this.clientFactory)
                .configure(options -> options
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .batchReceivePolicy(BatchReceivePolicy.builder()
                                .timeout(1, TimeUnit.SECONDS).build())
                        .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS));
        PulsarMessageListenerContainer<String> container = PulsarMessageListenerContainer
                .createWith(consumerFactory)
                .configure(options -> options
                        .schema(Schema.STRING)
                        .subscriptionName(subscriptionName)
                        .subscriptionType(subscriptionType)
                        .topics(topic)
                        .concurrency(concurrency)
                        .processingOrdering(ordering)
                        .messageListener(msg -> {
                            String key = MessageHeaderUtils.getHeaderAsString(msg, PulsarHeaders.PULSAR_MESSAGE_KEY_HEADER);
                            Consumer<String> consumer = MessageHeaderUtils.getHeader(msg, PulsarHeaders.PULSAR_CONSUMER_HEADER, Consumer.class);
                            logger.info("Received message {} with key {} in consumer {}", msg.getPayload(), key, consumer.getConsumerName());
                            keyPayloadMap.computeIfAbsent(key,
                                    newKey -> Collections.synchronizedList(new ArrayList<>())).add(msg.getPayload());
                            latch.countDown();
                        }));
        container.start();
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        container.stop();
        if (ProcessingOrdering.PARALLEL.equals(ordering)) {
            keys.forEach(key -> assertThat(keyPayloadMap.get(key)).containsExactlyInAnyOrderElementsOf(payloads));
        }
        else {
            keys.forEach(key -> assertThat(keyPayloadMap.get(key)).containsExactlyElementsOf(payloads));
        }
    }

    @Test
    void stopsProcessingOnErrorWithSharedSubscription() throws Exception {
        stopsProcessingAfterException(SubscriptionType.Shared, ProcessingOrdering.PARALLEL_BY_KEY, FailureHandlingMode.SEEK_LAST_ACKNOWLEDGED_BY_KEY, 1, 10, 20);
    }

    @Test
    void stopsProcessingOnErrorKeySharedSubscription() throws Exception {
        stopsProcessingAfterException(SubscriptionType.Key_Shared, ProcessingOrdering.PARALLEL_BY_KEY, FailureHandlingMode.NACK, 10, 10, 5);
    }

    @Test
    void stopsProcessingOnErrorWithExclusiveSubscription() throws Exception {
        stopsProcessingAfterException(SubscriptionType.Exclusive, ProcessingOrdering.SEQUENTIAL, FailureHandlingMode.SEEK_LAST_ACKNOWLEDGED_BY_KEY, 1, 5, 20);
    }

    @Test
    void stopsProcessingOnErrorWithFailOverSubscription() throws Exception {
        stopsProcessingAfterException(SubscriptionType.Failover, ProcessingOrdering.SEQUENTIAL, FailureHandlingMode.SEEK_CURRENT, 4, 5, 20);
    }

    @SuppressWarnings("unchecked")
    void stopsProcessingAfterException(SubscriptionType subscriptionType, ProcessingOrdering processingOrdering,
                                       FailureHandlingMode failureHandlingMode, int concurrency, int numberOfKeys, int numberOfMessagesPerKey) throws Exception {
        UUID id = UUID.randomUUID();
        String topic = "stops.processing.after.exception.topic." + id;
        String subscriptionName = "stops.processing.after.exception.subscription-" + id;
        List<String> keys = IntStream.range(0, numberOfKeys).mapToObj(String::valueOf).collect(toList());
        List<String> payloads = IntStream.range(0, numberOfMessagesPerKey).mapToObj(index -> "messagePayload - " + index).collect(toList());
        Producer<String> producer = clientFactory.createClient().newProducer(Schema.STRING).topic(topic).create();
        keys.forEach(key -> payloads.forEach(payload -> trySend(producer, key, payload)));
        ConsumerFactory<String> consumerFactory = DefaultPulsarConsumerFactory
                .<String>createWith(this.clientFactory)
                .configure(options -> options
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .batchReceivePolicy(BatchReceivePolicy.builder().maxNumMessages(10)
                                .timeout(1, TimeUnit.SECONDS).build())
                        .negativeAckRedeliveryDelay(1, TimeUnit.SECONDS));
        PulsarMessageListenerContainer<String> container = PulsarMessageListenerContainer
                .createWith(consumerFactory)
                .configure(options -> options
                                .schema(Schema.STRING)
                                .subscriptionName(subscriptionName)
                                .subscriptionType(subscriptionType)
                                .topics(topic)
                                .failureHandlingMode(failureHandlingMode)
                                .concurrency(concurrency)
                                .processingOrdering(processingOrdering));
        Map<String, List<String>> keyPayloadMapBeforeException = new ConcurrentHashMap<>();
        Map<String, List<String>> keyPayloadMapAfterException = new ConcurrentHashMap<>();
        int throwOn = 4;
        Map<String, AtomicInteger> messagesProcessedCount = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(numberOfKeys * numberOfMessagesPerKey);
        container.setMessageListener(msg -> {
            String key = MessageHeaderUtils.getHeaderAsString(msg, PulsarHeaders.PULSAR_MESSAGE_KEY_HEADER);
            int messageCount = messagesProcessedCount.computeIfAbsent(key, newKey -> new AtomicInteger()).incrementAndGet();
            if (messageCount == throwOn) {
                throw new RuntimeException("Expected exception for key " + key + " in stopsProcessingAfterException");
            }
            Consumer<String> consumer = MessageHeaderUtils.getHeader(msg, PulsarHeaders.PULSAR_CONSUMER_HEADER, Consumer.class);
            logger.info("Received message {} with key {} in consumer {}", msg.getPayload(), key, consumer.getConsumerName());
            if (messageCount < throwOn) {
                keyPayloadMapBeforeException.computeIfAbsent(key,
                        newKey -> Collections.synchronizedList(new ArrayList<>())).add(msg.getPayload());
            }
            else {
                keyPayloadMapAfterException.computeIfAbsent(key,
                        newKey -> Collections.synchronizedList(new ArrayList<>())).add(msg.getPayload());
            }
            latch.countDown();
        });
        container.start();
        assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
        container.stop();
        keys.forEach(key -> assertThat(keyPayloadMapBeforeException.get(key)).containsExactlyElementsOf(payloads.subList(0, throwOn - 1)));
        keys.forEach(key -> assertThat(keyPayloadMapAfterException.get(key)).containsExactlyElementsOf(payloads.subList(throwOn - 1, payloads.size())));
    }

    private <T> MessageId trySend(Producer<T> producer, String key, T payload)  {
        try {
            return producer.newMessage().key(key).value(payload).send();
        }
        catch (PulsarClientException e) {
            throw new RuntimeException("Could not send message");
        }
    }

    @EnablePulsar
    @Configuration
    static class PulsarConfiguration {

        @Bean
        PulsarClientFactory clientFactory() {
            return DefaultPulsarClientFactory
                    .create()
                    .configure(options -> options
                            .serviceUrl(PulsarBaseIntegrationTests.pulsar.getPulsarBrokerUrl()));
        }

        @Bean
        ConsumerFactory<Object> consumerFactory(PulsarClientFactory clientFactory) {
            return DefaultPulsarConsumerFactory
                    .createWith(clientFactory)
                    .configure(options -> options
                            .batchReceivePolicy(BatchReceivePolicy.builder()
                                    .timeout(1, TimeUnit.SECONDS).build())
                            .negativeAckRedeliveryDelay(100, TimeUnit.MILLISECONDS));
        }

    }

}
