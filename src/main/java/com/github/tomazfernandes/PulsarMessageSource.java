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

import io.awspring.cloud.sqs.ConfigUtils;
import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.MessageProcessingContext;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementCallback;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementExecutor;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.acknowledgement.ExecutingAcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.sink.adapter.AbstractDelegatingMessageListeningSinkAdapter;
import io.awspring.cloud.sqs.listener.source.AbstractPollingMessageSource;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class PulsarMessageSource<T> extends AbstractPollingMessageSource<T, Message<T>> {

    private Consumer<T> consumer;

    private Schema<T> schema;

    private String subscriptionName;

    private SubscriptionType subscriptionType;

    private ConsumerFactory<T> consumerFactory;

    private String topicsPattern;

    private Collection<String> topics;

    private ProcessingOrdering processingOrdering;

    public void setConsumerFactory(ConsumerFactory<T> consumerFactory) {
        Assert.notNull(consumerFactory, "consumerFactory must not be null");
        this.consumerFactory = consumerFactory;
    }

    public void setSubscriptionName(String subscriptionName) {
        Assert.hasText(subscriptionName, "subscriptionName must have text");
        this.subscriptionName = subscriptionName;
    }

    public void setSubscriptionType(SubscriptionType subscriptionType) {
        Assert.notNull(subscriptionType, "subscriptionType must not be null");
        this.subscriptionType = subscriptionType;
    }

    public void setSchema(Schema<T> schema) {
        Assert.notNull(schema, "schema cannot be null");
        this.schema = schema;
    }

    public void setTopics(Collection<String> topics) {
        Assert.notEmpty(topics, "topics must not be empty");
        this.topics = topics;
    }

    public void setTopicsPattern(String topicsPattern) {
        Assert.hasText(topicsPattern, "topicsPattern must have text");
        this.topicsPattern = topicsPattern;
    }

    public void setProcessingOrdering(ProcessingOrdering processingOrdering) {
        Assert.notNull(processingOrdering, "processingOrdering cannot be null");
        this.processingOrdering = processingOrdering;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doStart() {
        Assert.notNull(this.consumerFactory, "consumerFactory not set");
        this.consumer = this.consumerFactory.createConsumer(this.schema, this::configureConsumer);
        ConfigUtils.INSTANCE
                .acceptIfInstance(getAcknowledgmentProcessor(), ExecutingAcknowledgementProcessor.class,
                    eap -> eap.setAcknowledgementExecutor(createAcknowledgementExecutor()))
                .acceptIfInstance(getMessageConversionContext(), PulsarMessageConversionContext.class,
                        pmcc -> pmcc.setConsumer(this.consumer));
    }

    private AcknowledgementExecutor<T> createAcknowledgementExecutor() {
        return ProcessingOrdering.SEQUENTIAL.equals(this.processingOrdering) && isCumulativeCompatibleSubscription()
                ? new PulsarCumulativeAcknowledgementExecutor<>(this.consumer)
                : new PulsarAcknowledgementExecutor<>(this.consumer);
    }

    private boolean isCumulativeCompatibleSubscription() {
        return SubscriptionType.Exclusive.equals(this.subscriptionType) || SubscriptionType.Failover.equals(this.subscriptionType);
    }

    private ConsumerBuilder<T> configureConsumer(ConsumerBuilder<T> options) {
        if (StringUtils.hasText(this.topicsPattern)) {
            options.topicsPattern(this.topicsPattern);
        }
        else {
            options.topics(getAsList(this.topics));
        }
        return options
                .subscriptionName(this.subscriptionName)
                .subscriptionType(this.subscriptionType);
    }

    private List<String> getAsList(Collection<String> queueNames) {
        return queueNames instanceof List
                ? (List<String>) queueNames
                : new ArrayList<>(queueNames);
    }


    @Override
    protected void doConfigure(ContainerOptions containerOptions) {
    }

    @Override
    public void setMessageSink(MessageSink<T> messageSink) {
        super.setMessageSink(new ReleaseOnBatchCompleteMessageSinkAdapter<>(messageSink, this::releaseUnusedPermits));
    }

    @Override
    protected MessageProcessingContext<T> createContext() {
        return MessageProcessingContext.<T>create().setAcknowledgmentCallback(getAcknowledgementCallback());
    }

    @Override
    protected AcknowledgementProcessor<T> getAcknowledgmentProcessor() {
        return super.getAcknowledgmentProcessor();
    }

    @Override
    protected CompletableFuture<Collection<Message<T>>> doPollForMessages(int messagesToRequest) {
        return consumer.batchReceiveAsync()
                .thenApply(msgs -> StreamSupport.stream(msgs.spliterator(), false)
                        .collect(Collectors.toList()));
    }
    
    @Override
    protected void setupAcknowledgementForConversion(AcknowledgementCallback callback){
        super.setupAcknowledgementForConversion(getAcknowledgementCallback());
    }

    @Override
    protected AcknowledgementCallback<T> getAcknowledgementCallback() {
        return new PulsarAcknowledgementCallback(this.consumer, super.getAcknowledgementCallback());
    }

    @Override
    protected void doStop() {
        try {
            this.consumer.close();
        }
        catch (Exception e) {
            throw new PulsarException("Error closing consumer", e);
        }
    }

    private static class PulsarAcknowledgementCallback<T> implements NackAwareAcknowledgementCallback<T>, SeekAwareAcknowledgementCallback<T> {

        private final Consumer<T> consumer;

        private final AcknowledgementCallback<T> acknowledgementCallback;

        private PulsarAcknowledgementCallback(Consumer<T> consumer, AcknowledgementCallback<T> acknowledgementCallback) {
            this.consumer = consumer;
            this.acknowledgementCallback = acknowledgementCallback;
        }

        @Override
        public CompletableFuture<Void> onAcknowledge(org.springframework.messaging.Message<T> message) {
            return this.acknowledgementCallback.onAcknowledge(message);
        }

        @Override
        public CompletableFuture<Void> onAcknowledge(Collection<org.springframework.messaging.Message<T>> messages) {
            return this.acknowledgementCallback.onAcknowledge(messages);
        }

        @Override
        public CompletableFuture<Void> onNack(org.springframework.messaging.Message<T> message) {
            this.consumer.negativeAcknowledge(getMessageId(message));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> onNack(Collection<org.springframework.messaging.Message<T>> messages) {
            messages.forEach(msg -> this.consumer.negativeAcknowledge(getMessageId(msg)));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> onSeek(org.springframework.messaging.Message<T> message) {
            return this.consumer.seekAsync(getMessageId(message));
        }

        private MessageId getMessageId(org.springframework.messaging.Message<T> message) {
            return MessageHeaderUtils.getHeader(message, PulsarHeaders.PULSAR_MESSAGE_ID_HEADER, MessageId.class);
        }

    }

    private static class ReleaseOnBatchCompleteMessageSinkAdapter<T> extends AbstractDelegatingMessageListeningSinkAdapter<T> {

        private final BiConsumer<Integer, Collection<Message<T>>> releasingFunction;

        protected ReleaseOnBatchCompleteMessageSinkAdapter(MessageSink<T> delegate,
                                                           BiConsumer<Integer, Collection<Message<T>>> releasingFunction) {
            super(delegate);
            this.releasingFunction = releasingFunction;
        }

        @Override
        public CompletableFuture<Void> emit(Collection<org.springframework.messaging.Message<T>> messages, MessageProcessingContext<T> context) {
            return getDelegate().emit(messages, context).whenComplete((v, t) -> releasingFunction.accept(1, Collections.emptyList()));
        }

    }

}
