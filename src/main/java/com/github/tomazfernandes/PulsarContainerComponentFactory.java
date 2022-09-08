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

import io.awspring.cloud.sqs.listener.ContainerComponentFactory;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.ListenerMode;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementCallback;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementOrdering;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.acknowledgement.ImmediateAcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.OnSuccessAcknowledgementHandler;
import io.awspring.cloud.sqs.listener.sink.BatchMessageSink;
import io.awspring.cloud.sqs.listener.sink.FanOutMessageSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.sink.adapter.MessageGroupingSinkAdapter;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class PulsarContainerComponentFactory<T> implements ContainerComponentFactory<T> {

    private static final String DEFAULT_KEY_FOR_GROUPING_VALUE = "defaultKeyForGrouping";

    private final ProcessingOrdering processingOrdering;

    private final SubscriptionType subscriptionType;

    private final FailureHandlingMode failureHandlingMode;

    public PulsarContainerComponentFactory(ProcessingOrdering processingOrdering, SubscriptionType subscriptionType, FailureHandlingMode failureHandlingMode) {
        Assert.notNull(subscriptionType, "subscriptionType must not be null");
        Assert.notNull(processingOrdering, "processingOrdering must not be null");
        Assert.notNull(failureHandlingMode, "failureHandlingMode must not be null");
        this.processingOrdering = processingOrdering;
        this.subscriptionType = subscriptionType;
        this.failureHandlingMode = failureHandlingMode;
    }

    @Override
    public MessageSource<T> createMessageSource(ContainerOptions options) {
        return new PulsarMessageSource<>();
    }

    @Override
    public MessageSink<T> createMessageSink(ContainerOptions options) {
        validateOrderingAndSubscription();
        return maybeWrapWithGroupingSink(createDeliverySink(options));
    }

    private void validateOrderingAndSubscription() {
        Assert.isTrue(ProcessingOrdering.SEQUENTIAL.equals(this.processingOrdering)
                || SubscriptionType.Key_Shared.equals(this.subscriptionType)
                || SubscriptionType.Shared.equals(this.subscriptionType), "Only Key_Shared and Shared subscription types can use "
                + this.processingOrdering + " " + ProcessingOrdering.class.getSimpleName());
    }

    private MessageSink<T> maybeWrapWithGroupingSink(MessageSink<T> deliverySink) {
        return ProcessingOrdering.PARALLEL_BY_KEY.equals(this.processingOrdering)
                ? doWrapWithGroupingSink(deliverySink)
                : deliverySink;
    }

    private MessageSink<T> doWrapWithGroupingSink(MessageSink<T> deliverySink) {
        return new MessageGroupingSinkAdapter<>(deliverySink, this::getKeyFromMessage);
    }

    private String getKeyFromMessage(Message<T> message) {
        String keyHeader = message.getHeaders().get(PulsarHeaders.PULSAR_MESSAGE_KEY_HEADER, String.class);
        return keyHeader != null
                ? keyHeader
                : DEFAULT_KEY_FOR_GROUPING_VALUE;
    }

    private MessageSink<T> createDeliverySink(ContainerOptions options) {
        return ListenerMode.BATCH.equals(options.getListenerMode())
                ? new BatchMessageSink<>()
                : ProcessingOrdering.PARALLEL.equals(this.processingOrdering)
                    ? new FanOutMessageSink<>()
                    : new NackingOrderedMessageSink<>();
    }

    @Override
    public AcknowledgementProcessor<T> createAcknowledgementProcessor(ContainerOptions options) {
        ImmediateAcknowledgementProcessor<T> processor = new ImmediateAcknowledgementProcessor<>();
        processor.setMaxAcknowledgementsPerBatch(500); // Should add a no limit option
        options.toBuilder().acknowledgementOrdering(AcknowledgementOrdering.ORDERED).build().configure(processor);
        return processor;
    }

    @Override
    public AcknowledgementHandler<T> createAcknowledgementHandler(ContainerOptions options) {
        return isNackAcknowledgement(options)
                ? new NackAwareOnSuccessAcknowledgementHandler<>()
                : isSeekAcknowledgement(options)
                    ? new SeekAwareOnSuccessAcknowledgementHandler<>(this.failureHandlingMode)
                    : ContainerComponentFactory.super.createAcknowledgementHandler(options);
    }

    private boolean isNackAcknowledgement(ContainerOptions options) {
        return AcknowledgementMode.ON_SUCCESS.equals(options.getAcknowledgementMode())
                && FailureHandlingMode.NACK.equals(this.failureHandlingMode);
    }

    private boolean isSeekAcknowledgement(ContainerOptions options) {
        return AcknowledgementMode.ON_SUCCESS.equals(options.getAcknowledgementMode())
                && (FailureHandlingMode.SEEK_LAST_ACKNOWLEDGED.equals(this.failureHandlingMode)
                || FailureHandlingMode.SEEK_LAST_ACKNOWLEDGED_BY_KEY.equals(this.failureHandlingMode)
                || FailureHandlingMode.SEEK_CURRENT.equals(this.failureHandlingMode));
    }

    private static class NackAwareOnSuccessAcknowledgementHandler<T> extends OnSuccessAcknowledgementHandler<T> {

        @Override
        public CompletableFuture<Void> onError(Message<T> message, Throwable t, AcknowledgementCallback<T> callback) {
            return callback instanceof NackAwareAcknowledgementCallback
                    ? ((NackAwareAcknowledgementCallback<T>) callback).onNack(message)
                    : CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> onError(Collection<Message<T>> messages, Throwable t, AcknowledgementCallback<T> callback) {
            return callback instanceof NackAwareAcknowledgementCallback
                    ? ((NackAwareAcknowledgementCallback<T>) callback).onNack(messages)
                    : CompletableFuture.completedFuture(null);
        }

    }

    private static class SeekAwareOnSuccessAcknowledgementHandler<T> extends OnSuccessAcknowledgementHandler<T> {

        private static final String DEFAULT_KEY_FOR_SEEKING_VALUE = "defaultKeyForSeeking";

        private final ConcurrentHashMap<String, Message<T>> lastAcknowledgedMessageByKey = new ConcurrentHashMap<>();

        private final FailureHandlingMode failureHandlingMode;

        public SeekAwareOnSuccessAcknowledgementHandler(FailureHandlingMode failureHandlingMode) {
            Assert.notNull(failureHandlingMode, "failureHandlingMode cannot be null");
            this.failureHandlingMode = failureHandlingMode;
        }

        @Override
        public CompletableFuture<Void> onSuccess(Message<T> message, AcknowledgementCallback<T> callback) {
            updateLastAcknowledgedByKey(message);
            return super.onSuccess(message, callback);
        }

        @Override
        public CompletableFuture<Void> onSuccess(Collection<Message<T>> messages, AcknowledgementCallback<T> callback) {
            Message<T> lastMessageInBatch = getMessagesAsList(messages).get(messages.size() - 1);
            updateLastAcknowledgedByKey(lastMessageInBatch);
            return super.onSuccess(messages, callback);
        }

        @Override
        public CompletableFuture<Void> onError(Message<T> message, Throwable t, AcknowledgementCallback<T> callback) {
            return callback instanceof SeekAwareAcknowledgementCallback
                    ? ((SeekAwareAcknowledgementCallback<T>) callback).onSeek(retrieveLastAcknowledgedByKey(message))
                    : CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> onError(Collection<Message<T>> messages, Throwable t, AcknowledgementCallback<T> callback) {
            return callback instanceof SeekAwareAcknowledgementCallback
                    ? ((SeekAwareAcknowledgementCallback<T>) callback).onSeek(retrieveLastAcknowledgedByKey(messages.iterator().next()))
                    : CompletableFuture.completedFuture(null);
        }

        private Message<T> retrieveLastAcknowledgedByKey(Message<T> message) {
            return FailureHandlingMode.SEEK_CURRENT.equals(this.failureHandlingMode)
                    ? message
                    : this.lastAcknowledgedMessageByKey.computeIfAbsent(getMessageKey(message), newKey -> message);
        }

        private void updateLastAcknowledgedByKey(org.springframework.messaging.Message<T> message) {
            if (!FailureHandlingMode.SEEK_CURRENT.equals(this.failureHandlingMode)) {
                this.lastAcknowledgedMessageByKey.put(getMessageKey(message), message);
            }
        }

        private List<Message<T>> getMessagesAsList(Collection<org.springframework.messaging.Message<T>> messages) {
            return messages instanceof List
                    ? (List<org.springframework.messaging.Message<T>>) messages
                    : new ArrayList<>(messages);
        }

        private String getMessageKey(org.springframework.messaging.Message<T> message) {
            String key = message.getHeaders().get(PulsarHeaders.PULSAR_MESSAGE_KEY_HEADER, String.class);
            return StringUtils.hasText(key) && FailureHandlingMode.SEEK_LAST_ACKNOWLEDGED_BY_KEY.equals(this.failureHandlingMode)
                    ? key
                    : DEFAULT_KEY_FOR_SEEKING_VALUE;
        }

    }

}
