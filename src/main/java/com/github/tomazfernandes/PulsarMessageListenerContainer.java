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
import io.awspring.cloud.sqs.listener.AbstractMessageListenerContainer;
import io.awspring.cloud.sqs.listener.AbstractPipelineMessageListenerContainer;
import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.listener.BackPressureHandler;
import io.awspring.cloud.sqs.listener.BatchAwareBackPressureHandler;
import io.awspring.cloud.sqs.listener.ContainerComponentFactory;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.IdentifiableContainerComponent;
import io.awspring.cloud.sqs.listener.MessageListener;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.errorhandler.ErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.interceptor.MessageInterceptor;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import io.awspring.cloud.sqs.support.converter.SqsMessagingMessageConverter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class PulsarMessageListenerContainer<T> implements MessageListenerContainer<T> {

    private static final ProcessingOrdering DEFAULT_PROCESSING_ORDERING = ProcessingOrdering.SEQUENTIAL;

    private static final SubscriptionType DEFAULT_SUBSCRIPTION_TYPE = SubscriptionType.Shared;

    private static final FailureHandlingMode DEFAULT_FAILURE_HANDLING_MODE = FailureHandlingMode.NACK;

    private static final int DEFAULT_CONCURRENCY = 1;

    private final InnerContainer<T> innerContainer;

    private PulsarMessageListenerContainer(ConsumerFactory<T> consumerFactory) {
        this.innerContainer = new InnerContainer<>(consumerFactory);
    }

    @Override
    public String getId() {
        return innerContainer.getId();
    }

    @Override
    public void setId(String id) {
        this.innerContainer.setId(id);
    }

    @Override
    public void setMessageListener(MessageListener<T> messageListener) {
        this.innerContainer.setMessageListener(messageListener);
    }

    @Override
    public void setAsyncMessageListener(AsyncMessageListener<T> asyncMessageListener) {
        this.innerContainer.setAsyncMessageListener(asyncMessageListener);
    }

    @Override
    public void start() {
        this.innerContainer.start();
    }

    @Override
    public void stop() {
        this.innerContainer.stop();
    }

    @Override
    public boolean isRunning() {
        return this.innerContainer.isRunning();
    }

    public static <T> PulsarMessageListenerContainer<T> createWith(ConsumerFactory<T> consumerFactory) {
        return new PulsarMessageListenerContainer<>(consumerFactory);
    }

    public PulsarMessageListenerContainer<T> configure(Consumer<Configurer<T>> options) {
        options.accept(new Configurer<>(this.innerContainer));
        return this;
    }

    AbstractMessageListenerContainer<T> abstractContainer() {
        return this.innerContainer;
    }

    private static class InnerContainer<T> extends AbstractPipelineMessageListenerContainer<T> {

        private SubscriptionType subscriptionType = DEFAULT_SUBSCRIPTION_TYPE;

        private int concurrency = DEFAULT_CONCURRENCY;

        private ProcessingOrdering processingOrdering = DEFAULT_PROCESSING_ORDERING;

        private final ConsumerFactory<T> consumerFactory;

        private FailureHandlingMode failureHandlingMode = DEFAULT_FAILURE_HANDLING_MODE;

        private Schema<T> schema;

        private String subscriptionName;

        private String topicsPattern;

        private InnerContainer(ConsumerFactory<T> consumerFactory) {
            super(ContainerOptions.builder().build());
            this.consumerFactory = consumerFactory;
        }

        @Override
        protected Collection<ContainerComponentFactory<T>> getDefaultComponentFactories() {
            return Collections.singletonList(new PulsarContainerComponentFactory<>(this.processingOrdering, this.subscriptionType, this.failureHandlingMode));
        }

        @Override
        protected BackPressureHandler createBackPressureHandler() {
            return new SinglePollBackPressureHandler(this);
        }

        @Override
        protected TaskExecutor createComponentsTaskExecutor() {
            ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
            executor.setQueueCapacity(0);
            executor.setAllowCoreThreadTimeOut(true);
            executor.setThreadFactory(createThreadFactory());
            executor.afterPropertiesSet();
            return executor;
        }

        @Override
        protected Collection<MessageSource<T>> createMessageSources(ContainerComponentFactory<T> componentFactory) {
            return IntStream.range(0, this.concurrency)
                    .mapToObj(index -> doCreateMessageSource(componentFactory, index)).collect(Collectors.toList());
        }

        private MessageSource<T> doCreateMessageSource(ContainerComponentFactory<T> componentFactory, int index) {
            MessageSource<T> messageSource = componentFactory
                    .createMessageSource(getContainerOptions());
            ConfigUtils.INSTANCE.acceptIfInstance(messageSource, IdentifiableContainerComponent.class,
                    icc -> setMessageSourceId(index, icc));
            return messageSource;
        }

        private void setMessageSourceId(int index, IdentifiableContainerComponent messageSource) {
            messageSource.setId(getId() + "#" + index);
        }

        @Override
        protected void doConfigureMessageSources(Collection<MessageSource<T>> messageSources) {
            ConfigUtils.INSTANCE.acceptManyIfInstance(messageSources, PulsarMessageSource.class, this::configureSource);
            if (getContainerOptions().getMessageConverter() instanceof SqsMessagingMessageConverter) {
                getContainerOptions().toBuilder().messageConverter(new PulsarMessagingMessageConverter<>()).build()
                        .configure(messageSources);
            }
        }

        private void configureSource(PulsarMessageSource<T> source) {
            source.setTopics(getQueueNames());
            source.setConsumerFactory(this.consumerFactory);
            source.setSchema(this.schema);
            source.setSubscriptionName(this.subscriptionName);
            source.setProcessingOrdering(this.processingOrdering);
            source.setSubscriptionType(this.subscriptionType);
            source.setPollingEndpointName(StringUtils.hasText(this.topicsPattern) ? this.topicsPattern : String.join("-", getQueueNames()));
            ConfigUtils.INSTANCE.acceptIfNotNull(this.topicsPattern, source::setTopicsPattern);
        }

    }

    private static class SinglePollBackPressureHandler implements BatchAwareBackPressureHandler {

        private final Semaphore semaphore = new Semaphore(1);

        private final MessageListenerContainer<?> container;

        public SinglePollBackPressureHandler(MessageListenerContainer<?> container) {
            this.container = container;
        }

        @Override
        public int requestBatch() throws InterruptedException {
            return semaphore.tryAcquire(1, 10, TimeUnit.SECONDS) ? 1 : 0;
        }

        @Override
        public void releaseBatch() {
            // No messages were returned
            this.semaphore.release();
        }

        @Override
        public int request(int amount) throws InterruptedException {
            throw new UnsupportedOperationException("request not implemented");
        }

        @Override
        public void release(int amount) {
            // Do not release unless poll returns empty or container is stopped
            if (!this.container.isRunning()) {
                this.semaphore.release();
            }
        }

        @Override
        public boolean drain(Duration timeout) {
            try {
                return semaphore.tryAcquire(20, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while draining", e);
            }
        }
    }

    public static class Configurer<T> {

        private final InnerContainer<T> innerContainer;

        public Configurer(InnerContainer<T> innerContainer) {
            Assert.notNull(innerContainer, "innerContainer cannot be null");
            this.innerContainer = innerContainer;
        }

        public void messageListener(MessageListener<T> messageListener) {
            this.innerContainer.setMessageListener(messageListener);
        }

        public void asyncMessageListener(AsyncMessageListener<T> asyncMessageListener) {
            this.innerContainer.setAsyncMessageListener(asyncMessageListener);
        }

        public Configurer<T> id(String id) {
            this.innerContainer.setId(id);
            return this;
        }

        public Configurer<T> errorHandler(ErrorHandler<T> errorHandler) {
            this.innerContainer.setErrorHandler(errorHandler);
            return this;
        }

        public Configurer<T> errorHandler(AsyncErrorHandler<T> errorHandler) {
            this.innerContainer.setErrorHandler(errorHandler);
            return this;
        }

        public Configurer<T> messageInterceptor(MessageInterceptor<T> messageInterceptor) {
            this.innerContainer.addMessageInterceptor(messageInterceptor);
            return this;
        }

        public Configurer<T> messageInterceptor(AsyncMessageInterceptor<T> messageInterceptor) {
            this.innerContainer.addMessageInterceptor(messageInterceptor);
            return this;
        }

        public Configurer<T> topics(String... topics) {
            this.innerContainer.setQueueNames(topics);
            return this;
        }

        public Configurer<T> acknowledgementMode(AcknowledgementMode acknowledgementMode) {
            this.innerContainer.configure(options -> options.acknowledgementMode(acknowledgementMode));
            return this;
        }

        public Configurer<T> subscriptionType(SubscriptionType subscriptionType) {
            Assert.notNull(subscriptionType, "subscriptionType cannot be null");
            this.innerContainer.subscriptionType = subscriptionType;
            return this;
        }

        public Configurer<T> subscriptionName(String subscriptionName) {
            Assert.notNull(subscriptionName, "subscriptionName cannot be null");
            this.innerContainer.subscriptionName = subscriptionName;
            return this;
        }

        public Configurer<T> schema(Schema<T> schema) {
            Assert.notNull(schema, "schema cannot be null");
            this.innerContainer.schema = schema;
            return this;
        }

        public Configurer<T> topicsPattern(String topicPattern) {
            Assert.notNull(topicPattern, "topicPattern cannot be null");
            this.innerContainer.topicsPattern = topicPattern;
            return this;
        }

        public Configurer<T> concurrency(int concurrency) {
            Assert.isTrue(concurrency > 0, "concurrency must be greater than 0");
            this.innerContainer.concurrency = concurrency;
            return this;
        }

        public Configurer<T> processingOrdering(ProcessingOrdering processingOrdering) {
            Assert.notNull(processingOrdering, "processingOrdering cannot be null");
            this.innerContainer.processingOrdering = processingOrdering;
            return this;
        }

        public Configurer<T> failureHandlingMode(FailureHandlingMode failureHandlingMode) {
            Assert.notNull(failureHandlingMode, "processingOrdering cannot be null");
            this.innerContainer.failureHandlingMode = failureHandlingMode;
            return this;
        }

    }

}
