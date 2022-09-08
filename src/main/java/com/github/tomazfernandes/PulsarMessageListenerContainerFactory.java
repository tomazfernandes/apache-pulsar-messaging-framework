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
import io.awspring.cloud.sqs.config.AbstractMessageListenerContainerFactory;
import io.awspring.cloud.sqs.config.Endpoint;
import io.awspring.cloud.sqs.config.MessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.MessageListener;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementResultCallback;
import io.awspring.cloud.sqs.listener.acknowledgement.AsyncAcknowledgementResultCallback;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.errorhandler.ErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.interceptor.MessageInterceptor;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class PulsarMessageListenerContainerFactory<T> implements MessageListenerContainerFactory<PulsarMessageListenerContainer<T>> {

    private static final Map<Class<?>, Schema<?>> DEFAULT_SCHEMAS;

    static {
        Map<Class<?>, Schema<?>> schemas = new HashMap<>();
        schemas.put(ByteBuffer.class, Schema.BYTEBUFFER);
        schemas.put(String.class, Schema.STRING);
        schemas.put(byte[].class, Schema.BYTES);
        schemas.put(Byte.class, Schema.INT8);
        schemas.put(Short.class, Schema.INT16);
        schemas.put(Integer.class, Schema.INT32);
        schemas.put(Long.class, Schema.INT64);
        schemas.put(Boolean.class, Schema.BOOL);
        schemas.put(Float.class, Schema.FLOAT);
        schemas.put(Double.class, Schema.DOUBLE);
        schemas.put(Date.class, Schema.DATE);
        schemas.put(Time.class, Schema.TIME);
        schemas.put(Timestamp.class, Schema.TIMESTAMP);
        schemas.put(Instant.class, Schema.INSTANT);
        schemas.put(LocalDate.class, Schema.LOCAL_DATE);
        schemas.put(LocalDateTime.class, Schema.LOCAL_DATE_TIME);
        schemas.put(LocalTime.class, Schema.LOCAL_TIME);
        DEFAULT_SCHEMAS = schemas;
    }

    private static final SchemaFactory DEFAULT_SCHEMA_FACTORY =
            clazz -> DEFAULT_SCHEMAS.getOrDefault(clazz, Schema.JSON(clazz));

    private final InnerFactory<T> innerFactory;

    private PulsarMessageListenerContainerFactory(ConsumerFactory<T> consumerFactory) {
        this.innerFactory = new InnerFactory<>(consumerFactory);
    }

    @Override
    public PulsarMessageListenerContainer<T> createContainer(String... logicalEndpointNames) {
        return innerFactory.createContainer(logicalEndpointNames);
    }

    public PulsarMessageListenerContainer<T> createContainerFromPattern(String topicsPattern) {
        return innerFactory.createContainerFromPattern(topicsPattern);
    }

    @Override
    public PulsarMessageListenerContainer<T> createContainer(Endpoint endpoint) {
        return innerFactory.createContainer(endpoint);
    }

    public PulsarMessageListenerContainerFactory<T> configure(Consumer<Configurer<T>> options) {
        options.accept(new Configurer<>(this.innerFactory));
        return this;
    }

    public static <T> PulsarMessageListenerContainerFactory<T> createWith(ConsumerFactory<T> consumerFactory) {
        return new PulsarMessageListenerContainerFactory<>(consumerFactory);
    }

    private static class InnerFactory<T> extends AbstractMessageListenerContainerFactory<T, PulsarMessageListenerContainer<T>> {

        private final ConsumerFactory<T> consumerFactory;

        private SchemaFactory schemaFactory = DEFAULT_SCHEMA_FACTORY;

        private String subscriptionName;

        private SubscriptionType subscriptionType;

        private Integer concurrency;

        private ProcessingOrdering processingOrdering;

        private FailureHandlingMode failureHandlingMode;

        private InnerFactory(ConsumerFactory<T> consumerFactory) {
            Assert.notNull(consumerFactory, "consumerFactory cannot be null");
            this.consumerFactory = consumerFactory;
        }

        private PulsarMessageListenerContainer<T> createContainerFromPattern(String topicsPattern) {
            return super.createContainer(new TopicsPatternEndpointAdapter(topicsPattern));
        }

        @Override
        protected PulsarMessageListenerContainer<T> createContainerInstance(Endpoint endpoint, ContainerOptions containerOptions) {
            PulsarMessageListenerContainer<T> container = PulsarMessageListenerContainer.createWith(this.consumerFactory);
            container.abstractContainer()
                    .configure(options -> options.fromBuilder(containerOptions.toBuilder()));
            return container;
        }

        protected void configureContainer(PulsarMessageListenerContainer<T> container, Endpoint endpoint) {
            Assert.notNull(this.consumerFactory, "consumerFactory not set");
            Assert.notNull(this.schemaFactory, "schemaFactory not set");
            configureAbstractContainer(container.abstractContainer(), endpoint);
            ConfigUtils.INSTANCE
                    .acceptIfNotNull(endpoint.getId(), container::setId)
                    .acceptIfNotNull(this.processingOrdering, ordering -> container.configure(options -> options.processingOrdering(this.processingOrdering)))
                    .acceptIfNotNull(this.subscriptionType, ordering -> container.configure(options -> options.subscriptionType(this.subscriptionType)))
                    .acceptIfNotNull(this.failureHandlingMode, ordering -> container.configure(options -> options.failureHandlingMode(this.failureHandlingMode)))
                    .acceptIfInstance(endpoint, PulsarListenerEndpoint.class, ple -> configurePulsarEndpoint(container, ple));
            if (endpoint instanceof PulsarListenerEndpoint) {
                configurePulsarEndpoint(container, (PulsarListenerEndpoint) endpoint);
            }
            else {
                configureGenericEndpoint(container, endpoint);
            }
        }

        private void configureGenericEndpoint(PulsarMessageListenerContainer<T> container, Endpoint endpoint) {
            container.configure(options -> options
                        .schema(getSchema(Object.class))
                        .subscriptionName(this.subscriptionName));
            ConfigUtils.INSTANCE
                    .acceptIfInstance(endpoint, TopicsPatternEndpointAdapter.class,
                            pea -> container.configure(options -> options.topicsPattern(pea.getTopicsPattern())))
                    .acceptIfNotNull(this.concurrency, concurrency -> container.configure(options -> options.concurrency(concurrency)));
        }

        @SuppressWarnings("unchecked")
        private Schema<T> getSchema(Class<?> clazz) {
            return (Schema<T>) this.schemaFactory.create(clazz);
        }

        private void configurePulsarEndpoint(PulsarMessageListenerContainer<T> container, PulsarListenerEndpoint pulsarEndpoint) {
            ConfigUtils.INSTANCE
                .acceptFirstNonNull(subscriptionName -> container.configure(options -> options.subscriptionName(subscriptionName)),
                        pulsarEndpoint.getSubscriptionName(), this.subscriptionName)
                .acceptFirstNonNull(concurrency -> container.configure(options -> options.concurrency(concurrency)),
                        pulsarEndpoint.getConcurrency(), this.concurrency);
            if (StringUtils.hasText(pulsarEndpoint.getTopicsPattern())) {
                container.configure(options -> options.topicsPattern(pulsarEndpoint.getTopicsPattern()));
            }
            container.configure(options -> options.schema(getSchema(pulsarEndpoint.determinePayloadClass())));
        }

    }

    private static class TopicsPatternEndpointAdapter implements Endpoint {

        private final String topicsPattern;

        public TopicsPatternEndpointAdapter(String topicsPattern) {
            this.topicsPattern = topicsPattern;
        }

        @Override
        public void setupContainer(MessageListenerContainer container) {
            // No ops - container should be setup manually.
        }

        @Override
        public Collection<String> getLogicalNames() {
            return Collections.emptyList();
        }

        @Override
        public String getListenerContainerFactoryName() {
            // we're already in the factory
            return null;
        }

        @Override
        public String getId() {
            // Container will setup its own id
            return null;
        }

        public String getTopicsPattern() {
            return this.topicsPattern;
        }

    }

    public static class Configurer<T> {

        private final InnerFactory<T> innerFactory;

        private Configurer(InnerFactory<T> innerFactory) {
            Assert.notNull(innerFactory, "innerFactory cannot be null");
            this.innerFactory = innerFactory;
        }

        public Configurer<T> errorHandler(ErrorHandler<T> errorHandler) {
            this.innerFactory.setErrorHandler(errorHandler);
            return this;
        }

        public Configurer<T> errorHandler(AsyncErrorHandler<T> errorHandler) {
            this.innerFactory.setErrorHandler(errorHandler);
            return this;
        }

        public Configurer<T> messageInterceptor(MessageInterceptor<T> messageInterceptor) {
            this.innerFactory.addMessageInterceptor(messageInterceptor);
            return this;
        }

        public Configurer<T> messageInterceptor(AsyncMessageInterceptor<T> messageInterceptor) {
            this.innerFactory.addMessageInterceptor(messageInterceptor);
            return this;
        }

        public Configurer<T> messageListener(MessageListener<T> messageListener) {
            this.innerFactory.setMessageListener(messageListener);
            return this;
        }

        public Configurer<T> asyncMessageListener(AsyncMessageListener<T> messageListener) {
            this.innerFactory.setAsyncMessageListener(messageListener);
            return this;
        }

        public Configurer<T> acknowledgementResultCallback(AsyncAcknowledgementResultCallback<T> acknowledgementResultCallback) {
            this.innerFactory.setAcknowledgementResultCallback(acknowledgementResultCallback);
            return this;
        }

        public Configurer<T> acknowledgementResultCallback(AcknowledgementResultCallback<T> acknowledgementResultCallback) {
            this.innerFactory.setAcknowledgementResultCallback(acknowledgementResultCallback);
            return this;
        }

        public Configurer<T> acknowledgementMode(AcknowledgementMode acknowledgementMode) {
            this.innerFactory.configure(options -> options.acknowledgementMode(acknowledgementMode));
            return this;
        }

        public Configurer<T> schemaFactory(SchemaFactory schemaFactory) {
            Assert.notNull(schemaFactory, "schemaFactory cannot be null");
            this.innerFactory.schemaFactory = schemaFactory;
            return this;
        }

        public Configurer<T> subscriptionName(String subscriptionName) {
            Assert.notNull(subscriptionName, "subscriptionName cannot be null");
            this.innerFactory.subscriptionName = subscriptionName;
            return this;
        }

        public Configurer<T> subscriptionType(SubscriptionType subscriptionType) {
            Assert.notNull(subscriptionType, "subscriptionType cannot be null");
            this.innerFactory.subscriptionType = subscriptionType;
            return this;
        }

        public Configurer<T> concurrency(int concurrency) {
            Assert.isTrue(concurrency > 0, "concurrency must be greater than 0");
            this.innerFactory.concurrency = concurrency;
            return this;
        }

        public Configurer<T> processingOrdering(ProcessingOrdering processingOrdering) {
            Assert.notNull(processingOrdering, "processingOrdering cannot be null");
            this.innerFactory.processingOrdering = processingOrdering;
            return this;
        }

        public Configurer<T> failureHandlingMode(FailureHandlingMode failureHandlingMode) {
            Assert.notNull(failureHandlingMode, "processingOrdering cannot be null");
            this.innerFactory.failureHandlingMode = failureHandlingMode;
            return this;
        }

    }

}
