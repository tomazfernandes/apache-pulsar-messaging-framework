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

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.springframework.util.Assert;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class DefaultPulsarConsumerFactory<T> implements ConsumerFactory<T> {

    private final PulsarClient pulsarClient;

    private java.util.function.Consumer<ConsumerBuilder<T>> configurer = options -> {};

    private DefaultPulsarConsumerFactory(PulsarClient pulsarClient) {
        Assert.notNull(pulsarClient, "pulsarClient cannot be null");
        this.pulsarClient = pulsarClient;
    }

    private DefaultPulsarConsumerFactory(PulsarClientFactory pulsarClientFactory) {
        Assert.notNull(pulsarClientFactory, "pulsarClientFactory cannot be null");
        this.pulsarClient = pulsarClientFactory.createClient();
    }

    public static <T> ConsumerFactory<T> createWith(PulsarClientFactory pulsarClientFactory) {
        return new DefaultPulsarConsumerFactory<T>(pulsarClientFactory);
    }

    public static <T> ConsumerFactory<T> createWith(PulsarClient pulsarClient) {
        return new DefaultPulsarConsumerFactory<T>(pulsarClient);
    }

    @Override
    public ConsumerFactory<T> configure(java.util.function.Consumer<ConsumerBuilder<T>> configurer) {
        Assert.notNull(configurer, "configurer must not be null");
        this.configurer = this.configurer.andThen(configurer);
        return this;
    }

    @Override
    public Consumer<T> createConsumer(Schema<T> schema, java.util.function.Consumer<ConsumerBuilder<T>> instanceConfigurer) {
        Assert.notNull(schema, "schema cannot be null");
        Assert.notNull(instanceConfigurer, "instance configurer cannot be null");
        ConsumerBuilder<T> builder = pulsarClient.newConsumer(schema);
        this.configurer.accept(builder);
        instanceConfigurer.accept(builder);
        return tryBuild(builder);
    }

    @Override
    public Consumer<T> createConsumer(Schema<T> schema) {
        return createConsumer(schema, options -> {});
    }

    private Consumer<T> tryBuild(ConsumerBuilder<T> builder) {
        try {
            return builder.subscribe();
        } catch (PulsarClientException e) {
            throw new PulsarException("Error building consumer", e);
        }
    }

}
