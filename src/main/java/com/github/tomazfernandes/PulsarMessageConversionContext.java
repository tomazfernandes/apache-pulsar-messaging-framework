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

import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementCallback;
import io.awspring.cloud.sqs.support.converter.AcknowledgementAwareMessageConversionContext;
import org.apache.pulsar.client.api.Consumer;
import org.springframework.util.Assert;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
class PulsarMessageConversionContext<T> implements AcknowledgementAwareMessageConversionContext {

    private AcknowledgementCallback<?> acknowledgementCallback;

    private Consumer<T> consumer;

    public void setConsumer(Consumer<T> consumer) {
        Assert.notNull(consumer, "consumer must not be null");
        this.consumer = consumer;
    }

    @Override
    public void setAcknowledgementCallback(AcknowledgementCallback<?> acknowledgementCallback) {
        this.acknowledgementCallback = acknowledgementCallback;
    }

    public Consumer<T> getConsumer() {
        return this.consumer;
    }

    @Override
    public AcknowledgementCallback<?> getAcknowledgementCallback() {
        return this.acknowledgementCallback;
    }

}
