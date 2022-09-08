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

import org.springframework.lang.Nullable;

import java.util.Collection;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class PulsarListenerEndpoint extends AbstractPayloadTypeResolvingEndpoint {

    private String topicsPattern;

    private String subscriptionName;

    private Integer concurrency;

    protected PulsarListenerEndpoint(Collection<String> queueNames, String listenerContainerFactoryName, String id) {
        super(queueNames, listenerContainerFactoryName, id);
    }

    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    public void setTopicsPattern(String topicPattern) {
        this.topicsPattern = topicPattern;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    @Nullable
    public String getSubscriptionName() {
        return this.subscriptionName;
    }

    @Nullable
    public String getTopicsPattern() {
        return this.topicsPattern;
    }

    @Nullable
    public Integer getConcurrency() {
        return this.concurrency;
    }

}
