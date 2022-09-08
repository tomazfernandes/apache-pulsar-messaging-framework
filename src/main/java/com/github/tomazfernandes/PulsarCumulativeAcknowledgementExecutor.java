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
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementExecutor;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.springframework.messaging.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class PulsarCumulativeAcknowledgementExecutor<T> implements AcknowledgementExecutor<T> {

    private final Consumer<T> consumer;

    public PulsarCumulativeAcknowledgementExecutor(Consumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public CompletableFuture<Void> execute(Collection<Message<T>> messages) {
        List<Message<T>> messagesList = getMessagesAsList(messages);
        return consumer.acknowledgeCumulativeAsync(MessageHeaderUtils
                .getHeader(messagesList.get(messagesList.size() - 1), PulsarHeaders.PULSAR_MESSAGE_ID_HEADER, MessageId.class));
    }

    private List<Message<T>> getMessagesAsList(Collection<Message<T>> messages) {
        return messages instanceof List
                ? (List<Message<T>>) messages
                : new ArrayList<>(messages);
    }

}
