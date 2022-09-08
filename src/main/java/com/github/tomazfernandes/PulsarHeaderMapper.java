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
import io.awspring.cloud.sqs.support.converter.AcknowledgementAwareMessageConversionContext;
import io.awspring.cloud.sqs.support.converter.ContextAwareHeaderMapper;
import io.awspring.cloud.sqs.support.converter.MessageConversionContext;
import org.apache.pulsar.client.api.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageHeaderAccessor;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class PulsarHeaderMapper<T> implements ContextAwareHeaderMapper<Message<T>> {

    @Override
    public void fromHeaders(MessageHeaders headers, Message<T> target) {
        throw new UnsupportedOperationException("fromHeaders not implemented");
    }

    @Override
    public MessageHeaders toHeaders(Message<T> source) {
        MessageHeaderAccessor accessor = new MessageHeaderAccessor();
        accessor.setHeader(PulsarHeaders.PULSAR_MESSAGE_ID_HEADER, source.getMessageId());
        accessor.setHeader(PulsarHeaders.PULSAR_MESSAGE_KEY_HEADER, source.getKey());
        accessor.setHeader(PulsarHeaders.PULSAR_MESSAGE_PROPERTIES_HEADER, source.getProperties());
        accessor.setHeader(PulsarHeaders.PULSAR_ORIGINAL_MESSAGE_HEADER, source);
        accessor.setHeader(PulsarHeaders.PULSAR_TOPIC_NAME_HEADER, source.getTopicName());
        return accessor.toMessageHeaders();
    }

    @Override
    public MessageHeaders createContextHeaders(Message<T> source, MessageConversionContext context) {
        MessageHeaderAccessor accessor = new MessageHeaderAccessor();
        ConfigUtils.INSTANCE
                .acceptIfInstance(context, AcknowledgementAwareMessageConversionContext.class,
                        aamcc -> accessor.setHeader(PulsarHeaders.PULSAR_ACKNOWLEDGEMENT_CALLBACK_HEADER, aamcc.getAcknowledgementCallback()))
                .acceptIfInstance(context, PulsarMessageConversionContext.class,
                        pmcc -> accessor.setHeader(PulsarHeaders.PULSAR_CONSUMER_HEADER, pmcc.getConsumer()));
        return accessor.toMessageHeaders();
    }

}
