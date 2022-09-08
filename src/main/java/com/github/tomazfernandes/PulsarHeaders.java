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

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class PulsarHeaders {

    public static final String PULSAR_HEADER_PREFIX = "pulsar.";

    public static final String PULSAR_MESSAGE_ID_HEADER = PULSAR_HEADER_PREFIX + "messageId";

    public static final String PULSAR_MESSAGE_KEY_HEADER = PULSAR_HEADER_PREFIX + "messageKey";

    public static final String PULSAR_MESSAGE_PROPERTIES_HEADER = PULSAR_HEADER_PREFIX + "messageProperties";

    public static final String PULSAR_ORIGINAL_MESSAGE_HEADER = PULSAR_HEADER_PREFIX + "originalMessage";

    public static final String PULSAR_ACKNOWLEDGEMENT_CALLBACK_HEADER = PULSAR_HEADER_PREFIX + "acknowledgementCallback";

    public static final String PULSAR_CONSUMER_HEADER = PULSAR_HEADER_PREFIX + "consumer";

    public static final String PULSAR_TOPIC_NAME_HEADER = PULSAR_HEADER_PREFIX + "topicName";

}
