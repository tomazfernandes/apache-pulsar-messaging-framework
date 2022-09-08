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

import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import io.awspring.cloud.sqs.listener.errorhandler.ErrorHandler;

/**
 *
 * The standard way of increasing message consumption concurrency in Pulsar is by adding more consumers.
 * However, processing messages from the same consumer in parallel can lead to better throughput and resource
 * utilization.
 *
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public enum ProcessingOrdering {

    /**
     * Messages will be processed in the same order as they are received.
     * If a message processing throws an error, processing will be suspended and
     * messages haven't yet been processed and acknowledged will be redelivered,
     * including the failed one.
     * Set {@link AcknowledgementMode#ALWAYS} or configure an {@link ErrorHandler} to customize this behavior.
     */
    SEQUENTIAL,

    /**
     * Messages will be processed in parallel.
     * A new batch of messages will only be retrieved when all messages from the previous batch have been processed.
     * A future release should allow continuous flow of messages.
     * Messages are acknowledged individually.
     * Batch size should be configured taking in account the number of threads necessary for processing the whole batch in parallel.
     */
    PARALLEL,

    /**
     * Messages with the same key will be processed sequentially, with keys being processed in parallel.
     * Note that if too many different keys are present, this can lead to too many processing threads being spawned.
     * Configure batch size so that if each received message belongs a different key, the application will be able to support it.
     * If a message processing fails, messages from the same key that haven't yet been processed will be redelivered,
     * including the failed one.
     * Set {@link AcknowledgementMode#ALWAYS} or configure an {@link ErrorHandler} to customize this behavior.
     */
    PARALLEL_BY_KEY

}
