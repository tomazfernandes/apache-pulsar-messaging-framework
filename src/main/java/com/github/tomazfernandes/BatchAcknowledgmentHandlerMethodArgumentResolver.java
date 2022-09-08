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
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class BatchAcknowledgmentHandlerMethodArgumentResolver implements HandlerMethodArgumentResolver {

	@Override
	public boolean supportsParameter(MethodParameter parameter) {
		return ClassUtils.isAssignable(BatchAcknowledgement.class, parameter.getParameterType());
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object resolveArgument(MethodParameter parameter, Message<?> message) {
		Object payloadObject = message.getPayload();
		Assert.isInstanceOf(Collection.class, payloadObject, "Payload must be instance of Collection");
		Collection<Message<Object>> messages = (Collection<Message<Object>>) payloadObject;
		NackAwareAcknowledgementCallback<Object> callback = messages.iterator().next().getHeaders()
				.get(PulsarHeaders.PULSAR_ACKNOWLEDGEMENT_CALLBACK_HEADER, NackAwareAcknowledgementCallback.class);
		Assert.notNull(callback, "No acknowledgement found for message " + MessageHeaderUtils.getId(message)
				+ ". AcknowledgeMode should be MANUAL.");
		return createBatchAcknowledgement(messages, callback);
	}

	private <T> BatchAcknowledgement<T> createBatchAcknowledgement(Collection<Message<T>> messages,
																   NackAwareAcknowledgementCallback<T> callback) {
		return new PulsarBatchAcknowledgement<>(messages, callback);
	}

	private static class PulsarBatchAcknowledgement<T> implements BatchAcknowledgement<T> {

		private final NackAwareAcknowledgementCallback<T> callback;

		private final Collection<Message<T>> messages;

		public PulsarBatchAcknowledgement(Collection<Message<T>> messages, NackAwareAcknowledgementCallback<T> callback) {
			this.messages = messages;
			this.callback = callback;
		}

		@Override
		public void acknowledge() {
			acknowledgeAsync().join();
		}

		@Override
		public CompletableFuture<Void> acknowledgeAsync() {
			return callback.onAcknowledge(this.messages);
		}

		@Override
		public CompletableFuture<Void> acknowledgeAsync(Collection<Message<T>> messagesToAcknowledge) {
			return callback.onAcknowledge(messagesToAcknowledge);
		}

		@Override
		public void acknowledge(Collection<Message<T>> messagesToAcknowledge) {
			this.acknowledgeAsync().join();
		}

		@Override
		public void nack() {
			this.callback.onNack(this.messages);
		}

		@Override
		public void nack(Collection<Message<T>> messages) {
			this.callback.onNack(messages);
		}
	}

}
