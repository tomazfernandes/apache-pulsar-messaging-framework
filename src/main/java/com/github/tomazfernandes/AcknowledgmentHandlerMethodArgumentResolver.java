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

import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class AcknowledgmentHandlerMethodArgumentResolver implements HandlerMethodArgumentResolver {

	@Override
	public boolean supportsParameter(MethodParameter parameter) {
		return ClassUtils.isAssignable(Acknowledgement.class, parameter.getParameterType());
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object resolveArgument(MethodParameter parameter, Message<?> message) {
		NackAwareAcknowledgementCallback<Object> callback = message.getHeaders()
				.get(PulsarHeaders.PULSAR_ACKNOWLEDGEMENT_CALLBACK_HEADER, NackAwareAcknowledgementCallback.class);
		Assert.notNull(callback, "No acknowledgement found for message " + MessageHeaderUtils.getId(message)
				+ ". AcknowledgeMode should be MANUAL.");
		return new PulsarAcknowledgement((Message<Object>) message, callback);
	}

	private static class PulsarAcknowledgement implements Acknowledgement {

		private final NackAwareAcknowledgementCallback<Object> callback;

		private final Message<Object> message;

		public PulsarAcknowledgement(Message<Object> message, NackAwareAcknowledgementCallback<Object> callback) {
			this.message = message;
			this.callback = callback;
		}

		@Override
		public void acknowledge() {
			acknowledgeAsync().join();
		}

		@Override
		public CompletableFuture<Void> acknowledgeAsync() {
			return this.callback.onAcknowledge(this.message);
		}

		@Override
		public void nack() {
			this.callback.onNack(this.message);
		}
	}

}
