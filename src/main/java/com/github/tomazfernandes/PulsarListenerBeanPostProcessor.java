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

import io.awspring.cloud.sqs.annotation.AbstractListenerAnnotationBeanPostProcessor;
import io.awspring.cloud.sqs.config.Endpoint;
import io.awspring.cloud.sqs.config.EndpointRegistrar;
import io.awspring.cloud.sqs.support.resolver.BatchPayloadMethodArgumentResolver;
import org.springframework.core.MethodParameter;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.HeaderMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.HeadersMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.MessageMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public class PulsarListenerBeanPostProcessor extends AbstractListenerAnnotationBeanPostProcessor<PulsarListener> {

    private static final String GENERATED_ID_PREFIX = "org.apache.pulsar.spring.container";

    private static final String DEFAULT_PULSAR_CONTAINER_FACTORY_BEAN_NAME = "defaultPulsarContainerFactory";

    private final CompositeHandlerMethodArgumentResolver compositeResolver = new CompositeHandlerMethodArgumentResolver();

    private EndpointRegistrar endpointRegistrar;

    @Override
    protected Class<PulsarListener> getAnnotationClass() {
        return PulsarListener.class;
    }

    @Override
    protected Endpoint createEndpoint(PulsarListener annotation) {
        Set<String> topics = resolveStringArray(annotation.value(), "topics");
        String factory = resolveAsString(annotation.factory(), "factory");
        String id = resolveAsString(annotation.id(), "id");
        String topicsPattern = resolveAsString(annotation.topicsPattern(), "topicPattern");
        Set<String> topicsToUse = getTopicsToUse(topics, topicsPattern);
        PulsarListenerEndpoint endpoint = new PulsarListenerEndpoint(topicsToUse, factory, id);
        endpoint.setSubscriptionName(resolveAsString(annotation.subscriptionName(), "subscriptionName"));
        endpoint.setTopicsPattern(topicsPattern);
        endpoint.setConcurrency(resolveAsInteger(annotation.concurrency(), "concurrency"));
        endpoint.setResolvers(Collections.singletonList(this.compositeResolver));
        return endpoint;
    }

    protected List<HandlerMethodArgumentResolver> createArgumentResolvers(MessageConverter messageConverter) {
        // Override Acknowledgement resolvers
        return Arrays.asList(
                new AcknowledgmentHandlerMethodArgumentResolver(),
                new BatchAcknowledgmentHandlerMethodArgumentResolver(),
                new ConsumerHandlerMethodArgumentResolver(),
                new HeaderMethodArgumentResolver(new DefaultConversionService(), getConfigurableBeanFactory()),
                new HeadersMethodArgumentResolver(),
                new BatchPayloadMethodArgumentResolver(messageConverter),
                new MessageMethodArgumentResolver(messageConverter),
                new PayloadMethodArgumentResolver(messageConverter));
    }

    private List<HandlerMethodArgumentResolver> getAllResolversCopy() {
        CompositeMessageConverter compositeMessageConverter = createCompositeMessageConverter();
        List<HandlerMethodArgumentResolver> methodArgumentResolvers = new ArrayList<>(
                createAdditionalArgumentResolvers());
        methodArgumentResolvers.addAll(createArgumentResolvers(compositeMessageConverter));
        this.endpointRegistrar.getMethodArgumentResolversConsumer().accept(methodArgumentResolvers);
        return methodArgumentResolvers
                .stream()
                .filter(this::excludePayloadResolvers)
                .collect(Collectors.toList());
    }

    private boolean excludePayloadResolvers(HandlerMethodArgumentResolver resolver) {
        return !(resolver instanceof PayloadMethodArgumentResolver
                || resolver instanceof BatchPayloadMethodArgumentResolver
                || resolver instanceof PulsarMessageMethodArgumentResolver
                || resolver instanceof MessageMethodArgumentResolver);
    }

    private Set<String> getTopicsToUse(Set<String> topics, String topicsPattern) {
        return topics.isEmpty()
                ? validateAndGetTopicsPattern(topicsPattern)
                : topics;
    }

    @Override
    protected void configureDefaultHandlerMethodFactory(DefaultMessageHandlerMethodFactory handlerMethodFactory) {
        super.configureDefaultHandlerMethodFactory(handlerMethodFactory);
        this.compositeResolver.setArgumentResolvers(getAllResolversCopy());
    }

    private Set<String> validateAndGetTopicsPattern(String topicsPattern) {
        Assert.hasText(topicsPattern, "Either topics or topicsPattern must be provided");
        Set<String> topicsToUse = new HashSet<>();
        topicsToUse.add(topicsPattern);
        return topicsToUse;
    }

    @Override
    protected String resolveAsString(String value, String propertyName) {
        String resolved = super.resolveAsString(value, propertyName);
        return StringUtils.hasText(resolved) ? resolved : null;
    }

    @Override
    protected String getGeneratedIdPrefix() {
        return GENERATED_ID_PREFIX;
    }

    @Override
    protected EndpointRegistrar createEndpointRegistrar() {
        this.endpointRegistrar = new EndpointRegistrar();
        this.endpointRegistrar.setDefaultListenerContainerFactoryBeanName(DEFAULT_PULSAR_CONTAINER_FACTORY_BEAN_NAME);
        return this.endpointRegistrar;
    }

    @Override
    protected Collection<HandlerMethodArgumentResolver> createAdditionalArgumentResolvers() {
        return Collections.singletonList(new PulsarMessageMethodArgumentResolver());
    }

    private static class CompositeHandlerMethodArgumentResolver implements HandlerMethodArgumentResolver {

        private List<HandlerMethodArgumentResolver> argumentResolvers;

        public void setArgumentResolvers(List<HandlerMethodArgumentResolver> argumentResolvers) {
            Assert.notEmpty(argumentResolvers, "argumentResolvers cannot be empty");
            this.argumentResolvers = argumentResolvers;
        }

        public List<HandlerMethodArgumentResolver> getArgumentResolvers() {
            return this.argumentResolvers;
        }

        @Override
        public boolean supportsParameter(MethodParameter parameter) {
            return this.argumentResolvers.stream().anyMatch(resolver -> resolver.supportsParameter(parameter));
        }

        @Override
        public Object resolveArgument(MethodParameter parameter, Message<?> message) throws Exception {
            return this.argumentResolvers.stream().filter(resolver -> resolver.supportsParameter(parameter))
                    .findFirst()
                    .map(resolver -> tryResolve(parameter, message, resolver))
                    .orElseThrow(() -> new IllegalArgumentException("No resolver found for parameter " + parameter.getParameterName()));
        }

        private Object tryResolve(MethodParameter parameter, Message<?> message, HandlerMethodArgumentResolver resolver) {
            try {
                return resolver.resolveArgument(parameter, message);
            } catch (Exception e) {
                throw new IllegalArgumentException("Could not resolve parameter " + parameter.getParameterName());
            }
        }
    }

}
