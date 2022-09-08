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

import io.awspring.cloud.sqs.config.AbstractEndpoint;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.Assert;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Tomaz Fernandes
 * @since 0.1.0
 */
public abstract class AbstractPayloadTypeResolvingEndpoint extends AbstractEndpoint {

    private Method method;

    private Collection<HandlerMethodArgumentResolver> resolvers;

    protected AbstractPayloadTypeResolvingEndpoint(Collection<String> queueNames, String listenerContainerFactoryName, String id) {
        super(queueNames, listenerContainerFactoryName, id);
    }

    public void setResolvers(Collection<HandlerMethodArgumentResolver> resolvers) {
        Assert.notNull(resolvers, "resolvers must not be null");
        this.resolvers = resolvers;
    }

    @Override
    public void setMethod(Method method) {
        this.method = method;
        super.setMethod(method);
    }

    public Class<?> determinePayloadClass() {
        return getMethodParameters()
                .stream()
                .filter(parameter -> this.resolvers.stream().noneMatch(resolver -> resolver.supportsParameter(parameter)))
                .findFirst()
                .map(this::resolveType)
                .orElseThrow(() -> new IllegalArgumentException("could not infer payload type for method " + this.method.getName()));
    }

    private Class<?> resolveType(MethodParameter parameter) {
        ResolvableType resolvableType = ResolvableType.forMethodParameter(parameter);
        Class<?> rawClass = getNotNull(resolvableType.getRawClass(), parameter);
        if (!isEnclosingType(rawClass)) {
            return rawClass;
        }
        Class<?> firstGeneric = getNotNull(resolvableType.getNested(2).getRawClass(), parameter);
        if (!isMessageClass(firstGeneric)) {
            return firstGeneric;
        }
        // Collection<Message<T>>
        return getNotNull(resolvableType.getNested(3).getRawClass(), parameter);
    }

    private Class<?> getNotNull(Class<?> rawClass, MethodParameter parameter) {
        Assert.notNull(rawClass, "could not determine raw class for parameter " + parameter.getParameterName());
        return rawClass;
    }

    private boolean isMessageClass(Class<?> rawClass) {
        return rawClass.isAssignableFrom(Message.class) || rawClass.isAssignableFrom(org.apache.pulsar.client.api.Message.class);
    }

    private boolean isEnclosingType(Class<?> rawClass) {
        return rawClass.isAssignableFrom(List.class)
                || isMessageClass(rawClass);
    }

    private List<MethodParameter> getMethodParameters() {
        return IntStream.range(0, BridgeMethodResolver.findBridgedMethod(this.method).getParameterCount())
                .mapToObj(index -> new MethodParameter(this.method, index)).collect(Collectors.toList());
    }

}
