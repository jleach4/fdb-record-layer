/*
 * TListMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A <code>BindingMatcher</code> is an expression that can be matched against a
 * {@link RelationalExpression} tree, while binding certain expressions/references in the tree to expression matcher objects.
 * The bindings can be retrieved from the rule call once the binding is matched.
 *
 * <p>
 * Extreme care should be taken when implementing <code>ExpressionMatcher</code>, since it can be very delicate.
 * In particular, expression matchers may (or may not) be reused between successive rule calls and should be stateless.
 * Additionally, implementors of <code>ExpressionMatcher</code> must use the (default) reference equals.
 * </p>
 * @param <T> the bindable type that this matcher binds to
 */
@API(API.Status.EXPERIMENTAL)
public class TListMatcher<T> implements CollectionMatcher<T> {
    private final List<? extends BindingMatcher<T>> downstreams;

    private TListMatcher(@Nonnull final List<? extends BindingMatcher<T>> downstreams) {
        this.downstreams = downstreams;
    }

    /**
     * Attempt to match this matcher against the given expression reference.
     * Note that implementations of {@code matchWith()} should only attempt to match the given root with this planner
     * expression or attempt to access the members of the given reference.
     *
     * @param outerBindings preexisting bindings to be used by the matcher
     * @param in the bindable we attempt to match
     * @return a stream of {@link PlannerBindings} containing the matched bindings, or an empty stream is no match was found
     */

    @Nonnull
    @SuppressWarnings("java:S3958")
    public Stream<PlannerBindings> bindMatchesSafely(@Nonnull PlannerBindings outerBindings, @Nonnull Collection<? extends T> in) {
        if (in.size() != downstreams.size()) {
            return Stream.empty();
        }

        Stream<PlannerBindings> bindingStream = Stream.of(PlannerBindings.empty());
        final Iterator<? extends BindingMatcher<?>> downstreamIterator = downstreams.iterator();
        for (final T item : in) {
            final BindingMatcher<?> downstream = downstreamIterator.next();
            final List<PlannerBindings> individualBindings = downstream.bindMatches(outerBindings, item).collect(Collectors.toList());
            if (individualBindings.isEmpty()) {
                return Stream.empty();
            } else {
                bindingStream = bindingStream.flatMap(existing -> individualBindings.stream().map(existing::mergedWith));
            }
        }
        return bindingStream;
    }

    @Nonnull
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> TListMatcher<T> exactly(@Nonnull final BindingMatcher<T>... downstreams) {
        return new TListMatcher<>(Arrays.asList(downstreams));
    }

    @Nonnull
    public static <T> TListMatcher<T> exactly(@Nonnull final List<? extends BindingMatcher<T>> downstreams) {
        return new TListMatcher<>(downstreams);
    }
}
