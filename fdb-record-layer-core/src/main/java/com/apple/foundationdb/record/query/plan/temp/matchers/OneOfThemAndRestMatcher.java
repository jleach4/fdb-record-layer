/*
 * OneOfThemAndRestMatcher.java
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
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
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
public class OneOfThemAndRestMatcher<T, T1 extends T, T2 extends T> implements CollectionMatcher<T> {
    @Nonnull
    private final BindingMatcher<T1> selectedDownstream;
    @Nonnull
    private final CollectionMatcher<T2> remainingDownstream;

    private OneOfThemAndRestMatcher(@Nonnull final BindingMatcher<T1> selectedDownstream,
                                    @Nonnull final CollectionMatcher<T2> remainingDownstream) {
        this.selectedDownstream = selectedDownstream;
        this.remainingDownstream = remainingDownstream;
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
        final Stream.Builder<Stream<PlannerBindings>> streams = Stream.builder();

        for (int i = 0; i < in.size(); i ++) {
            @Nullable T selectedMaybe = null;
            final ImmutableList.Builder<T> remainingBuilder = ImmutableList.builder();
            final Iterator<? extends T> iterator = in.iterator();
            for (int j = 0; iterator.hasNext(); j ++) {
                final T t = iterator.next();
                if (j == i) {
                    selectedMaybe = t;
                } else {
                    remainingBuilder.add(t);
                }
            }
            final T selected = Objects.requireNonNull(selectedMaybe);

            final Stream<PlannerBindings> selectedStream = selectedDownstream.bindMatches(outerBindings, selected);
            final Stream<PlannerBindings> remainingStream = remainingDownstream.bindMatches(outerBindings, remainingBuilder.build());

            streams.add(selectedStream.flatMap(selectedBindings -> remainingStream.map(selectedBindings::mergedWith)));
        }
        return streams.build().flatMap(Function.identity());
    }

    @Nonnull
    public static <T, T1 extends T, T2 extends T> OneOfThemAndRestMatcher<T, T1, T2> oneOfThemAndRest(@Nonnull final BindingMatcher<T1> selectedDownstream,
                                                                                                      @Nonnull final CollectionMatcher<T2> remainingDownstream) {
        return new OneOfThemAndRestMatcher<>(selectedDownstream, remainingDownstream);
    }
}
