/*
 * FullTextQueryComponent.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class LuceneQueryComponent implements QueryComponent, ComponentWithComparison {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Lucene-Query");

    private final String query;
    private final Comparisons.Comparison comparison;

    public LuceneQueryComponent(String query) {
        this.query = query;
        this.comparison = new Comparisons.LuceneComparison(query);
    }

    @Nonnull
    @Override
    public <M extends Message> Boolean evalMessage(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final Message message) {
        return true;
    }

    @Override
    public void validate(@Nonnull final Descriptors.Descriptor descriptor) {
        // No-op
    }

    @Nonnull
    @Override
    public Comparisons.Comparison getComparison() {
        return comparison;
    }

    @Override
    public QueryComponent withOtherComparison(final Comparisons.Comparison comparison) {
        return null;
    }

    @Override
    public String getName() {
        return "LuceneQuery";
    }

    @Nonnull
    @Override
    public QueryPredicate normalizeForPlanner(@Nonnull final Source source, @Nonnull final List<String> fieldNamePrefix) {
        return null; // TODO FIX
    }

    @Override
    public int planHash() {
        return query.hashCode();
    }

    @Override
    public String toString() {
        return "LuceneQuery(" + query + ")";
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, query);
    }
}
