/*
 * RecordTypesProperty.java
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

package com.apple.foundationdb.record.query.plan.temp.properties;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexEntrySourceScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnorderedUnionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.TypeFilterExpression;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A property visitor that determines the set of record type names (as Strings) that a {@link RelationalExpression}
 * could produce. This property is used in determining whether type filters are necessary, among other things.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordTypesProperty implements PlannerProperty<Set<String>> {
    @Nonnull
    private final PlanContext context;

    private RecordTypesProperty(@Nonnull PlanContext context) {
        this.context = context;
    }

    @Nonnull
    @Override
    public Set<String> evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Set<String>> childResults) {
        // shouldVisit() ensures that we only visit relational planner expressions
        // If we mess this up, better to find out sooner rather than later.

        if (expression instanceof RecordQueryScanPlan ||
                expression instanceof FullUnorderedScanExpression) {
            return context.getMetaData().getRecordTypes().keySet();
        } else if (expression instanceof RecordQueryPlanWithIndex) {
            Index index = context.getIndexByName(((RecordQueryPlanWithIndex)expression).getIndexName());
            return context.getMetaData().recordTypesForIndex(index).stream()
                    .map(RecordType::getName).collect(Collectors.toSet());
        } else if (expression instanceof TypeFilterExpression) {
            return Sets.filter(childResults.get(0), ((TypeFilterExpression)expression).getRecordTypes()::contains);
        } else if (expression instanceof IndexEntrySourceScanExpression) {
            String indexName = ((IndexEntrySourceScanExpression)expression).getIndexName();
            if (indexName == null) {
                // TODO: This isn't quite right, because we might have matched a common prefix of the (non-common)
                // primary key and thus restricted the set of types that could be returned. Getting it right seems tricky.
                return context.getMetaData().getRecordTypes().keySet();
            } else {
                Index index = context.getIndexByName(indexName);
                return context.getMetaData().recordTypesForIndex(index).stream()
                        .map(RecordType::getName).collect(Collectors.toSet());
            }
        } else if (childResults.isEmpty()) {
            throw new RecordCoreException("tried to find record types for a relational expression with no children" +
                                          "but case wasn't handled");
        } else {
            int nonNullChildResult = 0;
            Set<String> firstChildResult = null;
            for (Set<String> result : childResults) {
                if (result != null) {
                    nonNullChildResult++;
                    if (firstChildResult == null) {
                        firstChildResult = result;
                    }
                }
            }

            if (nonNullChildResult == 1) {
                return firstChildResult;
            } else  {
                // If we have a single child, then there is a reasonable default for how most relational expressions will
                // change the set of record types (i.e., they won't change them at all). However, if you have several relational
                // children (like a union or intersection expression) then we must specify some way to combine them.
                if (expression instanceof RecordQueryUnionPlan ||
                        expression instanceof RecordQueryUnorderedUnionPlan ||
                        expression instanceof RecordQueryIntersectionPlan ||
                        expression instanceof LogicalUnorderedUnionExpression) {
                    final Set<String> union = new HashSet<>();
                    for (Set<String> childResulSet : childResults) {
                        union.addAll(childResulSet);
                    }
                    return union;
                } else {
                    throw new RecordCoreException("tried to find record types for a relational expression with multiple " +
                                                  "relational children, but no combiner was specified");
                }
            }
        }
    }

    @Nonnull
    @Override
    public Set<String> evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref,
                                     @Nonnull List<Set<String>> memberResults) {
        final Set<String> union = new HashSet<>();
        for (Set<String> resultSet : memberResults) {
            union.addAll(resultSet);
        }
        return union;
    }

    @Nonnull
    public static Set<String> evaluate(@Nonnull PlanContext context, ExpressionRef<? extends RelationalExpression> ref) {
        return ref.acceptPropertyVisitor(new RecordTypesProperty(context));
    }

    @Nonnull
    public static Set<String> evaluate(@Nonnull PlanContext context, @Nonnull RelationalExpression ref) {
        return ref.acceptPropertyVisitor(new RecordTypesProperty(context));
    }
}
