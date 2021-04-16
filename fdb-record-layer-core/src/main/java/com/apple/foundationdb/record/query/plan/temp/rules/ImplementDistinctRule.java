/*
 * ImplementDistinctRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.properties.CreatesDuplicatesProperty;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TListMatcher.exactly;

/**
 * A rule that implements a distinct expression by adding a {@link RecordQueryUnorderedPrimaryKeyDistinctPlan}
 * if necessary. In particular, it will only add that wrapping expression if the underlying plan itself might
 * produce duplicate results.
 *
 * <p>
 * This rule is somewhat suspect. In particular, if the inner plan that it matches against does not produce duplicates,
 * this rule will then return that plan. This is fine unless the plan is later modified in such a way that it then
 * <em>can</em> produce duplicates. At the moment, none of the rules modify a {@link RecordQueryPlan} once it has
 * been produced, but a future rule that does so may cause errors where plans erroneously produce duplicate records.
 * To address that, the plan is to add a mechanism for enforcing properties (e.g., distinctness or sort order)
 * on the plans produced by the planner. See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/635">Issue #653</a>.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementDistinctRule extends PlannerRule<LogicalDistinctExpression> {
    @Nonnull
    private static final BindingMatcher<RecordQueryPlan> innerPlanMatcher = anyPlan();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifier(innerPlanMatcher);
    @Nonnull
    private static final BindingMatcher<LogicalDistinctExpression> root = LogicalDistinctExpression.logicalDistinctExpression(exactly(innerQuantifierMatcher));

    public ImplementDistinctRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final RecordQueryPlan innerPlan = call.get(innerPlanMatcher);
        final Quantifier.ForEach innerQuantifier = call.get(innerQuantifierMatcher);
        final boolean createsDuplicates = CreatesDuplicatesProperty.evaluate(innerPlan, call.getContext());
        if (createsDuplicates) {
            call.yield(call.ref(new RecordQueryUnorderedPrimaryKeyDistinctPlan(
                    Quantifier.physicalBuilder()
                            .morphFrom(innerQuantifier)
                            .build(GroupExpressionRef.of(innerPlan)))));
        } else {
            call.yield(call.ref(innerPlan));
        }
    }
}
