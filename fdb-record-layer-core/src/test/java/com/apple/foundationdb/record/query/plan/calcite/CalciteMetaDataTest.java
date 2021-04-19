/*
 * CalciteMetaDataTest.java
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

package com.apple.foundationdb.record.query.plan.calcite;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecordsMultiProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.Descriptors;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Planner;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static com.apple.foundationdb.record.metadata.MetaDataProtoTest.verifyEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CalciteMetaDataTest {

    private static final Descriptors.FileDescriptor[] BASE_DEPENDENCIES = {
            RecordMetaDataOptionsProto.getDescriptor()
    };

    public static void assertSqlEquals(RecordMetaData recordMetaData, String sql, String expectedPlan) throws Exception {
        CalciteQueryPlanner planner = new CalciteQueryPlanner(recordMetaData);
        RelNode bestExp = planner.findBestExp(sql);
        assertEquals(expectedPlan, RelOptUtil.dumpPlan("", bestExp, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testSortAvoidanceBaseTableNoIndex() throws Exception {
        RecordMetaDataBuilder metaDataBuilder = new RecordMetaDataBuilder(TestRecords3Proto.getDescriptor());
        metaDataBuilder.getOnlyRecordType().setPrimaryKey(Key.Expressions.concatenateFields("parent_path", "child_name"));
        assertSqlEquals(metaDataBuilder.getRecordMetaData(),
                "select * from MyHierarchicalRecord order by num_value_indexed",
                "EnumerableTableScan(table=[[CloudKit, MyHierarchicalRecord]])\n"
                );
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testIndexUse() throws Exception {
        RecordMetaDataBuilder metaDataBuilder = new RecordMetaDataBuilder(TestRecords3Proto.getDescriptor());
        metaDataBuilder.getOnlyRecordType().setPrimaryKey(Key.Expressions.concatenateFields("parent_path", "child_name"));
        metaDataBuilder.addIndex("MyHierarchicalRecord", new Index("MHR$child$parentpath", Key.Expressions.concatenateFields("child_name", "parent_path"), IndexTypes.VALUE));
//        metaDataBuilder.build();
        assertSqlEquals(metaDataBuilder.getRecordMetaData(),
                "select num_value_indexed from MyHierarchicalRecord where num_value_indexed=5",
                "EnumerableTableScan(table=[[CloudKit, MyHierarchicalRecord]])\n"
        );
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testNestedTypes() throws Exception {
        RecordMetaDataBuilder metaDataBuilder = new RecordMetaDataBuilder(TestRecords4Proto.getDescriptor());
//        metaDataBuilder.addIndex("RestaurantRecord", new Index("RR$ratings", Key.Expressions.field("reviews", KeyExpression.FanType.FanOut).nest("rating").ungrouped(), IndexTypes.RANK));
//        metaDataBuilder.removeIndex("RestaurantReviewer$name");
         assertSqlEquals(metaDataBuilder.getRecordMetaData(),
                "select * from RestaurantRecord",
                "EnumerableProject(inputs=[0..1], exprs=[[ROW($2.reviewer, $2.rating), ROW($3.value, $3.weight), $4]])\n" +
                "  EnumerableTableScan(table=[[CloudKit, RestaurantRecord]])\n"
        );
    }

    @Test
    @SuppressWarnings("deprecation")
    public void metadataProtoComplex() throws KeyExpression.DeserializationException, KeyExpression.SerializationException, Exception {
        RecordMetaDataBuilder metaDataBuilder = new RecordMetaDataBuilder(TestRecords3Proto.getDescriptor());
        metaDataBuilder.getOnlyRecordType().setPrimaryKey(Key.Expressions.concatenateFields("parent_path", "child_name"));
        metaDataBuilder.addIndex("MyHierarchicalRecord", new Index("MHR$child$parentpath", Key.Expressions.concatenateFields("child_name", "parent_path"), IndexTypes.VALUE));
        RecordMetaData metaData = metaDataBuilder.getRecordMetaData();

        CalciteQueryPlanner planner = new CalciteQueryPlanner(metaData);
        RelNode bestExp = planner.findBestExp("select * from MyHierarchicalRecord where parent_path = 'sdfsfs' or parent_path = 'xxx' and num_value_indexed = 1 order by parent_path, child_name");
        assertEquals("Root {kind: SELECT, rel: rel#5:LogicalProject#5, rowType: RecordType(VARCHAR parent_path, VARCHAR child_name, INTEGER num_value_indexed), fields: [<0, parent_path>, <1, child_name>, <2, num_value_indexed>], collation: []}", RelOptUtil.dumpPlan("", bestExp, SqlExplainFormat.TEXT,
                SqlExplainLevel.DIGEST_ATTRIBUTES));
/*
        metaDataBuilder = new RecordMetaDataBuilder(TestRecords4Proto.getDescriptor());
        metaDataBuilder.addIndex("RestaurantRecord", new Index("RR$ratings", Key.Expressions.field("reviews", KeyExpression.FanType.FanOut).nest("rating").ungrouped(), IndexTypes.RANK));
        metaDataBuilder.removeIndex("RestaurantReviewer$name");
        metaData = metaDataBuilder.getRecordMetaData();
        metaDataRedone = RecordMetaData.newBuilder().addDependencies(BASE_DEPENDENCIES).setRecords(metaData.toProto()).getRecordMetaData();
        assertEquals(1, metaData.getFormerIndexes().size());
        assertFalse(metaData.isSplitLongRecords());
        assertFalse(metaData.isStoreRecordVersions());
        verifyEquals(metaData, metaDataRedone);
        // TODO JL assertEquals("UnionDescriptor=RecordType(RecordType(BIGINT rest_no, VARCHAR name, RecordType(BIGINT reviewer, INTEGER rating) reviews, RecordType(VARCHAR value, INTEGER weight) tags, VARCHAR customer) _RestaurantRecord, RecordType(BIGINT id, VARCHAR name, VARCHAR email, RecordType(BIGINT start_date, VARCHAR school_name, VARCHAR hometown) stats) _RestaurantReviewer)", metaData.generateSchema().toString());

        metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsMultiProto.getDescriptor());
        metaDataBuilder.addMultiTypeIndex(Arrays.asList(
                metaDataBuilder.getRecordType("MultiRecordOne"),
                metaDataBuilder.getRecordType("MultiRecordTwo"),
                metaDataBuilder.getRecordType("MultiRecordThree")),
                new Index("all$elements", Key.Expressions.field("element", KeyExpression.FanType.Concatenate),
                        Index.EMPTY_VALUE, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        metaDataBuilder.addMultiTypeIndex(Arrays.asList(
                metaDataBuilder.getRecordType("MultiRecordTwo"),
                metaDataBuilder.getRecordType("MultiRecordThree")),
                new Index("two&three$ego", Key.Expressions.field("ego"), Index.EMPTY_VALUE, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        metaDataBuilder.addIndex("MultiRecordOne", new Index("one$name", Key.Expressions.field("name"), IndexTypes.VALUE));
        metaDataBuilder.setRecordCountKey(Key.Expressions.field("blah"));
        metaDataBuilder.removeIndex("one$name");
        metaDataBuilder.setStoreRecordVersions(true);
        metaData = metaDataBuilder.getRecordMetaData();
        assertEquals(2, metaData.getAllIndexes().size());
        assertEquals(1, metaData.getFormerIndexes().size());
        assertTrue(metaData.isSplitLongRecords());
        assertTrue(metaData.isStoreRecordVersions());
        metaDataRedone = RecordMetaData.newBuilder().addDependencies(BASE_DEPENDENCIES).setRecords(metaData.toProto()).getRecordMetaData();
        verifyEquals(metaData, metaDataRedone);
        // TODO JL assertEquals("MultiDescriptor=RecordType(RecordType(BIGINT id, VARCHAR name, VARCHAR element) _MultiRecordOne, RecordType(BIGINT ego, VARCHAR value, VARCHAR element) _MultiRecordTwo, RecordType(BIGINT ego, VARCHAR data, VARCHAR element) _MultiRecordThree)", metaData.generateSchema().toString());
        */
    }

}
