/*
 * CalciteQueryPlanner.java
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
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.QueryPlanResult;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;

import javax.annotation.Nonnull;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class CalciteQueryPlanner implements QueryPlanner, Planner {
    public static final RelDataTypeFactory SQL_TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    public static final JavaTypeFactory JAVA_TYPE_FACTORY = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private RecordMetaData recordMetaData;
    private Planner planner;
    private HashMap<String, MaterializedViewTable> materializedViews = new HashMap<>();
    private FrameworkConfig config;

    public CalciteQueryPlanner (RecordMetaData recordMetaData) {
        this.recordMetaData = recordMetaData;
        this.planner = createPlanner();
    }

    public Schema generateSchema() {
    CalciteSchema calciteSchema = CalciteSchema.createRootSchema(true);
        recordMetaData.getRecordTypes().forEach((name, recordType) -> {
            calciteSchema.add(name, new CloudKitTable(recordType));
            recordType.getIndexes().forEach((index) -> {
                List<String> viewPath = calciteSchema.path(index.getName());
                RelDataTypeFactory.Builder builder = SQL_TYPE_FACTORY.builder();
                index.getRootExpression().buildDataType(builder, recordType.getDescriptor());
                RelDataType rowType = builder.build();
                String sql = index.getRootExpression().getSQL(recordType);
                MaterializedViewTable viewTable = new MaterializedViewTable(JAVA_TYPE_FACTORY.getJavaClass(rowType), RelDataTypeImpl.proto(rowType), sql, ImmutableList.of("CloudKit"), null, null);
                calciteSchema.add(index.getName(), viewTable);
                calciteSchema.add(index.getName(), new AbstractTable() {
                    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                        return rowType;
                    }
                });
            });
        });
        return calciteSchema.plus();
    }


    public Planner createPlanner() {
        return createPlanner(generateSchema());
    }

    public Planner createPlanner(Schema schema) {
            Properties properties = new Properties();
            properties.setProperty(
                    CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
                    Boolean.toString(true));
            properties.setProperty(
                CalciteConnectionProperty.CREATE_MATERIALIZATIONS.camelName(),
                Boolean.toString(true));

        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder()
                .defaultSchema(CalciteSchema.createRootSchema(false).add("CloudKit", schema).plus())
                .parserConfig(SqlParser.configBuilder(SqlParser.Config.DEFAULT)
                        .setCaseSensitive(false)
                        .build())
                .context(Contexts.of(new CalciteConnectionConfigImpl(properties)))
                .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
                .programs(Programs.standard());

        config = configBuilder.build();
        Planner planner = Frameworks.getPlanner(configBuilder.build());
        config = configBuilder.context(Contexts.of((RelOptTable.ViewExpander) planner,new CalciteConnectionConfigImpl(properties))).build();
        return planner;
    }

    public RelNode findBestExp(String sql) throws SqlParseException, RelConversionException, ValidationException {
        SqlNode node = planner.parse(sql);
        final SqlNode query2 = planner.validate(node);
        RelNode root = planner.rel(query2).project();
        RelOptCluster cluster = root.getCluster();
        final RelOptPlanner optPlanner = cluster.getPlanner();
        RelTraitSet desiredTraits =
                cluster.traitSet().replace(EnumerableConvention.INSTANCE);
        final RelNode newRoot = optPlanner.changeTraits(root, desiredTraits);
        optPlanner.setRoot(newRoot);

        System.out.println(
                RelOptUtil.dumpPlan("-- Logical Plan", newRoot, SqlExplainFormat.TEXT,
                        SqlExplainLevel.DIGEST_ATTRIBUTES));
        final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster,
                new CalciteCatalogReader(CalciteSchema.from(config.getDefaultSchema()), Collections.singletonList("CloudKit"), planner.getTypeFactory(), config.getContext().unwrap(CalciteConnectionConfig.class)));

       // RelNode scan = relBuilder.scan("MyHierarchicalRecord").project(relBuilder.field("num_value_indexed")).sort(relBuilder.field("num_value_indexed")).build();

       // final RelNode replacement = relBuilder.scan("MyHierarchicalRecord$num_value_indexed").build();
       // optPlanner.addMaterialization(new RelOptMaterialization(replacement, scan, null, Lists.newArrayList("MyHierarchicalRecord$num_value_indexed")));
        RelNode bestExp = optPlanner.findBestExp();
//        List <RelOptMaterialization> materializations = new ArrayList<>();
//        List < RelOptLattice > lattices = new ArrayList<>();
//        bestExp = Programs.standard().run(optPlanner, newRoot, desiredTraits, materializations, lattices);
        System.out.println(
                RelOptUtil.dumpPlan("-- Best Plan", bestExp, SqlExplainFormat.TEXT,
                        SqlExplainLevel.DIGEST_ATTRIBUTES));
        return bestExp;
    }

    @Nonnull
    @Override
    public RecordQueryPlan plan(@Nonnull RecordQuery query) {
        return null;
    }

    @Override
    public void setIndexScanPreference(@Nonnull IndexScanPreference indexScanPreference) {

    }

    @Override
    public SqlNode parse(String sql) throws SqlParseException {
        return planner.parse(sql);
    }

    @Override
    public SqlNode parse(Reader source) throws SqlParseException {
        return planner.parse(source);
    }

    @Override
    public SqlNode validate(SqlNode sqlNode) throws ValidationException {
        return planner.validate(sqlNode);
    }

    @Override
    public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) throws ValidationException {
        return planner.validateAndGetType(sqlNode);
    }

    @Override
    public RelRoot rel(SqlNode sql) throws RelConversionException {
        return planner.rel(sql);
    }

    @Override
    @Deprecated
    public RelNode convert(SqlNode sql) throws RelConversionException {
        return planner.convert(sql);
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
        return planner.getTypeFactory();
    }

    @Override
    public RelNode transform(int ruleSetIndex, RelTraitSet requiredOutputTraits, RelNode rel) throws RelConversionException {
        return planner.transform(ruleSetIndex, requiredOutputTraits, rel);
    }

    @Override
    public void reset() {
        planner.reset();
    }

    @Override
    public void close() {
        planner.close();
    }

    @Override
    public RelTraitSet getEmptyTraitSet() {
        return planner.getEmptyTraitSet();
    }

    @Nonnull
    @Override
    public QueryPlanResult planQuery(@Nonnull final RecordQuery query) {
        return null;
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return null;
    }

    @Nonnull
    @Override
    public RecordStoreState getRecordStoreState() {
        return null;
    }
}
