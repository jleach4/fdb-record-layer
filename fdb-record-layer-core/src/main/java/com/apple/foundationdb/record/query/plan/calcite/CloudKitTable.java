/*
 * CloudKitTable.java
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

import com.apple.foundationdb.record.metadata.RecordType;
import com.google.protobuf.Descriptors;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.calcite.CalciteQueryPlanner.SQL_TYPE_FACTORY;

public class CloudKitTable extends AbstractTable implements FilterableTable {
    private Statistic statistic;

    private RecordType recordType;


    public CloudKitTable(RecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        return null;
    }

    @Override
    public Statistic getStatistic() {
        return null;
        /*
        if (statistic == null) {
            statistic = new CloudKitStatistic(recordType);
        }
        return statistic;

         */
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeForDescriptor(typeFactory, recordType.getDescriptor());
    }

    public static RelDataType getRelDataType(Descriptors.FieldDescriptor fieldDescriptor) {
        return getRelDataType(SQL_TYPE_FACTORY, fieldDescriptor);
    }

    public static RelDataType getRelDataType(RelDataTypeFactory typeFactory, Descriptors.FieldDescriptor fieldDescriptor) {
        switch(fieldDescriptor.getType()) {
        case DOUBLE:
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        case FLOAT:
            return typeFactory.createSqlType(SqlTypeName.FLOAT);
        case SFIXED64:
        case SINT64:
        case INT64:
        case UINT64:
        case FIXED64:
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        case SINT32:
        case INT32:
        case UINT32:
        case SFIXED32:
        case FIXED32:
            return typeFactory.createSqlType(SqlTypeName.INTEGER);
        case BOOL:
            return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        case STRING:
            return typeFactory.createSqlType(SqlTypeName.VARCHAR);
        case GROUP:
        case MESSAGE:
            return typeForDescriptor(typeFactory, fieldDescriptor.getMessageType());
        case BYTES:
            return typeFactory.createSqlType(SqlTypeName.BINARY);
        case ENUM:
        default:
            throw new RuntimeException("OOPS");
        }
    }

    public static RelDataType getRelDataTypeField(Descriptors.FieldDescriptor fieldDescriptor) {
        return getRelDataType(SQL_TYPE_FACTORY, fieldDescriptor);
    }

    public static RelDataType getRelDataTypeField(RelDataTypeFactory typeFactory, Descriptors.FieldDescriptor fieldDescriptor) {
        return getRelDataType(typeFactory, fieldDescriptor);
    }

    public static RelDataType typeForDescriptor(RelDataTypeFactory typeFactory, Descriptors.Descriptor descriptor) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (Descriptors.FieldDescriptor fieldDescriptor : descriptor.getFields()) {
            builder.add(fieldDescriptor.getName(), getRelDataType(typeFactory, fieldDescriptor));
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return getRowType(SQL_TYPE_FACTORY).toString();
    }
}
