/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.dqp.internal.process;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.sql.lang.Criteria;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.tempdata.BaseIndexInfo;
import org.teiid.query.util.CommandContext;

class SchemaRecordTable extends RecordTable<Schema> {

    public SchemaRecordTable(int pkColumnIndex, List<ElementSymbol> columns) {
        super(new int[] {0}, columns.subList(pkColumnIndex, pkColumnIndex + 1));
    }

    protected boolean isValid(Schema s, VDBMetaData vdb, List<Object> rowBuffer, Criteria condition, CommandContext commandContext) throws TeiidProcessingException, TeiidComponentException {
        if (s == null || !vdb.isVisible(s.getName())) {
            return false;
        }
        return super.isValid(s, vdb, rowBuffer, condition, commandContext);
    }

    @Override
    public SimpleIterator<Schema> processQuery(
            VDBMetaData vdb, CompositeMetadataStore metadataStore,
            BaseIndexInfo<?> ii, TransformationMetadata metadata, CommandContext commandContext) {
        return processQuery(vdb, metadataStore.getSchemas(), ii, commandContext);
    }

}

abstract class SchemaChildRecordTable<T extends AbstractMetadataRecord> extends RecordTable<T> {

    private SchemaRecordTable schemaTable;

    public SchemaChildRecordTable(int schemaPkColumnIndex, int tablePkColumnIndex, List<ElementSymbol> columns) {
        super(new int[] {0}, columns.subList(tablePkColumnIndex, tablePkColumnIndex + 1));
        this.schemaTable = new SchemaRecordTable(schemaPkColumnIndex, columns);
    }

    @Override
    public SimpleIterator<T> processQuery(
            final VDBMetaData vdb, final CompositeMetadataStore metadataStore,
            final BaseIndexInfo<?> ii, final TransformationMetadata metadata, final CommandContext commandContext) {
        final SimpleIterator<Schema> schemas = schemaTable.processQuery(vdb, metadataStore.getSchemas(), ii, commandContext);
        return new ExpandingSimpleIterator<Schema, T>(schemas) {
            @Override
            protected SimpleIterator<T> getChildIterator(
                    Schema parent) {
                return processQuery(vdb, getChildren(parent, metadata), ii.next, commandContext);
            }
        };
    }

    @Override
    public BaseIndexInfo<RecordTable<?>> planQuery(Query query,
            Criteria condition, CommandContext context) {
        BaseIndexInfo<RecordTable<?>> ii = schemaTable.planQuery(query, query.getCriteria(), context);
        ii.next = super.planQuery(query, ii.getNonCoveredCriteria(), context);
        return ii;
    }

    @Override
    protected void fillRow(T s, List<Object> rowBuffer) {
        rowBuffer.add(s.getName());
    }

    protected abstract NavigableMap<String, T> getChildren(Schema s, TransformationMetadata metadata);

}

class ProcedureSystemTable extends SchemaChildRecordTable<Procedure> {
    public ProcedureSystemTable(int schemaPkColumnIndex,
            int tablePkColumnIndex, List<ElementSymbol> columns) {
        super(schemaPkColumnIndex, tablePkColumnIndex, columns);
    }

    @Override
    protected NavigableMap<String, Procedure> getChildren(Schema s, TransformationMetadata metadata) {
        return s.getProcedures();
    }
}

class FunctionSystemTable extends SchemaChildRecordTable<FunctionMethod> {
    public FunctionSystemTable(int schemaPkColumnIndex,
            int tablePkColumnIndex, List<ElementSymbol> columns) {
        super(schemaPkColumnIndex, tablePkColumnIndex, columns);
    }

    @Override
    protected NavigableMap<String, FunctionMethod> getChildren(Schema s, TransformationMetadata metadata) {
        //since there is no proper schema for a UDF model, no results will show up for legacy functions
        if (s.getName().equals(CoreConstants.SYSTEM_MODEL)) {
            //currently all system functions are contributed via alternative mechanisms
            //system source, push down functions.
            FunctionLibrary library = metadata.getFunctionLibrary();
            FunctionTree tree = library.getSystemFunctions();
            FunctionTree[] userFuncs = library.getUserFunctions();
            TreeMap<String, FunctionMethod> functions = new TreeMap<String, FunctionMethod>(String.CASE_INSENSITIVE_ORDER);
            for (FunctionTree userFunc : userFuncs) {
                if (userFunc.getSchemaName().equals(CoreConstants.SYSTEM_MODEL)) {
                    functions.putAll(userFunc.getFunctionsByUuid());
                }
            }
            functions.putAll(tree.getFunctionsByUuid());
            return functions;
        }
        return s.getFunctions();
    }
}

class TableSystemTable extends SchemaChildRecordTable<Table> {
    public TableSystemTable(int schemaPkColumnIndex,
            int tablePkColumnIndex, List<ElementSymbol> columns) {
        super(schemaPkColumnIndex, tablePkColumnIndex, columns);
    }

    @Override
    protected NavigableMap<String, Table> getChildren(Schema s, TransformationMetadata metadata) {
        return s.getTables();
    }
}