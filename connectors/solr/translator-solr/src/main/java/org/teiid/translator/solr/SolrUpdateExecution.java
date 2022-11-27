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
package org.teiid.translator.solr;

import static org.teiid.language.visitor.SQLStringVisitor.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.solr.SolrQueryExecution.SolrDocumentCallback;

public class SolrUpdateExecution implements UpdateExecution {
    private SolrExecutionFactory ef;
    private SolrConnection connection;
    private Command command;
    private int updateCount = 0;
    private RuntimeMetadata metadata;
    private ExecutionContext executionContext;

    public SolrUpdateExecution(SolrExecutionFactory ef,
            Command command, ExecutionContext executionContext,
            RuntimeMetadata metadata, SolrConnection connection) {
        this.ef = ef;
        this.command = command;
        this.connection = connection;
        this.executionContext = executionContext;
        this.metadata = metadata;
    }

    @Override
    public void execute() throws TranslatorException {
        process(this.command);
    }

    private void process(Command cmd) throws TranslatorException {
        if (cmd instanceof Insert) {
            performInsert((Insert)cmd);
        }
        else if (cmd instanceof Update) {
            performUpdate((Update)cmd);
        }
        else if (cmd instanceof Delete) {
            performUpdate((Delete)cmd);
        }
    }

    private void performUpdate(Delete obj) throws TranslatorException {
        Table table = obj.getTable().getMetadataObject();
        KeyRecord pk = table.getPrimaryKey();
        final String id = getRecordName(pk.getColumns().get(0));

        if (obj.getParameterValues() != null) {
            throw new TranslatorException(SolrPlugin.Event.TEIID20008, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20008));
        }

        SolrQueryExecution query = new SolrQueryExecution(ef, obj, this.executionContext, this.metadata, this.connection);
        query.execute();

        final UpdateRequest request = new UpdateRequest();
        query.walkDocuments(new SolrDocumentCallback() {
            @Override
            public void walk(SolrDocument doc) {
                SolrUpdateExecution.this.updateCount++;
                request.deleteById(doc.getFieldValue(id).toString());
            }
        });

        UpdateResponse response = this.connection.update(request);
        if (response.getStatus() != 0) {
            throw new TranslatorException(SolrPlugin.Event.TEIID20005, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20005, response.getStatus()));
        }
    }

    /**
     * Did not find any other suitable way to pass the query through solrj otherthan walking the documents,
     * all the examples were at the passing xml based query. so that would be a good update if the below does
     * not performs or gets into OOM
     * @param obj
     * @throws TranslatorException
     */
    private void performUpdate(final Update obj) throws TranslatorException {

        if (obj.getParameterValues() != null) {
            throw new TranslatorException(SolrPlugin.Event.TEIID20009, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20009));
        }

        SolrQueryExecution query = new SolrQueryExecution(ef, obj, this.executionContext, this.metadata, this.connection);
        query.execute();

        final UpdateRequest request = new UpdateRequest();

        query.walkDocuments(new SolrDocumentCallback() {
            @Override
            public void walk(SolrDocument doc) {
                SolrUpdateExecution.this.updateCount++;

                Table table = obj.getTable().getMetadataObject();
                SolrInputDocument updateDoc = new SolrInputDocument();
                for (String name:doc.getFieldNames()){
                    if (table.getColumnByName(name) != null){
                        updateDoc.setField(name, doc.getFieldValue(name));
                    }
                }

                int elementCount = obj.getChanges().size();
                for (int i = 0; i < elementCount; i++) {
                    String columnName = SolrSQLHierarchyVistor.getColumnName(obj.getChanges().get(i).getSymbol());
                    Literal value = (Literal)obj.getChanges().get(i).getValue();
                    updateDoc.setField(columnName, value.getValue());
                }
                request.add(updateDoc);
            }
        });

        if (request.getDocuments() != null && !request.getDocuments().isEmpty()){
            UpdateResponse response = this.connection.update(request);
            if (response.getStatus() != 0) {
                throw new TranslatorException(SolrPlugin.Event.TEIID20004, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20004, response.getStatus()));
            }
        }
    }

    private void performInsert(Insert insert) throws TranslatorException {
        // build insert
        List<ColumnReference> columns = insert.getColumns();
        if (insert.getParameterValues() == null) {
            final UpdateRequest request = new UpdateRequest();
            SolrInputDocument doc = new SolrInputDocument();
            List<Expression> values = ((ExpressionValueSource)insert.getValueSource()).getValues();
            for (int i = 0; i < columns.size(); i++) {
                String columnName = SolrSQLHierarchyVistor.getColumnName(columns.get(i));
                Object value = values.get(i);
                if (value instanceof Literal) {
                    doc.addField(columnName, ((Literal)value).getValue());
                }
                else {
                    throw new TranslatorException(SolrPlugin.Event.TEIID20002, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20002));
                }
            }
            this.updateCount++;
            request.add(doc);

            // check if the row already exists
            Select q = buildSelectQuery(insert);
            SolrQueryExecution query = new SolrQueryExecution(ef, q, this.executionContext, this.metadata, this.connection);
            query.execute();
            query.walkDocuments(new SolrDocumentCallback() {
                @Override
                public void walk(SolrDocument doc) {
                    request.clear();
                }
            });

            if (request.getDocuments().isEmpty()){
                throw new TranslatorException(SolrPlugin.Event.TEIID20007, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20007));
            }

            // write the mutation
            UpdateResponse response = this.connection.update(request);
            if (response.getStatus() != 0) {
                throw new TranslatorException(SolrPlugin.Event.TEIID20003, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20003, response.getStatus()));
            }
        }
        else {
            UpdateRequest request = new UpdateRequest();
            int batchSize = 1024;
            // bulk insert; should help
            Iterator<? extends List<?>> args = insert.getParameterValues();
            while (args.hasNext()) {
                List<?> arg = args.next();
                SolrInputDocument doc = new SolrInputDocument();
                for (int i = 0; i < columns.size(); i++) {
                    String columnName = SolrSQLHierarchyVistor.getColumnName(columns.get(i));
                    doc.addField(columnName, arg.get(i));
                }
                this.updateCount++;
                request.add(doc);

                if ((this.updateCount%batchSize) == 0) {
                    UpdateResponse response = this.connection.update(request);
                    if (response.getStatus() != 0) {
                        throw new TranslatorException(SolrPlugin.Event.TEIID20003, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20003, response.getStatus()));
                    }
                    request = new UpdateRequest();
                }
            }
            if (request.getDocuments()!= null && !request.getDocuments().isEmpty()) {
                // write the mutation
                UpdateResponse response = this.connection.update(request);
                if (response.getStatus() != 0) {
                    throw new TranslatorException(SolrPlugin.Event.TEIID20003, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20003, response.getStatus()));
                }
            }
        }
    }

    private Select buildSelectQuery(Insert insert) throws TranslatorException {
        Table table = insert.getTable().getMetadataObject();
        KeyRecord pk = table.getPrimaryKey();
        final String id = getRecordName(pk.getColumns().get(0));

        NamedTable g = insert.getTable();
        List<DerivedColumn> symbols = new ArrayList<DerivedColumn>();
        for (Column column:table.getColumns()){
            String columnName = getRecordName(column);
            symbols.add(new DerivedColumn(columnName, new ColumnReference(g, columnName, column, column.getJavaType())));
        }

        List groups = new ArrayList();
        groups.add(g);

        ColumnReference idCol = new ColumnReference(g, id, table.getColumnByName(id), table.getColumnByName(id).getJavaType());
        Comparison cc = new Comparison(idCol, getPKValue(id, insert), Operator.EQ);

        Select q = new Select(symbols, false, groups, cc, null, null, null);
        return q;
    }

    private Literal getPKValue(String pk, Insert insert) throws TranslatorException {
        List<ColumnReference> columns = insert.getColumns();
        List<Expression> values = ((ExpressionValueSource)insert.getValueSource()).getValues();
        for (int i = 0; i < columns.size(); i++) {
            String columnName = SolrSQLHierarchyVistor.getColumnName(columns.get(i));
            Object value = values.get(i);
            if (columnName.equals(pk)){
                if (value instanceof Literal) {
                    return (Literal)value;
                }
                throw new TranslatorException(SolrPlugin.Event.TEIID20002, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20002));
            }
        }
        throw new TranslatorException(SolrPlugin.Event.TEIID20005, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20005));
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException,
            TranslatorException {
        return new int [] {this.updateCount};
    }
    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }
}
