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
package org.teiid.translator.salesforce.execution;


import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.Parameter;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.Util;
import org.teiid.translator.salesforce.execution.visitors.InsertVisitor;

import com.sforce.async.BatchResult;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.async.Result;
import com.sforce.async.SObject;


public class InsertExecutionImpl extends AbstractUpdateExecution {
    private JobInfo activeJob;
    private List<String> batches = new ArrayList<String>();
    private Iterator<? extends List<?>> rowIter;
    private String objectName;
    private List<Integer> counts;

    public InsertExecutionImpl(SalesForceExecutionFactory ef, Command command,
            SalesforceConnection salesforceConnection,
            RuntimeMetadata metadata, ExecutionContext context) {
        super(ef, command, salesforceConnection, metadata, context);

        Insert insert = (Insert)command;
        if (insert.getParameterValues() != null) {
            this.rowIter = insert.getParameterValues();
        }
        InsertVisitor visitor = new InsertVisitor(getMetadata());
        visitor.visit(insert);
        this.objectName = visitor.getTableName();
    }

    @Override
    public void execute() throws TranslatorException {
        Insert insert = (Insert)command;
        if (insert.getParameterValues() == null) {
            DataPayload data = new DataPayload();
            data.setType(this.objectName);
            buildSingleRowInsertPayload(insert, data);
            if (insert.isUpsert()) {
                result = getConnection().upsert(data);
            } else {
                result = getConnection().create(data);
            }
        }
        else {
            if (this.activeJob == null) {
                this.activeJob = getConnection().createBulkJob(this.objectName, insert.isUpsert()?OperationEnum.upsert:OperationEnum.insert, false);
                counts = new ArrayList<Integer>();
            }
            if (this.activeJob.getState() == JobStateEnum.Open) {
                while (this.rowIter.hasNext()) {
                    List<SObject> rows = buildBulkRowPayload(insert, this.rowIter, this.executionFactory.getMaxBulkInsertBatchSize());
                    batches.add(getConnection().addBatch(rows, activeJob));
                }
                this.activeJob = getConnection().closeJob(this.activeJob.getId());
            }
            if (this.activeJob.getState() == JobStateEnum.Aborted) {
                throw new TranslatorException(this.activeJob.getState().name());
            }
            //failed still needs processed as the data is effectively committed

            BatchResult[] batchResult = getConnection().getBulkResults(this.activeJob, batches);
            for(BatchResult br:batchResult) {
                for (Result r : br.getResult()) {
                    if (r.isSuccess() && r.isCreated()) {
                        counts.add(1);
                    } else if (r.getErrors().length > 0) {
                        counts.add(Statement.EXECUTE_FAILED);
                        this.context.addWarning(new SQLWarning(r.getErrors()[0].getMessage(), r.getErrors()[0].getStatusCode().name()));
                    } else {
                        counts.add(Statement.SUCCESS_NO_INFO);
                    }
                }
            }
        }
    }

    private void buildSingleRowInsertPayload(Insert insert, DataPayload data) throws TranslatorException {

        List<ColumnReference> columns = insert.getColumns();
        List<Expression> values = ((ExpressionValueSource)insert.getValueSource()).getValues();
        if(columns.size() != values.size()) {
            throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13006));
        }

        for(int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i).getMetadataObject();
            Object value = values.get(i);

            if(!(value instanceof Literal)) {
                throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13007));
            }

            Literal literalValue = (Literal)values.get(i);
            Object val = Util.toSalesforceObjectValue(literalValue.getValue(), literalValue.getType());
            data.addField(column.getSourceName(), val);
        }
    }

    protected List<com.sforce.async.SObject> buildBulkRowPayload(Insert insert, Iterator<? extends List<?>> it, int rowCount) throws TranslatorException {
        List<com.sforce.async.SObject> rows = new ArrayList<com.sforce.async.SObject>();
        List<ColumnReference> columns = insert.getColumns();
        int boundCount = 0;
        List<Expression> literalValues = ((ExpressionValueSource)insert.getValueSource()).getValues();
        while (it.hasNext()) {
            if (boundCount >= rowCount) {
                break;
            }
            boundCount++;
            List<?> values = it.next();
            com.sforce.async.SObject sobj = new com.sforce.async.SObject();
            for(int i = 0; i < columns.size(); i++) {
                Expression ex = literalValues.get(i);
                ColumnReference element = columns.get(i);
                Column column = element.getMetadataObject();
                Class<?> type = ex.getType();
                Object value = null;
                if (ex instanceof Parameter) {
                    value = values.get(((Parameter)ex).getValueIndex());
                } else if(!(ex instanceof Literal)) {
                    throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13007));
                } else {
                    value = ((Literal)ex).getValue();
                }
                sobj.setField(column.getSourceName(), getStringValue(value, type));
            }
            rows.add(sobj);
        }
        return rows;
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
        if (counts != null) {
            int[] countArray = new int[counts.size()];
            for (int i = 0; i < countArray.length; i++) {
                countArray[i] = counts.get(i);
            }
            return countArray;
        }
        return new int[] { result };
    }

    @Override
    public void cancel() throws TranslatorException {
        if (this.activeJob != null) {
            getConnection().cancelBulkJob(this.activeJob);
        }
    }
}
