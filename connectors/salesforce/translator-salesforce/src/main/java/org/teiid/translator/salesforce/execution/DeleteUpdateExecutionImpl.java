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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.teiid.language.BulkCommand;
import org.teiid.language.Command;
import org.teiid.language.Comparison;
import org.teiid.language.Condition;
import org.teiid.language.Expression;
import org.teiid.language.In;
import org.teiid.language.Literal;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.execution.visitors.UpdateVisitor;

import com.sforce.async.BatchResult;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.async.Result;
import com.sforce.async.SObject;
import com.sforce.soap.partner.QueryResult;

/**
 * Implements the base logic for sync and async bulk handling with a root select
 * query to drive id fetching
 */
public abstract class DeleteUpdateExecutionImpl extends AbstractUpdateExecution {

    private JobInfo activeJob;
    private List<String> batches = new ArrayList<String>();
    protected UpdateVisitor visitor;
    protected boolean bulk;
    private boolean selectComplete;

    public DeleteUpdateExecutionImpl(SalesForceExecutionFactory ef, Command command,
            SalesforceConnection salesforceConnection,
            RuntimeMetadata metadata, ExecutionContext context) {
        super(ef, command, salesforceConnection, metadata, context);
        visitor = new UpdateVisitor(metadata);
        visitor.visitNode(command);
        bulk = ef.useBulk() || (context.getSourceHint() != null && context.getSourceHint().contains("bulk")); //$NON-NLS-1$
    }

    @Override
    public void execute() throws TranslatorException {
        if (!selectComplete) {
            execute(visitor.getWhere());
            selectComplete = true;
        }
        if (bulk && this.activeJob != null) {
            if (this.activeJob.getState() == JobStateEnum.Open) {
                this.activeJob = getConnection().closeJob(this.activeJob.getId());
            }
            if (this.activeJob.getState() == JobStateEnum.Aborted) {
                throw new TranslatorException(this.activeJob.getState().name());
            }

            BatchResult[] batchResult = getConnection().getBulkResults(this.activeJob, batches);
            for(BatchResult br:batchResult) {
                for (Result r : br.getResult()) {
                    if (r.isSuccess()) {
                        result += 1;
                    } else if (r.getErrors().length > 0) {
                        //failed
                        this.context.addWarning(new SQLWarning(r.getErrors()[0].getMessage(), r.getErrors()[0].getStatusCode().name()));
                    } else {
                        //no info
                    }
                }
            }
        }
    }

    protected int processIds(String[] ids)
            throws TranslatorException {
        if (bulk) {
            if (this.activeJob == null) {
                this.activeJob = getConnection().createBulkJob(
                        visitor.getTableName(),
                        getOperation(),
                        false);
            }
            if (this.activeJob.getState() == JobStateEnum.Open) {
                batches.add(getConnection().addBatch(buildBatch(ids),
                        activeJob));
            }
            return 0; //we'll determine the count later
        }
        return syncProcessIds(ids);
    }

    protected abstract int syncProcessIds(String[] ids) throws TranslatorException;

    private List<SObject> buildBatch(String[] ids) {
        return Arrays.stream(ids)
                        .map((id) -> {return toSObject(id);})
                        .collect(Collectors.toList());
    }

    protected SObject toSObject(String id) {
        SObject so = new SObject();
        so.setField("Id", id); //$NON-NLS-1$
        return so;
    }

    protected abstract OperationEnum getOperation();

    void execute(Condition criteria) throws TranslatorException {
        if (((BulkCommand)command).getParameterValues() != null) {
            throw new TranslatorException("Only bulk inserts are supported"); //$NON-NLS-1$
        }
        int batchSize = 2000; //Salesforce limit
        String[] Ids = null;
        if (visitor.hasOnlyIDCriteria()) {
            if (criteria instanceof Comparison) {
                Literal l = (Literal) ((Comparison)criteria).getRightExpression() ;
                Ids = new String[] { l.getValue().toString() };
            } else {
                List<Expression> rightExpressions = ((In)criteria).getRightExpressions();
                Ids = new String[rightExpressions.size()];
                for (int i = 0; i < Ids.length; i++) {
                    Ids[i] = ((Literal)rightExpressions.get(i)).getValue().toString();
                }
            }
            result = processIds(Ids);
        } else {
            String query = visitor.getQuery();
            context.logCommand(query);
            QueryResult results = getConnection().query(query, batchSize, Boolean.FALSE);
            ArrayList<String> idList = new ArrayList<String>(results.getRecords().length);
            //5000 is the default size from StreamHandler.getMaxRecordsInBatch
            //200 was the existing default for sync updates
            int updateBatchSize = bulk?Math.max(5000, executionFactory.getMaxBulkInsertBatchSize()):200;
            while (results != null) {
                if (results.getSize() > 0) {
                    for (int i = 0; i < results.getRecords().length; i++) {
                        com.sforce.soap.partner.sobject.SObject sObject = results.getRecords()[i];
                        idList.add(sObject.getId());
                        if (idList.size() == updateBatchSize) {
                            Ids = idList.toArray(new String[0]);
                            result += processIds(Ids);
                            idList.clear();
                        }
                    }
                }
                if (results.isDone()) {
                    break;
                }
                results = connection.queryMore(results.getQueryLocator(), batchSize);
            }
            if (!idList.isEmpty()) {
                Ids = idList.toArray(new String[0]);
                result += processIds(Ids);
            }
        }
    }

}
