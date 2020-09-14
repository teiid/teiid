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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.teiid.language.Argument;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.Util;
import org.teiid.translator.salesforce.execution.visitors.CriteriaVisitor;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;

public class DirectQueryExecution implements ProcedureExecution  {

    public static final String SEARCH = "search;"; //$NON-NLS-1$
    private static final String ATTRIBUTES = "attributes"; //$NON-NLS-1$
    private static final String TYPE = "type"; //$NON-NLS-1$
    private static final String ID = "id"; //$NON-NLS-1$

    protected List<Argument> arguments;
    protected Command command;
    private SalesforceConnection connection;
    protected RuntimeMetadata metadata;
    private ExecutionContext context;
    private QueryResult results;
    private List<List<Object>> currentBatch;
    private int updateCount = -1;
    private boolean updateQuery = false;
    private String query;
    private boolean returnsArray = true;

    /**
     *
     * @param arguments parameter arguments only
     * @param command
     * @param connection
     * @param metadata
     * @param context
     * @param query
     */
    public DirectQueryExecution(List<Argument> arguments, Command command, SalesforceConnection connection, RuntimeMetadata metadata, ExecutionContext context, String query, boolean returnsArray) {
        this.arguments = arguments;
        this.command = command;
        this.connection = connection;
        this.metadata = metadata;
        this.context = context;
        this.query = query;
        this.returnsArray = returnsArray;
    }

    @Override
    public void execute() throws TranslatorException {
        if (query.startsWith(SEARCH)) {
            StringBuilder buffer = new StringBuilder();
            SQLStringVisitor.parseNativeQueryParts(query, arguments, buffer, new SQLStringVisitor.Substitutor() {

                @Override
                public void substitute(Argument arg, StringBuilder builder, int index) {
                    Literal argumentValue = arg.getArgumentValue();
                    CriteriaVisitor.appendLiteralValue(builder, argumentValue);
                }
            });
            doSelect(buffer.toString().substring(7));
        }
        else if (query.startsWith("create;")) { //$NON-NLS-1$
            doInsert(query.substring(7));
        }
        else if (query.startsWith("upsert;")) { //$NON-NLS-1$
            doUpsert(query.substring(7));
        }
        else if (query.startsWith("update;")) { //$NON-NLS-1$
            doUpdate(query.substring(7));
        }
        else if (query.startsWith("delete;")) { //$NON-NLS-1$
            doDelete();
        }
        else {
            throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13002));
        }
    }

    private void doDelete() throws TranslatorException {
        List<String> ids = new ArrayList<String>();
        for (Argument arg : arguments) {
            Object val = arg.getArgumentValue().getValue();
            if (val != null) {
                ids.add(val.toString());
            }
        }
        this.updateCount = this.connection.delete(ids.toArray(new String[ids.size()]));
        this.updateQuery = true;
    }

    private void doUpdate(String update)  throws TranslatorException {
        DataPayload payload = buildDataPlayload(update);
        this.updateCount = this.connection.update(Arrays.asList(payload));
        this.updateQuery = true;
    }

    private void doInsert(String insert) throws TranslatorException {
        DataPayload payload = buildDataPlayload(insert);
        this.updateCount = this.connection.create(payload);
        this.updateQuery = true;
    }

    private void doUpsert(String upsert) throws TranslatorException {
        DataPayload payload = buildDataPlayload(upsert);
        this.updateCount = this.connection.upsert(payload);
        this.updateQuery = true;
    }

    private void doSelect(String select) throws TranslatorException {
        this.results = this.connection.query(select, this.context.getBatchSize(), Boolean.FALSE);
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        List<?> vals = getRow(this.results);
        if (vals == null) {
            return null;
        }
        if (returnsArray) {
            List<Object[]> row = new ArrayList<Object[]>(1);
            row.add(vals.toArray(new Object[vals.size()]));
            return row;
        }
        return vals;
    }

    private List<?> getRow(QueryResult result) throws TranslatorException {

        // for insert/update/delete clauses
        if (this.updateQuery) {
            if (this.updateCount != -1) {
                List<?> updateResult = Arrays.asList(this.updateCount);
                this.updateCount = -1;
                return updateResult;
            }
            return null;
        }

        // select clauses
        List<Object> row = null;

        if(this.currentBatch == null) {
            this.currentBatch = loadBatch(this.results);
        }

        if(!this.currentBatch.isEmpty()) {
            row = this.currentBatch.remove(0);
        }
        else {
            if(!result.isDone()) {
                // fetch more results
                this.results = this.connection.queryMore(results.getQueryLocator(), context.getBatchSize());
                this.currentBatch = loadBatch(this.results);

                // read next row
                row = this.currentBatch.remove(0);
            }
        }
        return row;
    }

    private List<List<Object>> loadBatch(QueryResult queryResult) {
        List<List<Object>> batch = new ArrayList<List<Object>>();
        for(SObject sObject : queryResult.getRecords()) {
            Iterator<XmlObject> fields = sObject.getChildren();
            List<Object> row = new ArrayList<Object>();
            while (fields.hasNext())  {
                XmlObject elem = fields.next();
                if (elem.getName().getLocalPart().equals("type")) { //$NON-NLS-1$
                    continue;
                }
                Object value = elem.getValue();
                row.add(value);
            }
            batch.add(row);
        }
        return batch;
    }

    @Override
    public void close() {
    }

    @Override
    public void cancel() throws TranslatorException {
    }

    private DataPayload buildDataPlayload(String update) throws TranslatorException {
        StringTokenizer st = new StringTokenizer(update, ";"); //$NON-NLS-1$
        if (!st.hasMoreTokens()) {
            throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13004));
        }

        String type = null;
        String id = null;
        DataPayload payload = new DataPayload();

        while(st.hasMoreElements()) {
            String var = st.nextToken();

            int index = var.indexOf('=');
            if (index == -1) {
                continue;
            }
            String key = var.substring(0, index).trim().toLowerCase();
            String value = var.substring(index+1).trim();


            if (key.equalsIgnoreCase(ATTRIBUTES)) {
                StringTokenizer attrTokens = new StringTokenizer(value, ","); //$NON-NLS-1$
                int attrCount = 0;
                while(attrTokens.hasMoreElements()) {
                    String name = attrTokens.nextToken().trim();
                    if (arguments.size() <= attrCount) {
                        throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13005, name));
                    }
                    Argument argument = arguments.get(attrCount++);
                    Object  anObj = argument.getArgumentValue().getValue();
                    anObj = Util.toSalesforceObjectValue(anObj, argument.getType());
                    payload.addField(name, anObj);
                }
            }
            else if (key.equalsIgnoreCase(TYPE)) {
                type = value;
            }
            else if (key.equalsIgnoreCase(ID)) {
                id = value;
            }
        }
        payload.setID(id);
        payload.setType(type);
        return payload;
    }

    @Override
    public List<?> getOutputParameterValues() throws TranslatorException {
        return null;
    }
}
