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

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.language.AggregateFunction;
import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.GroupBy;
import org.teiid.language.Join;
import org.teiid.language.Limit;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.language.TableReference;
import org.teiid.language.visitor.HierarchyVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesForceMetadataProcessor;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.SalesforceConnection.BatchResultInfo;
import org.teiid.translator.salesforce.SalesforceConnection.BulkBatchResult;
import org.teiid.translator.salesforce.execution.visitors.JoinQueryVisitor;
import org.teiid.translator.salesforce.execution.visitors.SelectVisitor;

import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;

public class QueryExecutionImpl implements ResultSetExecution {

    /**
     * A validator for and asynch bulk / pk chunking
     * https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_using_bulk_query.htm
     * https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/async_api_headers_enable_pk_chunking.htm
     */
    static final class BulkValidator extends HierarchyVisitor {
        boolean bulkEligible = true;
        boolean usePkChunking = true;
        static Set<String> allowed = new HashSet<String>(
                Arrays.asList("Account", "Campaign", "CampaignMember", "Case", "Contact", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
                        "Lead", "LoginHistory", "Opportunity", "Task", "User")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$

        @Override
        public void visit(AggregateFunction obj) {
            //the documentation implies only sum, count are not allowed, but in testing all are
            //if (obj.getName().equalsIgnoreCase(SQLConstants.NonReserved.COUNT) || obj.getName().equalsIgnoreCase(SQLConstants.NonReserved.SUM)) {
                bulkEligible = false;
            //} else {
            //    usePkChunking = false;
            //    super.visit(obj);
            //}
        }

        @Override
        public void visit(GroupBy obj) {
            //the documentation implies only rollup is not allowed, but in testing any grouping is
            //if (obj.isRollup()) { //not yet supported, but just in case
                bulkEligible = false;
            //} else {
            //    usePkChunking = false;
            //    super.visit(obj);
            //}
        }

        @Override
        public void visit(Limit obj) {
            if (obj.getRowOffset() > 0) {
                bulkEligible = false;
            } else {
                usePkChunking = false;
                super.visit(obj);
            }
        }

        @Override
        public void visit(NamedTable obj) {
            //since this is hint driven we'll assume that it is used selectively
            if (!allowed.contains(obj.getMetadataObject().getSourceName())
                    && !Boolean.valueOf(obj.getMetadataObject().getProperty(SalesForceMetadataProcessor.TABLE_CUSTOM, false))) {
                usePkChunking = false;
            }
        }

        @Override
        public void visit(OrderBy obj) {
            usePkChunking = false;
        }

        @Override
        public void visit(Select obj) {
            if (obj.getHaving() != null) {
                usePkChunking = false;
            }
            super.visit(obj);
        }

        public boolean isBulkEligible() {
            return bulkEligible;
        }

        public boolean usePkChunking() {
            return usePkChunking && bulkEligible;
        }
    }

    private static final String TYPE = "type"; //$NON-NLS-1$

    private static final String AGGREGATE_RESULT = "AggregateResult"; //$NON-NLS-1$

    private static final Pattern dateTimePattern = Pattern.compile("^(?:(\\d{4}-\\d{2}-\\d{2})T)?(\\d{2}:\\d{2}:\\d{2}(?:.\\d+)?)(.*)"); //$NON-NLS-1$

    private SalesForceExecutionFactory executionFactory;

    private SalesforceConnection connection;

    private RuntimeMetadata metadata;

    private ExecutionContext context;

    private SelectVisitor visitor;

    private QueryResult results;

    private List<List<Object>> resultBatch;

    // Identifying values
    private String connectionIdentifier;

    private String connectorIdentifier;

    private String requestIdentifier;

    private String partIdentifier;

    private String logPreamble;

    private QueryExpression query;

    Map<String, Map<String,Integer>> sObjectToResponseField = new HashMap<String, Map<String,Integer>>();

    private int topResultIndex = 0;

    private Calendar cal;

    //bulk support
    private JobInfo activeJob;
    private BatchResultInfo batchInfo;
    private BulkBatchResult batchResults;


    public QueryExecutionImpl(QueryExpression command, SalesforceConnection connection, RuntimeMetadata metadata, ExecutionContext context, SalesForceExecutionFactory salesForceExecutionFactory) {
        this.connection = connection;
        this.metadata = metadata;
        this.context = context;
        this.query = command;
        this.executionFactory = salesForceExecutionFactory;

        connectionIdentifier = context.getConnectionId();
        connectorIdentifier = context.getConnectorIdentifier();
        requestIdentifier = context.getRequestId();
        partIdentifier = context.getPartIdentifier();
    }

    public void cancel() throws TranslatorException {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.cancel"));//$NON-NLS-1$
        if (activeJob != null) {
            this.connection.cancelBulkJob(activeJob);
        }
    }

    public void close() {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.close")); //$NON-NLS-1$
        if (activeJob != null) {
            try {
                this.connection.closeJob(activeJob.getId());
            } catch (TranslatorException e) {
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Exception closing"); //$NON-NLS-1$
            }
        }
        if (batchResults != null) {
            batchResults.close();
            batchResults = null;
        }
    }

    @Override
    public void execute() throws TranslatorException {
        //redundant with command log
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, getLogPreamble(), "Incoming Query:", query); //$NON-NLS-1$
        List<TableReference> from = ((Select)query).getFrom();
        boolean join = false;
        if(from.get(0) instanceof Join) {
            join = true;
            visitor = new JoinQueryVisitor(metadata);
        } else {
            visitor = new SelectVisitor(metadata);
        }
        visitor.visitNode(query);
        if(visitor.canRetrieve()) {
            String retrieveFieldList = visitor.getRetrieveFieldList();
            if (!visitor.isSelectAggregate()) { //check after looking at the select
                context.logCommand("Using retrieve: ", retrieveFieldList, visitor.getTableName(), visitor.getIdInCriteria()); //$NON-NLS-1$
                results = this.executionFactory.buildQueryResult(connection.retrieve(retrieveFieldList,
                        visitor.getTableName(), visitor.getIdInCriteria()));
            }
        }
        if (results == null) {
            String finalQuery = visitor.getQuery().trim();
            //redundant
            LogManager.logDetail(LogConstants.CTX_CONNECTOR,  getLogPreamble(), "Executing Query:", finalQuery); //$NON-NLS-1$
            context.logCommand(finalQuery);

            if (!join && !visitor.getQueryAll()
                    && (executionFactory.useBulk() || (context.getSourceHint() != null && context.getSourceHint().contains("bulk")))) { //$NON-NLS-1$
                BulkValidator bulkValidator = new BulkValidator();
                query.acceptVisitor(bulkValidator);
                if (bulkValidator.isBulkEligible()) {
                    LogManager.logDetail(LogConstants.CTX_CONNECTOR,  getLogPreamble(), "Using bulk logic", bulkValidator.usePkChunking()?"with":"without", "pk chunking"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
                    this.activeJob = connection.createBulkJob(visitor.getTableName(), OperationEnum.query, bulkValidator.usePkChunking());
                    batchInfo = connection.addBatch(finalQuery, this.activeJob);
                    return;
                }
                LogManager.logDetail(LogConstants.CTX_CONNECTOR,  getLogPreamble(), "Ingoring bulk hint as the query is not bulk eligible"); //$NON-NLS-1$
            }

            results = connection.query(finalQuery, this.context.getBatchSize(), visitor.getQueryAll());
        }
    }

    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (activeJob != null) {
            List<String> row = null;
            try {
                while (row == null) {
                    if (batchResults == null) {
                        batchResults = connection.getBatchQueryResults(activeJob.getId(), batchInfo);
                        if (batchResults == null) {
                            return null;
                        }
                        //throw the header away
                        if (batchResults.nextRecord() == null) {
                            throw new AssertionError("Expected header row"); //$NON-NLS-1$
                        }
                    }
                    row = batchResults.nextRecord();
                    if (row == null) {
                        batchResults.close();
                        batchResults = null;
                    }
                }
            } catch (IOException e) {
                throw new TranslatorException(e);
            }
            List<Object> result = new ArrayList<Object>();
            for (int j = 0; j < visitor.getSelectSymbolCount(); j++) {
                Expression ex = visitor.getSelectSymbolMetadata(j);
                Class<?> type = ex.getType();
                if (ex instanceof ColumnReference) {
                    Column element = ((ColumnReference)ex).getMetadataObject();
                    type = element.getJavaType();
                }
                result.add(convertValue(type, row.get(j)));
            }
            return result;
        }
        List<?> result = getRow(results);
        return result;
    }

    private List<Object> getRow(QueryResult result) throws TranslatorException {
        List<Object> row;
        if(null == resultBatch) {
            loadBatch();
        }
        if(resultBatch.size() == topResultIndex) {
            row = null;
        } else {
            row = resultBatch.get(topResultIndex);
            topResultIndex++;
            if(resultBatch.size() == topResultIndex) {
                if(!result.isDone()) {
                    loadBatch();
                }
            }

        }
        return row;
    }

    private void loadBatch() throws TranslatorException {
        if(null != resultBatch) { // if we have an old batch, then we have to get new results
            results = connection.queryMore(results.getQueryLocator(), context.getBatchSize());
        }
        resultBatch = new ArrayList<List<Object>>();
        topResultIndex = 0;
        for(SObject sObject : results.getRecords()) {
            if (sObject == null) {
                continue;
            }
            List<Object[]> result = getObjectData(sObject);
            for(Iterator<Object[]> i = result.iterator(); i.hasNext(); ) {
                resultBatch.add(Arrays.asList(i.next()));
            }
        }
    }

        private List<Object[]> getObjectData(SObject sObject) throws TranslatorException {
            Iterator<XmlObject> topFields = sObject.getChildren();
            ArrayList<XmlObject> children = new ArrayList<XmlObject>();
            while (topFields.hasNext()) {
                children.add(topFields.next());
            }
            logAndMapFields(sObject.getType(), children);
            List<Object[]> result = new ArrayList<Object[]>();
            NamedTable targetTable = null;
            if(visitor instanceof JoinQueryVisitor) {
                JoinQueryVisitor joinVisitor = (JoinQueryVisitor)visitor;
                String name = joinVisitor.getChildName();
                targetTable = joinVisitor.getRootTable();
                if (name != null) {
                    NamedTable childTable = joinVisitor.getChildTable();
                    for(int i = 0; i < children.size(); i++) {
                        XmlObject element = children.get(i);
                        String localName = element.getName().getLocalPart();
                        if (name.equals(localName)) {
                            extactJoinResults(element, result, childTable);
                        }
                    }
                }
            }
            return extractDataFromFields(sObject, children, result, targetTable);
        }

        private void extactJoinResults(XmlObject node, List<Object[]> result, NamedTable childTable) throws TranslatorException {
            Iterator<XmlObject> records = node.getChildren("records"); //$NON-NLS-1$
            if (!records.hasNext()) {
                extractValuesFromElement(result, childTable, node);
            } else {
                while (records.hasNext()) {
                    XmlObject next = records.next();
                    extractValuesFromElement(result, childTable, next);
                }
            }
        }

        //TODO: this looks inefficient as getChild is linear
        private void extractValuesFromElement(List<Object[]> result,
                NamedTable childTable, XmlObject next)
                throws TranslatorException {
            Object val = next.getField(TYPE);
            if (!(val instanceof String)) {
                return;
            }
            Object[] row = new Object[visitor.getSelectSymbolCount()];
            for (int j = 0; j < row.length; j++) {
                //must be a column reference as we won't allow an agg over a join
                ColumnReference columnReference = (ColumnReference)visitor.getSelectSymbolMetadata(j);
                Column element = columnReference.getMetadataObject();
                AbstractMetadataRecord table = element.getParent();
                if(table.getSourceName().equals(val) && columnReference.getTable().equals(childTable)) {
                    XmlObject child = next.getChild(element.getSourceName());
                    Object cell = getCellDatum(element.getSourceName(), element.getJavaType(), child);
                    setElementValueInColumn(j, cell, row);
                }
            }
            result.add(row);
        }

    private List<Object[]> extractDataFromFields(SObject sObject,
            List<XmlObject> fields, List<Object[]> result, NamedTable targetTable) throws TranslatorException {
        Map<String,Integer> fieldToIndexMap = sObjectToResponseField.get(sObject.getType());
        int aggCount = 0;
        for (int j = 0; j < visitor.getSelectSymbolCount(); j++) {
            Expression ex = visitor.getSelectSymbolMetadata(j);
            if (ex instanceof ColumnReference) {
                ColumnReference columnReference = (ColumnReference)ex;
                Column element = columnReference.getMetadataObject();
                Table table = (Table)element.getParent();
                if ((table.getSourceName().equals(sObject.getType())
                        && (targetTable == null || columnReference.getTable().equals(targetTable)))
                        || AGGREGATE_RESULT.equalsIgnoreCase(sObject.getType())) {
                    String sourceName = element.getSourceName();
                    Integer index = fieldToIndexMap.get(sourceName);
                    if (null == index) {
                        extractParentValue(sObject, result, j, element, sourceName);
                    } else {
                        Object cell = getCellDatum(sourceName, element.getJavaType(), fields.get(index));
                        setValueInColumn(j, cell, result);
                    }
                } else {
                    if (visitor instanceof JoinQueryVisitor) {
                        JoinQueryVisitor joinVisitor = (JoinQueryVisitor)visitor;
                        String path = joinVisitor.getParents().get(columnReference.getTable());
                        if (path != null) {
                            extractParentValue(sObject, result, j, element, path+"."+element.getSourceName()); //$NON-NLS-1$
                        }
                    }
                }
            } else if (ex instanceof AggregateFunction) {
                String name = SelectVisitor.AGG_PREFIX + (aggCount++);
                Integer index = fieldToIndexMap.get(name);
                if (null == index) {
                    throw new TranslatorException(SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.missing.field", ex)); //$NON-NLS-1$
                }
                Object cell = getCellDatum(name, ex.getType(), fields.get(index));
                setValueInColumn(j, cell, result);
            }
        }
        return result;
    }

    private void extractParentValue(SObject sObject, List<Object[]> result,
            int j, Column element, String sourceName)
            throws TranslatorException {
        int dotIndex = sourceName.indexOf('.');
        XmlObject root = sObject;
        String fieldName = sourceName;
        while (dotIndex > 0 && root != null) {
            String parent = fieldName.substring(0, dotIndex);
            fieldName = fieldName.substring(dotIndex + 1);
            root = root.getChild(parent);
            dotIndex = fieldName.indexOf('.');
        }
        if (fieldName.equals(sourceName)) {
            throw new TranslatorException(SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.missing.field", sourceName)); //$NON-NLS-1$
        }
        if (root != null) {
            XmlObject child = root.getChild(fieldName);
            if (child != null) {
                Object cell = getCellDatum(fieldName, element.getJavaType(), child);
                setValueInColumn(j, cell, result);
            }
        }
    }

    private void setElementValueInColumn(int columnIndex, Object value, Object[] row) {
        if(value instanceof XmlObject) {
            XmlObject element = (XmlObject)value;
            if (element.hasChildren()) {
                row[columnIndex] = element.getChildren().next().getValue();
            } else {
                row[columnIndex] = element.getValue();
            }
        } else {
            row[columnIndex] = value;
        }
    }

    private void setValueInColumn(int columnIndex, Object value, List<Object[]> result) {
        if(result.isEmpty()) {
            Object[] row = new Object[visitor.getSelectSymbolCount()];
            result.add(row);
        }
        Iterator<Object[]> iter = result.iterator();
        while (iter.hasNext()) {
            Object[] row = iter.next();
            row[columnIndex] = value;
        }
    }

    /**
     * Load the map of response field names to index.
     * @param fields
     * @throws TranslatorException
     */
    private void logAndMapFields(String sObjectName,
            List<XmlObject> fields) throws TranslatorException {
        if (!sObjectToResponseField.containsKey(sObjectName)) {
            logFields(sObjectName, fields);
            Map<String, Integer> responseFieldToIndexMap = new HashMap<String, Integer>();
            for (int x = 0; x < fields.size(); x++) {
                XmlObject element = fields.get(x);
                responseFieldToIndexMap.put(element.getName().getLocalPart(), x);
            }
            sObjectToResponseField.put(sObjectName, responseFieldToIndexMap);
        }
    }

    private void logFields(String sObjectName, List<XmlObject> fields) {
        if (!LogManager.isMessageToBeRecorded(LogConstants.CTX_CONNECTOR, MessageLevel.DETAIL)) {
            return;
        }
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "SalesForce Object Name = " + sObjectName); //$NON-NLS-1$
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "FieldCount = " + fields.size()); //$NON-NLS-1$
        for(int i = 0; i < fields.size(); i++) {
            XmlObject element = fields.get(i);
            LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Field # " + i + " is " + element.getName().getLocalPart()); //$NON-NLS-1$ //$NON-NLS-2$
        }

    }

    /**
     * TODO: the logic here should be aware of xsi:type information and use a standard conversion
     * library.  Conversion to teiid types should then be a secondary effort - and will be automatically handled above here.
     */
    private Object getCellDatum(String name, Class<?> type, XmlObject elem) throws TranslatorException {
        if(!name.equals(elem.getName().getLocalPart())) {
            throw new TranslatorException(SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.column.mismatch1", name, elem.getName().getLocalPart())); //$NON-NLS-1$
        }
        Object value = elem.getValue();
        return convertValue(type, value);
    }

    private Object convertValue(Class<?> type, Object value)
            throws TranslatorException {
        if (value == null) {
            return null;
        }
        if (value instanceof String && (((String) value).isEmpty())) {
            if (type == String.class) {
                return value;
            }
            return null;
        } else if ((type.equals(java.sql.Timestamp.class) || type.equals(java.sql.Time.class)) && !(value instanceof Date)) {
            if (cal == null) {
                cal = Calendar.getInstance();
            }
            return parseDateTime(value.toString(), type, cal);
        }
        return value;
    }

    static Object parseDateTime(String value, Class<?> type, Calendar cal)
            throws TranslatorException {
        try {
            Matcher m = dateTimePattern.matcher(value);
            if (m.matches()) {
                String date = m.group(1);
                String time = m.group(2);
                String timeZone = m.group(3);
                Date d = null;
                if (date == null) {
                    //sql times don't care about fractional seconds
                    int milli = time.lastIndexOf('.');
                    if (milli > 0) {
                        time = time.substring(0, milli);
                    }
                    d = Time.valueOf(time);
                } else {
                    d = Timestamp.valueOf(date + " " + time); //$NON-NLS-1$
                }
                TimeZone tz = null;
                if (timeZone != null) {
                    if (timeZone.equals("Z")) { //$NON-NLS-1$
                        tz = TimeZone.getTimeZone("GMT"); //$NON-NLS-1$
                    } else if (timeZone.contains(":")) { //$NON-NLS-1$
                        tz = TimeZone.getTimeZone("GMT" + timeZone); //$NON-NLS-1$
                    } else {
                        //this is probably an exceptional case
                        tz = TimeZone.getTimeZone(timeZone);
                    }
                    cal.setTimeZone(tz);
                } else {
                    cal = null;
                }
                return TimestampWithTimezone.create(d, TimeZone.getDefault(), cal, type);
            }
            throw new TranslatorException(SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.datatime.parse") + value); //$NON-NLS-1$
        } catch (IllegalArgumentException e) {
            throw new TranslatorException(e, SalesForcePlugin.Util.getString("SalesforceQueryExecutionImpl.datatime.parse") + value); //$NON-NLS-1$
        }
    }

    private String getLogPreamble() {
        if (null == logPreamble) {
            StringBuffer preamble = new StringBuffer();
            preamble.append(connectorIdentifier);
            preamble.append('.');
            preamble.append(connectionIdentifier);
            preamble.append('.');
            preamble.append(requestIdentifier);
            preamble.append('.');
            preamble.append(partIdentifier);
            preamble.append(": "); //$NON-NLS-1$
            logPreamble = preamble.toString();
        }
        return logPreamble;
    }
}
