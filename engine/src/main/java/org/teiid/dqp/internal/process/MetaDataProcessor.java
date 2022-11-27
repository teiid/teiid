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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.client.metadata.MetadataResult;
import org.teiid.client.metadata.ResultsMetadataConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.JDBCSQLTypeInfo;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.dqp.message.RequestID;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.Column;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.FunctionLibrary;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SupportConstants;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.SPParameter;
import org.teiid.query.sql.lang.SetQuery;
import org.teiid.query.sql.lang.SetQuery.Operation;
import org.teiid.query.sql.lang.StoredProcedure;
import org.teiid.query.sql.symbol.*;
import org.teiid.query.sql.symbol.AggregateSymbol.Type;
import org.teiid.query.sql.util.SymbolMap;
import org.teiid.query.sql.visitor.ReferenceCollectorVisitor;
import org.teiid.query.tempdata.TempTableStore;


/**
 * Handles MetaDataMessages on behalf of DQPCore.
 */
public class MetaDataProcessor {

    public final static String XML_COLUMN_NAME = "xml"; //$NON-NLS-1$

    // Resources
    private DQPCore requestManager;
    private QueryMetadataInterface metadata;
    private SessionAwareCache<PreparedPlan> planCache;

    private String vdbName;
    private String vdbVersion;
    private RequestID requestID;

    private boolean labelAsName;

    private boolean useJDBCDefaultPrecision = true;

    public MetaDataProcessor(DQPCore requestManager, SessionAwareCache<PreparedPlan> planCache, String vdbName, Object vdbVersion) {
        this.requestManager = requestManager;
        this.planCache = planCache;
        this.vdbName = vdbName;
        this.vdbVersion = vdbVersion.toString();
    }

    /**
     * Process a metadata request message - this is typically either a request
     * for metadata for a prepared statement or a request for full metadata from
     * an already processed command.
     * @param preparedSql The sql from the client
     * @return The message for the client
     * @throws TeiidComponentException
     * @throws TeiidProcessingException
     */
    MetadataResult processMessage(RequestID requestId, DQPWorkContext workContext, String preparedSql, boolean allowDoubleQuotedVariable) throws TeiidComponentException, TeiidProcessingException {
        this.requestID = requestId;

        this.metadata = workContext.getVDB().getAttachment(QueryMetadataInterface.class);

        RequestWorkItem workItem = null;
        try {
            workItem = requestManager.getRequestWorkItem(requestID);
        } catch (TeiidProcessingException e) {
            if (preparedSql == null) {
                throw e;
            }
        }

        TempTableStore tempTableStore = null;
        if(requestManager != null) {
            ClientState state = requestManager.getClientState(workContext.getSessionId(), false);
            if (state != null) {
                tempTableStore = state.sessionTables;
            }
        }
        if(tempTableStore != null) {
            metadata = new TempMetadataAdapter(this.metadata, tempTableStore.getMetadataStore());
        }

        if(workItem != null) {
            return getMetadataForCommand(workItem.getOriginalCommand(), workContext);
        }
        return obtainMetadataForPreparedSql(preparedSql, workContext, allowDoubleQuotedVariable);
    }

    // For each projected symbol, construct a metadata map
    private MetadataResult getMetadataForCommand(Command originalCommand, DQPWorkContext workContext) throws TeiidComponentException {
        Map<Integer, Object>[] columnMetadata = null;

        switch(originalCommand.getType()) {
            case Command.TYPE_QUERY:
                if(originalCommand instanceof Query) {
                    if (((Query)originalCommand).getInto() == null) {
                        columnMetadata = createProjectedSymbolMetadata(originalCommand, workContext);
                    }
                } else {
                    columnMetadata = createProjectedSymbolMetadata(originalCommand, workContext);
                }
                break;
            case Command.TYPE_STORED_PROCEDURE:
                columnMetadata = createProjectedSymbolMetadata(originalCommand, workContext);
                break;
            case Command.TYPE_INSERT:
            case Command.TYPE_UPDATE:
            case Command.TYPE_DELETE:
            case Command.TYPE_CREATE:
            case Command.TYPE_DROP:
                break;
            default:
                if (originalCommand.returnsResultSet()) {
                    columnMetadata = createProjectedSymbolMetadata(originalCommand, workContext);
                }
        }

        Map<Reference, String> paramMap = Collections.emptyMap();
        if (originalCommand instanceof StoredProcedure) {
            StoredProcedure sp = (StoredProcedure)originalCommand;
            paramMap = new HashMap<Reference, String>();
            Collection<SPParameter> params = sp.getParameters();
            for (SPParameter spParameter : params) {
                if (spParameter.getParameterType() != SPParameter.INOUT
                        && spParameter.getParameterType() != SPParameter.IN
                        && spParameter.getParameterType() != SPParameter.RETURN_VALUE) {
                    continue;
                }
                Expression ex = spParameter.getExpression();
                if (ex instanceof Function && FunctionLibrary.isConvert((Function)ex)) {
                    ex = ((Function)ex).getArg(0);
                }
                if (ex instanceof Reference) {
                    paramMap.put((Reference)ex, spParameter.getParameterSymbol().getShortName());
                }
            }
        }
        List<Reference> params = ReferenceCollectorVisitor.getReferences(originalCommand);
        Map<Integer, Object>[] paramMetadata = new Map[params.size()];
        for (int i = 0; i < params.size(); i++) {
            Reference param = params.get(i);
            paramMetadata[i] = getDefaultColumn(null, paramMap.get(param), param.getType());
        }

        return new MetadataResult(columnMetadata, paramMetadata);
    }

    public static void updateMetadataAcrossBranches(SetQuery originalCommand, List<Column> columns, QueryMetadataInterface metadata) throws TeiidComponentException {
        String empty = ""; //$NON-NLS-1$
        MetaDataProcessor mdp = new MetaDataProcessor(null, null, empty, empty);
        mdp.metadata = metadata;
        mdp.useJDBCDefaultPrecision = false;
        Map<Integer, Object>[] metadataMaps = mdp.createProjectedSymbolMetadata(originalCommand, null);

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            Map<Integer, Object> metadataMap = metadataMaps[i];

            Integer val = (Integer)metadataMap.get(ResultsMetadataConstants.PRECISION);
            if (val != null) {
                column.setPrecision(val);
                column.setLength(val);
            }
            val = (Integer)metadataMap.get(ResultsMetadataConstants.SCALE);
            if (val != null) {
                column.setScale(val);
            }
        }
    }

    private Map<Integer, Object>[] createProjectedSymbolMetadata(Command originalCommand, DQPWorkContext workContext) throws TeiidComponentException {
        Map<Integer, Object>[] columnMetadata;
        // Allow command to use temporary metadata
        TempMetadataStore tempMetadata = originalCommand.getTemporaryMetadata();
        if(tempMetadata != null && tempMetadata.getData().size() > 0) {
            TempMetadataAdapter tempFacade = new TempMetadataAdapter(this.metadata, tempMetadata);
            this.metadata = tempFacade;
        }

        List<Expression> projectedSymbols = originalCommand.getProjectedSymbols();
        columnMetadata = new Map[projectedSymbols.size()];

        boolean pgColumnNames = false;
        if (workContext != null) {
            pgColumnNames = Boolean.TRUE.equals(workContext.getSession().getSessionVariables().get("pg_column_names")); //$NON-NLS-1$
        }

        Iterator<Expression> symbolIter = projectedSymbols.iterator();
        for(int i=0; symbolIter.hasNext(); i++) {
            Expression symbol = symbolIter.next();
            String shortColumnName = getColumnName(pgColumnNames, symbol);
            symbol = SymbolMap.getExpression(symbol);
            try {
                columnMetadata[i] = createColumnMetadata(shortColumnName, symbol);
            } catch(QueryMetadataException e) {
                 throw new TeiidComponentException(QueryPlugin.Event.TEIID30559, e);
            }
        }

        if (originalCommand instanceof SetQuery) {
            SetQuery setQuery = (SetQuery)originalCommand;

            //only redo the left if there are additional branches to consider
            if (!(setQuery.getLeftQuery() instanceof Query)) {
                Map<Integer, Object>[] leftResult = createProjectedSymbolMetadata(setQuery.getLeftQuery(), workContext);
                for(int i=0; i < leftResult.length; i++) {
                    setCombinedMax(columnMetadata, leftResult, i, ResultsMetadataConstants.PRECISION);
                    setCombinedMax(columnMetadata, leftResult, i, ResultsMetadataConstants.SCALE);
                }
            }

            //only use the right if we're a union
            if (setQuery.getOperation() == Operation.UNION) {
                Map<Integer, Object>[] rightResult = createProjectedSymbolMetadata(setQuery.getRightQuery(), workContext);

                for(int i=0; i < rightResult.length; i++) {
                    setCombinedMax(columnMetadata, rightResult, i, ResultsMetadataConstants.PRECISION);
                    setCombinedMax(columnMetadata, rightResult, i, ResultsMetadataConstants.SCALE);
                }
            }

            return columnMetadata;
        }
        return columnMetadata;
    }

    public static String getColumnName(boolean pgColumnNames, Expression symbol) {
        String name = Symbol.getOutputName(symbol);
        //pg uses the actual symbol name, not the generated name
        //we give to expression symbols
        if (pgColumnNames && symbol instanceof ExpressionSymbol) {
            Expression ex = ((ExpressionSymbol)symbol).getExpression();
            if (ex instanceof NamedExpression) {
                name = ((NamedExpression)ex).getName();
            }
        }

        return Symbol.getShortName(name);
    }

    private void setCombinedMax(Map<Integer, Object>[] leftResult,
            Map<Integer, Object>[] rightResult, int i, int key) {
        Integer lval = (Integer)leftResult[i].get(key);
        Integer rval = (Integer)rightResult[i].get(key);
        if (lval != null && rval != null) {
            leftResult[i].put(key, Math.max(lval, rval));
        }
    }

    private MetadataResult obtainMetadataForPreparedSql(String sql, DQPWorkContext workContext, boolean isDoubleQuotedVariablesAllowed) throws QueryParserException, QueryResolverException, TeiidComponentException {
        Command command = null;

        ParseInfo info = new ParseInfo();
        // Defect 19747 - the parser needs the following connection property to decide whether to treat double-quoted strings as variable names
        info.ansiQuotedIdentifiers = isDoubleQuotedVariablesAllowed;
        CacheID id = new CacheID(workContext, info, sql);
        PreparedPlan plan = planCache.get(id);
        if(plan != null) {
            command = plan.getCommand();
        } else {
            command = QueryParser.getQueryParser().parseCommand(sql, info);
            //no need to resolve explain - the metadata comes from format
            if (command.getType() != Command.TYPE_EXPLAIN) {
                QueryResolver.resolveCommand(command, this.metadata);
            }
        }
        return getMetadataForCommand(command, workContext);
    }

    /**
     * Set the easily determined metadata from symbol on the given Column
     * @param column
     * @param symbol
     * @param metadata
     * @throws QueryMetadataException
     * @throws TeiidComponentException
     */
    public static void setColumnMetadata(Column column, Expression symbol, QueryMetadataInterface metadata) throws QueryMetadataException, TeiidComponentException {
        //do a dummy initialization of the metadataprocessor
        String empty = ""; //$NON-NLS-1$
        MetaDataProcessor mdp = new MetaDataProcessor(null, null, empty, empty);
        mdp.metadata = metadata;
        mdp.useJDBCDefaultPrecision = false;
        Map<Integer, Object> metadataMap = mdp.createColumnMetadata(empty, symbol);

        //set the fields from the column metadata
        column.setCaseSensitive(Boolean.TRUE.equals(metadataMap.get(ResultsMetadataConstants.CASE_SENSITIVE)));
        column.setCurrency(Boolean.TRUE.equals(metadataMap.get(ResultsMetadataConstants.CURRENCY)));
        Object nullable = metadataMap.get(ResultsMetadataConstants.NULLABLE);
        if (nullable == ResultsMetadataConstants.NULL_TYPES.NOT_NULL) {
            column.setNullType(NullType.No_Nulls);
        } else if (nullable == ResultsMetadataConstants.NULL_TYPES.NULLABLE) {
            column.setNullType(NullType.Nullable);
        }
        Integer val = (Integer)metadataMap.get(ResultsMetadataConstants.PRECISION);
        if (val != null) {
            column.setPrecision(val);
            column.setLength(val);
        }
        val = (Integer)metadataMap.get(ResultsMetadataConstants.RADIX);
        if (val != null) {
            column.setRadix(val);
        }
        val = (Integer)metadataMap.get(ResultsMetadataConstants.SCALE);
        if (val != null) {
            column.setScale(val);
        }
        column.setSigned(Boolean.TRUE.equals(metadataMap.get(ResultsMetadataConstants.SIGNED)));
    }

    private Map<Integer, Object> createColumnMetadata(String label, Expression symbol) throws QueryMetadataException, TeiidComponentException {
        if(symbol instanceof ElementSymbol) {
            return createElementMetadata(label, (ElementSymbol) symbol);
        }
        symbol = SymbolMap.getExpression(symbol);
        if (symbol instanceof AggregateSymbol) {
            return createAggregateMetadata(label, (AggregateSymbol)symbol);
        } else if (symbol instanceof WindowFunction) {
            return createAggregateMetadata(label, ((WindowFunction)symbol).getFunction());
        }
        return createTypedMetadata(label, symbol);
    }

    private Map<Integer, Object> createElementMetadata(String label, ElementSymbol symbol) throws QueryMetadataException, TeiidComponentException {
        Object elementID = symbol.getMetadataID();

        Map<Integer, Object> column = new HashMap<Integer, Object>();
        column.put(ResultsMetadataConstants.AUTO_INCREMENTING, Boolean.valueOf(metadata.elementSupports(elementID, SupportConstants.Element.AUTO_INCREMENT)));
        column.put(ResultsMetadataConstants.CASE_SENSITIVE, Boolean.valueOf(metadata.elementSupports(elementID, SupportConstants.Element.CASE_SENSITIVE)));
        column.put(ResultsMetadataConstants.CURRENCY, Boolean.FALSE);
        Class<?> type = symbol.getType();
        column.put(ResultsMetadataConstants.DATA_TYPE, DataTypeManager.getDataTypeName(type));
        column.put(ResultsMetadataConstants.ELEMENT_LABEL, label);
        column.put(ResultsMetadataConstants.ELEMENT_NAME, labelAsName?label:metadata.getName(elementID));

        GroupSymbol group = symbol.getGroupSymbol();
        if(group == null || group.getMetadataID() == null) {
            column.put(ResultsMetadataConstants.GROUP_NAME, null);
        } else {
            column.put(ResultsMetadataConstants.GROUP_NAME, metadata.getFullName(group.getMetadataID()));
        }

        boolean allowsNull = metadata.elementSupports(elementID, SupportConstants.Element.NULL);
        boolean unknown = metadata.elementSupports(elementID, SupportConstants.Element.NULL_UNKNOWN);
        Integer nullable = null;
        if(unknown) {
            nullable = ResultsMetadataConstants.NULL_TYPES.UNKNOWN;
        } else {
            if(allowsNull) {
                nullable = ResultsMetadataConstants.NULL_TYPES.NULLABLE;
            } else {
                nullable = ResultsMetadataConstants.NULL_TYPES.NOT_NULL;
            }
        }
        column.put(ResultsMetadataConstants.NULLABLE, nullable);

        column.put(ResultsMetadataConstants.RADIX, new Integer(metadata.getRadix(elementID)));
        column.put(ResultsMetadataConstants.SCALE, new Integer(metadata.getScale(elementID)));


        int precision = getColumnPrecision(type, elementID);
        column.put(ResultsMetadataConstants.PRECISION, new Integer(precision));
        column.put(ResultsMetadataConstants.DISPLAY_SIZE, getColumnDisplaySize(precision, type, elementID));

        boolean comparable = metadata.elementSupports(elementID, SupportConstants.Element.SEARCHABLE_COMPARE);
        boolean likable = metadata.elementSupports(elementID, SupportConstants.Element.SEARCHABLE_LIKE);
        Integer searchable = null;
        if(comparable) {
            if(likable) {
                searchable = ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE;
            } else {
                searchable = ResultsMetadataConstants.SEARCH_TYPES.ALLEXCEPTLIKE;
            }
        } else {
            if(likable) {
                searchable = ResultsMetadataConstants.SEARCH_TYPES.LIKE_ONLY;
            } else {
                searchable = ResultsMetadataConstants.SEARCH_TYPES.UNSEARCHABLE;
            }
        }
        column.put(ResultsMetadataConstants.SEARCHABLE, searchable);

        column.put(ResultsMetadataConstants.SIGNED, Boolean.valueOf(metadata.elementSupports(elementID, SupportConstants.Element.SIGNED)));
        column.put(ResultsMetadataConstants.VIRTUAL_DATABASE_NAME, vdbName);
        column.put(ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION, vdbVersion);
        column.put(ResultsMetadataConstants.WRITABLE, new Boolean(metadata.elementSupports(elementID, SupportConstants.Element.UPDATE)));
        return column;
    }

    private Map<Integer, Object> createAggregateMetadata(String shortColumnName,
                                        AggregateSymbol symbol) throws QueryMetadataException, TeiidComponentException {

        Type function = symbol.getAggregateFunction();
        if(function == Type.MIN || function == Type.MAX){
            Expression expression = symbol.getArg(0);
            if(expression instanceof ElementSymbol) {
                return createColumnMetadata(shortColumnName, expression);
            }
        }
        return createTypedMetadata(shortColumnName, symbol);
    }

    private Map<Integer, Object> createTypedMetadata(String shortColumnName, Expression symbol) {
        Map<Integer, Object> result = getDefaultColumn(null, shortColumnName, symbol.getType());
        if (symbol instanceof Constant) {
            Constant c = (Constant)symbol;
            Object value = c.getValue();
            if (value instanceof String) {
                int length = ((String)value).length();
                result.put(ResultsMetadataConstants.PRECISION, length);
                result.put(ResultsMetadataConstants.DISPLAY_SIZE, length);
            } else if (value instanceof byte[]) {
                int length = ((byte[])value).length;
                result.put(ResultsMetadataConstants.PRECISION, length);
            } else if (value instanceof BigDecimal) {
                BigDecimal val = (BigDecimal)value;
                result.put(ResultsMetadataConstants.PRECISION, val.precision());
                result.put(ResultsMetadataConstants.DISPLAY_SIZE, val.precision()+1);
                result.put(ResultsMetadataConstants.SCALE, val.scale());
            } else if (value instanceof BigInteger) {
                BigInteger val = (BigInteger)value;
                int precision = new BigDecimal(val).precision();
                result.put(ResultsMetadataConstants.PRECISION, precision);
                result.put(ResultsMetadataConstants.DISPLAY_SIZE, precision+1);
            } else if (value == null) {
                if (symbol.getType() == DataTypeManager.DefaultDataClasses.STRING) {
                    result.put(ResultsMetadataConstants.PRECISION, 1);
                    result.put(ResultsMetadataConstants.DISPLAY_SIZE, 1);
                }
            }
        }
        return result;
    }

    private int getColumnPrecision(Class<?> dataType, Object elementID) throws QueryMetadataException, TeiidComponentException {
        if (!Number.class.isAssignableFrom(dataType)) {
            int length = metadata.getElementLength(elementID);
            if (length > 0) {
                return length;
            }
        } else {
            int precision =  metadata.getPrecision(elementID);
            if (precision > 0) {
                return precision;
            }
        }
        if (useJDBCDefaultPrecision) {
            return JDBCSQLTypeInfo.getDefaultPrecision(dataType).intValue();
        }
        return 0;
    }

    /**
     * This method would return the display size of the column to be returned by
     * getColumnDisplaySize() on ResultSetMetaData. This method would return the display
     * size based on the data type of the column.
     *
     * For numeric types, the display size for a numeric column will be equal to the precision of the column
     * plus 1 for the optional - sign plus "1" if the column is decimal to account for the ".".
     *
     * For string types, the display size is the length.
     *
     * @param dataType A string representing the MetaMatrix data type of the column
     * @return An int value giving the displaysize of the column
     */
    private Integer getColumnDisplaySize(int precision, Class<?> dataType, Object elementID) throws QueryMetadataException, TeiidComponentException {

       if(elementID != null && dataType.equals(DataTypeManager.DefaultDataClasses.STRING)) {
           int length = metadata.getElementLength(elementID);
           if(length > 0) {
               return new Integer(length);
           }

       } else if(Number.class.isAssignableFrom(dataType)) {
           if(precision > 0) {
               int displayLength = precision;

               // Add 1 for sign on all numbers
               displayLength = precision+1;

               // Add 1 for decimal point for FLOAT, DOUBLE, BIGDECIMAL
               if(dataType.equals(DataTypeManager.DefaultDataClasses.FLOAT) ||
                  dataType.equals(DataTypeManager.DefaultDataClasses.DOUBLE) ||
                  dataType.equals(DataTypeManager.DefaultDataClasses.BIG_DECIMAL)) {
                   displayLength++;
               }

               return new Integer(displayLength);
           }

       } else if(elementID != null &&
                       (dataType.equals(DataTypeManager.DefaultDataClasses.CLOB) ||
                       dataType.equals(DataTypeManager.DefaultDataClasses.BLOB) ||
                       dataType.equals(DataTypeManager.DefaultDataClasses.OBJECT))) {

           int length = metadata.getElementLength(elementID);
           if(length > 0) {
               return new Integer(length);
           }
       }
       // else BOOLEAN, DATE, TIME, TIMESTAMP, CHARACTER use max
       return JDBCSQLTypeInfo.getMaxDisplaySize(dataType);
    }

    public Map<Integer, Object> getDefaultColumn(String tableName, String columnName,
        Class<?> javaType) {
        return getDefaultColumn(tableName, columnName, columnName, javaType);
    }

    public Map<Integer, Object> getDefaultColumn(String tableName, String columnName, String columnLabel, Class<?> javaType ) {

        Map<Integer, Object> column = new HashMap<Integer, Object>();

        // set defaults
        column.put(ResultsMetadataConstants.VIRTUAL_DATABASE_NAME, vdbName);
        column.put(ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION, vdbVersion);
        column.put(ResultsMetadataConstants.GROUP_NAME, tableName);
        column.put(ResultsMetadataConstants.ELEMENT_NAME, labelAsName?columnLabel:columnName);
        column.put(ResultsMetadataConstants.ELEMENT_LABEL, columnLabel);
        column.put(ResultsMetadataConstants.AUTO_INCREMENTING, Boolean.FALSE);
        column.put(ResultsMetadataConstants.CASE_SENSITIVE, Boolean.TRUE);
        column.put(ResultsMetadataConstants.NULLABLE, ResultsMetadataConstants.NULL_TYPES.NULLABLE);
        column.put(ResultsMetadataConstants.SEARCHABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE);
        column.put(ResultsMetadataConstants.WRITABLE, Boolean.TRUE);
        column.put(ResultsMetadataConstants.CURRENCY, Boolean.FALSE);
        column.put(ResultsMetadataConstants.DATA_TYPE, DataTypeManager.getDataTypeName(javaType));
        column.put(ResultsMetadataConstants.RADIX, JDBCSQLTypeInfo.DEFAULT_RADIX);
        column.put(ResultsMetadataConstants.SIGNED, Boolean.TRUE);

        if (useJDBCDefaultPrecision) {
            column.put(ResultsMetadataConstants.PRECISION, JDBCSQLTypeInfo.getDefaultPrecision(javaType));
            column.put(ResultsMetadataConstants.SCALE, JDBCSQLTypeInfo.DEFAULT_SCALE);
        }
        //otherwise do not set precision and scale explicitly as we have default logic around the type

        column.put(ResultsMetadataConstants.DISPLAY_SIZE, JDBCSQLTypeInfo.getMaxDisplaySize(javaType));
        return column;
    }

}
