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
import org.teiid.core.types.XMLType;
import org.teiid.dqp.internal.process.DQPWorkContext.Version;
import org.teiid.dqp.internal.process.SessionAwareCache.CacheID;
import org.teiid.dqp.message.RequestID;
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
     * @param metadataMsg The message from the client
     * @return The message for the client
     * @throws TeiidComponentException
     * @throws TeiidProcessingException 
     */
    MetadataResult processMessage(RequestID requestId, DQPWorkContext workContext, String preparedSql, boolean allowDoubleQuotedVariable) throws TeiidComponentException, TeiidProcessingException {
        this.requestID = requestId;
        
        this.metadata = workContext.getVDB().getAttachment(QueryMetadataInterface.class);
        this.labelAsName = workContext.getClientVersion().compareTo(Version.SEVEN_3) <= 0;
        
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
        	return getMetadataForCommand(workItem.getOriginalCommand());
        } 
        return obtainMetadataForPreparedSql(preparedSql, workContext, allowDoubleQuotedVariable);
    }
    
    // For each projected symbol, construct a metadata map
    private MetadataResult getMetadataForCommand(Command originalCommand) throws TeiidComponentException {
        Map<Integer, Object>[] columnMetadata = null;
        
        switch(originalCommand.getType()) {
            case Command.TYPE_QUERY:
                if(originalCommand instanceof Query) {
                    if (((Query)originalCommand).getInto() == null) {
                        columnMetadata = createProjectedSymbolMetadata(originalCommand);
                    }
                } else {
                    columnMetadata = createProjectedSymbolMetadata(originalCommand);
                }
                break;
            case Command.TYPE_STORED_PROCEDURE:
                columnMetadata = createProjectedSymbolMetadata(originalCommand);
                break;
            case Command.TYPE_INSERT:    
            case Command.TYPE_UPDATE:    
            case Command.TYPE_DELETE:
            case Command.TYPE_CREATE:    
            case Command.TYPE_DROP:
                break;    
            default:
                if (originalCommand.returnsResultSet()) {
                    columnMetadata = createProjectedSymbolMetadata(originalCommand);
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

    private Map<Integer, Object>[] createProjectedSymbolMetadata(Command originalCommand) throws TeiidComponentException {
        Map<Integer, Object>[] columnMetadata;
        // Allow command to use temporary metadata
        TempMetadataStore tempMetadata = originalCommand.getTemporaryMetadata();
        if(tempMetadata != null && tempMetadata.getData().size() > 0) {
            TempMetadataAdapter tempFacade = new TempMetadataAdapter(this.metadata, tempMetadata);
            this.metadata = tempFacade; 
        }
        
        List<Expression> projectedSymbols = originalCommand.getProjectedSymbols();
        columnMetadata = new Map[projectedSymbols.size()];
        
        Iterator<Expression> symbolIter = projectedSymbols.iterator();
        for(int i=0; symbolIter.hasNext(); i++) {
            Expression symbol = symbolIter.next();
            String shortColumnName = Symbol.getShortName(Symbol.getOutputName(symbol));
            if(symbol instanceof AliasSymbol) {
                symbol = ((AliasSymbol)symbol).getSymbol();
            }
            try {
                columnMetadata[i] = createColumnMetadata(shortColumnName, symbol);
            } catch(QueryMetadataException e) {
                 throw new TeiidComponentException(QueryPlugin.Event.TEIID30559, e);
            }
        }
        return columnMetadata;
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
            QueryResolver.resolveCommand(command, this.metadata);                        
        }
        return getMetadataForCommand(command);            
    }

    private Map<Integer, Object> createXMLColumnMetadata(Query xmlCommand) {
        GroupSymbol doc = xmlCommand.getFrom().getGroups().get(0);
        Map<Integer, Object> xmlMetadata = getDefaultColumn(doc.getName(), XML_COLUMN_NAME, XMLType.class);

        // Override size as XML may be big        
        xmlMetadata.put(ResultsMetadataConstants.DISPLAY_SIZE, JDBCSQLTypeInfo.XML_COLUMN_LENGTH);
        
        return xmlMetadata;
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
        return getDefaultColumn(null, shortColumnName, symbol.getType());
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
        return JDBCSQLTypeInfo.getDefaultPrecision(dataType).intValue();
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
        column.put(ResultsMetadataConstants.CASE_SENSITIVE, Boolean.FALSE);
        column.put(ResultsMetadataConstants.NULLABLE, ResultsMetadataConstants.NULL_TYPES.NULLABLE);  
        column.put(ResultsMetadataConstants.SEARCHABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE);
        column.put(ResultsMetadataConstants.WRITABLE, Boolean.TRUE);
        column.put(ResultsMetadataConstants.CURRENCY, Boolean.FALSE);
        column.put(ResultsMetadataConstants.DATA_TYPE, DataTypeManager.getDataTypeName(javaType));
        column.put(ResultsMetadataConstants.RADIX, JDBCSQLTypeInfo.DEFAULT_RADIX);
        column.put(ResultsMetadataConstants.SCALE, JDBCSQLTypeInfo.DEFAULT_SCALE);
        column.put(ResultsMetadataConstants.SIGNED, Boolean.TRUE);
        
        column.put(ResultsMetadataConstants.PRECISION, JDBCSQLTypeInfo.getDefaultPrecision(javaType));
        column.put(ResultsMetadataConstants.DISPLAY_SIZE, JDBCSQLTypeInfo.getMaxDisplaySize(javaType));
        return column;        
    }
    
}
