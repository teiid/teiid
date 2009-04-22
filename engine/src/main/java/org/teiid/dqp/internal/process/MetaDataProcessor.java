/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.dqp.internal.process;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.dqp.internal.process.PreparedPlanCache.CacheID;
import org.teiid.dqp.internal.process.multisource.MultiSourceMetadataWrapper;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.QueryMetadataException;
import com.metamatrix.api.exception.query.QueryParserException;
import com.metamatrix.api.exception.query.QueryResolverException;
import com.metamatrix.common.application.ApplicationEnvironment;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.dqp.client.MetadataResult;
import com.metamatrix.dqp.message.RequestID;
import com.metamatrix.dqp.metadata.ResultsMetadataConstants;
import com.metamatrix.dqp.metadata.ResultsMetadataDefaults;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.VDBService;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.metadata.SupportConstants;
import com.metamatrix.query.metadata.TempMetadataAdapter;
import com.metamatrix.query.metadata.TempMetadataStore;
import com.metamatrix.query.parser.ParseInfo;
import com.metamatrix.query.parser.QueryParser;
import com.metamatrix.query.resolver.QueryResolver;
import com.metamatrix.query.sql.ReservedWords;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.sql.lang.Query;
import com.metamatrix.query.sql.lang.XQuery;
import com.metamatrix.query.sql.symbol.AggregateSymbol;
import com.metamatrix.query.sql.symbol.AliasSymbol;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.GroupSymbol;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.visitor.ReferenceCollectorVisitor;
import com.metamatrix.query.tempdata.TempTableStore;

/**
 * Handles MetaDataMessages on behalf of DQPCore.
 */
public class MetaDataProcessor {

    // Resources
    private MetadataService metadataService;
    private DQPCore requestManager;
    private QueryMetadataInterface metadata;
    private PreparedPlanCache planCache;
    private ApplicationEnvironment env;
        
    private TempTableStoresHolder tempTableStoresHolder;
    private String vdbName;
    private String vdbVersion;
    private RequestID requestID;
    
    public MetaDataProcessor(MetadataService metadataService, DQPCore requestManager, PreparedPlanCache planCache, ApplicationEnvironment env, TempTableStoresHolder tempTableStoresHolder) {
        this.metadataService = metadataService;    
        this.requestManager = requestManager;
        this.planCache = planCache;
        this.env = env;
        this.tempTableStoresHolder = tempTableStoresHolder;
    }
        
    /**
     * Process a metadata request message - this is typically either a request 
     * for metadata for a prepared statement or a request for full metadata from
     * an already processed command.
     * @param metadataMsg The message from the client
     * @return The message for the client
     * @throws MetaMatrixComponentException
     * @throws MetaMatrixProcessingException 
     */
    MetadataResult processMessage(RequestID requestId, DQPWorkContext workContext, String preparedSql, boolean allowDoubleQuotedVariable) throws MetaMatrixComponentException, MetaMatrixProcessingException {
        final VDBService vdbService = (VDBService)env.findService(DQPServiceNames.VDB_SERVICE);
        this.requestID = requestId;
        this.vdbName = workContext.getVdbName();
        this.vdbVersion = workContext.getVdbVersion();
        
        QueryMetadataInterface md = metadataService.lookupMetadata(vdbName, vdbVersion);
        // Defect 15029 - Use the QueryMetadataWrapper to hide models with private visibility when resolving the command.
        this.metadata = new QueryMetadataWrapper(md, vdbName, vdbVersion, vdbService);
        
        // If multi-source, use the multi-source wrapper as well
        Collection multiModels = vdbService.getMultiSourceModels(vdbName, vdbVersion);
        if(multiModels != null && multiModels.size() > 0) { 
            this.metadata = new MultiSourceMetadataWrapper(this.metadata, multiModels);
        }

        RequestWorkItem workItem = null;
        try {
        	workItem = requestManager.getRequestWorkItem(requestID);
        } catch (MetaMatrixProcessingException e) {
        	if (preparedSql == null) {
        		throw e;
        	}
        }
        
        TempTableStore tempTableStore = null;
        if(tempTableStoresHolder != null) {
            if (workItem != null) {
                tempTableStore = tempTableStoresHolder.getTempTableStore(workContext.getConnectionID());
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
    private MetadataResult getMetadataForCommand(Command originalCommand) throws MetaMatrixComponentException {
        Map[] columnMetadata = null;
        
        switch(originalCommand.getType()) {
            case Command.TYPE_QUERY:
                if(originalCommand instanceof Query) {
                    if (((Query)originalCommand).getIsXML()) {
                        columnMetadata = new Map[1];
                        columnMetadata[0] = createXMLColumnMetadata((Query)originalCommand);
                    } else if (((Query)originalCommand).getInto() == null) {
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
            case Command.TYPE_XQUERY:
                columnMetadata = new Map[1];
                columnMetadata[0] = createXQueryColumnMetadata((XQuery)originalCommand);
                break;
            default:
                columnMetadata = createProjectedSymbolMetadata(originalCommand);                   
        }
        
        return new MetadataResult(columnMetadata, ReferenceCollectorVisitor.getReferences(originalCommand).size());
    }

    private Map[] createProjectedSymbolMetadata(Command originalCommand) throws MetaMatrixComponentException {
        Map[] columnMetadata;
        // Allow command to use temporary metadata
        Map tempMetadata = originalCommand.getTemporaryMetadata();
        if(tempMetadata != null && tempMetadata.size() > 0) {
            TempMetadataAdapter tempFacade = new TempMetadataAdapter(this.metadata, new TempMetadataStore(tempMetadata));
            this.metadata = tempFacade; 
        }
        
        List projectedSymbols = originalCommand.getProjectedSymbols();
        columnMetadata = new Map[projectedSymbols.size()];
        
        Iterator symbolIter = projectedSymbols.iterator();
        for(int i=0; symbolIter.hasNext(); i++) {
            SingleElementSymbol symbol = (SingleElementSymbol) symbolIter.next();
            String shortColumnName = SingleElementSymbol.getShortName(symbol.getOutputName());
            if(symbol instanceof AliasSymbol) {
                symbol = ((AliasSymbol)symbol).getSymbol();
            }
            try {
                columnMetadata[i] = createColumnMetadata(shortColumnName, symbol);
            } catch(QueryMetadataException e) {
                throw new MetaMatrixComponentException(e);
            }
        }
        return columnMetadata;
    }

    private MetadataResult obtainMetadataForPreparedSql(String sql, DQPWorkContext workContext, boolean isDoubleQuotedVariablesAllowed) throws QueryParserException, QueryResolverException, MetaMatrixComponentException {
        Command command = null;
        
        ParseInfo info = new ParseInfo();
        // Defect 19747 - the parser needs the following connection property to decide whether to treat double-quoted strings as variable names
        info.allowDoubleQuotedVariable = isDoubleQuotedVariablesAllowed;
        CacheID id = new PreparedPlanCache.CacheID(workContext, info, sql);
        PreparedPlanCache.PreparedPlan plan = planCache.getPreparedPlan(id);
        if(plan != null) {
            command = plan.getCommand();
        } else {
        	command = QueryParser.getQueryParser().parseCommand(sql, info);
            QueryResolver.resolveCommand(command, Collections.EMPTY_MAP, false, this.metadata, AnalysisRecord.createNonRecordingRecord());                        
        }
        return getMetadataForCommand(command);            
    }

    private Map createXMLColumnMetadata(Query xmlCommand) {
        GroupSymbol doc = (GroupSymbol) xmlCommand.getFrom().getGroups().get(0);
        Map xmlMetadata = getDefaultColumn(vdbName, vdbVersion, doc.getName(), ResultsMetadataDefaults.XML_COLUMN_NAME, XMLType.class);

        // Override size as XML may be big        
        xmlMetadata.put(ResultsMetadataConstants.DISPLAY_SIZE, ResultsMetadataDefaults.XML_COLUMN_LENGTH);
        
        return xmlMetadata;
    }

    private Map createXQueryColumnMetadata(XQuery xqueryCommand) {
        Map xqueryMetadata = getDefaultColumn(vdbName, vdbVersion, null, ResultsMetadataDefaults.XML_COLUMN_NAME, XMLType.class);

        // Override size as XML may be big        
        xqueryMetadata.put(ResultsMetadataConstants.DISPLAY_SIZE, ResultsMetadataDefaults.XML_COLUMN_LENGTH);
        
        return xqueryMetadata;
    }

    private Map createColumnMetadata(String shortColumnName, SingleElementSymbol symbol) throws QueryMetadataException, MetaMatrixComponentException {
        if(symbol instanceof ElementSymbol) {
            return createElementMetadata(shortColumnName, (ElementSymbol) symbol);        
        } else if(symbol instanceof AggregateSymbol) {
            return createAggregateMetadata(shortColumnName, (AggregateSymbol) symbol);
        }
        return createTypedMetadata(shortColumnName, symbol);            
    }
    
    private Map createElementMetadata(String shortColumnName, ElementSymbol symbol) throws QueryMetadataException, MetaMatrixComponentException {
        Object elementID = symbol.getMetadataID();
        
        Map column = new HashMap();
        column.put(ResultsMetadataConstants.AUTO_INCREMENTING, Boolean.valueOf(metadata.elementSupports(elementID, SupportConstants.Element.AUTO_INCREMENT)));
        column.put(ResultsMetadataConstants.CASE_SENSITIVE, Boolean.valueOf(metadata.elementSupports(elementID, SupportConstants.Element.CASE_SENSITIVE)));
        column.put(ResultsMetadataConstants.CURRENCY, Boolean.FALSE);
        Class type = symbol.getType();
        column.put(ResultsMetadataConstants.DATA_TYPE, DataTypeManager.getDataTypeName(type));
        column.put(ResultsMetadataConstants.ELEMENT_LABEL, shortColumnName); 
        column.put(ResultsMetadataConstants.ELEMENT_NAME, shortColumnName);
        
        GroupSymbol group = symbol.getGroupSymbol();        
        if(group == null) {
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
    
    private Map createAggregateMetadata(String shortColumnName,
                                        AggregateSymbol symbol) throws QueryMetadataException, MetaMatrixComponentException {
        
        Expression expression = symbol.getExpression();
        String function = symbol.getAggregateFunction();
        if(function.equals(ReservedWords.MIN) || function.equals(ReservedWords.MAX)){
            if(expression instanceof ElementSymbol) {
                return createColumnMetadata(shortColumnName, (ElementSymbol)expression);
            }
        }
        return createTypedMetadata(shortColumnName, symbol);
    }

    private Map createTypedMetadata(String shortColumnName, SingleElementSymbol symbol) {
        return getDefaultColumn(vdbName, vdbVersion, null, shortColumnName, symbol.getType());
    }
    
    private int getColumnPrecision(Class dataType, Object elementID) throws QueryMetadataException, MetaMatrixComponentException {
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
        return ResultsMetadataDefaults.getDefaultPrecision(dataType).intValue();
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
    private Integer getColumnDisplaySize(int precision, Class dataType, Object elementID) throws QueryMetadataException, MetaMatrixComponentException {

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
       return ResultsMetadataDefaults.getMaxDisplaySize(dataType);
    }

    public Map getDefaultColumn(String vdbName, String vdbVersion, 
        String tableName, String columnName, Class javaType) {
            
        Map column = new HashMap();
        
        // set defaults
        column.put(ResultsMetadataConstants.VIRTUAL_DATABASE_NAME, vdbName);
        column.put(ResultsMetadataConstants.VIRTUAL_DATABASE_VERSION, vdbVersion);
        column.put(ResultsMetadataConstants.GROUP_NAME, tableName);
        column.put(ResultsMetadataConstants.ELEMENT_NAME, columnName);
        column.put(ResultsMetadataConstants.ELEMENT_LABEL, columnName);
        column.put(ResultsMetadataConstants.AUTO_INCREMENTING, Boolean.FALSE);
        column.put(ResultsMetadataConstants.CASE_SENSITIVE, Boolean.FALSE);
        column.put(ResultsMetadataConstants.NULLABLE, ResultsMetadataConstants.NULL_TYPES.NULLABLE);  
        column.put(ResultsMetadataConstants.SEARCHABLE, ResultsMetadataConstants.SEARCH_TYPES.SEARCHABLE);
        column.put(ResultsMetadataConstants.WRITABLE, Boolean.TRUE);
        column.put(ResultsMetadataConstants.CURRENCY, Boolean.FALSE);
        column.put(ResultsMetadataConstants.DATA_TYPE, DataTypeManager.getDataTypeName(javaType));
        column.put(ResultsMetadataConstants.RADIX, ResultsMetadataDefaults.DEFAULT_RADIX);
        column.put(ResultsMetadataConstants.SCALE, ResultsMetadataDefaults.DEFAULT_SCALE);
        column.put(ResultsMetadataConstants.SIGNED, Boolean.TRUE);
        
        column.put(ResultsMetadataConstants.PRECISION, ResultsMetadataDefaults.getDefaultPrecision(javaType));
        column.put(ResultsMetadataConstants.DISPLAY_SIZE, ResultsMetadataDefaults.getMaxDisplaySize(javaType));
        return column;        
    }
    
}
