/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

/*
 */
package com.metamatrix.connector.jdbc.util;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.metamatrix.connector.jdbc.JDBCPlugin;
import com.metamatrix.connector.jdbc.extension.ResultsTranslator;
import com.metamatrix.connector.jdbc.extension.ValueRetriever;
import com.metamatrix.connector.jdbc.extension.ValueTranslator;
import com.metamatrix.connector.jdbc.extension.impl.BasicValueTranslator;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ICommand;
import com.metamatrix.data.language.ICompareCriteria;
import com.metamatrix.data.language.IInsert;
import com.metamatrix.data.language.ILiteral;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IQueryCommand;
import com.metamatrix.data.language.IUpdate;
import com.metamatrix.data.metadata.runtime.Element;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;

/**
 */
public class JDBCExecutionHelper {

    /**
     * Return datatypes for projected columns.
     * @param command
     * @return Array of datatype class.
     */
    public static Class[] getColumnDataTypes(ICommand command) {
        Class[] dataTypes = null;
        if(command instanceof IQueryCommand){
            return ((IQueryCommand)command).getColumnTypes();
        }
        return dataTypes;
    }
    
    /**
     * Create batch for an update.
     * @param updateCount
     * @return Created batch.
     */
    public static Batch createBatch(int updateCount) {
        Batch batch = new BasicBatch();
        List row = new ArrayList(1);
        row.add(new Integer(updateCount));
        batch.addRow(row);
        batch.setLast();
        return batch;
    }

    /**
     * Create batch for a query.
     * @param results
     * @param columnDataTypes
     * @param maxBatchSize
     * @return
     */
    public static Batch createBatch(ResultSet results, Class[] columnDataTypes, int maxBatchSize, boolean trimStrings, ResultsTranslator resultsTranslator, ExecutionContext context, Calendar calendar, TypeFacility typeFacility) throws ConnectorException {
        Batch batch = new BasicBatch();
        if(results == null){
            return batch;
        }
        
        // Build up list of flags on whether to trim strings
        boolean[] trimColumn = new boolean[columnDataTypes.length];     // defaults to false
        int[] nativeTypes = new int[columnDataTypes.length];
        try {
            ResultSetMetaData rsmd = results.getMetaData();
            for(int i=0; i<columnDataTypes.length; i++) {
            	
            	nativeTypes[i] = rsmd.getColumnType(i+1);
            	if ((nativeTypes[i] == Types.BLOB && (columnDataTypes[i] == TypeFacility.RUNTIME_TYPES.BLOB || columnDataTypes[i] == TypeFacility.RUNTIME_TYPES.OBJECT))
						|| (nativeTypes[i] == Types.CLOB && (columnDataTypes[i] == TypeFacility.RUNTIME_TYPES.CLOB || columnDataTypes[i] == TypeFacility.RUNTIME_TYPES.OBJECT))) {
					context.keepExecutionAlive(true);
				}
            	
                if(columnDataTypes[i].equals(String.class)) {
                    if(trimStrings || nativeTypes[i] == Types.CHAR) {
                        trimColumn[i] = true;
                    } 
                }
            }
        } catch(SQLException e) {
            throw new ConnectorException(e.getMessage());
        }

        // Reusable list of transformations for the types, lazily loaded
        boolean[] transformKnown = new boolean[columnDataTypes.length];
        ValueTranslator[] transforms = new ValueTranslator[columnDataTypes.length];

        // Move the result data to the query results
        List vals = null;
        int numCols = columnDataTypes.length;
        int rowCnt = 0;
        List valueTranslators = resultsTranslator.getValueTranslators(); 
        ValueRetriever valueRetriever = resultsTranslator.getValueRetriever();
        
        try {
            while (rowCnt < maxBatchSize) {
                if (results.next()) {
                //while (results.next() && rowCnt <= maxBatchSize) {
                    // New row for result set
                    vals = new ArrayList(numCols);
    
                    for (int i = 0; i < numCols; i++) {
                        // Convert from 0-based to 1-based
                        Object value = valueRetriever.retrieveValue(results, i+1, columnDataTypes[i], nativeTypes[i], calendar, typeFacility);
                        if(value != null) {
                            // Determine transformation if unknown
                            if(! transformKnown[i]) {
                                Class valueType = value.getClass();
                                if(!columnDataTypes[i].isAssignableFrom(valueType)) {
                                    transforms[i] = determineTransformation(valueType, columnDataTypes[i], valueTranslators, resultsTranslator.getTypefacility());
                                }
                                transformKnown[i] = true;
                            }
    
                            // Transform value if necessary
                            if(transforms[i] != null) {
                                value = transforms[i].translate(value, context);
                            }
                                                        
                            // Trim string column if necessary
                            if(trimColumn[i]) {
                                value = trimString((String) value);
                            }
                        }
                        vals.add(value); 
                    }
    
                    // Add a row to the result set and  set the local variable to determine if more rows should be read
                    batch.addRow(vals);
                    rowCnt++;
                } else {
                    break;
                }
            }

            if(rowCnt < maxBatchSize){
                //no more row then set last batch
                batch.setLast();
            }

        } catch (ConnectorException e) {
            throw e;
        } catch (SQLException e) {
//            ConnectorLogManager.logError(null, null, e,
//                    "Unexpected exception while translating results: " + e.getMessage());
            throw new ConnectorException(e,
                    JDBCPlugin.Util.getString("JDBCTranslator.Unexpected_exception_translating_results___8", e.getMessage())); //$NON-NLS-1$
        } catch (Throwable e) {
            throw new ConnectorException(
                e,
                JDBCPlugin.Util.getString("JDBCTranslator.Unknown_error_translating_results___9", e.getMessage())); //$NON-NLS-1$
        }
        
        return batch;
    }
    
    /**
     * Modify the original params list to remove the resolved parameters, only the byte[],
     * Blob and Clob get left.
     * @param command ICommand
     * @param sql Translated sql string
     * @throws SQLException
     */
    public static List setParametersForUpdateLOB(ICommand command){
        List modifyParams = null;
        if (command instanceof IInsert) {
            List originalParams = ((IInsert)command).getValues();
            modifyParams = new LinkedList(originalParams);
            // remove all non-lob and non-stream/bytes[] params
            Iterator iter = modifyParams.iterator();
            while(iter.hasNext()){
                ILiteral param = (ILiteral) iter.next();
                Object literalValue = param.getValue();
                if (literalValue == null || (!List.class.isAssignableFrom(literalValue.getClass()) 
                                && !Blob.class.isAssignableFrom(literalValue.getClass())
                                && !Clob.class.isAssignableFrom(literalValue.getClass()))) {
                    iter.remove();
                }
            }
            
        } else if (command instanceof IUpdate) {
            List originalParams = ((IUpdate)command).getChanges();
            modifyParams = new LinkedList(originalParams);
            // remove all resolved CompareCriteria
            Iterator iter = modifyParams.iterator();
            while(iter.hasNext()){
                ICompareCriteria param = (ICompareCriteria)iter.next();
                Object right = ((ILiteral)param.getRightExpression()).getValue();                
                if (right == null || (!List.class.isAssignableFrom(right.getClass()) && !Blob.class.isAssignableFrom(right.getClass())
                    && !Clob.class.isAssignableFrom(right.getClass()))) {
                    iter.remove();
                }
            }  
        }                
        
        return modifyParams;    
    } 

    /**
     * @param parameters List of IParameter
     * @param sql
     * @return Map of IParameter to index in sql.
     */
    public static Map createParameterIndexMap(List parameters, String sql) {
        if(parameters == null || parameters.isEmpty()){
            return Collections.EMPTY_MAP;
        }
        Map paramsIndexes = new HashMap();
        int index  = 1;
        
        //return parameter, if there is any,  is the first parameter
        Iterator iter = parameters.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() == IParameter.RETURN){
                paramsIndexes.put(param, new Integer(index++));
                break;
            }
        }
                      
        iter = parameters.iterator();
        while(iter.hasNext()){
            IParameter param = (IParameter)iter.next();
            if(param.getDirection() != IParameter.RESULT_SET && param.getDirection() != IParameter.RETURN){
                paramsIndexes.put(param, new Integer(index++));
            }
        }
        return paramsIndexes;
    }

    /**
     * @param results
     * @return
     */
    public static Class[] getColumnDataTypes(List params, RuntimeMetadata metadata) throws ConnectorException {
        if (params != null) { 
            IParameter resultSet = null;
            Iterator iter = params.iterator();
            while(iter.hasNext()){
                IParameter param = (IParameter)iter.next();
                if(param.getDirection() == IParameter.RESULT_SET){
                    resultSet = param;
                    break;
                }
            }

            if(resultSet != null){
                List columnMetadata = null;
                columnMetadata = resultSet.getMetadataID().getChildIDs();

                int size = columnMetadata.size();
                Class[] coulmnDTs = new Class[size];
                for(int i =0; i<size; i++ ){
                    MetadataID mID = (MetadataID)columnMetadata.get(i);
                    Object mObj = metadata.getObject(mID);
                    coulmnDTs[i] = ((Element)mObj).getJavaType();
                }
                return coulmnDTs;
            }

        }
        return new Class[0];
    }
    
    public static Object convertValue(Object value, Class expectedType, List valueTranslators, TypeFacility typeFacility, boolean trimStrings, ExecutionContext context) throws ConnectorException {
        if(expectedType.isAssignableFrom(value.getClass())){
            return value;
        }
        ValueTranslator translator = determineTransformation(value.getClass(), expectedType, valueTranslators, typeFacility);
        Object result = translator.translate(value, context);
        if(trimStrings && result instanceof String){
            result = ((String)result).trim();
        }
        return result;
    }

    /**
     * @param actualType
     * @param expectedType
     * @return Transformation between actual and expected type
     */
    protected static ValueTranslator determineTransformation(Class actualType, Class expectedType, List valueTranslators, TypeFacility typeFacility) throws ConnectorException {
        ValueTranslator valueTranslator = null;
        
        //check valueTranslators first
        if(valueTranslators != null && !valueTranslators.isEmpty()){        
            Iterator iter = valueTranslators.iterator();
            while(iter.hasNext()){
                ValueTranslator translator = (ValueTranslator)iter.next();
                
                //Evaluate expressions in this order for performance.
                if(expectedType.equals(translator.getTargetType()) && translator.getSourceType().isAssignableFrom(actualType)){
                    valueTranslator = translator;
                    break;
                }
            }
        }
        
        if(valueTranslator == null){
            valueTranslator = new BasicValueTranslator(actualType, expectedType, typeFacility);
        }
        return valueTranslator;
    }
        
    /**
     * Expects string to never be null 
     * @param value Incoming value
     * @return Right trimmed value  
     * @since 4.2
     */
    static String trimString(String value) {
        for(int i=value.length()-1; i>=0; i--) {
            if(value.charAt(i) != ' ') {
                // end of trim, return what's left
                return value.substring(0, i+1);
            }
        }

        // All spaces, so trim it all
        return ""; //$NON-NLS-1$        
    }
}
