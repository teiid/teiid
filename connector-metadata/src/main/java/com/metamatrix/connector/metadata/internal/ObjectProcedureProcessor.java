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

package com.metamatrix.connector.metadata.internal;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.common.types.BlobType;
import com.metamatrix.common.types.ClobImpl;
import com.metamatrix.common.types.ClobType;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.vdb.api.VDBFile;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.metadata.MetadataConnectorConstants;
import com.metamatrix.connector.metadata.ResultsIterator;
import com.metamatrix.core.util.ArgCheck;


/**
 * ObjectProcedureProcessor
 * @since 4.2
 */
public class ObjectProcedureProcessor implements ResultsIterator.ResultsProcessor {

    // object to use to query metadata
    private final IObjectSource objectSource;
    
    private String resultDefiningMethodName;
    private String resultSetSourceName;
    private ObjectProcedure procedure;


    /**
     * ObjectProcedureProcessor
     * @param objectSource The object used to query metadata and get back results
     * @since 4.2
     */
    public ObjectProcedureProcessor(final IObjectSource objectSource) {
        this.objectSource = objectSource;
    }

    /**
     * This should process or execute the ObjectProcedure by evaluating the resultSet on the
     * procedure and also any output parameters that need to be set when the procedure is executed.
     * @param procedure The ObjectProcedure containing procedure info
     * @throws ConnectorException This should never occur
     * @since 4.2
     */
    public ResultsIterator process(final ObjectProcedure procedure)  throws ConnectorException {
        // nameInSource on the resultSet
        // if this is null then the procedure does not have
        // a resultSet
    	ArgCheck.isNotNull(procedure);
    	this.procedure = procedure;
        String resultSetNameInSource = procedure.getResultSetNameInSource();
        ArgCheck.isNotNull(resultSetNameInSource);
                
        // check if the name has a methodName segment on it
        int startMethodNameIndex = resultSetNameInSource.lastIndexOf(String.valueOf(MetadataConnectorConstants.START_METHOD_NAME_CHAR));
        if (startMethodNameIndex > 0) {
            int endMethodNameIndex = resultSetNameInSource.lastIndexOf(String.valueOf(MetadataConnectorConstants.END_METHOD_NAME_CHAR));
            if ( endMethodNameIndex > 0) {
                if (endMethodNameIndex > startMethodNameIndex) {
                    this.resultDefiningMethodName = resultSetNameInSource.substring(startMethodNameIndex + 1, endMethodNameIndex);
                    this.resultSetSourceName = resultSetNameInSource.substring(0, startMethodNameIndex);
                }
            }
        }

        // if there is no method segment
        if(this.resultSetSourceName == null) {
            this.resultSetSourceName = resultSetNameInSource;
        }  
        // object to use to query metadata
        IObjectSource objectSource = ObjectProcedureProcessor.this.objectSource;
        Collection results = objectSource.getObjects(resultSetSourceName, procedure.getCriteria());
        return new ResultsIterator(this, results.iterator());
    }

    /**
     * Create a row by obtaining values by calling the getMethods on the
     * resultObject, before the values are fetched all the inputparameter values
     * get set on the resultObject 
     * @param resultObject
     * @since 4.2
     */
    public void createRows(Object resultObject, List rows) {
        ArgCheck.isNotNull(resultObject);

        ReflectionWrapper wrapper = new ReflectionWrapper(resultObject);
        // set all the inputparameter values on the wrapper before
        // trying to get the values from the result Object
        Map propValues = this.procedure.getPropValues();
        if(propValues != null) {
            for(final Iterator entryIter = propValues.entrySet().iterator(); entryIter.hasNext();) {
                Map.Entry entry = (Map.Entry) entryIter.next();
                Object key = entry.getKey();
                Object[] value = {entry.getValue()};
                wrapper.set(key.toString(), value);
            }
        }
        if (this.resultDefiningMethodName == null) {
            addRow(wrapper, null, rows);
        } else {
            Collection subTableCollection = null;
            Object subTableResults = wrapper.get(this.resultDefiningMethodName);
            if (subTableResults.getClass().isArray()) {
                subTableCollection = Arrays.asList((Object[]) subTableResults);
            } else if (subTableResults instanceof Collection) {
                subTableCollection = (Collection) subTableResults;
            }
            createRowsFor(wrapper, subTableCollection, rows);
        }
    }

    private void createRowsFor(ReflectionWrapper wrapperAroundResultObject, Collection subTableCollection, List rows) {
        for (Iterator iterator=subTableCollection.iterator(); iterator.hasNext(); ) {
            Object subTableObject = iterator.next();
            addRow(wrapperAroundResultObject, subTableObject, rows);
        }        
    }

    private void addRow(ReflectionWrapper wrapper, Object subTableObject, List rows) {
        List newRow = new ArrayList();
        String[] columnNames = this.procedure.getColumnNamesInSource();
        for (int i = 0; i < columnNames.length; i++) {
            Object value = null;
            if (columnNames[i].equals(this.resultDefiningMethodName)) {
                value = subTableObject;
            } else if (columnNames[i].startsWith(this.resultDefiningMethodName + MetadataConnectorConstants.METHOD_DELIMITER)) {
                ReflectionWrapper subWrapper = new ReflectionWrapper( subTableObject );
                String columnMethodName = columnNames[i].substring(this.resultDefiningMethodName.length()+1);
                value = subWrapper.get(columnMethodName);
            } else {
                value = wrapper.get(columnNames[i]);
            }
            Class type = this.procedure.getResultSetColumnType(i);
            // create a value reference if the column type is a clob or a blob
            if(type.equals(ClobType.class)) {
            	// since FileRecord is in modeler core, and we do not want the dependency
            	
                if (value instanceof VDBFile) {
                    try {
                    	VDBFile record = (VDBFile)value;
                        value = new ClobImpl(record.getContent(), Charset.defaultCharset(), (int)record.getFileLength());
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
               
                value = DataTypeManager.convertToRuntimeType(value);
            } else if(type.equals(BlobType.class)) {
            	value = DataTypeManager.convertToRuntimeType(value);
            } else {
                this.procedure.checkType(i, value);
            }
            newRow.add(value);
        }
        rows.add(newRow);
    }
}
