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

package org.teiid.connector.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.api.ConnectorException;


import com.metamatrix.connector.metadata.internal.IObjectSource;

/**
 * Uses an IObjectSource to process ObjectQuery requests and to produce results which are Lists of Lists of objects.
 * Each inner List corresponds to a row in the result set.  Each object in the inner List corresponds to the value
 * for a specific column and row in the result set.
 * 
 * If the table name contains a method name in "()" at the end of the string then the method name will be called
 * on each result object.  And each result of the method call will result in a new row being added to the result set.
 * In effect, this facility allows multi-valued method calls to serve as the basis of table definitions.
 * 
 * So a table name of the form "TABLE_NAME(getList)" will cause the method "getList" to be called on all result objects.
 * 
 * The method name will be stripped off when the table name is passed down to the object source. 
 */
public class ObjectQueryProcessor implements ResultsIterator.ResultsProcessor {
    private final IObjectSource objectSource;
    private String tableDefiningMethodName = null;
    private IObjectQuery query;
    
    public ObjectQueryProcessor(final IObjectSource objectSource) {
        this.objectSource = objectSource;
    }

    public ResultsIterator process(final IObjectQuery query) throws ConnectorException {
        tableDefiningMethodName = null;
        String tableName = query.getTableNameInSource();
        String tableNameForObjectSource = tableName;
        int startMethodNameIndex = tableName.lastIndexOf(String.valueOf(MetadataConnectorConstants.START_METHOD_NAME_CHAR));
        if (startMethodNameIndex > 0) {
            int endMethodNameIndex = tableName.lastIndexOf(String.valueOf(MetadataConnectorConstants.END_METHOD_NAME_CHAR));
            if ( endMethodNameIndex > 0) {
                if (endMethodNameIndex > startMethodNameIndex) {
                    tableDefiningMethodName = tableName.substring(startMethodNameIndex + 1, endMethodNameIndex);
                    tableNameForObjectSource = tableName.substring(0, startMethodNameIndex);
                }
            }
        }
        
        this.query = query;
        Collection results = objectSource.getObjects(tableNameForObjectSource, query.getCriteria());
        return new ResultsIterator(this, results.iterator());
    }
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.metadata.internal.ResultsProcessor#createRows(java.lang.Object, java.util.List)
	 */
    public void createRows(Object resultObject, List rows) {        
        ReflectionWrapper wrapper = new ReflectionWrapper(resultObject);
        if (tableDefiningMethodName == null) {
            addRow(wrapper, null, rows);
        } else {
            Collection subTableCollection = null;
            Object subTableResults = wrapper.get(tableDefiningMethodName);
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
        String[] columnNames = query.getColumnNames();
        //Class[] columnTypes = query.getColumnTypes();
        for (int i = 0; i < columnNames.length; i++) {
            Object value = null;
            
            if (columnNames[i].equals(tableDefiningMethodName)) {
                value = subTableObject;
            } else if (columnNames[i].startsWith(tableDefiningMethodName + MetadataConnectorConstants.METHOD_DELIMITER)) {
                ReflectionWrapper subWrapper = new ReflectionWrapper( subTableObject );
                String columnMethodName = columnNames[i].substring(tableDefiningMethodName.length()+1);
                value = subWrapper.get(columnMethodName);
            } else {
                value = wrapper.get(columnNames[i]);
            }
            query.checkType(i, value);
            if(value != null) {
                query.checkCaseType(i, value);
                Integer caseType = query.getCaseType(i);
                if(caseType.equals(IObjectQuery.UPPER_CASE)) {
                    value = value.toString().toUpperCase();
                } else if(caseType.equals(IObjectQuery.LOWER_CASE)) {
                    value = value.toString().toLowerCase();
                }
            }
            newRow.add(value);
        }
        rows.add(newRow);
    }
}
