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

package com.metamatrix.connector.metadata.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.IMetadataReference;
import com.metamatrix.connector.language.IParameter;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.metadata.MetadataConnectorConstants;
import com.metamatrix.connector.metadata.MetadataConnectorPlugin;
import com.metamatrix.connector.metadata.index.MetadataLiteralCriteria;
import com.metamatrix.connector.metadata.runtime.Element;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.metadata.runtime.MetadataObject;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ArgCheck;
import com.metamatrix.core.util.StringUtil;


/** 
 * ObjectProcedure
 * @since 4.2
 */
public class ObjectProcedure {

    private final RuntimeMetadata metadata;
    private final IProcedure procedure;

    private IParameter resultSetParameter;
    // in/inout parameters
    private Collection inParams;

    private Map criteriaMap;
    private Map propValueMap;
    
    // column properties of the resultset 
    private String[] columnNames = null;
    private String[] columnNamesInSource = null;
    private Class[] columnTypes = null;    

    /** 
     * ObjectProcedure
     * @since 4.2
     */
    public ObjectProcedure(final RuntimeMetadata metadata, final ICommand command) throws ConnectorException {
        ArgCheck.isNotNull(metadata);
        ArgCheck.isNotNull(command);

        this.metadata = metadata;
        this.procedure = (IProcedure) command;
        initParameters();
        initSetProperties();
        initCriteria();
    }
    
    public RuntimeMetadata getMetadata() {
        return this.metadata;
    }
    
    public Collection getInParameters() {
        return this.inParams;
    }

    /**
     * Collect inparameters and resultset..  
     * @since 4.2
     */
    private void initParameters() throws ConnectorException {
        Collection parameters = this.procedure.getParameters();
        if(parameters != null) {
            this.inParams = new HashSet(parameters.size());
            for(final Iterator iter = parameters.iterator(); iter.hasNext();) {
                IParameter parameter = (IParameter) iter.next();
                // if there is one result set parameter
                if(parameter.getDirection() == IParameter.RESULT_SET) {
                    this.resultSetParameter = parameter;
                    initResultSet();
                }
                if(parameter.getDirection() == IParameter.IN || parameter.getDirection() == IParameter.INOUT) {
                    inParams.add(parameter);
                }                
            }
        }
    }

    /**
     * Collect names, types, nameinsources of all columns in the resultset.  
     * @since 4.2
     */
    private void initResultSet() throws ConnectorException {
        List columnMetadata = resultSetParameter.getMetadataID().getChildIDs();
        int size = columnMetadata.size();
        columnNames = new String[size];
        columnNamesInSource = new String[size];
        columnTypes = new Class[size];
        for(int i =0; i<size; i++ ){
            MetadataID mID = (MetadataID)columnMetadata.get(i);
            Element element = (Element) metadata.getObject(mID);
            columnNames[i] = element.getMetadataID().getFullName();
            columnNamesInSource[i] = element.getNameInSource();
            columnTypes[i] = element.getJavaType();
        }
    }

    /** 
     * @return Returns the columnNames on resultset.
     * @since 4.2
     */
    public String[] getColumnNames() {
        return this.columnNames;
    }
    /** 
     * @return Returns the columnNamesInSource  on resultset.
     * @since 4.2
     */
    public String[] getColumnNamesInSource() {
        return this.columnNamesInSource;
    }
    /** 
     * @return Returns the columnTypes  on resultset.
     * @since 4.2
     */
    public Class[] getColumnTypes() {
        return this.columnTypes;
    }
    public String getResultSetNameInSource() throws ConnectorException {
        return getParameterNameInSource(this.resultSetParameter);
    }

    public String getParameterNameInSource(final IParameter parameter) throws ConnectorException {
        return getMetadataObjectName(parameter);
    }

    public String getProcedureNameInSource() throws ConnectorException {
        return getMetadataObjectName(this.procedure);
    }

    /**
     * Build the criteriaMap used for querying the index files.
     * This is built by using all input parameters having get method names
     * as their name in sources.   
     * @since 4.2
     */
    private void initCriteria() throws ConnectorException {
        criteriaMap = new HashMap();
        if(this.inParams != null) {
	        for(final Iterator iter = this.inParams.iterator(); iter.hasNext();) {
	            IParameter parameter = (IParameter) iter.next();
	            String nameInSource = getParameterNameInSource(parameter);
	            if(nameInSource != null) {
	                if(StringUtil.startsWithIgnoreCase(nameInSource, MetadataConnectorConstants.GET_METHOD_PREFIX)) {
	                    Object value = parameter.getValue();
	                    if(value != null) {
                            MetadataLiteralCriteria literalCriteria = new MetadataLiteralCriteria(nameInSource, value);                            
	                        criteriaMap.put(nameInSource.toUpperCase(), literalCriteria);
	                    }
	                }
	            }
	        }
        }
    }
    
    /**
     * Build the propValueMap used for setting paramter values on the results.
     * This is built by using all input parameters having set method names
     * as their name in sources.   
     * @since 4.2
     */
    private void initSetProperties() throws ConnectorException {
        this.propValueMap = new HashMap();
        if(this.inParams != null) {
	        for(final Iterator iter = this.inParams.iterator(); iter.hasNext();) {
	            IParameter parameter = (IParameter) iter.next();
	            String nameInSource = getParameterNameInSource(parameter);
	            if(nameInSource != null) {
	                if(StringUtil.startsWithIgnoreCase(nameInSource, MetadataConnectorConstants.SET_METHOD_PREFIX)) {
	                    Object value = parameter.getValue();
	                    if(value != null) {
	                        propValueMap.put(nameInSource, value);
	                    }
	                }
	            }
	        }
        }
    }

    /**
     * Get the map of methodName(input parameter nameInSource) to values, used to set
     * inputParameters on the result object before returning its values.
     * @since 4.2
     */
    public Map getPropValues() {
        return this.propValueMap;
    }

    /**
     * Get the map of columnName to values, used as a criteria
     * for querying index records. 
     * @since 4.2
     */
    public Map getCriteria(){
        return criteriaMap;    
    }

    /**
     * Check the value at the given index on a row in the resultSet
     * has the same type as the column type 
     * @since 4.2
     */
    public void checkType(final int columnIndex, final Object value) {
        if (value != null) {
            if (columnTypes != null && !columnTypes[columnIndex].isAssignableFrom(value.getClass())) {
                throw new MetaMatrixRuntimeException(
                    MetadataConnectorPlugin.Util.getString(
                        "ObjectQuery.Type_mismatch", //$NON-NLS-1$
                        new Object[] {columnNames[columnIndex],
                            columnTypes[columnIndex].getName(),
                            value.getClass().getName()}));
            }
        }
    }

    /**
     * Get the type of the column on the resultSet of the procedure at
     * the given index.
     * @param  columnIndex The type of the column at the given index.
     * @since 4.2
     */
    public Class getResultSetColumnType(final int columnIndex) {
        if(this.columnTypes == null) {
            return columnTypes[columnIndex];
        }
        return this.columnTypes[columnIndex];
    }    

    /**
     * Get the nameInSource of the given object. 
     * @since 4.2
     */
    private String getMetadataObjectName(IMetadataReference reference) throws ConnectorException {
        if(reference == null) {
            return null;
        }
        MetadataID id = reference.getMetadataID();
        MetadataObject obj = metadata.getObject(id);
        if (obj != null && obj.getNameInSource() != null) {
            return obj.getNameInSource();
        }
        throw new MetaMatrixRuntimeException(
            MetadataConnectorPlugin.Util.getString("ObjectQuery.Could_not_resolve_name_for_query___1", //$NON-NLS-1$
            new Object[] {procedure.toString()}));
    }

}
