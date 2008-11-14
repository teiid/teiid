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

package com.metamatrix.connector.sysadmin.extension.command;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.metamatrix.connector.sysadmin.SysAdminPlugin;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IParameter;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.Element;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;


/** 
 * Note: This is different from the @link ObjectProcedure because this doesn't have specific code 
 *      looking for nameInSources with a specific naming conventions for getters and setters for 
 *      querying the index files
 *      
 * @since 4.3
 */
public abstract class BaseProcedureCommand extends ObjectCommand {
    
    private IProcedure procedure=null;
    private String procName;
    private String procNameInSource;
    
    private IParameter resultSetParameter=null;
    private boolean hasResults = false;
    
    // column properties of the resultset 
    private String[] columnNames = null;
    private String[] columnNamesInSource = null;
    private Class[] columnTypes = null;  
    
    
    
    private List criteriaValues = new LinkedList();
    private List criteriaTypes = new LinkedList();
    private Map criteriaMap = new HashMap();
    
    private List parameters = new LinkedList();

    
    public BaseProcedureCommand(final RuntimeMetadata metadata, final IProcedure command) throws ConnectorException {
        super(metadata, command);
        this.procedure = command;

        initParameters();
    }
        
    public String getGroupName() {
        return getProcedureName();
    }    
    
    
    /** 
     * @see com.metamatrix.connector.object.extension.IObjectCommand#getGroupNameInSource()
     * @since 4.3
     */
    public String getGroupNameInSource() {
        return getProcedureNameInSource();
    }

    private String getProcedureName() {
        return procName;
    }
    
    private String getProcedureNameInSource() {
        return procNameInSource;
    }
    
    public Map getCriteria() {
        return this.criteriaMap;
    }
    
    public List getCriteriaTypes() {
        return criteriaTypes;
    }
    
    public List getCriteriaValues() {
        return criteriaValues;
    }    
    
    public String[] getResultColumnNames() {
        return this.columnNames;
    }

    /**
     * returns an ordered list of parameters
     * @return
     * @since 4.3
     */
    public List getParameters() {
        return this.parameters;
    }
    
    /** 
     * @see com.metamatrix.connector.object.extension.IObjectCommand#getResultNamesInSource()
     * @since 4.3
     */
    public String[] getResultNamesInSource() {
        return this.columnNamesInSource;
    }

    public Class[] getResultColumnTypes() {
        return columnTypes;
    }  
    
    public boolean hasResults() {
        return hasResults;
    }
    
    
    private void initParameters() throws ConnectorException {
        
        this.procName=this.procedure.getMetadataID().getName();
               
        this.procNameInSource = getMetadataObjectNameInSource(procedure); 
        
        
        Collection parameters = this.procedure.getParameters();
        if(parameters != null) {
 
            for(final Iterator iter = parameters.iterator(); iter.hasNext();) {
                IParameter parameter = (IParameter) iter.next();
                // if there is one result set parameter
                if(parameter.getDirection() == IParameter.RESULT_SET) {
                    this.resultSetParameter = parameter;
                    initResultSet();
                } else if(parameter.getDirection() == IParameter.IN || parameter.getDirection() == IParameter.INOUT) {
                    initCriteria(parameter);
                } else if (parameter.getDirection() == IParameter.RETURN && this.resultSetParameter==null) { 
                    this.resultSetParameter = parameter;
                    initResultReturn();
                }
            }
        }
        
        setCriteria();

    } 
    
    // The implementing class will code this method to pull information from #parameters to build
    // the criteria set by calling #addCriteria
    abstract void setCriteria() throws ConnectorException ;
    
    

    protected void addCriteria(Object type, Object value, String nameInSource) {
        criteriaTypes.add(type);
        criteriaValues.add(value);
        criteriaMap.put(nameInSource, value);       
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
            Element element = (Element) this.getMetadata().getObject(mID);
            if (element.getNameInSource() != null && element.getNameInSource().length() > 0) {
                columnNamesInSource[i] = element.getNameInSource();
            } else {
                columnNamesInSource[i] = null;
            }
            columnNames[i] = element.getMetadataID().getName();

            columnTypes[i] = element.getJavaType();
            hasResults=true;
        }
    }   
    
    private void initResultReturn() throws ConnectorException {
        columnNames = new String[1];
        columnTypes = new Class[1];
        columnNamesInSource = new String[1];

        columnNames[0] = resultSetParameter.getMetadataID().getName();
        columnTypes[0] = resultSetParameter.getType();
        columnNamesInSource[0] = this.getMetadataObjectNameInSource(resultSetParameter);
        
        hasResults=true;        
    }     
    
    /**
     * Build the criteriaMap used for querying.
     * This is built by using all input parameters having get method names
     * as their name in sources.   
     * @since 4.3
     */
    private void initCriteria(IParameter parameter) throws ConnectorException {
        // first load up all the parameters before the setcriteria() is called.
//        String nameInSource = determineName(parameter); 
        parameters.add(parameter);
//        parameters.put(nameInSource, parameter);
        
        
//        
//        String nameInSource = determineName(parameter); 
//            
//        criteriaTypes.add(parameter.getType());
//        Object value = parameter.getValue();
//        criteriaValues.add(value);
//        criteriaMap.put(nameInSource, value);
    }  
    
    /**
     * Check the value at the given index on a row in the resultSet
     * has the same type as the column type 
     * @since 4.3
     */
    public void checkType(final int columnIndex, final Object value) {
        if (value != null) {
            if (columnTypes != null && !columnTypes[columnIndex].isAssignableFrom(value.getClass())) {
                throw new MetaMatrixRuntimeException(
                    SysAdminPlugin.Util.getString(
                        "ObjectQuery.Type_mismatch", //$NON-NLS-1$
                        new Object[] {columnNames[columnIndex],
                            columnTypes[columnIndex].getName(),
                            value.getClass().getName()}));
            }
        }
    }    
 
    
}
