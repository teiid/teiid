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

package com.metamatrix.connector.object.extension.command;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.language.IParameter;
import org.teiid.connector.language.IProcedure;
import org.teiid.connector.language.IParameter.Direction;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.connector.object.ObjectPlugin;
import com.metamatrix.connector.object.util.ObjectConnectorUtil;
import com.metamatrix.core.MetaMatrixRuntimeException;


/** 
 * @since 4.3
 */
public class ProcedureCommand extends ObjectCommand {
    
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
    
    
    public ProcedureCommand(final RuntimeMetadata metadata, final IProcedure command) throws ConnectorException {
        super(metadata, command);
        this.procedure = command;

        initParameters();
    }
    
    public String getGroupName() {
        if (getProcedureNameInSource()==null) {
            return getProcedureName();
        }
        return getProcedureNameInSource();
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
    
    
    protected void setCriteriaValues(List values) {
        this.criteriaValues = values;
    }
    
    protected void setCriteriaTypes(List types) {
        this.criteriaTypes = types;
    }
    
    
    /** 
     * @see com.metamatrix.connector.object.extension.IObjectCommand#getCritera()
     * @since 4.3
     */
    public Map getCriteria() {
        return criteriaMap;
    }
    
    public static final String getMethodName(final RuntimeMetadata metadata, final IProcedure procedure) throws ConnectorException {
        String procName=procedure.getMetadataObject().getName();
        
        String procNameInSource =  ObjectConnectorUtil.getMetadataObjectNameInSource(metadata, procedure, procedure);

        if (procNameInSource==null) {
            return procName;
        }
        return procNameInSource;
    }

    protected void initParameters() throws ConnectorException {
        
        this.procName=this.procedure.getMetadataObject().getName();
               
        this.procNameInSource = getMetadataObjectNameInSource(procedure);         
        
        Collection parameters = this.procedure.getParameters();
        if(parameters != null) {
//            this.inParams = new HashSet(parameters.size());
 
            for(final Iterator iter = parameters.iterator(); iter.hasNext();) {
                IParameter parameter = (IParameter) iter.next();
                // if there is one result set parameter
                if(parameter.getDirection() == Direction.RESULT_SET) {
                    this.resultSetParameter = parameter;
                    initResultSet();
                } else if(parameter.getDirection() == Direction.IN || parameter.getDirection() == Direction.INOUT) {
                    initCriteria(parameter);
//                    inParams.add(parameter);
                } else if (parameter.getDirection() == Direction.RETURN && this.resultSetParameter==null) { 
                    this.resultSetParameter = parameter;
                    initResultReturn();
                }
            }
        }
    } 
    
    /**
     * Collect names, types, nameinsources of all columns in the resultset.  
     * @since 4.2
     */
    protected void initResultSet() throws ConnectorException {
        List<Element> columnMetadata = resultSetParameter.getMetadataObject().getChildren();
        int size = columnMetadata.size();
        columnNames = new String[size];
        columnNamesInSource = new String[size];
        columnTypes = new Class[size];
        for(int i =0; i<size; i++ ){
            Element element = (Element) columnMetadata.get(i);
            if (element.getNameInSource() != null && element.getNameInSource().length() > 0) {
                columnNamesInSource[i] = element.getNameInSource();
            } else {
                columnNamesInSource[i] = null;
            }
            columnNames[i] = element.getName();

            columnTypes[i] = element.getJavaType();
            hasResults=true;
        }
    }   
    
    protected void initResultReturn() throws ConnectorException {
        columnNames = new String[1];
        columnTypes = new Class[1];
        columnNamesInSource = new String[1];

        columnNames[0] = resultSetParameter.getMetadataObject().getName();
        columnTypes[0] = resultSetParameter.getType();
        columnNamesInSource[0] = this.getMetadataObjectNameInSource(resultSetParameter);
        
        hasResults=true;        
    }     
    
    /**
     * Build the criteriaMap used for querying.
     * This is built by using all input parameters having get method names
     * as their name in sources.   
     * @since 4.2
     */
    protected void initCriteria(IParameter parameter) throws ConnectorException {
        
            String nameInSource = determineName(parameter); 
            
            setCriteria(parameter.getType(),parameter.getValue(), nameInSource);
                
//            criteriaTypes.add(parameter.getType());
//            Object value = parameter.getValue();
//            criteriaValues.add(value);
//            criteriaMap.put(nameInSource, value);
    } 

    protected void setCriteria(Object type, Object value, String nameInSource) {
        criteriaTypes.add(type);
        criteriaValues.add(value);
        criteriaMap.put(nameInSource, value);       
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
                    ObjectPlugin.Util.getString(
                        "ObjectCommand.Type_mismatch", //$NON-NLS-1$
                        new Object[] {columnNames[columnIndex],
                            columnTypes[columnIndex].getName(),
                            value.getClass().getName()}));
            }
        }
    }     
    

}
