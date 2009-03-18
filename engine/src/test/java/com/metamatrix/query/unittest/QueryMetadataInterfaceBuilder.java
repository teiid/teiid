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

package com.metamatrix.query.unittest;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.dqp.internal.datamgr.metadata.RuntimeMetadataImpl;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.message.ParameterInfo;
import com.metamatrix.query.metadata.QueryMetadataInterface;

public class QueryMetadataInterfaceBuilder {

    private FakeMetadataObject physicalModel;
    private FakeMetadataObject group;
    private FakeMetadataObject procedure;
    private FakeMetadataObject resultSet;

    private List elementNames;
    private List elementTypes;
    private List inParamNames;
    private List inParamTypes;
    private List resultElementNames;
    private List resultElementTypes;
    private String resultSetName;

    private FakeMetadataStore store = new FakeMetadataStore();
    private boolean storePopulated = false;

    public QueryMetadataInterfaceBuilder() {
        initElements();
    }
    
    public void addPhysicalModel(String name) {
        physicalModel = createPhysicalModel(name);
    }

    public void addGroup(String name) {
        addGroup(name, name);
    }

    public void addProcedure(String name) {
        addProcedure(name, name);
    }

    public void addGroup(String name, String nameInSource) {
        if (group != null) {
            addGroup();
        }
        group = createPhysicalGroup(name, nameInSource, physicalModel);
    }

    public void addProcedure(String name, String nameInSource) {
        procedure = createPhysicalProcedure(name, nameInSource, physicalModel);
        addProcedure();
    }

    private void addGroup(){
        store.addObject(group);
        store.addObjects(getElements());
        initElements();
    }
    
    private void addProcedure(){
        store.addObject(procedure);
        if(this.resultSet != null) {
            store.addObject(this.resultSet);
        }
    }

    private void initElements() {
        elementNames = new ArrayList();
        elementTypes = new ArrayList();
    }

    private void initInputParameters() {
        if(this.inParamNames == null || this.inParamTypes == null) {        
            inParamNames = new ArrayList();
            inParamTypes = new ArrayList();
        }
    }

    private void initResultSetColumns() {
        if(this.resultElementNames == null) {
            resultElementNames = new ArrayList();
        }

        if(this.resultElementTypes == null) {            
            resultElementTypes = new ArrayList();
        }
    }    

    public void addElement(String name, Class dataType) {
        elementNames.add(name);
        elementTypes.add(DataTypeManager.getDataTypeName(dataType));
    }
    
    public void addInputParameter(String name, Class dataType) {
        initInputParameters();
        inParamNames.add(name);
        inParamTypes.add(DataTypeManager.getDataTypeName(dataType));
    }
    
    public void addResultSet(String resultSetName, String[] names, Class[] dataTypes) {
        initResultSetColumns();
        for(int i=0; i < names.length; i++) {
	        resultElementNames.add(names[i]);
	        resultElementTypes.add(DataTypeManager.getDataTypeName(dataTypes[i]));
        }
        this.resultSetName = resultSetName;
    }

    private List getElements(){
        return createElements(group, (String[]) elementNames.toArray(new String[0]),
            (String[]) elementTypes.toArray(new String[0]));
    }

    private List getParameters() {
        List inParameters = createInParameters(this.inParamNames, this.inParamTypes);
        if ( (resultElementNames == null || resultElementNames.size() ==0) || (resultElementTypes == null || resultElementTypes.size() == 0)) {
            return inParameters;
        }
        this.resultSet = createResultSetParameter(this.physicalModel, resultSetName, (String[]) resultElementNames.toArray(new String[resultElementNames.size()]),
                                                  (String[]) resultElementTypes.toArray(new String[resultElementTypes.size()]));
        inParameters.add(this.resultSet);
        return inParameters;
    }

    private void populateStoreIfNeeded(){
        if (!storePopulated) {
            if(this.group != null) {
	            store.addObject(physicalModel);
	            addGroup();
            } else if(this.procedure != null) {
	            store.addObject(physicalModel);
	            addProcedure();                
            }
            storePopulated = true;
        }
    }
    
    public QueryMetadataInterface getQueryMetadata() {
        populateStoreIfNeeded();
        return new FakeMetadataFacade(store);
    }
    
    public RuntimeMetadata getRuntimeMetadata(){
        return new RuntimeMetadataImpl(getQueryMetadata());
    }
    
    private FakeMetadataObject createPhysicalModel(String name) {
        return FakeMetadataFactory.createPhysicalModel(name);
    }

    private static FakeMetadataObject createPhysicalGroup(String name, String nameInSource, FakeMetadataObject model) {
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.GROUP);
        obj.putProperty(FakeMetadataObject.Props.MODEL, model);
        obj.putProperty(FakeMetadataObject.Props.IS_VIRTUAL, model.getProperty(FakeMetadataObject.Props.IS_VIRTUAL));
        obj.putProperty(FakeMetadataObject.Props.UPDATE, Boolean.TRUE); 
        obj.putProperty(FakeMetadataObject.Props.TEMP, Boolean.FALSE);
        obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, nameInSource); 
        return obj; 
    }

    private FakeMetadataObject createPhysicalProcedure(String name, String nameInSource, FakeMetadataObject model) {
        FakeMetadataObject obj = new FakeMetadataObject(name, FakeMetadataObject.PROCEDURE);
        obj.putProperty(FakeMetadataObject.Props.MODEL, model);
        obj.putProperty(FakeMetadataObject.Props.IS_VIRTUAL, model.getProperty(FakeMetadataObject.Props.IS_VIRTUAL));
        obj.putProperty(FakeMetadataObject.Props.NAME_IN_SOURCE, nameInSource);
        obj.putProperty(FakeMetadataObject.Props.PARAMS, getParameters());
        if(this.resultSet != null) {
            obj.putProperty(FakeMetadataObject.Props.RESULT_SET, this.resultSet);
        }
        return obj; 
    }

    private List createElements(FakeMetadataObject group, String[] names, String[] types) {
        return FakeMetadataFactory.createElements(group, names, types);
    }

    private List createInParameters(List names, List types) {
        List params = new ArrayList();
        if (names == null || types == null) {
            return params;
        }
        final Iterator iter1 = names.iterator();
        final Iterator iter2 = types.iterator();        
        
        int i=0;
        while(iter1.hasNext() && iter2.hasNext()) {
            String name = (String) iter1.next();
            String className = (String) iter2.next();
            params.add(FakeMetadataFactory.createParameter(name, i, ParameterInfo.IN, className, null));
            i++;
        }
        return params;
    }    

    private FakeMetadataObject createResultSetParameter(FakeMetadataObject model, String name, String[] colNames, String[] colTypes) {
        if (colNames == null || colTypes == null) {
            return null;
        }
        FakeMetadataObject resultSet = FakeMetadataFactory.createResultSet(name, model, colNames, colTypes);
        int s = (this.inParamNames == null ? 0 : this.inParamNames.size());
        resultSet.putProperty(FakeMetadataObject.Props.INDEX, new Integer(s));
        resultSet.putProperty(FakeMetadataObject.Props.DIRECTION, new Integer(ParameterInfo.RESULT_SET));
        resultSet.putProperty(FakeMetadataObject.Props.RESULT_SET, resultSet);
        return resultSet;
    }

    public void addMetadataForType(String tableName, String tableNameInSource, Class type, String tableDefiningMethodName) {
        addPhysicalModel("model"); //$NON-NLS-1$
        addGroup(tableName, tableNameInSource);
        Method[] methods = type.getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getParameterTypes().length == 0) {
                Class returnType = methods[i].getReturnType();
                if (returnType != null) {
                    if (returnType.isPrimitive()) {
                        if (returnType.equals(Integer.TYPE)) {
                            returnType = Integer.class;
                        }
                    }
                    String methodName = methods[i].getName();
                    if (methodName.equals(tableDefiningMethodName)) {
                        if (returnType.isArray()) {
                            returnType = returnType.getComponentType();
                        } else if (Collection.class.isAssignableFrom(returnType)) {
                            returnType = String.class;
                        }
                    }
                    if (DataTypeManager.getDataTypeName(returnType) == null) {
                        if (methodName.equals(tableDefiningMethodName)) {
                            throw new MetaMatrixRuntimeException("Data type not supported " + returnType.getName()); //$NON-NLS-1$
                        }
                    } else {
                        addElement(methodName, returnType);
                    }
                }
            }
        }
    }      
}
