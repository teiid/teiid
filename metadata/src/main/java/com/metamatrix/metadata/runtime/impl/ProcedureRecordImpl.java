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

package com.metamatrix.metadata.runtime.impl;

import java.util.List;

import com.metamatrix.modeler.core.metadata.runtime.MetadataConstants;
import com.metamatrix.modeler.core.metadata.runtime.ProcedureRecord;

/**
 * ProcedureRecordImpl
 */
public class ProcedureRecordImpl extends AbstractMetadataRecord implements ProcedureRecord {
    
    private List parameterIDs;
    private boolean isFunction;
    private boolean isVirtual;
    private Object resultSetID;
    private int updateCount;

    // ==================================================================================
    //                        C O N S T R U C T O R S
    // ==================================================================================

    public ProcedureRecordImpl() {
    	this(new MetadataRecordDelegate());
    }
    
    protected ProcedureRecordImpl(MetadataRecordDelegate delegate) {
    	this.delegate = delegate;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureRecord#getParameterIDs()
     */
    public List getParameterIDs() {
        return parameterIDs;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureRecord#isFunction()
     */
    public boolean isFunction() {
        return isFunction;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureRecord#isVirtual()
     */
    public boolean isVirtual() {
        return this.isVirtual;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureRecord#getResultSetID()
     */
    public Object getResultSetID() {
        return resultSetID;
    }

    /*
     * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureRecord#getType()
     */
    public short getType() {
        return this.getProcedureType();
    }
    
    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.ProcedureRecord#getUpdateCount()
     * @since 5.5.3
     */
    public int getUpdateCount() {
        return this.updateCount;
    }

    /**
     * @param list
     */
    public void setParameterIDs(List list) {
        parameterIDs = list;
    }

    /**
     * @param object
     */
    public void setResultSetID(Object object) {
        resultSetID = object;
    }

    /**
     * @param b
     */
    public void setFunction(boolean b) {
        isFunction = b;
    }

    /**
     * @param b
     */
    public void setVirtual(boolean b) {
        isVirtual = b;
    }
    
    public void setUpdateCount(int count) {
    	this.updateCount = count;
    }

    protected short getProcedureType() {
        if (isFunction()) {
            return MetadataConstants.PROCEDURE_TYPES.FUNCTION;
        }
        if (isVirtual()) {
            return MetadataConstants.PROCEDURE_TYPES.STORED_QUERY;
        }
        return MetadataConstants.PROCEDURE_TYPES.STORED_PROCEDURE;
    }

}