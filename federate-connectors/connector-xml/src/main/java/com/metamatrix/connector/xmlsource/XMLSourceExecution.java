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

package com.metamatrix.connector.xmlsource;

import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;

import javax.xml.transform.Source;

import com.metamatrix.data.DataPlugin;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IParameter;


/** 
 * This is main class which will execute request in the XML Source
 */
public abstract class XMLSourceExecution implements ProcedureExecution {

    // Connector environment
    protected ConnectorEnvironment env;
    
    /**
     * ctor 
     * @param context
     * @param metadata
     */
    public XMLSourceExecution(ConnectorEnvironment env) {
        this.env = env;
    }
    
    protected SQLXML convertToXMLType(Source value) throws ConnectorException {
    	if (value == null) {
    		return null;
    	}
    	Object result = env.getTypeFacility().convertToRuntimeType(value);
    	if (!(result instanceof SQLXML)) {
    		throw new ConnectorException(DataPlugin.Util.getString("unknown_object_type_to_tranfrom_xml"));
    	}
    	return (SQLXML)result;
    }
    
    protected abstract Source getReturnValue();
    
    /** 
     * @see com.metamatrix.connector.xmlsource.XMLSourceExecution#nextBatch()
     */
    public Batch nextBatch() throws ConnectorException {
        Batch b = new BasicBatch();
        List row = new ArrayList();
        row.add(convertToXMLType(getReturnValue()));   
        b.addRow(row);
        b.setLast();
        return b;
    }  
    
    /** 
     * @see com.metamatrix.data.api.ProcedureExecution#getOutputValue(com.metamatrix.data.language.IParameter)
     */
    public Object getOutputValue(IParameter parameter) throws ConnectorException {
        throw new ConnectorException(XMLSourcePlugin.Util.getString("No_outputs_allowed")); //$NON-NLS-1$
    }

    /** 
     * @see com.metamatrix.data.api.Execution#close()
     */
    public void close() throws ConnectorException {
        // no-op
    }

    /** 
     * @see com.metamatrix.data.api.Execution#cancel()
     */
    public void cancel() throws ConnectorException {
        // no-op
    }

}
