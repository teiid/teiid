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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.metamatrix.data.DataPlugin;
import com.metamatrix.data.api.Batch;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ProcedureExecution;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.basic.BasicBatch;
import com.metamatrix.data.exception.ConnectorException;


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
    
    /**
     * Convert input stream to an byte array. 
     * @param in
     * @return
     * @throws IOException
     */
    protected byte[] convertToByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(10 * 1024);
        int b = 0;
        while ((b = in.read()) != -1) {
            out.write(b);
        }
        return out.toByteArray();    
    }     
    
    /** 
     * The result is always returned as RETURN output parameter, so the
     * batch access is not needed.
     * @see com.metamatrix.data.api.BatchedExecution#nextBatch()
     */
    public Batch nextBatch() throws ConnectorException {
        Batch b = new BasicBatch();       
        b.setLast();
        return b;
    } 
    
    protected Object convertToXMLType(Object value) throws ConnectorException {
    	if (value == null) {
    		return null;
    	}
    	value = env.getTypeFacility().convertToRuntimeType(value);
    	if (value.getClass() == TypeFacility.RUNTIME_TYPES.XML) {
    		return value;
    	}
    	if (!env.getTypeFacility().hasTransformation(value.getClass(), TypeFacility.RUNTIME_TYPES.XML)) {
    		throw new ConnectorException(DataPlugin.Util.getString("unknown_object_type_to_tranfrom_xml"));
    	}
    	return env.getTypeFacility().transformValue(value, value.getClass(), TypeFacility.RUNTIME_TYPES.XML);
    }
}
