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


package com.metamatrix.connector.xml;

import java.io.Serializable;

import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.xml.base.Response;

/**
 * @author mharris
 *
 * Implementations are responsible for implementing these functions and providing an appropriate getter
 * for the "Document" it will produce.
 */
public interface DocumentProducer
{
    // Derived classes can consider the following call sequence to be guaranteed (i.e. part of the interface):
    // 1) construction
    // 2) call to getDocumentCount
    // 3) calls to getCacheKey and the specific document getter (e.g. getDocumentStream) in that order,
    //      once for each value of i (where i is less than the value returned by getDocuementCount)
    // 4) the call sequence call be terminated at any time before the sequence is complete
    
    public int getDocumentCount() throws ConnectorException;
    public String getCacheKey(int i) throws ConnectorException;
    public Response getXMLResponse(int invocationNumber) throws ConnectorException;
    public Serializable getRequestObject(int i) throws ConnectorException;
}
