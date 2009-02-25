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

package com.metamatrix.connector.xmlsource;

import java.lang.reflect.Constructor;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;



/** 
 * This Connection facory which will handle the different connection semantics
 * the supported type will be
 * - File Based XML Document
 * - SOAP Based XML Document
 * - REST Based XML Document
 * 
 */
public class XMLConnectionFacory {
    ConnectorEnvironment env;

    
    public XMLConnectionFacory(ConnectorEnvironment env) {
        this.env = env;
    }
    
    /**
     * Create connection to the source 
     * @param context
     * @return
     * @throws ConnectorException
     */
    public Connection createConnection(ExecutionContext context) 
        throws ConnectorException {
        String connectionTypeClass = env.getProperties().getProperty("ConnectionType"); //$NON-NLS-1$
        try {
            Class clazz = Thread.currentThread().getContextClassLoader().loadClass(connectionTypeClass);
            Constructor c = clazz.getConstructor(new Class[] {ConnectorEnvironment.class});                       
            XMLSourceConnection conn = (XMLSourceConnection)c.newInstance(new Object[] {this.env});
            return conn;
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
    }
}
