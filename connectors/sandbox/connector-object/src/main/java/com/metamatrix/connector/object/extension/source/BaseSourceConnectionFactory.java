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

/*
 */
package com.metamatrix.connector.object.extension.source;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorIdentity;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.SingleIdentity;

import com.metamatrix.connector.object.ObjectConnection;
import com.metamatrix.connector.object.SourceConnectionFactory;
import com.metamatrix.connector.object.extension.IObjectSource;

/**
 * Represents a base factory class for the creation of the source connection.  Subclasses
 * are expected to implement the #getObjectSource method to provide the source specific
 * implmentation to interact with the source.
 */
public abstract class BaseSourceConnectionFactory implements SourceConnectionFactory {
    
    private ConnectorEnvironment environment;
  
    /**
     *
     */
    public BaseSourceConnectionFactory() { 
        super();
    }
    
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        this.environment = env;
   }
    
    protected ConnectorEnvironment getEnvironment() {
        return environment;
    }

    
    /** 
     * @see com.metamatrix.data.pool.SourceConnectionFactory#createConnection(org.teiid.connector.api.ConnectorIdentity)
     * @since 4.3
     */
    public final Connection createConnection(ConnectorIdentity id) throws ConnectorException {

        // must pass the factory to the object source because the factory
        // is used to close the connection
        IObjectSource objectSource = getObjectSource(id);

        return new ObjectConnection(getEnvironment(), objectSource);
      
    }
    
   protected abstract IObjectSource getObjectSource(final ConnectorIdentity id) throws ConnectorException ;
   

    /** 
     * @see com.metamatrix.data.pool.SourceConnectionFactory#createIdentity(com.metamatrix.data.api.SecurityContext)
     * @since 4.3
     */
    public ConnectorIdentity createIdentity(ExecutionContext context) throws ConnectorException {
        return new SingleIdentity();
    }
    
}
