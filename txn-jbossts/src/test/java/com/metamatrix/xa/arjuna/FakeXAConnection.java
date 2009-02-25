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

package com.metamatrix.xa.arjuna;

import javax.transaction.xa.XAResource;

import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.Execution;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;
import org.teiid.connector.xa.api.XAConnection;


class FakeXAConnection extends BasicConnection implements XAConnection {
    String name;
    boolean released = false;
    boolean failToCreateXAResource = false;
    FakeXAResource resource;
    
    public FakeXAConnection(String name) {
        this.name = name;
        this.resource = new FakeXAResource(name);
    }
    @Override
    public Execution createExecution(ICommand command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return null;
    }
    @Override
    public void close() {
    	this.released = true;
    }
    public ConnectorCapabilities getCapabilities() {
        return null;
    }
    
    public String getResourceName() {
        return null;
    }
    
    public XAResource getXAResource() throws ConnectorException {
        if (!released) {
            
            if (this.failToCreateXAResource) {
                throw new ConnectorException("Failed to create XA Resource"); //$NON-NLS-1$
            }
            
            return this.resource;
        }
        throw new ConnectorException("Connection Closed"); //$NON-NLS-1$
    }

    public boolean isInTxn() {
        return false;
    }

    public boolean isLeased() {
        return false;
    }
    
    public void failToCreateResource(boolean flag) {
        failToCreateXAResource = flag;
    }
}
