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

package com.metamatrix.xa.arjuna;

import com.metamatrix.common.xa.TransactionContext;
import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.xa.api.XAConnection;
import com.metamatrix.data.xa.api.XAConnector;

class FakeXAConnector implements XAConnector{
    String name;
    FakeXAConnection conn;
    
    public FakeXAConnector(String name){
        this.name = name;
        this.conn = new FakeXAConnection(this.name);
    }
    
    public XAConnection getXAConnection(SecurityContext securityContext, TransactionContext transactionContext) 
        throws ConnectorException {
        return this.conn;
    }
    
    public Connection getConnection(SecurityContext context) throws ConnectorException {
        return null;
    }
    
    public void initialize(ConnectorEnvironment environment) throws ConnectorException {}
    public void start() throws ConnectorException {}
    public void stop() {}
    
    public void failToCreateResource(boolean flag) {
        conn.failToCreateResource(flag);
    }    
}
