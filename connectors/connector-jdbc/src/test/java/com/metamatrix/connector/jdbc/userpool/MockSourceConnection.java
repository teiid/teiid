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

package com.metamatrix.connector.jdbc.userpool;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.Execution;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.pool.PoolAwareConnection;

/**
 */
public class MockSourceConnection implements PoolAwareConnection, Connection {

    private String url;
    private int transLevel;
    private String user;
    private String password;

    public MockSourceConnection(String url, int transLevel, String user, String password) {
        super();
        this.url = url;
        this.transLevel = transLevel;
        this.user = user;
        this.password = password;
    }

    public boolean isAlive() {
        return false;
    }
    
    public String getUrl() {
        return this.url;
    }
    
    public int getTransactionIsolationLevel() {
        return this.transLevel;
    }

    public String getUser() {
        return this.user;
    }
    
    public String getPassword() {
        return this.password;
    }

	@Override
	public Execution createExecution(ICommand command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws ConnectorException {
		return null;
	}

	@Override
	public ConnectorCapabilities getCapabilities() {
		return null;
	}

	@Override
	public void close() {
		
	}

	@Override
	public void closeCalled() {
		
	}
    
}
