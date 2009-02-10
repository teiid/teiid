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

/*
 */
package com.metamatrix.dqp.internal.pooling.connector;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.Connector;
import com.metamatrix.connector.api.ConnectorCapabilities;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.Execution;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.pool.PoolAwareConnection;

/**
 */
public class FakeSourceConnectionFactory implements Connector {
    static int connCnt;
    
    static boolean alive = true;
    
    class FakeSourceConnection implements Connection, PoolAwareConnection {
        int id;
        
        FakeSourceConnection(int id){
            FakeSourceConnection.this.id = id;
        }
        
        public boolean isAlive() {
            return alive;
        }
        
        public boolean equals(Object other){
            return this.id == ((FakeSourceConnection)other).id;
        }
        
        public String toString(){
            return "Connection " + id; //$NON-NLS-1$
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

	@Override
	public Connection getConnection(ExecutionContext context)
			throws ConnectorException {
		return new FakeSourceConnection(connCnt++);
	}

	@Override
	public void start(ConnectorEnvironment environment) throws ConnectorException {
	}

	@Override
	public void stop() {
	}
	
	@Override
	public ConnectorCapabilities getCapabilities() {
		return null;
	}

}
