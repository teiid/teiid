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

import com.metamatrix.data.api.Connection;
import com.metamatrix.data.api.Connector;
import com.metamatrix.data.api.ConnectorCapabilities;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.Execution;
import com.metamatrix.data.api.ExecutionContext;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.metadata.runtime.RuntimeMetadata;
import com.metamatrix.data.pool.PoolAwareConnection;

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
		public Execution createExecution(int executionMode,
				ExecutionContext executionContext, RuntimeMetadata metadata)
				throws ConnectorException {
			return null;
		}

		@Override
		public ConnectorCapabilities getCapabilities() {
			return null;
		}

		@Override
		public void release() {
			
		}
		
		@Override
		public void connectionReleased() {
			
		}
    }

	@Override
	public Connection getConnection(SecurityContext context)
			throws ConnectorException {
		return new FakeSourceConnection(connCnt++);
	}

	@Override
	public void initialize(ConnectorEnvironment environment)
			throws ConnectorException {
	}

	@Override
	public void start() throws ConnectorException {
	}

	@Override
	public void stop() {
	}

}
