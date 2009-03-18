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
package org.teiid.dqp.internal.pooling.connector;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.Execution;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.basic.BasicConnector;
import org.teiid.connector.language.ICommand;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

/**
 */
public class FakeSourceConnectionFactory extends BasicConnector {
    static int connCnt;
    
    static boolean alive = true;
    
    class FakeSourceConnection extends BasicConnection {
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
