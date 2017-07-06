/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.resource.spi;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;
import javax.resource.cci.ConnectionMetaData;
import javax.resource.cci.Interaction;
import javax.resource.cci.LocalTransaction;
import javax.resource.cci.ResultSetInfo;
import javax.resource.spi.ManagedConnection;
import javax.transaction.xa.XAResource;

public abstract class BasicConnection implements Connection {

	@Override
	public Interaction createInteraction() throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$
	}

	@Override
	public LocalTransaction getLocalTransaction() throws ResourceException {
		return null;
	}

	@Override
	public ConnectionMetaData getMetaData() throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$
	}

	@Override
	public ResultSetInfo getResultSetInfo() throws ResourceException {
		throw new ResourceException("This operation not supported"); //$NON-NLS-1$
	}
	
	public XAResource getXAResource() throws ResourceException {
		return null;
	}
	
	/**
	 * Tests the connection to see if it is still valid.
	 * @return
	 */
	public boolean isAlive() {
		return true;
	}
	
	/**
	 * Called by the {@link ManagedConnection} to indicate the physical connection
	 * should be cleaned up for reuse.
	 */
	public void cleanUp() {
		
	}

}
