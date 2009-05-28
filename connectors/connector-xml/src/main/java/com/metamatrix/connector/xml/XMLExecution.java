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

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.Execution;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.ResultSetExecution;

public interface XMLExecution extends Execution, ResultSetExecution {
	
	public XMLConnection getConnection();
	
	public ExecutionContext getExeContext();

	/**
	 * Gets all the InputStreams for a single ExecutionInfo instance.
	 * This could be any number or streams and is implementation dependent.
	 * @return
	 * @throws ConnectorException 
	 */
	public ResultProducer  getStreamProducer() throws ConnectorException;
}
