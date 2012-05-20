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

package org.teiid.client;

import org.teiid.core.TeiidException;

/**
 * <p> This class is used to store the details of an atomic query warning.
 * It stores model name on which the atomic query is based, name of the 
 * connector binding for the data source against which the atomic query 
 * is executed, and the actual exception thrown when the atomic 
 * query is executed.</p>
 */

public class SourceWarning extends TeiidException {
	
	private static final StackTraceElement[] EMPTY_STACK_TRACE = new StackTraceElement[0];
	private String modelName = "UNKNOWN"; // variable stores the name of the model for the atomic query //$NON-NLS-1$
	private String connectorBindingName = "UNKNOWN"; // variable stores name of the connector binding //$NON-NLS-1$
	private boolean partialResults;
	
    /**
	 * <p>Constructor that stores atomic query failure details.</p>
	 * @param model Name of the model for the atomic query
	 * @param connectorBinding Name of the connector binding name for the atomic query
	 * @param ex Exception thrown when atomic query fails
	 */ 
	public SourceWarning(String model, String connectorBinding, Throwable ex, boolean partialResults) {
		super(ex); 
		if(model != null) {
			this.modelName = model;
		}		
		if(connectorBinding != null) {			
			this.connectorBindingName = connectorBinding;
		}		
		this.partialResults = partialResults;
		this.setStackTrace(EMPTY_STACK_TRACE);
	}
	
	/**
	 * <p>Get's the model name for the atomic query.</p>
	 * @return The name of the model
	 */
	public String getModelName() {
		return modelName;		
	}
	
	/**
	 * <p>Get's the connector binding name for the atomic query.</p>
	 * @return The Connector Binding Name
	 */	
	public String getConnectorBindingName() {
		return connectorBindingName;
	}
	
	public boolean isPartialResultsError() {
		return partialResults;
	}
	
	/**
	 * <p>Gets a message detailing the source against which the atomic query failed.</p>
	 * @return Message containing details of the source for which there is a failure.
	 */
	public String toString() {
		StringBuffer warningBuf = new StringBuffer();
		if (partialResults) {
			warningBuf.append("Error "); //$NON-NLS-1$
		} else {
			warningBuf.append("Warning "); //$NON-NLS-1$
		}
		warningBuf.append("querying the connector with binding name "); //$NON-NLS-1$
		warningBuf.append(connectorBindingName);
		warningBuf.append(" for the model "); //$NON-NLS-1$
		warningBuf.append(modelName);
		warningBuf.append(" : "); //$NON-NLS-1$
		warningBuf.append(this.getCause());
		return warningBuf.toString();
	}
	
} // END CLASS
