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

package org.teiid.translator.loopback;

import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.BasicExecutionFactory;
import org.teiid.translator.ConnectorCapabilities;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorProperty;

/**
 * Loopback connector.
 */
public class LoopbackExecutionFactory extends BasicExecutionFactory {

	private int waitTime = 0;
	private int rowCount = 1;
	private boolean throwError = false;
	private long pollIntervalInMilli = -1;
	
	@TranslatorProperty(name="wait-time", display="Max Random Wait Time",required=true, advanced=true, defaultValue="0")
	public int getWaitTime() {
		return waitTime;
	}
	
	public void setWaitTime(Integer waitTime) {
		this.waitTime = waitTime.intValue();
	}
	
	@TranslatorProperty(name="row-count", display="Rows Per Query",required=true, advanced=true, defaultValue="1")
	public int getRowCount() {
		return rowCount;
	}
	
	public void setRowCount(Integer rowCount) {
		this.rowCount = rowCount;
	}
	
	@TranslatorProperty(name="throw-error", display="Always Throw Error", defaultValue="false")
	public boolean isThrowError() {
		return this.throwError;
	}
	
	public void setThrowError(Boolean error) {
		this.throwError = error.booleanValue();
	}
	
	@TranslatorProperty(name="poll-intervel", display="Poll interval if using a Asynchronous Connector", defaultValue="-1")
	public long getPollIntervalInMilli() {
		return this.pollIntervalInMilli;
	}
	
	public void setPollIntervalInMilli(Long intervel) {
		this.pollIntervalInMilli = intervel.longValue();
	}
	
	@Override
	public void start() throws ConnectorException {
		super.start();
	}

    @Override
    public Class<? extends ConnectorCapabilities> getDefaultCapabilities() {
    	return LoopbackCapabilities.class;
    }
    
    @Override
    public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionfactory)
    		throws ConnectorException {
        return new LoopbackExecution(command, this);
    }   
    
	@Override
	public boolean isSourceRequired() {
		return false;
	}    
}
