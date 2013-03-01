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

import org.teiid.core.util.ApplicationInfo;
import org.teiid.language.Command;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.BaseDelegatingExecutionFactory;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.jdbc.teiid.TeiidExecutionFactory;

/**
 * Loopback translator.
 */
@Translator(name="loopback", description="A translator for testing, that returns mock data")
public class LoopbackExecutionFactory extends BaseDelegatingExecutionFactory {

	private int waitTime = 0;
	private int rowCount = 1;
	private boolean throwError = false;
	private long pollIntervalInMilli = -1;
	private boolean incrementRows = false;
	private int charValueSize = 10;
	
	public LoopbackExecutionFactory() {
		TeiidExecutionFactory tef = new TeiidExecutionFactory();
		tef.setDatabaseVersion(ApplicationInfo.getInstance().getReleaseNumber());
		this.setDelegate(tef);
	}
	
	@Override
	public void start() throws TranslatorException {
		if (this.getDelegateName() == null && this.getDelegate() != null) {
			this.getDelegate().start();
		}
		super.start();
	}
	
	@TranslatorProperty(display="Size of values for CLOB, VARCHAR, etc.", advanced=true)
	public int getCharacterValuesSize() {
		return charValueSize;
	}
	
	public void setCharacterValuesSize(int charValSize){
		this.charValueSize = charValSize;
	}
	
	@TranslatorProperty(display="If set to true each value in each column is being incremented with each row", advanced=true)
	public boolean getIncrementRows() {
		return incrementRows;
	}	
	
	public void setIncrementRows(boolean incrementRows) {
		this.incrementRows = incrementRows;
	}	
	
	@Override
	public Object getConnection(Object factory) throws TranslatorException {
		return null;
	}
	
	@Override
	public Object getConnection(Object factory,
			ExecutionContext executionContext) throws TranslatorException {
		return null;
	}
	
	@TranslatorProperty(display="Max Random Wait Time", advanced=true)
	public int getWaitTime() {
		return waitTime;
	}
	
	public void setWaitTime(int waitTime) {
		this.waitTime = waitTime;
	}
	
	@TranslatorProperty(display="Rows Per Query", advanced=true)
	public int getRowCount() {
		return rowCount;
	}
	
	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}
	
	@TranslatorProperty(display="Always Throw Error")
	public boolean isThrowError() {
		return this.throwError;
	}
	
	public void setThrowError(boolean error) {
		this.throwError = error;
	}
	
	@TranslatorProperty(display="Poll interval if using a Asynchronous Connector")
	public long getPollIntervalInMilli() {
		return this.pollIntervalInMilli;
	}
	
	public void setPollIntervalInMilli(long intervel) {
		this.pollIntervalInMilli = intervel;
	}

    @Override
    public Execution createExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connection)
    		throws TranslatorException {
        return new LoopbackExecution(command, this);
    }   
    
	@Override
	public boolean isSourceRequired() {
		return false;
	}   
	
	@Override
	public boolean isSourceRequiredForMetadata() {
		return false;
	}
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory, Object conn)
			throws TranslatorException {
	}
	
	//override to set as non required
	@Override
	@TranslatorProperty(display = "Delegate name", required = false)
	public String getDelegateName() {
		return super.getDelegateName();
	}
	
}
