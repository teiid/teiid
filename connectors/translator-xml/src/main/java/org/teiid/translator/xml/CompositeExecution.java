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
package org.teiid.translator.xml;

import java.util.ArrayList;
import java.util.List;

import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class CompositeExecution implements ResultSetExecution {

	private boolean closed = false;
	private List<ResultSetExecution> executions = new ArrayList<ResultSetExecution>();
	private int currentExecutionNumber = 0;
	private ResultSetExecution currentExecution;
	
	public void addExecution(ResultSetExecution execution) {
		if (this.closed) {
			throw new IllegalStateException("This execution is already closed"); //$NON-NLS-1$
		}
		this.executions.add(execution);
	}
	
	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {
		List result = null;
		if (this.currentExecution != null) {
			result = this.currentExecution.next();	
		}
		
		if (result == null) {
			if(this.currentExecutionNumber < this.executions.size()) {
				this.currentExecution = this.executions.get(this.currentExecutionNumber++);
				result = this.currentExecution.next();
			}
		}
		return result;
	}

	@Override
	public void cancel() throws TranslatorException {
		this.closed = true;
		for(ResultSetExecution exec:this.executions) {
			exec.cancel();
		}
	}

	@Override
	public void close() {
		this.closed = true;
		for(ResultSetExecution exec:this.executions) {
			exec.close();
		}
	}

	@Override
	public void execute() throws TranslatorException {
		this.closed = true;
		for(ResultSetExecution exec:this.executions) {
			exec.execute();
		}
	}

}
