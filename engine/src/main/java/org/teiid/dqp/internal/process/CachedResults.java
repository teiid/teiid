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

package org.teiid.dqp.internal.process;

import java.io.Serializable;

import org.teiid.common.buffer.TupleBuffer;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.sql.lang.Command;


public class CachedResults implements Serializable {
	private static final long serialVersionUID = -5603182134635082207L;
	private Command command;
	private AnalysisRecord analysisRecord;
	private TupleBuffer results;
	
	public AnalysisRecord getAnalysisRecord() {
		return analysisRecord;
	}
	
	public void setAnalysisRecord(AnalysisRecord analysisRecord) {
		this.analysisRecord = analysisRecord;
	}
	
	public TupleBuffer getResults() {
		return results;
	}
	
	public void setResults(TupleBuffer results) {
		this.results = results;
	}
	
	public void setCommand(Command command) {
		this.command = command;
	}
	
	public Command getCommand() {
		return command;
	}
		
}
