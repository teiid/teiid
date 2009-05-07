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

package org.teiid.dqp.internal.cache;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.dqp.DQPPlugin;
import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.sql.lang.Command;

public class CacheResults {
	private List[] results;
	private List elements;
	private AnalysisRecord analysisRecord;
	private Command command;
	private Map paramValues;
	
	private boolean isFinal;
	private int firstRow = 1;
    //size of this results in memory
	private long size= -1;
	private int finalRow = -1;
		
	public CacheResults(List[] results, int firstRow, boolean isFinal){
		this(results, null, firstRow, isFinal);
	}
	
	public CacheResults(List[] results, List elements, int firstRow, boolean isFinal){
		this.results = results;
		this.firstRow = firstRow;
		this.isFinal = isFinal;
		this.elements = elements;
	}

	public CacheResults(Map paramValues, boolean isFinal){
		this.paramValues = paramValues;
		this.isFinal = isFinal;
	}
	
	public int getFirstRow() {
		return firstRow;
	}

	public boolean isFinal() {
		return isFinal;
	}

	public List[] getResults() {
		return results;
	}

	public List getElements() {
		return elements;
	}

	public Command getCommand() {
		return command;
	}

	public void setCommand(Command command) {
		this.command = command;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public AnalysisRecord getAnalysisRecord() {
		return analysisRecord;
	}

	public void setAnalysisRecord(AnalysisRecord analysisRecord) {
		this.analysisRecord = analysisRecord;
	}

	public int getFinalRow() {
		return finalRow;
	}

	public void setFinalRow(int finalRow) {
		this.finalRow = finalRow;
	}

	public Map getParamValues() {
		return paramValues;
	}

	public void setParamValues(Map paramValues) {
		if(this.paramValues == null){
			this.paramValues = paramValues;
		}else if(paramValues != null){
			this.paramValues.putAll(paramValues);
		}
	}
		
    //add the results to the existing one, this is used
    //when building the batched results
	boolean addResults(CacheResults cacheResults){
		if (this.firstRow + results.length != cacheResults.getFirstRow()) {
			throw new MetaMatrixRuntimeException(DQPPlugin.Util.getString("ResultSetCache.1"));//$NON-NLS-1$
		}
		this.size += cacheResults.size;
		List[] batchResults = cacheResults.getResults();
		if (results == null) {
			this.results = batchResults;
		} else if (batchResults.length > 0){
			this.results = Arrays.copyOf(this.results, this.results.length + batchResults.length);
			System.arraycopy(batchResults, 0, this.results, this.results.length - batchResults.length, batchResults.length);
		}
			
		if(cacheResults.isFinal()){
			this.command = cacheResults.getCommand();
			this.analysisRecord = cacheResults.getAnalysisRecord();
			this.firstRow = 1;
			this.isFinal = true;
			this.finalRow = this.results.length;
		}
		return true;
	}
	
}
