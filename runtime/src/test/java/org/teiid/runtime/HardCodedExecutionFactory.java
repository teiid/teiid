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

package org.teiid.runtime;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

@Translator(name="hardcoded")
public class HardCodedExecutionFactory extends ExecutionFactory<Object, Object> {
	Map<String, List<? extends List<?>>> dataMap = new HashMap<String, List<? extends List<?>>>();
	Map<String, int[]> updateMap = new HashMap<String, int[]>();
	
	public HardCodedExecutionFactory() {
		setSourceRequired(false);
	}
	
	@Override
	public ProcedureExecution createProcedureExecution(Call command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			Object connection) throws TranslatorException {
		List<? extends List<?>> list = getData(command);
		if (list == null) {
			throw new RuntimeException(command.toString());
		}
		final Iterator<? extends List<?>> result = list.iterator();
		return new ProcedureExecution() {
			
			@Override
			public void execute() throws TranslatorException {
				
			}
			
			@Override
			public void close() {
				
			}
			
			@Override
			public void cancel() throws TranslatorException {
				
			}
			
			@Override
			public List<?> next() throws TranslatorException, DataNotAvailableException {
				if (result.hasNext()) {
					return result.next();
				}
				return null;
			}

			@Override
			public List<?> getOutputParameterValues()
					throws TranslatorException {
				return null;
			}
		};
	}
	
	@Override
	public ResultSetExecution createResultSetExecution(
			final QueryExpression command, ExecutionContext executionContext,
			RuntimeMetadata metadata, Object connection)
			throws TranslatorException {
		List<? extends List<?>> list = getData(command);
		if (list == null) {
			throw new RuntimeException(command.toString());
		}
		final Iterator<? extends List<?>> result = list.iterator();
		return new ResultSetExecution() {
			
			@Override
			public void execute() throws TranslatorException {
				
			}
			
			@Override
			public void close() {
				
			}
			
			@Override
			public void cancel() throws TranslatorException {
				
			}
			
			@Override
			public List<?> next() throws TranslatorException, DataNotAvailableException {
				if (result.hasNext()) {
					return result.next();
				}
				return null;
			}
		};
	}
	
	protected List<? extends List<?>> getData(final QueryExpression command) {
		return getData((Command)command);
	}

	protected List<? extends List<?>> getData(final Command command) {
		return dataMap.get(command.toString());
	}
	
	@Override
	public UpdateExecution createUpdateExecution(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			Object connection) throws TranslatorException {
		final int[] result = updateMap.get(command.toString());
		if (result == null) {
			throw new RuntimeException(command.toString());
		}
		return new UpdateExecution() {
			
			@Override
			public void execute() throws TranslatorException {
				
			}
			
			@Override
			public void close() {
				
			}
			
			@Override
			public void cancel() throws TranslatorException {
				
			}
			
			@Override
			public int[] getUpdateCounts() throws DataNotAvailableException,
					TranslatorException {
				return result;
			}
			
		};
	}
	
	public void addData(String key, List<? extends List<?>> list) {
		this.dataMap.put(key, list);
	}
	
	public void addUpdate(String key, int[] counts) {
		this.updateMap.put(key, counts);
	}
	
}