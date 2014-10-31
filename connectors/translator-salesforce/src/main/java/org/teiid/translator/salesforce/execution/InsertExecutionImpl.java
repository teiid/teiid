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
package org.teiid.translator.salesforce.execution;


import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.resource.ResourceException;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.Parameter;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.execution.visitors.InsertVisitor;

import com.sforce.async.BatchResult;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.Result;
import com.sforce.async.SObject;


public class InsertExecutionImpl extends AbstractUpdateExecution {
	private static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); //$NON-NLS-1$
	private JobInfo activeJob;
	private List<String> batches = new ArrayList<String>();
	private Iterator<? extends List<?>> rowIter;
	private String objectName;
	private List<Integer> counts;
	
	public InsertExecutionImpl(SalesForceExecutionFactory ef, Command command,
			SalesforceConnection salesforceConnection,
			RuntimeMetadata metadata, ExecutionContext context) throws TranslatorException {
		super(ef, command, salesforceConnection, metadata, context);
		
		Insert insert = (Insert)command;
		if (insert.getParameterValues() != null) {
			this.rowIter = insert.getParameterValues();
		}
		InsertVisitor visitor = new InsertVisitor(getMetadata());	
		visitor.visit(insert);
		this.objectName = visitor.getTableName();
	}

	@Override
	public void execute() throws TranslatorException {
		try {
			Insert insert = (Insert)command;
			if (insert.getParameterValues() == null) {
				DataPayload data = new DataPayload();
				data.setType(this.objectName);
				buildSingleRowInsertPayload(insert, data);
				result = getConnection().create(data);
			}
			else {
				if (this.activeJob == null) {
					this.activeJob = getConnection().createBulkJob(this.objectName);
					counts = new ArrayList<Integer>();
				}
				if (this.activeJob.getState() == JobStateEnum.Open) {
					while (this.rowIter.hasNext()) {			
						List<SObject> rows = buildBulkRowPayload(insert, this.rowIter, this.executionFactory.getMaxBulkInsertBatchSize());
						batches.add(getConnection().addBatch(rows, activeJob));
					}
					this.activeJob = getConnection().closeJob(this.activeJob.getId());
				}
				
				BatchResult[] batchResult = getConnection().getBulkResults(this.activeJob, batches);
				for(BatchResult br:batchResult) {
					for (Result r : br.getResult()) {
						if (r.isSuccess() && r.isCreated()) {
							counts.add(1);
						} else if (r.getErrors().length > 0) {
							counts.add(Statement.EXECUTE_FAILED);
							this.context.addWarning(new SQLWarning(r.getErrors()[0].getMessage(), r.getErrors()[0].getStatusCode().name()));
						} else {
							counts.add(Statement.SUCCESS_NO_INFO);
						}
					}
				}
				// now process the next set of batch rows
				this.activeJob = null;
			}
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}		
	}

	private void buildSingleRowInsertPayload(Insert insert, DataPayload data) throws TranslatorException {
		
		List<ColumnReference> columns = insert.getColumns();
		List<Expression> values = ((ExpressionValueSource)insert.getValueSource()).getValues();
		if(columns.size() != values.size()) {
			throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13006));
		}

		for(int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i).getMetadataObject();
			Object value = values.get(i);
			
			if(!(value instanceof Literal)) {
				throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13007));
			}
			
			Literal literalValue = (Literal)values.get(i);
			Object val = literalValue.getValue();
			Class<?> type = literalValue.getType();
			data.addField(column.getSourceName(), getValue(val, type));
		}
	}

	private String getValue(Object val, Class<?> type) {
		if (val == null) {
			return null;
		}
		if (type.equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
			return SDF.format(val);
		}
		return val.toString();
	}	
	
	protected List<com.sforce.async.SObject> buildBulkRowPayload(Insert insert, Iterator<? extends List<?>> it, int rowCount) throws TranslatorException {
		List<com.sforce.async.SObject> rows = new ArrayList<com.sforce.async.SObject>();
		List<ColumnReference> columns = insert.getColumns();
		int boundCount = 0;
		List<Expression> literalValues = ((ExpressionValueSource)insert.getValueSource()).getValues();
		while (it.hasNext()) {
			if (boundCount >= rowCount) {
				break;
			}
			boundCount++;
			List<?> values = it.next();
			com.sforce.async.SObject sobj = new com.sforce.async.SObject();
			for(int i = 0; i < columns.size(); i++) {
				Expression ex = literalValues.get(i);
				ColumnReference element = columns.get(i);
				Column column = element.getMetadataObject();
				Class<?> type = ex.getType();
				Object value = null;
				if (ex instanceof Parameter) {
					value = values.get(((Parameter)ex).getValueIndex());
				} else if(!(ex instanceof Literal)) {
					throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13007));
				} else {
					value = ((Literal)ex).getValue();
				}
				sobj.setField(column.getSourceName(), getValue(value, type));
			}
			rows.add(sobj);
		}
		return rows;
	}	
	
	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		if (counts != null) {
			int[] countArray = new int[counts.size()];
			for (int i = 0; i < countArray.length; i++) {
				countArray[i] = counts.get(i);
			}
			return countArray;
		}
		return new int[] { result };
	}
	
	@Override
	public void cancel() throws TranslatorException {
		if (this.activeJob != null) {
			try {
				getConnection().cancelBulkJob(this.activeJob);
			} catch (ResourceException e) {
				throw new TranslatorException(e);
			}
		}
	}
}
