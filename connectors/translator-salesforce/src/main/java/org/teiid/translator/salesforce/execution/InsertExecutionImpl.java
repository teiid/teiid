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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.resource.ResourceException;
import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.teiid.core.types.DataTypeManager;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesForcePlugin;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.Util;
import org.teiid.translator.salesforce.execution.visitors.InsertVisitor;

import com.sforce.async.BatchResult;
import com.sforce.async.JobInfo;
import com.sforce.async.Result;
import com.sforce.async.SObject;


public class InsertExecutionImpl extends AbstractUpdateExecution {
	private static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); //$NON-NLS-1$
	private JobInfo activeJob;
	private Iterator<? extends List<?>> rowIter;
	private String objectName;
	
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
				data.setMessageElements(buildSingleRowInsertPayload(insert));
				result = getConnection().create(data);
			}
			else {
				if (this.activeJob == null) {
					this.activeJob = runBulkInsert(insert);
				}
				if (this.activeJob != null) {
					BatchResult batchResult = getConnection().getBulkResults(this.activeJob);
					for(Result result:batchResult.getResult()) {
						if (result.isSuccess() && result.isCreated()) {
							this.result++;
						}
						else {
							if (result.getErrors().length > 0) {
								this.context.addWarning(new SQLWarning(result.getErrors()[0].getMessage(), result.getErrors()[0].getStatusCode().name()));
							}
						}
					}
					// now process the next set of batch rows
					this.activeJob = null;
					throw new DataNotAvailableException();
				}
			}
		} catch (ResourceException e) {
			throw new TranslatorException(e);
		}		
	}

	private JobInfo runBulkInsert(Insert insert) throws ResourceException {
		if (this.rowIter.hasNext()) {			
			List<SObject> rows = buildBulkRowPayload(insert, this.rowIter, this.executionFactory.getMaxBulkInsertBatchSize());
			return getConnection().executeBulkJob(this.objectName, rows);
		}
		return null;
	}
	
	private List<JAXBElement> buildSingleRowInsertPayload(Insert insert) throws TranslatorException {
		
		List<ColumnReference> columns = insert.getColumns();
		List<Expression> values = ((ExpressionValueSource)insert.getValueSource()).getValues();
		if(columns.size() != values.size()) {
			throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13006));
		}

		List<JAXBElement> elements = new ArrayList<JAXBElement>();
		for(int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i).getMetadataObject();
			QName qname = new QName(column.getNameInSource());
			Object value = values.get(i);
			
			if (value == null) {
			    JAXBElement jbe = new JAXBElement( qname, String.class, null);
				elements.add(jbe);
				continue;
			}
			
			if(!(value instanceof Literal)) {
				throw new TranslatorException(SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13007));
			}
			
			String val;
			Literal literalValue = (Literal)values.get(i);
			if (literalValue.getType().equals(DataTypeManager.DefaultDataClasses.STRING)) {
				val = Util.stripQutes((String)literalValue.getValue());	
			}
			else {
				val = literalValue.getValue().toString();
			}
			
		    JAXBElement jbe = new JAXBElement( qname, String.class, val );
			elements.add(jbe);
		}
		return elements;
	}	
	

	protected List<com.sforce.async.SObject> buildBulkRowPayload(Insert insert, Iterator<? extends List<?>> it, int rowCount) {
		List<com.sforce.async.SObject> rows = new ArrayList<com.sforce.async.SObject>();
		List<ColumnReference> columns = insert.getColumns();
		int boundCount = 0;
		
		while (it.hasNext()) {
			if (boundCount >= rowCount) {
				break;
			}
			boundCount++;
			List<?> values = it.next();
			com.sforce.async.SObject sobj = new com.sforce.async.SObject();
			for(int i = 0; i < columns.size(); i++) {
				ColumnReference element = columns.get(i);
				Column column = element.getMetadataObject();
				Object value = values.get(i);
				if (value == null) {
					sobj.setField(column.getNameInSource(),  null);
					continue;
				}
				if (DataTypeManager.getRuntimeType(value.getClass()).equals(DataTypeManager.DefaultDataClasses.STRING)) {
					sobj.setField(column.getNameInSource(),  Util.stripQutes((String)value));	
				}
				else if (DataTypeManager.getRuntimeType(value.getClass()).equals(DataTypeManager.DefaultDataClasses.TIMESTAMP)) {
					sobj.setField(column.getNameInSource(), SDF.format(value));
				}
				else {
					sobj.setField(column.getNameInSource(), value.toString());
				}
			}
			rows.add(sobj);
		}
		return rows;
	}	
	
	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
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
