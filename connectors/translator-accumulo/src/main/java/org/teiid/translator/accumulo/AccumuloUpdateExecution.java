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
package org.teiid.translator.accumulo;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.teiid.language.ColumnReference;
import org.teiid.language.Command;
import org.teiid.language.Delete;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.Literal;
import org.teiid.language.SetClause;
import org.teiid.language.Update;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

public class AccumuloUpdateExecution implements UpdateExecution {
	private Command command;
	private AccumuloConnection connection;
	private AccumuloExecutionFactory aef;
	private int updateCount = 0;
	private ExecutionContext executionContext;
	
	public AccumuloUpdateExecution(AccumuloExecutionFactory aef, Command command,
			ExecutionContext executionContext, @SuppressWarnings("unused") RuntimeMetadata metadata,
			AccumuloConnection connection) {
		this.aef = aef;
		this.command = command;
		this.connection = connection;
		this.executionContext = executionContext;
	}

	@Override
	public void execute() throws TranslatorException {
		try {
			if (this.command instanceof Insert) {
				Insert insert = (Insert)this.command;
				performInsert(insert);
			}
			else if (this.command instanceof Update) {
				Update update = (Update)this.command;
				performUpdate(update);
			}
			else if (this.command instanceof Delete) {
				Delete delete = (Delete)this.command;
				performDelete(delete);
			}
		} catch (MutationsRejectedException e) {
			throw new TranslatorException(e);
		} catch (TableNotFoundException e) {
			throw new TranslatorException(e);
		}
	}

	private void performInsert(Insert insert) throws TranslatorException, TableNotFoundException, MutationsRejectedException {
		Table table = insert.getTable().getMetadataObject();
		this.updateCount = 0;
		
		Connector connector = this.connection.getInstance();
		BatchWriter writer = createBatchWriter(table, connector);	        	
		
		List<ColumnReference> columns = insert.getColumns();
		if (insert.getParameterValues() == null) {
			List<Expression> values = ((ExpressionValueSource)insert.getValueSource()).getValues();
			writeMutation(writer, columns, values);
			this.updateCount++;
		}
		else {
			int batchSize = this.executionContext.getBatchSize();
			// bulk insert; should help 
			Iterator<? extends List<Expression>> args = (Iterator<? extends List<Expression>>) insert.getParameterValues();
			while (args.hasNext()) {
				List<Expression> values = args.next();
				writeMutation(writer, columns, values);
				this.updateCount++;
				
				if ((this.updateCount%batchSize) == 0) {
					writer.close();
					writer = createBatchWriter(table, connector);
				}
			}
		}
		// write the mutation
		writer.close();
	}

	private void writeMutation(BatchWriter writer, List<ColumnReference> columns, List<Expression> values) throws MutationsRejectedException {
		byte[] rowId = getRowId(columns, values);
		for (int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i).getMetadataObject();			
			if (SQLStringVisitor.getRecordName(column).equalsIgnoreCase(AccumuloMetadataProcessor.ROWID)) {
				continue;
			}
			Object value = values.get(i);
			if (value instanceof Literal) {
				writer.addMutation(buildMutation(rowId, column, ((Literal)value).getValue()));
			}
			else {
				writer.addMutation(buildMutation(rowId, column, value));
			}
		}
	}
	
	private void performUpdate(Update update) throws TranslatorException, TableNotFoundException, MutationsRejectedException {
		Table table = update.getTable().getMetadataObject();

		if (update.getParameterValues() != null) {
			throw new TranslatorException(AccumuloPlugin.Event.TEIID19005, AccumuloPlugin.Util.gs(AccumuloPlugin.Event.TEIID19005));			
		}
		
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor(this.aef);
		visitor.visitNode(update.getWhere());
		if (!visitor.exceptions.isEmpty()) {
			throw visitor.exceptions.get(0);
		}

		Connector connector = this.connection.getInstance();
		BatchWriter writer = createBatchWriter(table, connector);	        	
		
		Text prevRow = null;
		Iterator<Entry<Key,Value>> results = AccumuloQueryExecution.runQuery(this.aef, this.connection.getInstance(), this.connection.getAuthorizations(), visitor.getRanges(), table, visitor.scanIterators());
		while (results.hasNext()) {
			Key key = results.next().getKey();
			Text rowId = key.getRow();

			if (prevRow == null || !prevRow.equals(rowId)) {
				prevRow = rowId;
				List<SetClause> changes = update.getChanges();
				for (SetClause clause:changes) {
					Column column = clause.getSymbol().getMetadataObject();
					if (SQLStringVisitor.getRecordName(column).equalsIgnoreCase(AccumuloMetadataProcessor.ROWID)) {
						throw new TranslatorException(AccumuloPlugin.Event.TEIID19002, AccumuloPlugin.Util.gs(AccumuloPlugin.Event.TEIID19002, table.getName())); 
					}
					Expression value = clause.getValue();
					if (value instanceof Literal) {
						Mutation mutation = buildMutation(rowId.getBytes(), column, ((Literal)value).getValue());
						writer.addMutation(mutation);
					}
					else {
						throw new TranslatorException(AccumuloPlugin.Event.TEIID19001, AccumuloPlugin.Util.gs(AccumuloPlugin.Event.TEIID19001));
					}
				}
				this.updateCount++;
			}
		}
		writer.close();
	}

	private BatchWriter createBatchWriter(Table table, Connector connector) throws TranslatorException, TableNotFoundException {
		String tableName = SQLStringVisitor.getRecordName(table);
		BatchWriter writer;
		try {
			writer = connector.createBatchWriter(tableName, new BatchWriterConfig());
		} catch (TableNotFoundException e) {
			try {
				connector.tableOperations().create(tableName);
			} catch (AccumuloException e1) {
				throw new TranslatorException(e1);
			} catch (AccumuloSecurityException e1) {
				throw new TranslatorException(e1);
			} catch (TableExistsException e1) {
				throw new TranslatorException(e1);
			}
			writer = connector.createBatchWriter(tableName, new BatchWriterConfig());
		}
		return writer;
	}
	
	private void performDelete(Delete delete) throws TableNotFoundException, MutationsRejectedException, TranslatorException {
		
		if (delete.getParameterValues() != null) {
			throw new TranslatorException(AccumuloPlugin.Event.TEIID19005, AccumuloPlugin.Util.gs(AccumuloPlugin.Event.TEIID19005));			
		}
		
		Table table = delete.getTable().getMetadataObject();
		AccumuloQueryVisitor visitor = new AccumuloQueryVisitor(this.aef);
		visitor.visitNode(delete.getWhere());
		if (!visitor.exceptions.isEmpty()) {
			throw visitor.exceptions.get(0);
		}
		
		/*
		// To get the update count I am taking longer route..
		Connector connector = this.connection.getInstance();
		BatchDeleter deleter = connector.createBatchDeleter(SQLStringVisitor.getRecordName(table), auths, this.aef.getQueryThreadsCount(), new BatchWriterConfig());	        	
		deleter.setRanges(visitor.getRanges());
		deleter.delete();
		deleter.close();
		*/
		
		Text prevRow = null;
		Connector connector = this.connection.getInstance();
		BatchWriter writer = createBatchWriter(table, connector);
		Iterator<Entry<Key,Value>> results = AccumuloQueryExecution.runQuery(this.aef, this.connection.getInstance(), this.connection.getAuthorizations(), visitor.getRanges(), table, null);
		while (results.hasNext()) {
			Key key = results.next().getKey();
			Text rowId = key.getRow();

			if (prevRow == null || !prevRow.equals(rowId)) {
				this.updateCount++;
			}
			prevRow = rowId;
			Mutation mutation = new Mutation(rowId);
			mutation.putDelete(key.getColumnFamily(), key.getColumnQualifier());
			writer.addMutation(mutation);
		}
		writer.close();
	}	
	
	private byte[] getRowId(List<ColumnReference> columns, List<Expression> values) {
		for (int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i).getMetadataObject();
			String rowId = SQLStringVisitor.getRecordName(column);
			if (rowId.equalsIgnoreCase(AccumuloMetadataProcessor.ROWID) || AccumuloQueryVisitor.isPartOfPrimaryKey(column)) {
				Object value = values.get(i);
				if (value instanceof Literal) {
					return AccumuloDataTypeManager.convertToAccumuloType(((Literal)value).getValue(), this.aef.getChasetEncoding());
				}
				return AccumuloDataTypeManager.convertToAccumuloType(value, this.aef.getChasetEncoding());
			}
		}		
		return null;
	}
	
	private Mutation buildMutation(byte[] rowid, Column column, Object value) {
		String CF = column.getProperty(AccumuloMetadataProcessor.CF, false);
		String CQ = column.getProperty(AccumuloMetadataProcessor.CQ, false);
		String valuePattern = column.getProperty(AccumuloMetadataProcessor.VALUE_IN, false);
		if (valuePattern == null) {
			valuePattern = AccumuloMetadataProcessor.DEFAULT_VALUE_PATTERN;
		}
		
		byte[] columnFamilty = CF.getBytes();
		byte[] columnQualifier = (CQ == null)?AccumuloDataTypeManager.EMPTY_BYTES:CQ.getBytes();
		byte[] columnValue = AccumuloDataTypeManager.EMPTY_BYTES;
		
		Mutation mutation = new Mutation(rowid);
		valuePattern = valuePattern.substring(1, valuePattern.length()-1); // remove the curleys
		if (valuePattern.equals(AccumuloMetadataProcessor.ValueIn.VALUE.name())) {
			columnValue = AccumuloDataTypeManager.convertToAccumuloType(value, this.aef.getChasetEncoding());
		}
		else if (valuePattern.equals(AccumuloMetadataProcessor.ValueIn.CQ.name())) {
			columnQualifier = AccumuloDataTypeManager.convertToAccumuloType(value, this.aef.getChasetEncoding());
		}
		mutation.put(columnFamilty, columnQualifier, columnValue);
		return mutation;
	}	

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException, TranslatorException {
		return new int[] {this.updateCount};
	}

	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}
}
