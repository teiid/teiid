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
package org.teiid.translator.solr;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.teiid.language.*;
import org.teiid.language.Comparison.Operator;
import org.teiid.metadata.Column;
import org.teiid.metadata.KeyRecord;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.solr.SolrQueryExecution.SolrDocumentCallback;

public class SolrUpdateExecution implements UpdateExecution {
	private SolrExecutionFactory ef;
	private SolrConnection connection;
	private Command command;
	private int updateCount = 0;
	private RuntimeMetadata metadata;
	private ExecutionContext executionContext;
	
	public SolrUpdateExecution(SolrExecutionFactory ef,
			Command command, ExecutionContext executionContext,
			RuntimeMetadata metadata, SolrConnection connection) {
		this.ef = ef;
		this.command = command;
		this.connection = connection;
		this.executionContext = executionContext;
		this.metadata = metadata;
	}

	@Override
	public void execute() throws TranslatorException {
		if (this.command instanceof Insert) {
			Insert insert = (Insert)this.command;
			performInsert(insert);
		}
		else {
			if (this.command instanceof Update) {
				Update update = (Update)this.command;
				performUpdate(update);
			}
			else if (this.command instanceof Delete) {
				Delete delete = (Delete)this.command;
				performUpdate(delete);
			}
		}
	}

	private void performUpdate(Delete obj) throws TranslatorException{
		Table table = obj.getTable().getMetadataObject();
		KeyRecord pk = table.getPrimaryKey();
		final String id = pk.getColumns().get(0).getName();
		
		SolrQueryExecution query = new SolrQueryExecution(ef, obj, this.executionContext, this.metadata, this.connection);
		query.execute();
		
		final UpdateRequest request = new UpdateRequest();
		query.walkDocuments(new SolrDocumentCallback() {
			@Override
			public void walk(SolrDocument doc) {
				SolrUpdateExecution.this.updateCount++;
				request.deleteById(doc.getFieldValue(id).toString());
			}
		});
		
		UpdateResponse response = this.connection.update(request);
		if (response.getStatus() != 0) {
			throw new TranslatorException(SolrPlugin.Event.TEIID20005, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20005, response.getStatus()));
		}		
	}

	/**
	 * Did not find any other suitable way to pass the query through solrj otherthan walking the documents,
	 * all the examples were at the passing xml based query. so that would be a good update if the below does
	 * not performs or gets into OOM
	 * @param obj
	 * @throws TranslatorException
	 */
	private void performUpdate(final Update obj) throws TranslatorException {
		SolrQueryExecution query = new SolrQueryExecution(ef, obj, this.executionContext, this.metadata, this.connection);
		query.execute();
		
		final UpdateRequest request = new UpdateRequest();
		
		query.walkDocuments(new SolrDocumentCallback() {
			@Override
			public void walk(SolrDocument doc) {
				SolrUpdateExecution.this.updateCount++;

				Table table = obj.getTable().getMetadataObject();
				SolrInputDocument updateDoc = new SolrInputDocument();
				for (String name:doc.getFieldNames()){
					if (table.getColumnByName(name) != null){
						updateDoc.setField(name, doc.getFieldValue(name));
					}
				}

				int elementCount = obj.getChanges().size();
				for (int i = 0; i < elementCount; i++) {
					Column column = obj.getChanges().get(i).getSymbol().getMetadataObject();
					Literal value = (Literal)obj.getChanges().get(i).getValue();
					updateDoc.setField(column.getName(), value.getValue());
				}
				request.add(updateDoc);
			}
		});
				
		if (!request.getDocuments().isEmpty()){
			UpdateResponse response = this.connection.update(request);
			if (response.getStatus() != 0) {
				throw new TranslatorException(SolrPlugin.Event.TEIID20004, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20004, response.getStatus()));
			}
		}
	}

	private void performInsert(Insert insert) throws TranslatorException {
		// build insert
		final UpdateRequest request = new UpdateRequest();
		SolrInputDocument doc = new SolrInputDocument();
		List<ColumnReference> columns = insert.getColumns();
		List<Expression> values = ((ExpressionValueSource)insert.getValueSource()).getValues();
		for (int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i).getMetadataObject();			
			Object value = values.get(i);
			if (value instanceof Literal) {
				doc.addField(column.getName(), ((Literal)value).getValue());
			}
			else {
				throw new TranslatorException(SolrPlugin.Event.TEIID20002, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20002));
			}				
		}
		request.add(doc);
		
		// check if the row already exists
        Select q = buildSelectQuery(insert);		
		SolrQueryExecution query = new SolrQueryExecution(ef, q, this.executionContext, this.metadata, this.connection);
		query.execute();
		query.walkDocuments(new SolrDocumentCallback() {
			@Override
			public void walk(SolrDocument doc) {
				request.clear();
			}
		});		
		
		if (request.getDocuments().isEmpty()){
			throw new TranslatorException(SolrPlugin.Event.TEIID20007, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20007));			
		}
		
		// write the mutation
		UpdateResponse response = this.connection.update(request);
		if (response.getStatus() == 0) {
			this.updateCount = 1;
		}
		else {
			throw new TranslatorException(SolrPlugin.Event.TEIID20003, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20003, response.getStatus()));
		}
	}

	private Select buildSelectQuery(Insert insert) throws TranslatorException {
		Table table = insert.getTable().getMetadataObject();
		KeyRecord pk = table.getPrimaryKey();
		final String id = pk.getColumns().get(0).getName();
		
		NamedTable g = insert.getTable(); 
        List<DerivedColumn> symbols = new ArrayList<DerivedColumn>();
        for (Column column:table.getColumns()){
        	symbols.add(new DerivedColumn(column.getName(), new ColumnReference(g, column.getName(), column, column.getJavaType()))); 	
        }
        
        List groups = new ArrayList();
        groups.add(g);
        
        ColumnReference idCol = new ColumnReference(g, id, table.getColumnByName(id), table.getColumnByName(id).getJavaType());
        Comparison cc = new Comparison(idCol, getPKValue(id, insert), Operator.EQ);
        
        Select q = new Select(symbols, false, groups, cc, null, null, null);
		return q;
	}
	
	private Literal getPKValue(String pk, Insert insert) throws TranslatorException {
		List<ColumnReference> columns = insert.getColumns();
		List<Expression> values = ((ExpressionValueSource)insert.getValueSource()).getValues();
		for (int i = 0; i < columns.size(); i++) {
			Column column = columns.get(i).getMetadataObject();			
			Object value = values.get(i);
			if (column.getName().equals(pk)){
				if (value instanceof Literal) {
					return (Literal)value;
				}
				throw new TranslatorException(SolrPlugin.Event.TEIID20002, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20002));
			}
		}
		throw new TranslatorException(SolrPlugin.Event.TEIID20005, SolrPlugin.Util.gs(SolrPlugin.Event.TEIID20005));
	}

	@Override
	public int[] getUpdateCounts() throws DataNotAvailableException,
			TranslatorException {
		return new int [] {this.updateCount};
	}
	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}
}
