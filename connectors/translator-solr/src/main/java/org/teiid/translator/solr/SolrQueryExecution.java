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
import java.util.Iterator;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class SolrQueryExecution implements ResultSetExecution {
	private ExecutionContext executionContext;
	private SolrConnection connection;
	private SolrSQLHierarchyVistor visitor;
	private Iterator<SolrDocument> resultsItr;
	private Class<?>[] expectedTypes;
	private SolrExecutionFactory executionFactory;
	private int offset = 0;
	private Long resultSize;

	public SolrQueryExecution(SolrExecutionFactory ef, Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			SolrConnection connection) {
		this.executionFactory = ef;
		this.executionContext = executionContext;
		this.connection = connection;
		if (command instanceof QueryExpression) {
			this.expectedTypes = ((QueryExpression)command).getColumnTypes();
		}
		
		this.visitor = new SolrSQLHierarchyVistor(metadata, this.executionFactory);
		this.visitor.visitNode(command);
	}

	@Override
	public void execute() throws TranslatorException {
		LogManager.logDetail("Solr Source Query:", this.visitor.getSolrQuery()); //$NON-NLS-1$
		nextBatch();
	}
	
	public void nextBatch() throws TranslatorException {
		SolrQuery query = this.visitor.getSolrQuery();
		if (!this.visitor.isLimitInUse()) {
			query.setStart(this.offset);
			query.setRows(this.executionContext.getBatchSize());
		}
		
		QueryResponse queryResponse = connection.query(this.visitor.getSolrQuery());
		SolrDocumentList docList = queryResponse.getResults();
		this.resultSize = docList.getNumFound();
		this.resultsItr = docList.iterator();
	}

	/*
	 * This iterates through the documents from Solr and maps their fields to
	 * rows in the Teiid table
	 * 
	 * @see org.teiid.translator.ResultSetExecution#next()
	 */
	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {

		final List<Object> row = new ArrayList<Object>();

		if (this.visitor.isCountStarInUse() && this.resultsItr != null) {
			row.add(this.resultSize);
			this.resultsItr = null;
			return row;
		}
		
		// is there any solr docs
		if (this.resultsItr != null && this.resultsItr.hasNext()) {
			SolrDocument doc = this.resultsItr.next();
			for (int i = 0; i < this.visitor.getFieldNameList().size(); i++) {
				String columnName = this.visitor.getFieldNameList().get(i);
				Object obj = doc.getFieldValue(columnName);
				row.add(this.executionFactory.convertFromSolrType(obj, this.expectedTypes[i]));
			}
			this.offset++;
			
			// if we are at the end of the current cursor set, then get next ones.
			if (!this.resultsItr.hasNext() && !this.visitor.isLimitInUse()) {
				nextBatch();
			}
			return row;
		}
		return null;
	}
	
	interface SolrDocumentCallback {
		void walk(SolrDocument doc);
	}
	
	public void walkDocuments(SolrDocumentCallback callback) throws TranslatorException {
		while(this.resultsItr != null && this.resultsItr.hasNext()) {
			SolrDocument doc = this.resultsItr.next();
			callback.walk(doc);
			this.offset++;
			
			// if we are at the end of the current cursor set, then get next ones.
			if (!this.resultsItr.hasNext()) {
				nextBatch();
			}
		}		
	}
	
	@Override
	public void close() {
	}

	@Override
	public void cancel() throws TranslatorException {
	}	
}
