package org.teiid.translator.solr.execution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import org.teiid.language.DerivedColumn;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.solr.SolrConnection;
import org.teiid.translator.solr.SolrExecutionFactory;

public class SolrQueryExecution implements ResultSetExecution {

	private Select query;
	@SuppressWarnings("unused")
	private ExecutionContext executionContext;
	private SolrConnection connection;
	private SolrSQLHierarchyVistor visitor;
	private SolrQuery params = new SolrQuery();
	private QueryResponse queryResponse = null;
	private List<DerivedColumn> fieldList = null;
	private Iterator<SolrDocument> docItr;
	private Class<?>[] expectedTypes;
	private SolrExecutionFactory executionFactory = new SolrExecutionFactory();

	public SolrQueryExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			SolrConnection connection) {
		this.query = (Select) command;
		this.executionContext = executionContext;
		this.connection = connection;
		this.expectedTypes = command.getColumnTypes();

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void cancel() throws TranslatorException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.teiid.translator.Execution#execute()
	 */
	@SuppressWarnings("static-access")
	@Override
	public void execute() throws TranslatorException {

		this.visitor = new SolrSQLHierarchyVistor();

		// transform SQL query to Solr query
		this.visitor.visitNode(query);

		fieldList = this.visitor.getFieldNameList();

		// add response fields to Solr query
		for (DerivedColumn field : fieldList) {
			params.addField(visitor.getShortName((field.toString())));
		}

		// set document return limit
		params.setRows(this.visitor.getDocLimit());

		// TODO add order by capabilities
		// set sort
		// params.setSort(this.visitor.getSort())

		// set Solr Query
		params.setQuery(this.visitor.getTranslatedSQL());

//		LogManager.logInfo("This is the solr query: ", this.visitor.getTranslatedSQL());

		// execute query
		queryResponse = connection.executeQuery(params);

		SolrDocumentList docList = queryResponse.getResults();
				
		docItr = docList.iterator();

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
		String columnName;

		// is there any solr docs
		if (this.docItr != null && this.docItr.hasNext()) {

			SolrDocument doc = this.docItr.next();

			for (int i = 0; i < this.visitor.fieldNameList.size(); i++) {
				// TODO handle multiple tables
				columnName = this.visitor.getShortFieldName(i);
				row.add(this.executionFactory.convertToTeiid(
						doc.getFieldValue(columnName), this.expectedTypes[i])); 
			}
			return row;
		}
		return null;
	}
}
