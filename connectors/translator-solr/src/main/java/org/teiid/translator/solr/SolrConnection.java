package org.teiid.translator.solr;

import javax.resource.cci.Connection;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;

public interface SolrConnection extends Connection {

	/*
	 * Executes a Solr query.
	 */
	public QueryResponse executeQuery(SolrQuery params);

}
