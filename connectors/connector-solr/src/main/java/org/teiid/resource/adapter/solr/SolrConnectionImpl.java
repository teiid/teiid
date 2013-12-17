package org.teiid.resource.adapter.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.solr.SolrConnection;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;

import java.io.IOException;

import javax.resource.ResourceException;

import org.teiid.logging.*;

/**
 * @author Jason Marley 
 * TODO add sort, orderby, limit configurations
 */
public class SolrConnectionImpl extends BasicConnection implements
		SolrConnection {

	private HttpSolrServer server;

	/**
	 * @param config
	 */
	public SolrConnectionImpl(SolrManagedConnectionFactory config) {
		try {
			server = new HttpSolrServer(config.getUrl());

			if (config.getSoTimeout() != null) {
				server.setSoTimeout(new Integer(config.getSoTimeout()));
			}
			if (config.getConnTimeout() != null) {
				server.setConnectionTimeout(new Integer(config.getConnTimeout()));
			}
			if (config.getMaxConns() != null) {
				server.setMaxTotalConnections(new Integer(config.getMaxConns()));
			}
			if (config.getAllowCompression() != null) {
				server.setAllowCompression(new Boolean(config
						.getAllowCompression()));
			}
			if (config.getMaxRetries() != null) {
				server.setMaxRetries(new Integer(config.getMaxRetries()));
			}

		} catch (Exception ne) {
			LogManager
					.logError(
							"Property could not be converted to correctly. Please check the binding properties.",
							ne);
		}

	}

	@Override
	public QueryResponse executeQuery(SolrQuery params) {
		
		QueryResponse rsp = null;
		try {
			rsp = this.server.query(params);
		} catch (SolrServerException e) {
			
			LogManager.logError("Issue with solr execution, check configuration", e.getStackTrace());
		}
		
		return rsp;
	}

	/**
	 * /* Close the server connection
	 */
	@Override
	public void close() throws ResourceException {
		if (server != null)
			server.shutdown();
	}

	@Override
	public boolean isAlive() {
		try {
			server.ping();
		} catch (SolrServerException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
