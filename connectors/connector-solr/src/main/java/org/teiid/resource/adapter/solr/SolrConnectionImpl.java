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
package org.teiid.resource.adapter.solr;

import java.io.IOException;

import javax.resource.ResourceException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.solr.SolrConnection;

public class SolrConnectionImpl extends BasicConnection implements SolrConnection {

	private HttpSolrServer server;
	private String coreName;

	public SolrConnectionImpl(SolrManagedConnectionFactory config) {
		String url = config.getUrl();
		if (!url.endsWith("/")) { //$NON-NLS-1$
			url = config.getUrl()+"/"; //$NON-NLS-1$
		}
		url = url + config.getCoreName();
		this.server = new HttpSolrServer(url);

		if (config.getSoTimeout() != null) {
			this.server.setSoTimeout(config.getSoTimeout());
		}
		if (config.getConnTimeout() != null) {
			this.server.setConnectionTimeout(config.getConnTimeout());
		}
		if (config.getMaxConns() != null) {
			this.server.setMaxTotalConnections(config.getMaxConns());
		}
		if (config.getAllowCompression() != null) {
			this.server.setAllowCompression(config.getAllowCompression());
		}
		if (config.getMaxRetries() != null) {
			this.server.setMaxRetries(config.getMaxRetries());
		}
		
		this.coreName = config.getCoreName();
	}

	@Override
	public void close() throws ResourceException {
		if (this.server != null)
			this.server.shutdown();
	}

	@Override
	public boolean isAlive() {
		try {
			this.server.ping();
		} catch (SolrServerException e) {
			return false;
		} catch (IOException e) {
			return false;
		}
		return true;
	}

	@Override
	public QueryResponse query(SolrQuery params) throws TranslatorException {
		try {
			return server.query(params);
		} catch (SolrServerException e) {
			throw new TranslatorException(e);
		}
	}

	@Override
	public UpdateResponse update(UpdateRequest request) throws TranslatorException {
		try {
			request.setCommitWithin(-1);
			request.setAction(UpdateRequest.ACTION.COMMIT, false, false );
			return request.process(this.server);
		} catch (SolrServerException e) {
			throw new TranslatorException(e);
		} catch (IOException e) {
			throw new TranslatorException(e);
		}
	}

	@Override
	public LukeResponse metadata(LukeRequest request) throws TranslatorException {
		try {
			return request.process(this.server);
		} catch (SolrServerException e) {
			throw new TranslatorException(e);
		} catch (IOException e) {
			throw new TranslatorException(e);
		}
	}

	@Override
	public String getCoreName() {
		return this.coreName;
	}
}
