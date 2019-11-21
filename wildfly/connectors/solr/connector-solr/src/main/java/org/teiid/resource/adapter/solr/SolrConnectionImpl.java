/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.resource.adapter.solr;

import java.io.IOException;

import javax.resource.ResourceException;
import javax.security.auth.Subject;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.impl.SolrHttpRequestRetryHandler;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.teiid.resource.spi.BasicConnection;
import org.teiid.resource.spi.ConnectionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.solr.SolrConnection;

public class SolrConnectionImpl extends BasicConnection implements SolrConnection {

    private HttpSolrClient server;
    private String coreName;

    public SolrConnectionImpl(SolrManagedConnectionFactory config) {
        String url = config.getUrl();
        if (!url.endsWith("/")) { //$NON-NLS-1$
            url = config.getUrl()+"/"; //$NON-NLS-1$
        }
        ModifiableSolrParams params = new ModifiableSolrParams();
        String userName = config.getAuthUserName();
        String password = config.getAuthPassword();
        // if security-domain is specified and caller identity is used; then use
        // credentials from subject
        Subject subject = ConnectionContext.getSubject();
        if (subject != null) {
            userName = ConnectionContext.getUserName(subject, config, userName);
            password = ConnectionContext.getPassword(subject, config, userName, password);
        }
        if (userName != null) {
            params.set(HttpClientUtil.PROP_BASIC_AUTH_USER, userName);
            params.set(HttpClientUtil.PROP_BASIC_AUTH_PASS, password);
        }
        String baseUrl = url + config.getCoreName();
        this.server = new Builder(baseUrl).withInvariantParams(params).build();

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
            ((DefaultHttpClient)this.server.getHttpClient()).setHttpRequestRetryHandler(new SolrHttpRequestRetryHandler(config.getMaxRetries()));
        }
        this.coreName = config.getCoreName();
    }

    @Override
    public void close() throws ResourceException {
        if (this.server != null) {
            try {
                this.server.close();
            } catch (IOException e) {
                throw new ResourceException(e);
            }
        }
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
        } catch (IOException e) {
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
