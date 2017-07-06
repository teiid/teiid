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

import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class SolrManagedConnectionFactory extends BasicManagedConnectionFactory {

	private static final long serialVersionUID = -2751565394772750705L;
	private String url;
	private Integer soTimeout;
	private Boolean allowCompression;
	private Integer connTimeout; // min 5 seconds to establish TCP
	private Integer maxConns;
	private Integer maxRetries;
	private String coreName;

	@Override
	public SolrConnectionFactory createConnectionFactory()
			throws ResourceException {
		return new SolrConnectionFactory(SolrManagedConnectionFactory.this);
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Integer getSoTimeout() {
		return soTimeout;
	}

	public void setSoTimeout(Integer soTimeout) {
		this.soTimeout = soTimeout;
	}

	public Boolean getAllowCompression() {
		return allowCompression;
	}

	public void setAllowCompression(Boolean allowCompression) {
		this.allowCompression = allowCompression;
	}

	public Integer getConnTimeout() {
		return connTimeout;
	}

	public void setConnTimeout(Integer connTimeout) {
		this.connTimeout = connTimeout;
	}

	public Integer getMaxConns() {
		return maxConns;
	}

	public void setMaxConns(Integer maxConns) {
		this.maxConns = maxConns;
	}

	public Integer getMaxRetries() {
		return maxRetries;
	}

	public void setMaxRetries(Integer maxRetries) {
		this.maxRetries = maxRetries;
	}
	
	public String getCoreName() {
		return coreName;
	}

	public void setCoreName(String coreName) {
		this.coreName = coreName;
	}	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SolrManagedConnectionFactory other = (SolrManagedConnectionFactory) obj;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}

}