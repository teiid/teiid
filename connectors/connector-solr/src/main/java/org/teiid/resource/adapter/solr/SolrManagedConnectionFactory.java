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