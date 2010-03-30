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

package com.metamatrix.connector.xml.base;

import java.sql.SQLXML;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.connector.basic.BasicManagedConnectionFactory;

public class XMLBaseManagedConnectionFactory extends BasicManagedConnectionFactory {
		
	private String saxFilterProviderClass;
	private Map<String, SQLXML> responses = new ConcurrentHashMap<String, SQLXML>();

	public String getSaxFilterProviderClass() {
		return this.saxFilterProviderClass;
	}

	public void setSaxFilterProviderClass(String saxFilterProviderClass) {
		this.saxFilterProviderClass = saxFilterProviderClass;
	}
	
	private String queryPreprocessorClass;

	public String getQueryPreprocessorClass() {
		return this.queryPreprocessorClass;
	}

	public void setQueryPreprocessorClass(String queryPreprocessorClass) {
		this.queryPreprocessorClass = queryPreprocessorClass;
	}

	private String inputStreamFilterClass;

	public String getInputStreamFilterClass() {
		return this.inputStreamFilterClass;
	}

	public void setInputStreamFilterClass(String inputStreamFilterClass) {
		this.inputStreamFilterClass = inputStreamFilterClass;
	}
	
	private boolean logRequestResponseDocs;
	
	public boolean isLogRequestResponseDocs() {
		return logRequestResponseDocs;
	}

	public void setLogRequestResponseDocs(Boolean logRequestResponseDocs) {
		this.logRequestResponseDocs = logRequestResponseDocs;
	}

	public SQLXML getResponse(String key) {
		return this.responses.get(key);
	}
	
	public void setResponse(String key, SQLXML xml) {
		this.responses.put(key, xml);
	}
	
	public SQLXML removeResponse(String key) {
		return this.responses.remove(key);
	}	
	
}
