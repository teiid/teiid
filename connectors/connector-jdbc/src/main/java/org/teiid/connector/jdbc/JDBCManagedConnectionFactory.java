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
package org.teiid.connector.jdbc;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.basic.BasicManagedConnectionFactory;
import org.teiid.connector.jdbc.translator.Translator;

import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.util.ReflectionHelper;
import com.metamatrix.core.util.StringUtil;

public class JDBCManagedConnectionFactory extends BasicManagedConnectionFactory {
	
	private static final long serialVersionUID = -6719853084334318668L;
	// JDBC connector properties
	private boolean useBindVariables = false;
	private String databaseTimeZone;
	private String extensionTranslationClassName;
	private String sourceJNDIName;
	private boolean trimStrings=false;
	private boolean useCommentsInSourceQuery = false;
	private int fetchSize = -1;
		
	// derived
	private Translator sqlTranslator;
	
	public boolean isUseBindVariables() {
		return useBindVariables;
	}

	public String getDatabaseTimeZone() {
		return databaseTimeZone;
	}

	public String getExtensionTranslationClassName() {
		return extensionTranslationClassName;
	}
	
	public Translator getExtensionTranslationClass() throws ConnectorException {
		if (this.sqlTranslator == null) {
	        try {
	        	String className = getExtensionTranslationClassName();
	        	if (!StringUtil.isValid(className)) {
	        		this.sqlTranslator = new Translator();
	        	} else { 
	        		this.sqlTranslator = (Translator)ReflectionHelper.create(className, null, Thread.currentThread().getContextClassLoader());
	        	}
	            sqlTranslator.initialize(this);
	        } catch (MetaMatrixCoreException e) {
	            throw new ConnectorException(e);
			}
		}
		return this.sqlTranslator;
	}	

	public String getSourceJNDIName() {
		return sourceJNDIName;
	}

	public boolean isTrimStrings() {
		return trimStrings;
	}

	public boolean isUseCommentsInSourceQuery() {
		return useCommentsInSourceQuery;
	}
	
	public void setUseBindVariables(Boolean arg0) {
		this.useBindVariables = arg0.booleanValue();
	}
	
	public void setDatabaseTimeZone(String arg0) {
		this.databaseTimeZone = arg0;
	}
	
	public void setExtensionTranslationClassName(String arg0) {
		this.extensionTranslationClassName = arg0;
	}
	
	public void setSourceJNDIName(String arg0) {
		this.sourceJNDIName = arg0;
	}
	
	public void setTrimStrings(Boolean arg0) {
		this.trimStrings = arg0.booleanValue();
	}

	public void setUseCommentsInSourceQuery(Boolean arg0) {
		this.useCommentsInSourceQuery = arg0.booleanValue();
	}
	
	public void setFetchSize(Integer arg0) {
		this.fetchSize = arg0.intValue();
	}
	
	public int getFetchSize() {
		return this.fetchSize;
	}	
}
