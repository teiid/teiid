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

package org.teiid.query.util;

import java.util.Properties;

/**
 * A holder for options
 */
public class Options {

	public static final String UNNEST_DEFAULT = "org.teiid.subqueryUnnestDefault"; //$NON-NLS-1$
	public static final String PUSHDOWN_DEFAULT_NULL_ORDER = "org.teiid.pushdownDefaultNullOrder"; //$NON-NLS-1$ 

	private Properties properties;
	private boolean subqueryUnnestDefault;
	private boolean pushdownDefaultNullOrder;
	
	public Properties getProperties() {
		return properties;
	}
	
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
	public boolean isSubqueryUnnestDefault() {
		return subqueryUnnestDefault;
	}
	
	public void setSubqueryUnnestDefault(boolean subqueryUnnestDefault) {
		this.subqueryUnnestDefault = subqueryUnnestDefault;
	}
	
	public Options subqueryUnnestDefault(boolean s) {
		this.subqueryUnnestDefault = s;
		return this;
	} 
	
	public boolean isPushdownDefaultNullOrder() {
		return pushdownDefaultNullOrder;
	}
	
	public void setPushdownDefaultNullOrder(boolean virtualizeDefaultNullOrdering) {
		this.pushdownDefaultNullOrder = virtualizeDefaultNullOrdering;
	}
	
	public Options pushdownDefaultNullOrder(boolean p) {
		this.pushdownDefaultNullOrder = p;
		return this;
	}

}
