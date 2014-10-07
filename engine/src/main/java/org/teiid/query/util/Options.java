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
	public static final String IMPLICIT_MULTISOURCE_JOIN = "org.teiid.implicitMultiSourceJoin"; //$NON-NLS-1$
	public static final String JOIN_PREFETCH_BATCHES = "org.teiid.joinPrefetchBatches"; //$NON-NLS-1$
	public static final String SANITIZE_MESSAGES = "org.teiid.sanitizeMessages"; //$NON-NLS-1$
	public static final String REQUIRE_COLLATION = "org.teiid.requireTeiidCollation"; //$NON-NLS-1$

	private Properties properties;
	private boolean subqueryUnnestDefault;
	private boolean pushdownDefaultNullOrder;
	private boolean implicitMultiSourceJoin = true;
	private int joinPrefetchBatches = 10;
	private boolean sanitizeMessages;
	private float dependentJoinPushdownThreshold = 0;
	private boolean requireTeiidCollation;
	
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
	
	public void setImplicitMultiSourceJoin(boolean implicitMultiSourceJoin) {
		this.implicitMultiSourceJoin = implicitMultiSourceJoin;
	}
	
	public boolean isImplicitMultiSourceJoin() {
		return implicitMultiSourceJoin;
	}
	
	public Options implicitMultiSourceJoin(boolean b) {
		this.implicitMultiSourceJoin = b;
		return this;
	}
	
	public void setJoinPrefetchBatches(int joinPrefetchBatches) {
		this.joinPrefetchBatches = joinPrefetchBatches;
	}
	
	public int getJoinPrefetchBatches() {
		return joinPrefetchBatches;
	}
	
	public Options joinPrefetchBatches(int i) {
		this.joinPrefetchBatches = i;
		return this;
	}
	
	public void setSanitizeMessages(boolean sanitizeMessages) {
		this.sanitizeMessages = sanitizeMessages;
	}
	
	public boolean isSanitizeMessages() {
		return sanitizeMessages;
	}
	
	public Options sanitizeMessages(boolean b) {
		this.sanitizeMessages = b;
		return this;
	}
	
	public float getDependentJoinPushdownThreshold() {
		return dependentJoinPushdownThreshold;
	}
	
	public void setDependentJoinPushdownThreshold(
			float dependentJoinPushdownThreshold) {
		this.dependentJoinPushdownThreshold = dependentJoinPushdownThreshold;
	}
	
	public Options dependentJoinPushdownThreshold(
			float f) {
		this.dependentJoinPushdownThreshold = f;
		return this;
	}
	
	public boolean isRequireTeiidCollation() {
		return requireTeiidCollation;
	}
	
	public void setRequireTeiidCollation(boolean requireTeiidCollation) {
		this.requireTeiidCollation = requireTeiidCollation;
	}
	
	public Options requireTeiidCollation(boolean b) {
		this.requireTeiidCollation = b;
		return this;
	}

}
