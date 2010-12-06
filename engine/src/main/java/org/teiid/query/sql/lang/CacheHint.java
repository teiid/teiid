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

package org.teiid.query.sql.lang;

import java.io.Serializable;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.sql.visitor.SQLStringVisitor;

public class CacheHint implements Serializable {

	private static final long serialVersionUID = -4119606289701982511L;
	
	public static final String PREF_MEM = "pref_mem"; //$NON-NLS-1$
	public static final String TTL = "ttl:"; //$NON-NLS-1$
	public static final String UPDATABLE = "updatable"; //$NON-NLS-1$
	public static final String CACHE = "cache"; //$NON-NLS-1$
	public static final String SCOPE = "scope:"; //$NON-NLS-1$
	
	private static final String SESSION = "session"; //$NON-NLS-1$
	private static final String VDB = "vdb"; //$NON-NLS-1$
	private static final String USER = "user"; //$NON-NLS-1$
	
	private boolean prefersMemory;
	private boolean updatable;
	private Long ttl;
	private String scope;
	
	public CacheHint() {
	}
	
	public CacheHint(boolean prefersMemory, Long ttl) {
		this.prefersMemory = prefersMemory;
		this.ttl = ttl;
	}

	public boolean getPrefersMemory() {
		return prefersMemory;
	}
	
	public void setPrefersMemory(boolean prefersMemory) {
		this.prefersMemory = prefersMemory;
	}
	
	public Long getTtl() {
		return ttl;
	}
	
	public void setTtl(Long ttl) {
		this.ttl = ttl;
	}
	
	@Override
	public String toString() {
		SQLStringVisitor ssv = new SQLStringVisitor();
		ssv.addCacheHint(this);
		return ssv.getSQLString();
	}
	
	public boolean isUpdatable() {
		return updatable;
	}
	
	public void setUpdatable(boolean updatable) {
		this.updatable = updatable;
	}
	
	public Determinism getDeterminism() {
		if (this.scope == null) {
			return null;
		}
		
		if (scope.equals(SESSION)) { 
			return Determinism.SESSION_DETERMINISTIC;
		}
		else if (this.scope.equals(VDB)) {
			return Determinism.VDB_DETERMINISTIC;
		}
		else if (this.scope.equals(USER)) { 
			return  Determinism.USER_DETERMINISTIC;
		}
		return null;
	}
	
	public String getScope() {
		return this.scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof CacheHint)) {
			return false;
		}
		CacheHint other = (CacheHint)obj;
		return this.prefersMemory == other.prefersMemory 
		&& EquivalenceUtil.areEqual(this.ttl, other.ttl) 
		&& this.updatable == other.updatable
		&& EquivalenceUtil.areEqual(this.scope, other.scope);
	}

}
