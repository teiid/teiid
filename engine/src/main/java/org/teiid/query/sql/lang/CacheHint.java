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

import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.translator.CacheDirective;

public class CacheHint extends CacheDirective {

	private static final long serialVersionUID = -4119606289701982511L;
	
	public static final String PREF_MEM = "pref_mem"; //$NON-NLS-1$
	public static final String TTL = "ttl:"; //$NON-NLS-1$
	public static final String UPDATABLE = "updatable"; //$NON-NLS-1$
	public static final String CACHE = "cache"; //$NON-NLS-1$
	public static final String SCOPE = "scope:"; //$NON-NLS-1$
	
	public CacheHint() {
	}
	
	public CacheHint(Boolean prefersMemory, Long ttl) {
		super(prefersMemory, ttl);
	}
	
	public boolean isPrefersMemory() {
		if (getPrefersMemory() != null) {
			return getPrefersMemory();
		}
		return false;
	}

	@Override
	public String toString() {
		SQLStringVisitor ssv = new SQLStringVisitor();
		ssv.addCacheHint(this);
		return ssv.getSQLString();
	}
	
	public Determinism getDeterminism() {
		if (this.getScope() == null) {
			return null;
		}
		switch (getScope()) {
		case SESSION:
			return Determinism.SESSION_DETERMINISTIC;
		case VDB:
			return Determinism.VDB_DETERMINISTIC;
		}
		return Determinism.USER_DETERMINISTIC;
	}
	
	public void setScope(String scope) {
		if (scope == null) {
			setScope((Scope)null);
		} else {
			setScope(Scope.valueOf(scope.toUpperCase()));
		}
	}
	
	public boolean isUpdatable(boolean b) {
		if (getUpdatable() != null) {
			return getUpdatable();
		}
		return b;
	}

}
