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

package org.teiid.translator;

import java.io.Serializable;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;

public class CacheDirective implements Serializable {
	
	public enum Scope {
		NONE,
		SESSION,
		USER,
		VDB
	}

	private static final long serialVersionUID = -4119606289701982511L;
	
	private Boolean prefersMemory;
	private Boolean updatable;
	private Boolean readAll;
	private Long ttl;
	private Scope scope;
	
	public CacheDirective() {
	}
	
	public CacheDirective(Boolean prefersMemory, Long ttl) {
		this.prefersMemory = prefersMemory;
		this.ttl = ttl;
	}

	public Boolean getPrefersMemory() {
		return prefersMemory;
	}
	
	public void setPrefersMemory(Boolean prefersMemory) {
		this.prefersMemory = prefersMemory;
	}
	
	/**
	 * Get the time to live in milliseconds
	 * @return
	 */
	public Long getTtl() {
		return ttl;
	}
	
	/**
	 * Set the time to live in milliseconds
	 * @param ttl
	 */
	public void setTtl(Long ttl) {
		this.ttl = ttl;
	}
	
	/**
	 * Get whether the result is updatable and therefore sensitive to data changes.
	 * @return
	 */
	public Boolean getUpdatable() {
		return updatable;
	}
	
	public void setUpdatable(Boolean updatable) {
		this.updatable = updatable;
	}
	
	public Scope getScope() {
		return this.scope;
	}

	public void setScope(Scope scope) {
		this.scope = scope;
	}
	
	/**
	 * Whether the engine should read and cache the entire results.
	 * @return
	 */
	public Boolean getReadAll() {
		return readAll;
	}
	
	public void setReadAll(Boolean readAll) {
		this.readAll = readAll;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (!(obj instanceof CacheDirective)) {
			return false;
		}
		CacheDirective other = (CacheDirective)obj;
		return EquivalenceUtil.areEqual(this.prefersMemory, other.prefersMemory)
		&& EquivalenceUtil.areEqual(this.readAll, other.readAll) 
		&& EquivalenceUtil.areEqual(this.ttl, other.ttl) 
		&& EquivalenceUtil.areEqual(this.updatable, other.updatable)
		&& EquivalenceUtil.areEqual(this.scope, other.scope);
	}
	
	@Override
	public int hashCode() {
		return HashCodeUtil.hashCode(1, scope, ttl, updatable);
	}

}
