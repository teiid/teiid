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

package org.teiid.common.buffer;

public class BaseCacheEntry implements Comparable<BaseCacheEntry> {

	private Long id;
	protected float lastAccess;
	protected float orderingValue;
	
	public BaseCacheEntry(Long id) {
		this.id = id;
	}
	
	public Long getId() {
		return id;
	}

	@Override
	public int hashCode() {
		return getId().hashCode();
	}
	
	@Override
	public String toString() {
		return getId().toString();
	}

	public float getLastAccess() {
		return lastAccess;
	}
	
	public void setLastAccess(float lastAccess) {
		this.lastAccess = lastAccess;
	}
	
	public float getOrderingValue() {
		return orderingValue;
	}
	
	public void setOrderingValue(float orderingValue) {
		this.orderingValue = orderingValue;
	}
	
	@Override
	public int compareTo(BaseCacheEntry o) {
		int result = (int) Math.signum(orderingValue - o.orderingValue);
		if (result == 0) {
			result = (int)Math.signum(lastAccess - o.lastAccess);
			if (result == 0) {
				return Long.signum(id - o.id);
			}
		}
		return result;
	}

}