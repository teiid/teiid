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

import java.lang.ref.WeakReference;

public class CacheEntry extends BaseCacheEntry {
	private boolean persistent;
	private Object object;
	private final int sizeEstimate;
	private WeakReference<? extends Serializer<?>> serializer;
	
	public CacheEntry(Long oid) {
		this(new CacheKey(oid, 0, 0), 0, null, null, false);
	}
	
	public CacheEntry(CacheKey key, int sizeEstimate, Object object, WeakReference<? extends Serializer<?>> serializer, boolean persistent) {
		super(key);
		this.sizeEstimate = sizeEstimate;
		this.object = object;
		this.serializer = serializer;
		this.persistent = persistent;
	}
	
	public int getSizeEstimate() {
		return sizeEstimate;
	}
	
	public Object nullOut() {
		Object result = getObject();
		this.object = null;
		this.serializer = null;
		return result;
	}

	public Object getObject() {
		return object;
	}

	public void setPersistent(boolean persistent) {
		this.persistent = persistent;
	}

	public boolean isPersistent() {
		return persistent;
	}

	public void setSerializer(WeakReference<? extends Serializer<?>> serializer) {
		this.serializer = serializer;
	}

	public Serializer<?> getSerializer() {
		WeakReference<? extends Serializer<?>> ref = this.serializer;
		if (ref == null) {
			return null;
		}
		return ref.get();
	}

}