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
package org.teiid.cache.jboss;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jboss.cache.Fqn;
import org.jboss.cache.config.CacheLoaderConfig.IndividualCacheLoaderConfig;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidRuntimeException;

public class TupleBatchCacheLoader extends ClusteredTupleBatchCacheLoader {

	private BufferManager bufferMgr;
	private IndividualCacheLoaderConfig config;
	
	@Override
	public boolean exists(Fqn fqn) throws Exception {
		String id = fqn.getLastElementAsString();
		int index = id.indexOf(',');
		if (index != -1) {
			return true;
		}
		return false;
	}

	@Override
	public Map<Object, Object> get(Fqn fqn) throws Exception {
		String id = fqn.getLastElementAsString();
		int index = id.indexOf(',');
		if (index != -1) {
			String uuid = id.substring(0, index);
			int row = Integer.parseInt(id.substring(index+1));
			TupleBuffer tb = this.bufferMgr.getTupleBuffer(uuid);
			if (tb != null) {
				Map map = new HashMap();
				TupleBatch b = tb.getBatch(row);
				b.preserveTypes();
				map.put(id, b);
				return map;
			}
		}
		return super.get(fqn);
	}

	@Override
	public Set<?> getChildrenNames(Fqn fqn) throws Exception {
		return super.getChildrenNames(fqn);
	}

	@Override
	public IndividualCacheLoaderConfig getConfig() {
		return this.config;
	}

	@Override
	public Object put(Fqn fqn, Object key, Object value) throws Exception {
		return super.put(fqn, key, value);
	}

	@Override
	public void remove(Fqn fqn) throws Exception {
		super.remove(fqn);
	}

	@Override
	public Object remove(Fqn fqn, Object key) throws Exception {
		return super.remove(fqn, key);
	}

	@Override
	public void removeData(Fqn fqn) throws Exception {
		super.removeData(fqn);
	}

	@Override
	public void setConfig(IndividualCacheLoaderConfig config) {
		if (!(config instanceof TupleBatchCacheLoaderConfig)) {
			throw new TeiidRuntimeException("Wrong Configuration"); //$NON-NLS-1$
		}
		this.config = config;
		TupleBatchCacheLoaderConfig bmc = (TupleBatchCacheLoaderConfig)config;
		this.bufferMgr = bmc.getBufferService().getBufferManager();
		super.setConfig(config);
	}

}
