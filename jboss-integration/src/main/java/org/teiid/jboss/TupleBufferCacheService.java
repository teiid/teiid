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
package org.teiid.jboss;

import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.query.ObjectReplicator;

class TupleBufferCacheService implements Service<TupleBufferCache>{
	public final InjectedValue<ObjectReplicator> replicatorInjector = new InjectedValue<ObjectReplicator>();
	protected InjectedValue<BufferManager> bufferMgrInjector = new InjectedValue<BufferManager>();
	
	private TupleBufferCache tupleBufferCache;
	
	@Override
	public void start(StartContext context) throws StartException {
		if (this.replicatorInjector.getValue() != null) {
			try {
				//use a mux name that will not conflict with any vdb
				this.tupleBufferCache = this.replicatorInjector.getValue().replicate("$TEIID_BM$", TupleBufferCache.class, bufferMgrInjector.getValue(), 0); //$NON-NLS-1$
			} catch (Exception e) {
				throw new StartException(e);
			}
		}
	}

	@Override
	public void stop(StopContext context) {
		if (this.replicatorInjector.getValue() != null && this.tupleBufferCache != null) {
			this.replicatorInjector.getValue().stop(this.tupleBufferCache);
		}
	}

	@Override
	public TupleBufferCache getValue() throws IllegalStateException, IllegalArgumentException {
		if (this.tupleBufferCache!= null) {
			return tupleBufferCache;
		}
		return bufferMgrInjector.getValue();
	}
}
