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
import org.teiid.dqp.service.BufferService;
import org.teiid.query.ObjectReplicator;
import org.teiid.services.BufferServiceImpl;

class BufferManagerService implements Service<BufferService>, BufferService {

	private BufferServiceImpl bufferService;
	public final InjectedValue<String> pathInjector = new InjectedValue<String>();
	public final InjectedValue<ObjectReplicator> replicatorInjector = new InjectedValue<ObjectReplicator>();
	private BufferManager manager;
	private TupleBufferCache tupleBufferCache;
	
	public BufferManagerService(BufferServiceImpl buffer) {
		this.bufferService = buffer;
	}
	
	@Override
	public void start(StartContext context) throws StartException {
		bufferService.setDiskDirectory(pathInjector.getValue());
		bufferService.start();
		manager = bufferService.getBufferManager();
		tupleBufferCache = manager;
		if (replicatorInjector.getValue() != null) {
			try {
				//use a mux name that will not conflict with any vdb
				tupleBufferCache = this.replicatorInjector.getValue().replicate("$BM$", TupleBufferCache.class, this.manager, 0); //$NON-NLS-1$
			} catch (Exception e) {
				throw new StartException(e);
			}
		}
	}

	@Override
	public void stop(StopContext context) {
		bufferService.stop();
		if (this.replicatorInjector.getValue() != null) {
			this.replicatorInjector.getValue().stop(bufferService);
		}
	}
	
	@Override
	public BufferManager getBufferManager() {
		return manager;
	}
	
	@Override
	public TupleBufferCache getTupleBufferCache() {
		return tupleBufferCache;
	}

	@Override
	public BufferService getValue() throws IllegalStateException,IllegalArgumentException {
		return this.bufferService;
	}

}
