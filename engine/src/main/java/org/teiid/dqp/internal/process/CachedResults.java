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

package org.teiid.dqp.internal.process;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.teiid.cache.Cachable;
import org.teiid.cache.Cache;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.util.Assertion;
import org.teiid.dqp.DQPPlugin;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.sql.lang.Command;


public class CachedResults implements Serializable, Cachable {
	private static final long serialVersionUID = -5603182134635082207L;
	
	private Command command;
	private AnalysisRecord analysisRecord;
	private transient TupleBuffer results;
	
	private List<?> schema;
	private int batchSize;
	
	protected ArrayList<UUID> cachedBatches = new ArrayList<UUID>();
	
	public AnalysisRecord getAnalysisRecord() {
		return analysisRecord;
	}
	
	public void setAnalysisRecord(AnalysisRecord analysisRecord) {
		this.analysisRecord = analysisRecord;
	}
	
	public TupleBuffer getResults() {
		return results;
	}
	
	public void setResults(TupleBuffer results) {
		this.results = results;
		this.schema = results.getSchema();
		this.batchSize = results.getBatchSize();
	}
	
	public void setCommand(Command command) {
		this.command = command;
	}
	
	public Command getCommand() {
		return command;
	}

	@Override
	public boolean prepare(Cache cache, BufferManager bufferManager) {
		Assertion.assertTrue(!this.results.isForwardOnly());
		try {
			for (int row = 1; row <= this.results.getRowCount(); row+=this.results.getBatchSize()) {
				TupleBatch batch = results.getBatch(row);
				UUID uuid = java.util.UUID.randomUUID();
				batch.preserveTypes();
				cache.put(uuid, batch);
				this.cachedBatches.add(uuid);
			}
			return true;
		} catch (TeiidComponentException e) {
			LogManager.logDetail(LogConstants.CTX_DQP, DQPPlugin.Util.getString("failed_to_put_in_cache")); //$NON-NLS-1$
		}
		return false;
	}

	@Override
	public synchronized boolean restore(Cache cache, BufferManager bufferManager) {
		try {
			if (this.results == null) {
				TupleBuffer buffer = bufferManager.createTupleBuffer(this.schema, "cached", TupleSourceType.FINAL); //$NON-NLS-1$
				buffer.setBatchSize(this.batchSize);
	
				for (UUID uuid : this.cachedBatches) {
					TupleBatch batch =  (TupleBatch)cache.get(uuid);
					if (batch != null) {					
						buffer.addTupleBatch(batch, true);
					}
				}
				this.results = buffer;				
			}
			return true;
		} catch (TeiidComponentException e) {
			LogManager.logDetail(LogConstants.CTX_DQP, DQPPlugin.Util.getString("not_found_cache")); //$NON-NLS-1$
		}
		return false;
	}	
}
