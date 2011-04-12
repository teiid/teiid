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

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.cache.Cachable;
import org.teiid.cache.Cache;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBatch;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.symbol.ElementSymbol;


public class CachedResults implements Serializable, Cachable {
	private static final long serialVersionUID = -5603182134635082207L;
	
	private transient Command command;
	private transient TupleBuffer results;

	private AnalysisRecord analysisRecord;

	private String[] types;
	private CacheHint hint;
	private int batchSize;
	private String uuid;
	private int rowCount;
	private boolean hasLobs;
	
	private AccessInfo accessInfo = new AccessInfo();
	
	public String getId() {
		return this.uuid;
	}
	
	public AnalysisRecord getAnalysisRecord() {
		return analysisRecord;
	}
	
	public void setAnalysisRecord(AnalysisRecord analysisRecord) {
		this.analysisRecord = analysisRecord;
	}
	
	public TupleBuffer getResults() {
		return results;
	}
	
	public void setResults(TupleBuffer results, ProcessorPlan plan) {
		this.results = results;
		this.batchSize = results.getBatchSize();
		this.types = TupleBuffer.getTypeNames(results.getSchema());
		this.rowCount = results.getRowCount();
		this.uuid = results.getId();
		this.hasLobs = results.isLobs();
		this.accessInfo.populate(plan, plan.getContext());
	}
	
	public void setCommand(Command command) {
		this.command = command;
		this.hint = command.getCacheHint();
	}
	
	public void setHint(CacheHint hint) {
		this.hint = hint;
	}
	
	public CacheHint getHint() {
		return hint;
	}
	
	public synchronized Command getCommand(String sql, QueryMetadataInterface metadata, ParseInfo info) throws QueryParserException, QueryResolverException, TeiidComponentException {
		if (command == null) {
			command = QueryParser.getQueryParser().parseCommand(sql, info);
		}
		QueryResolver.resolveCommand(command, metadata);
		return command;
	}

	@Override
	public boolean prepare(Cache cache, BufferManager bufferManager) {
		Assertion.assertTrue(!this.results.isForwardOnly());
		bufferManager.addTupleBuffer(this.results);
		return true;
	}

	@Override
	public synchronized boolean restore(Cache cache, BufferManager bufferManager) {
		try {
			if (this.results == null) {
				if (this.hasLobs) {
					return false;
				}
				List<ElementSymbol> schema = new ArrayList<ElementSymbol>(types.length);
				for (String type : types) {
					ElementSymbol es = new ElementSymbol("x"); //$NON-NLS-1$
					es.setType(DataTypeManager.getDataTypeClass(type));
					schema.add(es);
				}
				TupleBuffer buffer = bufferManager.createTupleBuffer(schema, "cached", TupleSourceType.FINAL); //$NON-NLS-1$
				buffer.setBatchSize(this.batchSize);
				if (this.hint != null) {
					buffer.setPrefersMemory(this.hint.getPrefersMemory());
				}
				
				for (int row = 1; row <= this.rowCount; row+=this.batchSize) {
					TupleBatch batch = (TupleBatch)cache.get(uuid+","+row); //$NON-NLS-1$
					if (batch != null) {					
						buffer.addTupleBatch(batch, true);
					}					
				}
				this.results = buffer;	
				bufferManager.addTupleBuffer(this.results);
			}
			this.accessInfo.restore();
			return true;
		} catch (TeiidException e) {
			LogManager.logDetail(LogConstants.CTX_DQP, e, QueryPlugin.Util.getString("not_found_cache")); //$NON-NLS-1$
		}
		return false;
	}	
	
	@Override
	public AccessInfo getAccessInfo() {
		return accessInfo;
	}
	
}
