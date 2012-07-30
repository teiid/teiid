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

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.cache.Cachable;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleBufferCache;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidException;
import org.teiid.core.util.Assertion;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.parser.ParseInfo;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.sql.lang.Command;


public class CachedResults implements Serializable, Cachable {
	private static final long serialVersionUID = -5603182134635082207L;
	
	private transient Command command;
	private transient TupleBuffer results;

	private String uuid;
	private boolean hasLobs;
	private int rowLimit;
	
	private AccessInfo accessInfo = new AccessInfo();
	
	public String getId() {
		return this.uuid;
	}
	
	public TupleBuffer getResults() {
		return results;
	}
	
	public void setResults(TupleBuffer results, ProcessorPlan plan) {
		this.results = results;
		this.uuid = results.getId();
		this.hasLobs = results.isLobs();
		if (plan != null) {
			this.accessInfo.populate(plan.getContext(), true);
		}
	}
	
	public void setCommand(Command command) {
		this.command = command;
	}
	
	public synchronized Command getCommand(String sql, QueryMetadataInterface metadata, ParseInfo info) throws QueryParserException, QueryResolverException, TeiidComponentException {
		if (command == null) {
			command = QueryParser.getQueryParser().parseCommand(sql, info);
		}
		QueryResolver.resolveCommand(command, metadata);
		return command;
	}

	@Override
	public boolean prepare(TupleBufferCache bufferManager) {
		Assertion.assertTrue(!this.results.isForwardOnly());
		bufferManager.distributeTupleBuffer(this.results.getId(), results);
		return true;
	}

	@Override
	public synchronized boolean restore(TupleBufferCache bufferManager) {
		if (this.results == null) {
			if (this.hasLobs) {
				return false; //the lob store is local only and not distributed
			}
			TupleBuffer buffer = bufferManager.getTupleBuffer(this.uuid);
			if (buffer != null) {
				this.results = buffer;
			}
			
			try {
				this.accessInfo.restore();
			} catch (TeiidException e) {
				LogManager.logWarning(LogConstants.CTX_DQP, e, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30025));
				return false;
			}
		}
		return (this.results != null);
	}	
	
	@Override
	public AccessInfo getAccessInfo() {
		return accessInfo;
	}
	
	public int getRowLimit() {
		return rowLimit;
	}
	
	public void setRowLimit(int rowLimit) {
		this.rowLimit = rowLimit;
	}
	
}
