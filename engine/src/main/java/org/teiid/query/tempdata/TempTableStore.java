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

package org.teiid.query.tempdata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.language.SQLConstants;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.command.TempTableResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

public class TempTableStore {
	
	public enum MatState {
		NEEDS_LOADING,
		LOADING,
		FAILED_LOAD,
		LOADED
	}
	
	public static class MatTableInfo {
		private long updateTime = -1;
		private MatState state = MatState.NEEDS_LOADING;
		private long ttl = -1;
		private boolean valid;
		
		synchronized boolean shouldLoad() throws TeiidComponentException {
    		for (;;) {
			switch (state) {
			case NEEDS_LOADING:
			case FAILED_LOAD:
				setState(MatState.LOADING);
				return true;
			case LOADING:
				if (valid) {
					return false;
				}
				try {
					wait();
				} catch (InterruptedException e) {
					throw new TeiidComponentException(e);
				}
				continue;
			case LOADED:
				if (ttl >= 0 && System.currentTimeMillis() - updateTime - ttl > 0) {
					setState(MatState.LOADING);
					return true;
				}
				return false;
			}
    		}
		}
		
		public synchronized MatState setState(MatState state, Boolean valid, Long timestamp) {
			MatState oldState = this.state;
			LogManager.logDetail(LogConstants.CTX_MATVIEWS, this, "setting matState to", state, valid, timestamp, "old values", oldState, this.valid); //$NON-NLS-1$ //$NON-NLS-2$
			if (valid != null) {
				this.valid = valid;
			}
			setState(state);
			if (timestamp != null) {
				this.updateTime = timestamp;
			}
			notifyAll();
			return oldState;
		}
		
		private void setState(MatState state) {
			this.state = state;
			this.updateTime = System.currentTimeMillis();
		}
		
		public synchronized void setTtl(long ttl) {
			this.ttl = ttl;
		}
		
		public synchronized long getUpdateTime() {
			return updateTime;
		}
		
		public synchronized MatState getState() {
			return state;
		}
		
		public synchronized boolean isValid() {
			return valid;
		}
		
		public synchronized long getTtl() {
			return ttl;
		}
		
	}
	
	private ConcurrentHashMap<String, MatTableInfo> matTables = new ConcurrentHashMap<String, MatTableInfo>();
	
    private TempMetadataStore tempMetadataStore = new TempMetadataStore(new ConcurrentHashMap<String, TempMetadataID>());
    private Map<String, TempTable> groupToTupleSourceID = new ConcurrentHashMap<String, TempTable>();
    private String sessionID;
    private TempTableStore parentTempTableStore;
    
    public TempTableStore(String sessionID) {
        this.sessionID = sessionID;
    }
    
	public MatTableInfo getMatTableInfo(final String tableName) {
		MatTableInfo newInfo = new MatTableInfo();
		MatTableInfo info = matTables.putIfAbsent(tableName, newInfo);
		if (info == null) {
			info = newInfo;
		}
		return info;
	}
    
    public void setParentTempTableStore(TempTableStore parentTempTableStore) {
		this.parentTempTableStore = parentTempTableStore;
	}
    
    public boolean hasTempTable(String tempTableName) {
    	return groupToTupleSourceID.containsKey(tempTableName);
    }

    TempTable addTempTable(String tempTableName, Create create, BufferManager buffer, boolean add) {
    	List<ElementSymbol> columns = create.getColumnSymbols();
    	TempMetadataID id = tempMetadataStore.getTempGroupID(tempTableName);
    	if (id == null) {
	        //add metadata
	    	id = tempMetadataStore.addTempGroup(tempTableName, columns, false, true);
	        TempTableResolver.addAdditionalMetadata(create, id);
    	}
    	columns = new ArrayList<ElementSymbol>(create.getColumnSymbols());
        if (!create.getPrimaryKey().isEmpty()) {
    		//reorder the columns to put the key in front
    		List<ElementSymbol> primaryKey = create.getPrimaryKey();
    		columns.removeAll(primaryKey);
    		columns.addAll(0, primaryKey);
    	}
        TempTable tempTable = new TempTable(id, buffer, columns, create.getPrimaryKey().size(), sessionID);
        if (add) {
        	groupToTupleSourceID.put(tempTableName, tempTable);
        }
        return tempTable;
    }
    
    void swapTempTable(String tempTableName, TempTable tempTable) {
    	groupToTupleSourceID.put(tempTableName, tempTable);
    }

    public void removeTempTableByName(String tempTableName) {
        tempMetadataStore.removeTempGroup(tempTableName);
        TempTable table = this.groupToTupleSourceID.remove(tempTableName);
        if(table != null) {
            table.remove();
        }      
    }
    
    public TempMetadataStore getMetadataStore() {
        return tempMetadataStore;
    }
            
    public void removeTempTables() {
        for (String name : groupToTupleSourceID.keySet()) {
            removeTempTableByName(name);
        }
    }
    
    public void setUpdatable(String name, boolean updatable) {
    	TempTable table = groupToTupleSourceID.get(name);
    	if (table != null) {
    		table.setUpdatable(updatable);
    	}
    }
    
    TempTable getOrCreateTempTable(String tempTableID, Command command, BufferManager buffer, boolean delegate) throws QueryProcessingException{
    	TempTable tsID = groupToTupleSourceID.get(tempTableID);
        if(tsID != null) {
            return tsID;
        }
        if(delegate && this.parentTempTableStore != null){
    		tsID = this.parentTempTableStore.groupToTupleSourceID.get(tempTableID);
    	    if(tsID != null) {
    	        return tsID;
    	    }
        }
        //allow implicit temp group definition
        List<ElementSymbol> columns = null;
        if (command instanceof Insert) {
            Insert insert = (Insert)command;
            GroupSymbol group = insert.getGroup();
            if(group.isImplicitTempGroupSymbol()) {
                columns = insert.getVariables();
            }
        }
        if (columns == null) {
        	throw new QueryProcessingException(QueryPlugin.Util.getString("TempTableStore.table_doesnt_exist_error", tempTableID)); //$NON-NLS-1$
        }
        Create create = new Create();
        create.setTable(new GroupSymbol(tempTableID));
        create.setElementSymbolsAsColumns(columns);
        return addTempTable(tempTableID, create, buffer, true);       
    }
    
    public Set<String> getAllTempTables() {
        return new HashSet<String>(this.groupToTupleSourceID.keySet());
    }

	public TempMetadataID getGlobalTempTableMetadataId(Object viewId, QueryMetadataInterface metadata)
			throws QueryMetadataException, TeiidComponentException, QueryResolverException, QueryValidatorException {
		String matViewName = metadata.getFullName(viewId);
		String matTableName = RelationalPlanner.MAT_PREFIX+matViewName.toUpperCase();
		GroupSymbol group = new GroupSymbol(matViewName);
		group.setMetadataID(viewId);
		TempMetadataID id = tempMetadataStore.getTempGroupID(matTableName);
		//define the table preserving the key/index information and ensure that only a single instance exists
		if (id == null) {
			synchronized (viewId) {
				id = tempMetadataStore.getTempGroupID(matTableName);
				if (id == null) {
					id = tempMetadataStore.addTempGroup(matTableName, ResolverUtil.resolveElementsInGroup(group, metadata), false, true);
					id.setQueryNode(metadata.getVirtualPlan(viewId));
					id.setCardinality(metadata.getCardinality(viewId));
					id.setOriginalMetadataID(viewId);
					
					Object pk = metadata.getPrimaryKey(viewId);
					if (pk != null) {
						ArrayList<TempMetadataID> primaryKey = resolveIndex(metadata, id, pk);
						id.setPrimaryKey(primaryKey);
					}
					Collection keys = metadata.getUniqueKeysInGroup(viewId);
					for (Object key : keys) {
						id.addUniqueKey(resolveIndex(metadata, id, key));
					}
					Collection indexes = metadata.getIndexesInGroup(viewId);
					for (Object index : indexes) {
						id.addIndex(resolveIndex(metadata, id, index));
					}
				}
			}
		}
		updateCacheHint(viewId, metadata, group, id);
		return id;
	}

	private void updateCacheHint(Object viewId,
			QueryMetadataInterface metadata, GroupSymbol group,
			TempMetadataID id) throws TeiidComponentException,
			QueryMetadataException, QueryResolverException,
			QueryValidatorException {
		Command c = QueryResolver.resolveView(group, metadata.getVirtualPlan(viewId), SQLConstants.Reserved.SELECT, metadata).getCommand();
		CacheHint hint = c.getCacheHint();
		id.setCacheHint(hint);
	}
	
	static ArrayList<TempMetadataID> resolveIndex(
			QueryMetadataInterface metadata, TempMetadataID id, Object pk)
			throws TeiidComponentException, QueryMetadataException {
		List cols = metadata.getElementIDsInKey(pk);
		ArrayList<TempMetadataID> primaryKey = new ArrayList<TempMetadataID>(cols.size());
		for (Object coldId : cols) {
			int pos = metadata.getPosition(coldId) - 1;
			primaryKey.add(id.getElements().get(pos));
		}
		return primaryKey;
	}
    
}
