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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.api.exception.query.QueryProcessingException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.execution.QueryExecPlugin;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.resolver.command.TempTableResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

public class TempTableStore {
	
	public enum MatState {
		NOT_LOADED,
		LOADING,
		FAILED_LOAD,
		LOADED
	}
	
	public static class MatTableInfo {
		private long updateTime = -1;
		private MatState state = MatState.NOT_LOADED;
		private long ttl = -1;
		private boolean valid;
		
		synchronized boolean shouldLoad() throws TeiidComponentException {
    		for (;;) {
			switch (state) {
			case NOT_LOADED:
				updateTime = System.currentTimeMillis();
			case FAILED_LOAD:
				state = MatState.LOADING;
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
					state = MatState.LOADING;
					return true;
				}
				return false;
			}
    		}
		}
		
		public synchronized MatState setState(MatState state) {
			MatState oldState = this.state;
			if (state == MatState.LOADED) {
				valid = true;
			}
			this.state = state;
			this.updateTime = System.currentTimeMillis();
			notifyAll();
			return oldState;
		}
		
		public synchronized void setTtl(long ttl) {
			this.ttl = ttl;
		}
		
		public long getUpdateTime() {
			return updateTime;
		}
		
		public MatState getState() {
			return state;
		}
		
		public boolean isValid() {
			return valid;
		}
		
		public void setValid(boolean valid) {
			this.valid = valid;
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

    TempTable addTempTable(String tempTableName, Create create, BufferManager buffer) {
    	List<ElementSymbol> columns = create.getColumns();
    	TempMetadataID existingId = tempMetadataStore.getTempGroupID(tempTableName);
        //add metadata
    	TempMetadataID id = tempMetadataStore.addTempGroup(tempTableName, columns, false, true);
    	if (existingId != null) {
    		id.setCacheHint(existingId.getCacheHint());
    	}
        TempTableResolver.addPrimaryKey(create, id);
    	columns = new ArrayList<ElementSymbol>(create.getColumns());
        if (!create.getPrimaryKey().isEmpty()) {
    		//reorder the columns to put the key in front
    		List<ElementSymbol> primaryKey = create.getPrimaryKey();
    		columns.removeAll(primaryKey);
    		columns.addAll(0, primaryKey);
    	}
        TempTable tempTable = new TempTable(id, buffer, columns, create.getPrimaryKey().size(), sessionID);
        groupToTupleSourceID.put(tempTableName, tempTable);
        return tempTable;
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
        	throw new QueryProcessingException(QueryExecPlugin.Util.getString("TempTableStore.table_doesnt_exist_error", tempTableID)); //$NON-NLS-1$
        }
        Create create = new Create();
        create.setTable(new GroupSymbol(tempTableID));
        create.setColumns(columns);
        return addTempTable(tempTableID, create, buffer);       
    }
    
    public Set<String> getAllTempTables() {
        return new HashSet<String>(this.groupToTupleSourceID.keySet());
    }
    
}
