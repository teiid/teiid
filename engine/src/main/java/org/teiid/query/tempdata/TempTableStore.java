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
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.resolver.command.TempTableResolver;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.lang.Insert;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;

public class TempTableStore {
	
    TempMetadataStore tempMetadataStore = new TempMetadataStore(new ConcurrentHashMap<String, TempMetadataID>());
    private Map<String, TempTable> tempTables = new ConcurrentHashMap<String, TempTable>();
    private String sessionID;
    private TempTableStore parentTempTableStore;
    
    public TempTableStore(String sessionID) {
        this.sessionID = sessionID;
    }
    
    public void setParentTempTableStore(TempTableStore parentTempTableStore) {
		this.parentTempTableStore = parentTempTableStore;
	}
    
    public boolean hasTempTable(String tempTableName) {
    	return tempTables.containsKey(tempTableName);
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
        	tempTables.put(tempTableName, tempTable);
        }
        return tempTable;
    }
    
    void swapTempTable(String tempTableName, TempTable tempTable) {
    	tempTables.put(tempTableName, tempTable);
    }

    public void removeTempTableByName(String tempTableName) {
        tempMetadataStore.removeTempGroup(tempTableName);
        TempTable table = this.tempTables.remove(tempTableName);
        if(table != null) {
            table.remove();
        }      
    }
    
    public TempMetadataStore getMetadataStore() {
        return tempMetadataStore;
    }
            
    public void removeTempTables() {
        for (String name : tempTables.keySet()) {
            removeTempTableByName(name);
        }
    }
    
    public void setUpdatable(String name, boolean updatable) {
    	TempTable table = tempTables.get(name);
    	if (table != null) {
    		table.setUpdatable(updatable);
    	}
    }
    
    TempTable getTempTable(String tempTableID) {
        return this.tempTables.get(tempTableID);
    }
    
    TempTable getOrCreateTempTable(String tempTableID, Command command, BufferManager buffer, boolean delegate) throws QueryProcessingException{
    	TempTable tempTable = getTempTable(tempTableID, command, buffer, delegate);
    	if (tempTable != null) {
    		return tempTable;
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
        LogManager.logDetail(LogConstants.CTX_DQP, "Creating temporary table", tempTableID); //$NON-NLS-1$
        Create create = new Create();
        create.setTable(new GroupSymbol(tempTableID));
        create.setElementSymbolsAsColumns(columns);
        return addTempTable(tempTableID, create, buffer, true);       
    }

	private TempTable getTempTable(String tempTableID, Command command,
			BufferManager buffer, boolean delegate)
			throws QueryProcessingException {
		TempTable tsID = tempTables.get(tempTableID);
        if(tsID != null) {
            return tsID;
        }
        if(delegate && this.parentTempTableStore != null){
    		return this.parentTempTableStore.getTempTable(tempTableID, command, buffer, delegate);
        }
        return null;
	}
    
    public Set<String> getAllTempTables() {
        return new HashSet<String>(this.tempTables.keySet());
    }
    
    Map<String, TempTable> getTempTables() {
		return tempTables;
	}
    
}
