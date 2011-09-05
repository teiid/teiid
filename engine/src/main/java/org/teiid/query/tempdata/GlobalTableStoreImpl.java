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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.SQLConstants;
import org.teiid.language.SQLConstants.Reserved;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.ReplicatedObject;
import org.teiid.query.mapping.relational.QueryNode;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.resolver.QueryResolver;
import org.teiid.query.resolver.util.ResolverUtil;
import org.teiid.query.sql.lang.CacheHint;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Create;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.tempdata.TempTableStore.TransactionMode;

public class GlobalTableStoreImpl implements GlobalTableStore, ReplicatedObject {
	
	public enum MatState {
		NEEDS_LOADING,
		LOADING,
		FAILED_LOAD,
		LOADED
	}
	
	public class MatTableInfo {
		private long updateTime = -1;
		private MatState state = MatState.NEEDS_LOADING;
		private Serializable loadingAddress;
		private long ttl = -1;
		private boolean valid;
		
		protected MatTableInfo() {}
		
		private synchronized boolean shouldLoad(Serializable possibleLoadingAddress, boolean firstPass, boolean refresh, boolean invalidate) {
			if (invalidate) {
				LogManager.logDetail(LogConstants.CTX_MATVIEWS, this, "invalidating"); //$NON-NLS-1$
				valid = false;
			}
			switch (state) {
			case NEEDS_LOADING:
			case FAILED_LOAD:
				if (!firstPass) {
					this.loadingAddress = possibleLoadingAddress;
					setState(MatState.LOADING, null);
				}
				return true;
			case LOADING:
				if (!firstPass && localAddress instanceof Comparable<?> && ((Comparable)localAddress).compareTo(possibleLoadingAddress) < 0) {
					this.loadingAddress = possibleLoadingAddress; //ties go to the lowest address
					return true;
				}
				return false;
			case LOADED:
				if (!firstPass
						|| refresh 
						|| ttl >= 0 && System.currentTimeMillis() - updateTime - ttl > 0) {
					if (firstPass) {
						setState(MatState.NEEDS_LOADING, null);
					} else {
						this.loadingAddress = possibleLoadingAddress;
						setState(MatState.LOADING, null);
					}
					return true;
				}
				return false;
    		}
			throw new AssertionError();
		}
		
		private synchronized void setState(MatState state, Boolean valid) {
			MatState oldState = this.state;
			long timestamp = System.currentTimeMillis();
			LogManager.logDetail(LogConstants.CTX_MATVIEWS, this, "setting matState to", state, valid, timestamp, "old values", oldState, this.valid); //$NON-NLS-1$ //$NON-NLS-2$
			if (valid != null) {
				this.valid = valid;
			}
			this.state = state;
			this.updateTime = System.currentTimeMillis();
			notifyAll();
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
		
		public synchronized boolean isUpToDate() {
			return isValid() && (ttl < 0 || System.currentTimeMillis() - updateTime - ttl <= 0);
		}
		
		public synchronized boolean isValid() {
			return valid;
		}
		
		public synchronized long getTtl() {
			return ttl;
		}
		
	}
	
	private ConcurrentHashMap<String, MatTableInfo> matTables = new ConcurrentHashMap<String, MatTableInfo>();
	private TempTableStore tableStore = new TempTableStore("SYSTEM", TransactionMode.ISOLATE_READS); //$NON-NLS-1$
	private BufferManager bufferManager;
	private QueryMetadataInterface metadata;
	private Serializable localAddress;
	
	public GlobalTableStoreImpl(BufferManager bufferManager, QueryMetadataInterface metadata) {
		this.bufferManager = bufferManager;
		this.metadata = new TempMetadataAdapter(metadata, new TempMetadataStore());
	}

	public synchronized MatTableInfo getMatTableInfo(final String tableName) {
		MatTableInfo info = matTables.get(tableName);
		if (info == null) {
			info = new MatTableInfo();
			matTables.put(tableName, info);
		}
		return info;
	}
	
	@Override
	public void failedLoad(String matTableName) {
		MatTableInfo info = getMatTableInfo(matTableName);
		synchronized (info) {
			if (info.state != MatState.LOADED) {
				info.setState(MatState.FAILED_LOAD, null);
			}
		}
	}
	
	@Override
	public boolean needsLoading(String matTableName, Serializable loadingAddress, boolean firstPass, boolean refresh, boolean invalidate) {
		MatTableInfo info = getMatTableInfo(matTableName);
		return info.shouldLoad(loadingAddress, firstPass, refresh, invalidate);
	}
			
	@Override
	public TempMetadataID getGlobalTempTableMetadataId(Object viewId)
	throws QueryMetadataException, TeiidComponentException, QueryResolverException, QueryValidatorException {
		String matViewName = metadata.getFullName(viewId);
		String matTableName = RelationalPlanner.MAT_PREFIX+matViewName.toUpperCase();
		GroupSymbol group = new GroupSymbol(matViewName);
		group.setMetadataID(viewId);
		TempMetadataID id = tableStore.getMetadataStore().getTempGroupID(matTableName);
		//define the table preserving the key/index information and ensure that only a single instance exists
		if (id == null) {
			synchronized (viewId) {
				id = tableStore.getMetadataStore().getTempGroupID(matTableName);
				if (id == null) {
					id = tableStore.getMetadataStore().addTempGroup(matTableName, ResolverUtil.resolveElementsInGroup(group, metadata), false, true);
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
		updateCacheHint(viewId, group, id);
		return id;
	}
	
	@Override
	public TempMetadataID getCodeTableMetadataId(
			String codeTableName, String returnElementName,
			String keyElementName, String matTableName) throws TeiidComponentException,
			QueryMetadataException {
		ElementSymbol keyElement = new ElementSymbol(matTableName + ElementSymbol.SEPARATOR + keyElementName);
    	ElementSymbol returnElement = new ElementSymbol(matTableName + ElementSymbol.SEPARATOR + returnElementName);
		keyElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementType(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + keyElementName))));
    	returnElement.setType(DataTypeManager.getDataTypeClass(metadata.getElementType(metadata.getElementID(codeTableName + ElementSymbol.SEPARATOR + returnElementName))));
    	TempMetadataID id = this.getTempTableStore().getMetadataStore().getTempGroupID(matTableName);
    	if (id == null) {
    		synchronized (this) {
    	    	id = this.getTempTableStore().getMetadataStore().addTempGroup(matTableName, Arrays.asList(keyElement, returnElement), false, true);
    	    	String queryString = Reserved.SELECT + ' ' + keyElementName + " ," + returnElementName + ' ' + Reserved.FROM + ' ' + codeTableName; //$NON-NLS-1$ 
    	    	id.setQueryNode(new QueryNode(queryString));
    	    	id.setPrimaryKey(id.getElements().subList(0, 1));
    	    	CacheHint hint = new CacheHint(true, null);
    	    	id.setCacheHint(hint);
			}
    	}
		return id;
	}
		
	private void updateCacheHint(Object viewId, GroupSymbol group,
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

	@Override
	public void loaded(String matTableName, TempTable table) {
		swapTempTable(matTableName, table);
		this.getMatTableInfo(matTableName).setState(MatState.LOADED, true);
	}
	
	private void swapTempTable(String tempTableName, TempTable tempTable) {
    	this.tableStore.getTempTables().put(tempTableName, tempTable);
    }

	@Override
	public List<?> updateMatViewRow(String matTableName, List<?> tuple,
			boolean delete) throws TeiidComponentException {
		TempTable tempTable = tableStore.getTempTable(matTableName);
		if (tempTable != null) {
			TempMetadataID id = tableStore.getMetadataStore().getTempGroupID(matTableName);
			synchronized (id) {
				boolean clone = tempTable.getActiveReaders().get() != 0;
				if (clone) {
					tempTable = tempTable.clone();
				}
				List<?> result = tempTable.updateTuple(tuple, delete);
				if (clone) {
					swapTempTable(matTableName, tempTable);
				}
				return result;
			}
		}
		return null;
	}

	@Override
	public TempTableStore getTempTableStore() {
		return this.tableStore;
	}
	
	@Override
	public TempTable createMatTable(final String tableName, GroupSymbol group) throws TeiidComponentException,
	QueryMetadataException, TeiidProcessingException {
		Create create = new Create();
		create.setTable(group);
		List<ElementSymbol> allColumns = ResolverUtil.resolveElementsInGroup(group, metadata);
		create.setElementSymbolsAsColumns(allColumns);
		Object pk = metadata.getPrimaryKey(group.getMetadataID());
		if (pk != null) {
			List<ElementSymbol> pkColumns = resolveIndex(metadata, allColumns, pk);
			create.getPrimaryKey().addAll(pkColumns);
		}
		TempTable table = getTempTableStore().addTempTable(tableName, create, bufferManager, false, null);
		table.setUpdatable(false);
		CacheHint hint = table.getCacheHint();
		if (hint != null) {
			table.setPreferMemory(hint.getPrefersMemory());
			if (hint.getTtl() != null) {
				getMatTableInfo(tableName).setTtl(hint.getTtl());
			}
			if (pk != null) {
				table.setUpdatable(hint.isUpdatable());
			}
		}
		return table;
	}
	
	/**
	 * Return a list of ElementSymbols for the given index/key object
	 */
	public static List<ElementSymbol> resolveIndex(QueryMetadataInterface metadata, List<ElementSymbol> allColumns, Object pk)
			throws TeiidComponentException, QueryMetadataException {
		Collection<?> pkIds = metadata.getElementIDsInKey(pk);
		List<ElementSymbol> pkColumns = new ArrayList<ElementSymbol>(pkIds.size());
		for (Object col : pkIds) {
			pkColumns.add(allColumns.get(metadata.getPosition(col)-1));
		}
		return pkColumns;
	}

	//begin replication methods
	
	@Override
	public void setLocalAddress(Serializable address) {
		this.localAddress = address;
	}
	
	@Override
	public Serializable getLocalAddress() {
		return localAddress;
	}
	
	@Override
	public void getState(OutputStream ostream) {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(ostream);
			for (Map.Entry<String, TempTable> entry : tableStore.getTempTables().entrySet()) {
				sendTable(entry.getKey(), oos, true);
			}
			oos.writeObject(null);
			oos.close();
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		} catch (TeiidComponentException e) {
			throw new TeiidRuntimeException(e);
		}
	}

	@Override
	public void setState(InputStream istream) {
		try {
			ObjectInputStream ois = new ObjectInputStream(istream);
			while (true) {
				String tableName = (String)ois.readObject();
				if (tableName == null) {
					break;
				}
				loadTable(tableName, ois);
			}
			ois.close();
		} catch (Exception e) {
			throw new TeiidRuntimeException(e);
		}
	}

	@Override
	public void getState(String stateId, OutputStream ostream) {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(ostream);
			sendTable(stateId, oos, false);
			oos.close();
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		} catch (TeiidComponentException e) {
			throw new TeiidRuntimeException(e);
		}
	}

	private void sendTable(String stateId, ObjectOutputStream oos, boolean writeName)
			throws IOException, TeiidComponentException {
		TempTable tempTable = this.tableStore.getTempTable(stateId);
		if (tempTable == null) {
			return;
		}
		MatTableInfo info = getMatTableInfo(stateId);
		if (!info.isValid()) {
			return;
		}
		if (writeName) {
			oos.writeObject(stateId);
		}
		oos.writeLong(info.updateTime);
		oos.writeObject(info.loadingAddress);
		oos.writeObject(info.state);
		tempTable.writeTo(oos);
	}

	@Override
	public void setState(String stateId, InputStream istream) {
		try {
			ObjectInputStream ois = new ObjectInputStream(istream);
			loadTable(stateId, ois);
			ois.close();
		} catch (Exception e) {
			MatTableInfo info = this.getMatTableInfo(stateId);
			info.setState(MatState.FAILED_LOAD, null);	
			throw new TeiidRuntimeException(e);
		}
	}

	private void loadTable(String stateId, ObjectInputStream ois)
			throws TeiidComponentException, QueryMetadataException,
			IOException,
			ClassNotFoundException, TeiidProcessingException {
		LogManager.logDetail(LogConstants.CTX_DQP, "loading table from remote stream", stateId); //$NON-NLS-1$
		long updateTime = ois.readLong();
		Serializable loadingAddress = (Serializable) ois.readObject();
		MatState state = (MatState)ois.readObject();
		GroupSymbol group = new GroupSymbol(stateId);
		if (stateId.startsWith(RelationalPlanner.MAT_PREFIX)) {
			String viewName = stateId.substring(RelationalPlanner.MAT_PREFIX.length());
			Object viewId = this.metadata.getGroupID(viewName);
			group.setMetadataID(getGlobalTempTableMetadataId(viewId));
		} else {
			String viewName = stateId.substring(TempTableDataManager.CODE_PREFIX.length());
			int index = viewName.lastIndexOf('.');
			String returnElementName = viewName.substring(index + 1);
			viewName = viewName.substring(0, index);
			index = viewName.lastIndexOf('.');
			String keyElementName = viewName.substring(index + 1);
			viewName = viewName.substring(0, index);
			group.setMetadataID(getCodeTableMetadataId(viewName, returnElementName, keyElementName, stateId));
		}
		TempTable tempTable = this.createMatTable(stateId, group);
		tempTable.readFrom(ois);
		MatTableInfo info = this.getMatTableInfo(stateId);
		synchronized (info) {
			swapTempTable(stateId, tempTable);
			info.setState(state, true);
			info.updateTime = updateTime;
			info.loadingAddress = loadingAddress;
		}
	}

	@Override
	public void droppedMembers(Collection<Serializable> addresses) {
		for (MatTableInfo info : this.matTables.values()) {
			synchronized (info) {
				if (info.getState() == MatState.LOADING 
						&& addresses.contains(info.loadingAddress)) {
					info.setState(MatState.FAILED_LOAD, null);
				}
			}
		}
	}

}
