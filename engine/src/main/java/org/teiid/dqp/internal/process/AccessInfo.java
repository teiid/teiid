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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.api.exception.query.QueryResolverException;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.util.CommandContext;

/**
 * Tracks what views were used and what tables are accessed
 */
public class AccessInfo implements Serializable {
	
	private static final long serialVersionUID = -2608267960584191359L;
	
	private transient Set<Table> viewsAccessed;
	private transient Set<Object> tablesAccessed;
	
	private List<List<String>> externalTableNames;
	private List<List<String>> externalViewNames;
	
	private transient long creationTime = System.currentTimeMillis();
	
	private void writeObject(java.io.ObjectOutputStream out)  throws IOException {
		externalTableNames = initExternalList(externalTableNames, tablesAccessed);
		externalViewNames = initExternalList(externalViewNames, viewsAccessed);
		out.defaultWriteObject();
	}
	
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		this.creationTime = System.currentTimeMillis();
	}
	
	private List<List<String>> initExternalList(List<List<String>> externalNames, Set<? extends Object> accessed) {
		if (externalNames == null) {
			externalNames = new ArrayList<List<String>>(accessed.size());
			for (Object object : accessed) {
				if (object instanceof Table) {
					Table t = (Table)object;
					externalNames.add(Arrays.asList(t.getParent().getName(), t.getName()));
				} else if (object instanceof TempMetadataID) {
					TempMetadataID t = (TempMetadataID)object;
					externalNames.add(Arrays.asList(t.getID()));
				}
			}
		}
		return externalNames;
	}

	public Set<Table> getViewsAccessed() {
		return viewsAccessed;
	}
	
	public Set<Object> getTablesAccessed() {
		return tablesAccessed;
	}
	
	public long getCreationTime() {
		return creationTime;
	}
	
	void populate(ProcessorPlan plan, CommandContext context) {
		List<GroupSymbol> groups = new ArrayList<GroupSymbol>();
		plan.getAccessedGroups(groups);
		if (!groups.isEmpty()) {
			tablesAccessed = new HashSet<Object>();
			for (GroupSymbol groupSymbol : groups) {
				tablesAccessed.add(groupSymbol.getMetadataID());
			}	
		} else {
			tablesAccessed = Collections.emptySet();
		}
		if (!context.getViewsAccessed().isEmpty()) {
			this.viewsAccessed = new HashSet<Table>(context.getViewsAccessed());
		} else {
			this.viewsAccessed = Collections.emptySet();
		}
	}
	
	void restore() throws QueryResolverException, QueryValidatorException, TeiidComponentException {
		if (this.viewsAccessed != null) {
			return;
		}
		VDBMetaData vdb = DQPWorkContext.getWorkContext().getVDB();
		TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
		TempTableStore globalStore = vdb.getAttachment(TempTableStore.class);
		if (!externalViewNames.isEmpty()) {
			this.viewsAccessed = new HashSet<Table>();
			for (List<String> key : this.externalViewNames) {
				this.viewsAccessed.add(tm.getMetadataStore().getSchema(key.get(0).toUpperCase()).getTables().get(key.get(1).toUpperCase()));
			}
		} else {
			this.viewsAccessed = Collections.emptySet();
		}
		this.externalViewNames = null;
		if (!externalTableNames.isEmpty()) {
			for (List<String> key : this.externalTableNames) {
				if (key.size() == 1) {
					String matTableName = key.get(0);
					TempMetadataID id = globalStore.getMetadataStore().getTempGroupID(matTableName);
					if (id == null) {
						//if the id is null, then create a local instance
						String viewFullName = matTableName.substring(RelationalPlanner.MAT_PREFIX.length());
						id = globalStore.getGlobalTempTableMetadataId(tm.getGroupID(viewFullName), tm);
					}
					this.tablesAccessed.add(id);
				} else {
					this.tablesAccessed.add(tm.getMetadataStore().getSchema(key.get(0).toUpperCase()).getTables().get(key.get(1).toUpperCase()));
				}
			}
		} else {
			this.tablesAccessed = Collections.emptySet();
		}
		this.externalTableNames = null;
	}
	
	boolean validate(boolean data, long modTime) {
		if (this.tablesAccessed == null || modTime < 0) {
			return true;
		}
		if (!data) {
			for (Table t : getViewsAccessed()) {
				if (t.getLastModified() - modTime > this.creationTime) {
					return false;
				}
			}
		}
		for (Object o : getTablesAccessed()) {
			if (o instanceof Table) {
				Table t = (Table)o;
				if ((data?t.getLastDataModification():t.getLastModified()) - modTime > this.creationTime) {
					return false;
				}
			} else if (o instanceof TempMetadataID) {
				TempMetadataID tid = (TempMetadataID)o;
				if ((data?tid.getTableData().getLastDataModification():tid.getTableData().getLastModified()) - modTime > this.creationTime) {
					return false;
				}
			}
		}
		return true;
	}
	
}
