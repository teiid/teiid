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
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Schema;
import org.teiid.metadata.AbstractMetadataRecord.DataModifiable;
import org.teiid.metadata.AbstractMetadataRecord.Modifiable;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.relational.RelationalPlanner;
import org.teiid.query.tempdata.TempTableStore;
import org.teiid.query.util.CommandContext;

/**
 * Tracks what views were used and what tables are accessed
 */
public class AccessInfo implements Serializable {
	
	private static final long serialVersionUID = -2608267960584191359L;
	
	private transient Set<Object> objectsAccessed;
	
	private List<List<String>> externalNames;
	
	private transient long creationTime = System.currentTimeMillis();
	
	private void writeObject(java.io.ObjectOutputStream out)  throws IOException {
		externalNames = initExternalList(externalNames, objectsAccessed);
		out.defaultWriteObject();
	}
	
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		this.creationTime = System.currentTimeMillis();
	}
	
	private static List<List<String>> initExternalList(List<List<String>> externalNames, Set<? extends Object> accessed) {
		if (externalNames == null) {
			externalNames = new ArrayList<List<String>>(accessed.size());
			for (Object object : accessed) {
				if (object instanceof AbstractMetadataRecord) {
					AbstractMetadataRecord t = (AbstractMetadataRecord)object;
					externalNames.add(Arrays.asList(t.getParent().getName(), t.getName()));
				} else if (object instanceof TempMetadataID) {
					TempMetadataID t = (TempMetadataID)object;
					externalNames.add(Arrays.asList(t.getID()));
				}
			}
		}
		return externalNames;
	}
	
	public Set<Object> getObjectsAccessed() {
		return objectsAccessed;
	}
	
	public long getCreationTime() {
		return creationTime;
	}
	
	void populate(CommandContext context, boolean data) {
		Set<Object> objects = null;
		if (data) {
			objects = context.getDataObjects();
		} else {
			objects = context.getPlanningObjects();
		}
		if (objects == null || objects.isEmpty()) {
			this.objectsAccessed = Collections.emptySet(); 
		} else {
			this.objectsAccessed = objects;
		}
	}
	
	/**
	 * Restore reconnects to the live metadata objects
	 * @throws QueryResolverException
	 * @throws QueryValidatorException
	 * @throws TeiidComponentException
	 */
	void restore() throws QueryResolverException, QueryValidatorException, TeiidComponentException {
		if (this.objectsAccessed != null) {
			return;
		}
		VDBMetaData vdb = DQPWorkContext.getWorkContext().getVDB();
		TransformationMetadata tm = vdb.getAttachment(TransformationMetadata.class);
		TempTableStore globalStore = vdb.getAttachment(TempTableStore.class);
		if (!externalNames.isEmpty()) {
			this.objectsAccessed = new HashSet<Object>(externalNames.size());
			for (List<String> key : this.externalNames) {
				if (key.size() == 1) {
					String matTableName = key.get(0);
					TempMetadataID id = globalStore.getMetadataStore().getTempGroupID(matTableName);
					if (id == null) {
						//if the id is null, then create a local instance
						String viewFullName = matTableName.substring(RelationalPlanner.MAT_PREFIX.length());
						id = globalStore.getGlobalTempTableMetadataId(tm.getGroupID(viewFullName), tm);
					}
					this.objectsAccessed.add(id);
				} else {
					Schema s = tm.getMetadataStore().getSchema(key.get(0).toUpperCase());
					Modifiable m = s.getTables().get(key.get(1).toUpperCase());
					if (m == null) {
						m = s.getProcedures().get(key.get(1).toUpperCase());
					}
					if (m != null) {
						this.objectsAccessed.add(m);
					}
				}
			}
		} else {
			this.objectsAccessed = Collections.emptySet();
		}
		this.externalNames = null;
	}
	
	boolean validate(boolean data, long modTime) {
		if (this.objectsAccessed == null || modTime < 0) {
			return true;
		}
		for (Object o : this.objectsAccessed) {
			if (!data) {
				if (o instanceof Modifiable && ((Modifiable)o).getLastModified() - modTime > this.creationTime) {
					return false;
				}
			} else if (o instanceof DataModifiable && ((DataModifiable)o).getLastDataModification() - modTime > this.creationTime) {
				return false;
			}
		}
		return true;
	}
	
}
