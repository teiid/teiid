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
package org.teiid.deployers;

import java.util.List;

import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.runtime.RuntimePlugin;

public abstract class RuntimeVDB {
	private VDBMetaData vdb;
	private VDBModificationListener listener;
	private boolean restartInProgress = false;
	
	public static class ReplaceResult {
		public boolean isNew;
		public String removedDs;
	}
	
	public interface VDBModificationListener {
		void dataRoleChanged(String policyName) throws AdminProcessingException;
		void connectionTypeChanged() throws AdminProcessingException;
		void dataSourceChanged(String modelName, String sourceName, String translatorName, String dsName) throws AdminProcessingException;
		void onRestart(List<String> modelNames) throws AdminProcessingException;
	}
	
	public RuntimeVDB(VDBMetaData vdb, VDBModificationListener listener) {
		this.vdb = vdb;
		this.listener = listener;
	}
	
	public void addDataRole(String policyName, String mappedRole) throws AdminProcessingException {
		synchronized (this.vdb) {
			DataPolicyMetadata policy = getPolicy(policyName);		
			List<String> previous = policy.getMappedRoleNames();
			policy.addMappedRoleName(mappedRole);
			try {
				this.listener.dataRoleChanged(policyName);
			} catch(AdminProcessingException e) {
				policy.setMappedRoleNames(previous);
				throw e;
			}
		}
	}
	
	public void remoteDataRole(String policyName, String mappedRole) throws AdminProcessingException{
		synchronized (this.vdb) {
			DataPolicyMetadata policy = getPolicy(policyName);
			List<String> previous = policy.getMappedRoleNames();
			policy.removeMappedRoleName(mappedRole);
			try {
				this.listener.dataRoleChanged(policyName);
			} catch(AdminProcessingException e) {
				policy.setMappedRoleNames(previous);
				throw e;
			}
		}
	}	
	
	public void addAnyAuthenticated(String policyName) throws AdminProcessingException{
		synchronized (this.vdb) {
			DataPolicyMetadata policy = getPolicy(policyName);		
			boolean previous = policy.isAnyAuthenticated();
			policy.setAnyAuthenticated(true);
			try {
				this.listener.dataRoleChanged(policyName);
			} catch(AdminProcessingException e) {
				policy.setAnyAuthenticated(previous);
				throw e;
			}
		}
	}	
	
	public void removeAnyAuthenticated(String policyName) throws AdminProcessingException{
		synchronized (this.vdb) {
			DataPolicyMetadata policy = getPolicy(policyName);
			boolean previous = policy.isAnyAuthenticated();
			policy.setAnyAuthenticated(false);
			try {
				this.listener.dataRoleChanged(policyName);
			} catch(AdminProcessingException e) {
				policy.setAnyAuthenticated(previous);
				throw e;
			}
		}
	}		
	
	public void changeConnectionType(ConnectionType type) throws AdminProcessingException {
		synchronized (this.vdb) {
			ConnectionType previous = this.vdb.getConnectionType();
			this.vdb.setConnectionType(type);
			try {
				this.listener.connectionTypeChanged();
			} catch(AdminProcessingException e) {
				this.vdb.setConnectionType(previous);
				throw e;
			}
		}
	}
	
	public ReplaceResult assignDatasource(String modelName, String sourceName, String translatorName, String dsName) throws AdminProcessingException{
		synchronized (this.vdb) {
			ModelMetaData model = this.vdb.getModel(modelName);
			
			if (model == null) {
				 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40090, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40090, modelName, this.vdb.getName(), this.vdb.getVersion()));
			}
			
			SourceMappingMetadata source = model.getSourceMapping(sourceName);
			if(source == null) {
				 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40091, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40091, sourceName, modelName, this.vdb.getName(), this.vdb.getVersion()));
			}
			
			String previousTranslatorName = source.getTranslatorName();
			String previousDsName = source.getConnectionJndiName();
			
			source.setTranslatorName(translatorName);
			source.setConnectionJndiName(dsName);
			
			try {
				this.listener.dataSourceChanged(modelName, sourceName, translatorName, dsName);
				ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
				ReplaceResult rr = new ReplaceResult();
				if (dsName != null) {
					rr.isNew = !dsExists(dsName, cmr);
				}
				boolean replaced = getVDBStatusChecker().dataSourceReplaced(vdb.getName(), vdb.getVersion(), modelName, sourceName, translatorName, dsName);
				if (replaced && previousDsName != null && !dsExists(previousDsName, cmr)) {
					rr.removedDs = previousDsName;
				}
				return rr;
			} catch(AdminProcessingException e) {
				source.setTranslatorName(previousTranslatorName);
				source.setConnectionJndiName(previousDsName);
				throw e;
			}			
		}
	}

	private boolean dsExists(String dsName, ConnectorManagerRepository cmr) {
		String baseDsName = VDBStatusChecker.stripContext(dsName);
		for (ConnectorManager cm : cmr.getConnectorManagers().values()) {
			if (baseDsName.equals(VDBStatusChecker.stripContext(cm.getConnectionName()))) {
				return true;
			}
		}
		return false;
	}
	
	public void restart(List<String> modelNames) throws AdminProcessingException {
		synchronized(this.vdb) {
			this.restartInProgress = true;
			this.listener.onRestart(modelNames);
		}
	}
	
	private DataPolicyMetadata getPolicy(String policyName)
			throws AdminProcessingException {
		DataPolicyMetadata policy = vdb.getDataPolicyMap().get(policyName);
		
		if (policy == null) {
			 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40092, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40092, policyName, vdb.getName(), vdb.getVersion()));
		}
		return policy;
	}	
	
	public boolean isRestartInProgress() {
		return this.restartInProgress;
	}
	
	public VDBMetaData getVdb() {
		return vdb;
	}
	
	protected abstract VDBStatusChecker getVDBStatusChecker();
}
