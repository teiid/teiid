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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.adminapi.impl.ModelMetaData.ValidationError.Severity;
import org.teiid.core.TeiidException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.ExecutionFactory;


public abstract class VDBStatusChecker {
	private static final String JAVA_CONTEXT = "java:/"; //$NON-NLS-1$
	private TranslatorRepository translatorRepository;
	
	/**
	 * @param translatorName  
	 */
	public void translatorAdded(String translatorName) {
	}
	
	/**
	 * @param translatorName  
	 */
	public void translatorRemoved(String translatorName) {
	}
	
	public void dataSourceAdded(String dataSourceName) {
		if (dataSourceName.startsWith(JAVA_CONTEXT)) {
			dataSourceName = dataSourceName.substring(5);
		}
		resourceAdded(dataSourceName);
	}
	
	public void dataSourceRemoved(String dataSourceName) {
		if (dataSourceName.startsWith(JAVA_CONTEXT)) {
			dataSourceName = dataSourceName.substring(5);
		}
		resourceRemoved(dataSourceName);
	}	

	public void dataSourceReplaced(String vdbName, int vdbVersion,
			String modelName, String sourceName, String translatorName,
			String dsName) throws AdminProcessingException {
		if (dsName.startsWith(JAVA_CONTEXT)) {
			dsName = dsName.substring(5);
		}		
		
		VDBMetaData vdb = getVDBRepository().getVDB(vdbName, vdbVersion);
		ModelMetaData model = vdb.getModel(modelName);

		synchronized (vdb) {
			ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
			ConnectorManager cm = cmr.getConnectorManager(sourceName);
			ExecutionFactory<Object, Object> ef = cm.getExecutionFactory();
			
			boolean dsReplaced = false;
			if ((dsName != null && !dsName.equals(cm.getConnectionName())) || (dsName == null && cm.getConnectionName() != null)) {
				LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40076, vdb.getName(), vdb.getVersion(), model.getSourceTranslatorName(sourceName), dsName));
				cm = new ConnectorManager(translatorName, dsName); 
				cm.setExecutionFactory(ef);
				cmr.addConnectorManager(sourceName, cm);
				dsReplaced = true;
			}
			
			if (!cm.getTranslatorName().equals(translatorName)) {
				try {
					TranslatorRepository repo = vdb.getAttachment(TranslatorRepository.class);
					VDBTranslatorMetaData t = null;
					if (repo != null) {
						t = repo.getTranslatorMetaData(translatorName);
					}
					if (t == null) {
						t = this.translatorRepository.getTranslatorMetaData(translatorName);
					}
					if (t == null) {
						 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40032, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40032, vdb.getName(), vdb.getVersion(), translatorName));
					}
					ef = TranslatorUtil.buildExecutionFactory(t, t.getAttachment(ClassLoader.class));
					cm.setExecutionFactory(ef);
				} catch (TeiidException e) {
					 throw new AdminProcessingException(RuntimePlugin.Event.TEIID40033, e.getCause());
				}
			}
			
			if (dsReplaced) {
				ArrayList<Runnable> runnables = new ArrayList<Runnable>(1);
				checkStatus(runnables, vdb, model, cm);
				updateVDB(runnables, vdb);
			}
		}
	}

	public void resourceAdded(String resourceName) {
		List<Runnable> runnables = new ArrayList<Runnable>();
		for (VDBMetaData vdb:getVDBRepository().getVDBs()) {
			synchronized (vdb) {
				ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
				boolean usesResourse = false;
				for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
					if (!model.hasRuntimeErrors()) {
						continue;
					}
	
					String sourceName = getSourceName(resourceName, model);
					if (sourceName == null) {
						continue;
					}

					usesResourse = true;
					ConnectorManager cm = cmr.getConnectorManager(sourceName);
					checkStatus(runnables, vdb, model, cm);
				}
	
				if (usesResourse) {
					updateVDB(runnables, vdb);
				}
			}
		}
	}

	private void updateVDB(List<Runnable> runnables, VDBMetaData vdb) {
		if (!runnables.isEmpty()) {
			//the task themselves will set the status on completion/failure
			for (Runnable runnable : runnables) {						
				getExecutor().execute(runnable);
			}
			runnables.clear();
		} else if (vdb.hasErrors()) {
			LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40003,vdb.getName(), vdb.getVersion(), vdb.getStatus()));
		}
	}

	private void checkStatus(List<Runnable> runnables, VDBMetaData vdb,
			ModelMetaData model, ConnectorManager cm) {
		//get the pending metadata load
		Runnable r = model.removeAttachment(Runnable.class);
		if (r != null) {
			runnables.add(r);
		} else {
			String status = cm.getStausMessage();
			if (status != null && status.length() > 0) {
				Severity severity = vdb.getStatus() == Status.LOADING?Severity.WARNING:Severity.ERROR;
				model.addRuntimeError(severity, status);
				LogManager.logInfo(LogConstants.CTX_RUNTIME, status);
			} else if (vdb.getStatus() != Status.LOADING){
				model.clearRuntimeErrors();
			}
		}
	}
	
	public void resourceRemoved(String resourceName) {
		for (VDBMetaData vdb:getVDBRepository().getVDBs()) {
			synchronized (vdb) {
				ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
				for (ModelMetaData model:vdb.getModelMetaDatas().values()) {
					String sourceName = getSourceName(resourceName, model);
					if (sourceName == null) {
						continue;
					}
					ConnectorManager cm = cmr.getConnectorManager(sourceName);
					if (cm.getExecutionFactory().isSourceRequired()) {
						String msg = RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40012, vdb.getName(), vdb.getVersion(), resourceName); 
						Severity severity = vdb.getStatus() == Status.LOADING?Severity.WARNING:Severity.ERROR;
						model.addRuntimeError(severity, msg);
						LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);
					}
				}
			}
		}
	}

	private String getSourceName(String factoryName, ModelMetaData model) {
		for (SourceMappingMetadata source:model.getSources().values()) {
			String jndiName = source.getConnectionJndiName();
			if (jndiName == null) {
				continue;
			}
			if (jndiName.startsWith(JAVA_CONTEXT)) {
				jndiName = jndiName.substring(5);
			}
			if (factoryName.equals(jndiName)) {
				return source.getName();
			}
		}
		return null;
	}
	
	public abstract Executor getExecutor();
	
	public abstract VDBRepository getVDBRepository();
	
	public void setTranslatorRepository(TranslatorRepository repo) {
		this.translatorRepository = repo;
	}
}
