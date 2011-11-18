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

import java.util.LinkedList;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.util.threadpool.ThreadPool;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.translator.ExecutionFactory;


public class VDBStatusChecker {
	private static final String JAVA_CONTEXT = "java:"; //$NON-NLS-1$
	private VDBRepository vdbRepository;
	private ThreadPool threadPool;
	private TranslatorRepository translatorRepository;
	
	public void translatorAdded(String translatorName) {
		resourceAdded(translatorName, true);
	}
	
	public void translatorRemoved(String translatorName) {
		resourceremoved(translatorName, true);
	}
	
	public void dataSourceAdded(String dataSourceName) {
		if (dataSourceName.startsWith(JAVA_CONTEXT)) {
			dataSourceName = dataSourceName.substring(5);
		}
		resourceAdded(dataSourceName, false);
	}
	
	public void dataSourceRemoved(String dataSourceName) {
		if (dataSourceName.startsWith(JAVA_CONTEXT)) {
			dataSourceName = dataSourceName.substring(5);
		}
		resourceremoved(dataSourceName, false);
	}	
	
	public void dataSourceReplaced(String vdbName, int vdbVersion,
			String modelName, String sourceName, String translatorName,
			String dsName) throws AdminException {
		if (dsName.startsWith(JAVA_CONTEXT)) {
			dsName = dsName.substring(5);
		}		
		
		VDBMetaData vdb = this.vdbRepository.getVDB(vdbName, vdbVersion);
		ModelMetaData model = vdb.getModel(modelName);

		synchronized (vdb) {
			ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
			ConnectorManager cm = cmr.getConnectorManager(sourceName);
			ExecutionFactory<Object, Object> ef = cm.getExecutionFactory();
			
			boolean dsReplaced = false;
			if (!cm.getConnectionName().equals(dsName)){
				vdb.setStatus(VDB.Status.INACTIVE);
				String msg = RuntimePlugin.Util.getString("datasource_replaced", vdb.getName(), vdb.getVersion(), model.getSourceTranslatorName(sourceName), dsName); //$NON-NLS-1$
				model.addError(ModelMetaData.ValidationError.Severity.ERROR.name(), msg);
				cm = new ConnectorManager(translatorName, dsName); 
				cm.setExecutionFactory(ef);
				cm.setModelName(modelName);
				cmr.addConnectorManager(sourceName, cm);
				dsReplaced = true;
			}
			
			if (!cm.getTranslatorName().equals(translatorName)) {
				try {
					Translator t = this.translatorRepository.getTranslatorMetaData(translatorName);
					ef = TranslatorUtil.buildExecutionFactory(t);
					cm.setExecutionFactory(ef);
				} catch (DeploymentException e) {
					throw new AdminProcessingException(e.getCause());
				}
			}
			
			if (dsReplaced) {
				resourceAdded(dsName, false);
			}
		}
	}
	
	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}	
	
	public void resourceAdded(String resourceName, boolean translator) {
		for (VDBMetaData vdb:this.vdbRepository.getVDBs()) {
			if (vdb.getStatus() == VDB.Status.ACTIVE || vdb.isPreview()) {
				continue;
			}
			LinkedList<Runnable> runnables = new LinkedList<Runnable>();
			synchronized (vdb) {
				ConnectorManagerRepository cmr = vdb.getAttachment(ConnectorManagerRepository.class);
				
				for (Model m:vdb.getModels()) {
					ModelMetaData model = (ModelMetaData)m;
					if (model.getErrors().isEmpty()) {
						continue;
					}
	
					String sourceName = getSourceName(resourceName, model, translator);
					if (sourceName == null) {
						continue;
					}
					ConnectorManager cm = cmr.getConnectorManager(sourceName);
					String status = cm.getStausMessage();
					if (status != null && status.length() > 0) {
						model.addError(ModelMetaData.ValidationError.Severity.ERROR.name(), status);
						LogManager.logInfo(LogConstants.CTX_RUNTIME, status);					
					} else {
						//get the pending metadata load
						Runnable r = model.removeAttachment(Runnable.class);
						if (r != null) {
							runnables.add(r);
						} else {
							model.clearErrors();
						}
					}
				}
	
				boolean valid = true;
				for (Model m:vdb.getModels()) {
					ModelMetaData model = (ModelMetaData)m;
					if (!model.getErrors().isEmpty()) {
						valid = false;
						break;
					}
				}
				
				if (!runnables.isEmpty()) {
					//the task themselves will set the status on completion/failure
					for (Runnable runnable : runnables) {
						this.threadPool.run(runnable);
					}
				} else if (valid) {
					vdb.setStatus(VDB.Status.ACTIVE);
					LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_activated",vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
				}
			}
		}
	}
	
	public void resourceremoved(String resourceName, boolean translator) {
		for (VDBMetaData vdb:this.vdbRepository.getVDBs()) {
			if (vdb.isPreview()) {
				continue;
			}
			synchronized (vdb) {
				for (Model m:vdb.getModels()) {
					ModelMetaData model = (ModelMetaData)m;
					
					String sourceName = getSourceName(resourceName, model, translator);
					if (sourceName != null) {
						vdb.setStatus(VDB.Status.INACTIVE);
						String msg = null;
						if (translator) {
							msg = RuntimePlugin.Util.getString("translator_not_found", vdb.getName(), vdb.getVersion(), model.getSourceTranslatorName(sourceName)); //$NON-NLS-1$
						}
						else {
							msg = RuntimePlugin.Util.getString("datasource_not_found", vdb.getName(), vdb.getVersion(), model.getSourceTranslatorName(sourceName)); //$NON-NLS-1$
						}
						model.addError(ModelMetaData.ValidationError.Severity.ERROR.name(), msg);
						LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);					
						LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_inactivated",vdb.getName(), vdb.getVersion())); //$NON-NLS-1$							
					}
				}
			}
		}
	}

	private String getSourceName(String translatorName, ModelMetaData model, boolean translator) {
		for (String sourceName:model.getSourceNames()) {
			if (translator) {
				if (translatorName.equals(model.getSourceTranslatorName(sourceName))) {
					return sourceName;
				}
			} else {
				String jndiName = model.getSourceConnectionJndiName(sourceName);
				if (jndiName.startsWith(JAVA_CONTEXT)) {
					jndiName = jndiName.substring(5);
				}
				if (translatorName.equals(jndiName)) {
					return sourceName;
				}
			}
		}
		return null;
	}
	
	public void setThreadPool(ThreadPool threadPool) {
		this.threadPool = threadPool;
	}
	
	public void setTranslatorRepository(TranslatorRepository repo) {
		this.translatorRepository = repo;
	}
}
