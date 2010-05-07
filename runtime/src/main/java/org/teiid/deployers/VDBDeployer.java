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

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.deployer.helpers.AbstractSimpleRealDeployer;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.jboss.deployers.vfs.spi.structure.VFSDeploymentUnit;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.dqp.internal.cache.DQPContextCache;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.impl.TranslatorRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.TransformationMetadata.Resource;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.resource.ConnectorException;
import org.teiid.resource.cci.ExecutionFactory;
import org.teiid.runtime.RuntimePlugin;

import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.util.FileUtils;

public class VDBDeployer extends AbstractSimpleRealDeployer<VDBMetaData> {
	private VDBRepository vdbRepository;
	private ConnectorManagerRepository connectorManagerRepository;
	private TranslatorRepository translatorRepository;
	private DQPContextCache contextCache;
	private ObjectSerializer serializer;
	
	public VDBDeployer() {
		super(VDBMetaData.class);
		setInput(VDBMetaData.class);
		setOutput(VDBMetaData.class);
		setRelativeOrder(3001); // after the data sources
	}

	@Override
	public void deploy(DeploymentUnit unit, VDBMetaData deployment) throws DeploymentException {
		if (this.vdbRepository.getVDB(deployment.getName(), deployment.getVersion()) != null) {
			this.vdbRepository.removeVDB(deployment.getName(), deployment.getVersion());
			LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("redeploying_vdb", deployment)); //$NON-NLS-1$ 
		}
		
		boolean preview = deployment.isPreview();
		
		if (!preview) {
			List<String> errors = deployment.getValidityErrors();
			if (errors != null && !errors.isEmpty()) {
				throw new DeploymentException(RuntimePlugin.Util.getString("validity_errors_in_vdb", deployment)); //$NON-NLS-1$
			}
		}
		
		// get the metadata store of the VDB (this is build in parse stage)
		MetadataStoreGroup store = unit.getAttachment(MetadataStoreGroup.class);
		
		// add required connector managers; if they are not already there
		createConnectorManagers(deployment);
		
		// if store is null and vdb dynamic vdb then try to get the metadata
		if (store == null && deployment.isDynamic()) {
			MetadataStoreGroup dynamicStore = new MetadataStoreGroup();
			for (Model model:deployment.getModels()) {
				if (model.getName().equals(CoreConstants.SYSTEM_MODEL)){
					continue;
				}
				dynamicStore.addStore(buildDynamicMetadataStore((VFSDeploymentUnit)unit, deployment, (ModelMetaData)model));
			}
			store = dynamicStore;		
		}
		
		if (store == null) {
			LogManager.logError(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("failed_matadata_load", deployment.getName(), deployment.getVersion())); //$NON-NLS-1$
		}
		
		// check if this is a VDB with index files, if there are then build the TransformationMetadata
		UDFMetaData udf = unit.getAttachment(UDFMetaData.class);
		
		LinkedHashMap<String, Resource> visibilityMap = null;
		IndexMetadataFactory indexFactory = unit.getAttachment(IndexMetadataFactory.class);		
		if (indexFactory != null) {
			visibilityMap = indexFactory.getEntriesPlusVisibilities();
		}
				
		// add the metadata objects as attachments
		deployment.removeAttachment(IndexMetadataFactory.class);
		deployment.removeAttachment(UDFMetaData.class);
		
		// add transformation metadata to the repository.
		this.vdbRepository.addVDB(deployment, store, visibilityMap, udf);
		
		try {
			saveMetadataStore((VFSDeploymentUnit)unit, deployment, store);
		} catch (IOException e1) {
			LogManager.logWarning(LogConstants.CTX_RUNTIME, e1, RuntimePlugin.Util.getString("vdb_save_failed", deployment.getName()+"."+deployment.getVersion())); //$NON-NLS-1$ //$NON-NLS-2$			
		}
				
		boolean valid = true;
		if (!preview) {
			valid = validateSources(deployment);
			
			// Check if the VDB is fully configured.
			if (valid) {
				deployment.setStatus(VDB.Status.ACTIVE);
			} else {
				deployment.setStatus(VDB.Status.INACTIVE);
			}			
		}
		else {
			deployment.setStatus(VDB.Status.ACTIVE);
		}
		LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_deployed",deployment, valid?"active":"inactive")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	}

	private void createConnectorManagers(VDBMetaData deployment) {
		for (Model model:deployment.getModels()) {
			if (model.getName().equals(CoreConstants.SYSTEM_MODEL)){
				continue;
			}
			for (String source:model.getSourceNames()) {
				if (this.connectorManagerRepository.getConnectorManager(source) == null) {
					ConnectorManager cm = new ConnectorManager(model.getSourceTranslatorName(source), model.getSourceConnectionJndiName(source)) {
						protected ExecutionFactory getExecutionFactory() {
							return VDBDeployer.this.translatorRepository.getTranslator(getTranslatorName());
						}
					};
					this.connectorManagerRepository.addConnectorManager(source, cm);
				}
			}
		}
	}

	private boolean validateSources(VDBMetaData deployment) {
		boolean valid = true;
		for(Model m:deployment.getModels()) {
			ModelMetaData model = (ModelMetaData)m;
			List<SourceMappingMetadata> mappings = model.getSourceMappings();
			for (SourceMappingMetadata mapping:mappings) {
				if (mapping.getName().equals(CoreConstants.SYSTEM_MODEL)) {
					continue;
				}
				ConnectorManager cm = this.connectorManagerRepository.getConnectorManager(mapping.getName());
				String msg = cm.getStausMessage();
				if (msg != null && msg.length() > 0) {
					valid = false;
					model.addError(ModelMetaData.ValidationError.Severity.ERROR.name(), cm.getStausMessage());
					LogManager.logInfo(LogConstants.CTX_RUNTIME, cm.getStausMessage());
				}
			}
		}
		return valid;
	}

	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}
	
	@Override
	public void undeploy(DeploymentUnit unit, VDBMetaData deployment) {
		super.undeploy(unit, deployment);
		
		// there is chance that two different VDBs using the same source name, and their
		// connector manager is removed. should we prefix vdb name??
		for (Model model:deployment.getModels()) {
			if (model.getName().equals(CoreConstants.SYSTEM_MODEL)){
				continue;
			}
			for (String source:model.getSourceNames()) {
				if (this.connectorManagerRepository.getConnectorManager(source) != null) {
					this.connectorManagerRepository.removeConnectorManager(source);
				}
			}
		}
		
		if (this.vdbRepository != null) {
			this.vdbRepository.removeVDB(deployment.getName(), deployment.getVersion());
		}
		
		if (this.contextCache != null) {
			// remove any vdb specific context cache
			this.contextCache.removeVDBScopedCache(deployment.getName(), deployment.getVersion());			
		}

		try {
			deleteMetadataStore((VFSDeploymentUnit)unit, deployment);
		} catch (IOException e) {
			LogManager.logWarning(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_delete_failed", e.getMessage())); //$NON-NLS-1$
		}

		LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_undeployed", deployment)); //$NON-NLS-1$
	}

	public void setContextCache(DQPContextCache cache) {
		this.contextCache = cache;
	}
	
	public void setObjectSerializer(ObjectSerializer serializer) {
		this.serializer = serializer;
	}		
	
	public void setConnectorManagerRepository(ConnectorManagerRepository repo) {
		this.connectorManagerRepository = repo;
	}  
	
	private void saveMetadataStore(VFSDeploymentUnit unit, VDBMetaData vdb, MetadataStoreGroup store) throws IOException {
		File cacheFileName = this.serializer.getAttachmentPath(unit, vdb.getName()+"_"+vdb.getVersion()); //$NON-NLS-1$
		if (!cacheFileName.exists()) {
			this.serializer.saveAttachment(cacheFileName,store);
		}
	}
	
	private void deleteMetadataStore(VFSDeploymentUnit unit, VDBMetaData vdb) throws IOException {
		if (!unit.getRoot().exists()) {
			File cacheFileName = this.serializer.getAttachmentPath(unit, vdb.getName()+"_"+vdb.getVersion()); //$NON-NLS-1$
			if (cacheFileName.exists()) {
				FileUtils.removeDirectoryAndChildren(cacheFileName.getParentFile());
			}
		}
	}
	
    private MetadataStore buildDynamicMetadataStore(VFSDeploymentUnit unit, VDBMetaData vdb, ModelMetaData model) throws DeploymentException{
    	if (model.getSourceNames().isEmpty()) {
    		throw new DeploymentException(RuntimePlugin.Util.getString("fail_to_deploy", vdb.getName()+"-"+vdb.getVersion(), model.getName())); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	
    	boolean cache = "cached".equalsIgnoreCase(vdb.getPropertyValue("UseConnectorMetadata")); //$NON-NLS-1$ //$NON-NLS-2$
    	File cacheFile = null;
    	if (cache) {
    		cacheFile = buildCachedFileName(unit, vdb,model.getName());
			MetadataStore store = this.serializer.loadSafe(cacheFile, MetadataStore.class);
			if (store != null) {
				return store;
			}
    	}
    	
    	Exception exception = null;
    	for (String sourceName: model.getSourceNames()) {
    		ConnectorManager cm = this.connectorManagerRepository.getConnectorManager(sourceName);
    		if (cm == null) {
    			continue;
    		}
    		try {
    			MetadataStore store = cm.getMetadata(model.getName(), this.vdbRepository.getBuiltinDatatypes(), model.getProperties());
    			if (cache) {
    				this.serializer.saveAttachment(cacheFile, store);
    			}
    			return store;
			} catch (ConnectorException e) {
				if (exception != null) {
					exception = e;
				}
			} catch (IOException e) {
				if (exception != null) {
					exception = e;
				}				
			}
    	}
    	throw new DeploymentException(RuntimePlugin.Util.getString("failed_to_retrive_metadata", vdb.getName()+"-"+vdb.getVersion(), model.getName()), exception); //$NON-NLS-1$ //$NON-NLS-2$
	}	
    
	private File buildCachedFileName(VFSDeploymentUnit unit, VDBMetaData vdb, String modelName) {
		return this.serializer.getAttachmentPath(unit, vdb.getName()+"_"+vdb.getVersion()+"_"+modelName); //$NON-NLS-1$ //$NON-NLS-2$
	}    
	
	public void setTranslatorRepository(TranslatorRepository repo) {
		this.translatorRepository = repo;
	}	
}
