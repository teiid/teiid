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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.spi.deployer.helpers.AbstractSimpleRealDeployer;
import org.jboss.deployers.structure.spi.DeploymentUnit;
import org.jboss.deployers.vfs.spi.structure.VFSDeploymentUnit;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.dqp.internal.cache.DQPContextCache;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.metadata.CompositeMetadataStore;
import org.teiid.metadata.TransformationMetadata;
import org.teiid.metadata.TransformationMetadata.Resource;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.runtime.RuntimePlugin;

import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.query.function.metadata.FunctionMethod;
import com.metamatrix.query.metadata.QueryMetadataInterface;

public class VDBDeployer extends AbstractSimpleRealDeployer<VDBMetaData> {
	private VDBRepository vdbRepository;
	private ConnectorManagerRepository connectorManagerRepository;
	private DQPContextCache contextCache;
	private ObjectSerializer serializer;
	
	public VDBDeployer() {
		super(VDBMetaData.class);
		setInput(VDBMetaData.class);
		setOutput(VDBMetaData.class);
	}

	@Override
	public void deploy(DeploymentUnit unit, VDBMetaData deployment) throws DeploymentException {
		if (this.vdbRepository.getVDB(deployment.getName(), deployment.getVersion()) != null) {
			this.vdbRepository.removeVDB(deployment.getName(), deployment.getVersion());
			LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("redeploying_vdb", deployment)); //$NON-NLS-1$ 
		}
		
		List<String> errors = deployment.getValidityErrors();
		if (errors != null && !errors.isEmpty()) {
			throw new DeploymentException(RuntimePlugin.Util.getString("validity_errors_in_vdb", deployment)); //$NON-NLS-1$
		}
		
		// get the metadata store of the VDB (this is build in parse stage)
		CompositeMetadataStore store = unit.getAttachment(CompositeMetadataStore.class);
		
		// if store is null and vdb dynamic vdb then try to get the metadata
		if (store == null && deployment.isDynamic()) {
			ArrayList<MetadataStore> stores = new ArrayList<MetadataStore>();
			for (Model model:deployment.getModels()) {
				if (model.getName().equals(CoreConstants.SYSTEM_MODEL)){
					continue;
				}
				stores.add(buildDynamicMetadataStore((VFSDeploymentUnit)unit, deployment, (ModelMetaData)model));
			}
			store = new CompositeMetadataStore(stores);
			unit.addAttachment(CompositeMetadataStore.class, store);			
		}
		
		if (store == null) {
			LogManager.logError(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("failed_matadata_load", deployment.getName(), deployment.getVersion())); //$NON-NLS-1$
		}
		
		// check if this is a VDB with index files, if there are then build the TransformationMetadata
		TransformationMetadata metadata = null;
		IndexMetadataFactory indexFactory = unit.getAttachment(IndexMetadataFactory.class);
		UDFMetaData udf = unit.getAttachment(UDFMetaData.class);
		if (indexFactory != null) {
			LinkedHashMap<String, Resource> visibilityMap = indexFactory.getEntriesPlusVisibilities();
			metadata = buildTransformationMetaData(deployment, visibilityMap, store, udf);
		}
		else {
			// this dynamic VDB
			metadata = buildTransformationMetaData(deployment, null, store, udf);
		}
				
		// add the metadata objects as attachments
		deployment.removeAttachment(IndexMetadataFactory.class);
		deployment.removeAttachment(UDFMetaData.class);
		deployment.addAttchment(QueryMetadataInterface.class, metadata);
		deployment.addAttchment(TransformationMetadata.class, metadata);
		
		// add transformation metadata to the repository.
		this.vdbRepository.addMetadata(deployment, metadata);
		this.vdbRepository.addMetadataStore(deployment, store);
		this.vdbRepository.addVDB(deployment);
		
		try {
			saveMetadataStore((VFSDeploymentUnit)unit, deployment, metadata.getMetadataStore());
		} catch (IOException e1) {
			LogManager.logWarning(LogConstants.CTX_RUNTIME, e1, RuntimePlugin.Util.getString("vdb_save_failed", deployment.getName()+"."+deployment.getVersion())); //$NON-NLS-1$ //$NON-NLS-2$			
		}
				
		boolean valid = validateSources(deployment);
		
		// Check if the VDB is fully configured.
		if (valid) {
			deployment.setStatus(VDB.Status.ACTIVE);
		} else {
			deployment.setStatus(VDB.Status.INACTIVE);
		}
		LogManager.logInfo(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("vdb_deployed",deployment, valid?"active":"inactive")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
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
				try {
					InitialContext ic = new InitialContext();
					ic.lookup(mapping.getJndiName());
				} catch (NamingException e) {
					valid = false;
					String msg = RuntimePlugin.Util.getString("jndi_not_found", deployment.getName(), deployment.getVersion(), mapping.getJndiName(),mapping.getName()); //$NON-NLS-1$
					model.addError(ModelMetaData.ValidationError.Severity.ERROR.name(), msg);
					LogManager.logInfo(LogConstants.CTX_RUNTIME, msg);
				}
			}
		}
		return valid;
	}


	// does this need to be synchronized? 
	private TransformationMetadata buildTransformationMetaData(VDBMetaData vdb, LinkedHashMap<String, Resource> visibilityMap, CompositeMetadataStore store, UDFMetaData udf) throws DeploymentException {
		
		// get the system VDB metadata store
		MetadataStore systemStore = this.vdbRepository.getMetadataStore(CoreConstants.SYSTEM_VDB, 1);
		if (systemStore == null) {
			throw new DeploymentException(RuntimePlugin.Util.getString("system_vdb_load_error")); //$NON-NLS-1$
		}
		
		store.addMetadataStore(systemStore);
		
		Collection <FunctionMethod> methods = null;
		if (udf != null) {
			methods = udf.getFunctions();
		}
		
		TransformationMetadata metadata =  new TransformationMetadata(vdb, store, visibilityMap, methods);
				
		return metadata;
	}	
	
	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}
	
	@Override
	public void undeploy(DeploymentUnit unit, VDBMetaData deployment) {
		super.undeploy(unit, deployment);
		
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
	
	private void saveMetadataStore(VFSDeploymentUnit unit, VDBMetaData vdb, CompositeMetadataStore store) throws IOException {
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
    		 try {
    			cacheFile = buildCachedFileName(unit, vdb,model.getName());
    			if (cacheFile.exists()) {
    				return this.serializer.loadAttachment(cacheFile, MetadataStore.class);
    			}
			} catch (IOException e) {
				LogManager.logWarning(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("invalid_metadata_file", cacheFile.getAbsolutePath())); //$NON-NLS-1$
			} catch (ClassNotFoundException e) {
				LogManager.logWarning(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("invalid_metadata_file", cacheFile.getAbsolutePath())); //$NON-NLS-1$
			} 
    	}
    	
    	
    	Exception exception = null;
    	for (String sourceName: model.getSourceNames()) {
    		ConnectorManager cm = this.connectorManagerRepository.getConnectorManager(model.getSourceJndiName(sourceName));
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
}
