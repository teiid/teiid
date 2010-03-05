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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.jboss.deployers.spi.DeploymentException;
import org.jboss.deployers.vfs.spi.deployer.AbstractVFSParsingDeployer;
import org.jboss.deployers.vfs.spi.structure.VFSDeploymentUnit;
import org.jboss.virtual.VirtualFile;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.metadata.runtime.MetadataStore;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManager;
import org.teiid.dqp.internal.datamgr.impl.ConnectorManagerRepository;
import org.teiid.metadata.CompositeMetadataStore;

import com.metamatrix.core.CoreConstants;

public class DynamicVDBDeployer extends AbstractVFSParsingDeployer<VDBMetaData> {
	private VDBRepository vdbRepository;
	private ConnectorManagerRepository connectorManagerRepository;
	private ObjectSerializer serializer;
	
	public DynamicVDBDeployer() {
		super(VDBMetaData.class);
		setSuffix("-vdb.xml");
	}

	@Override
	protected VDBMetaData parse(VFSDeploymentUnit unit, VirtualFile file, VDBMetaData root) throws Exception {
		JAXBContext jc = JAXBContext.newInstance(new Class<?>[] {VDBMetaData.class});
		Unmarshaller un = jc.createUnmarshaller();
		VDBMetaData def = (VDBMetaData)un.unmarshal(file.openStream());
		
		def.setUrl(unit.getRoot().toURL().toExternalForm());	
		log.debug("VDB "+unit.getRoot().getName()+" has been parsed.");
		
		
		ArrayList<MetadataStore> stores = new ArrayList<MetadataStore>();
		for (ModelMetaData model:def.getModels()) {
			if (model.getName().equals(CoreConstants.SYSTEM_MODEL)){
				continue;
			}
			stores.add(buildDynamicMetadataStore(unit, def, model));
		}
		
		CompositeMetadataStore store = new CompositeMetadataStore(stores);
		unit.addAttachment(CompositeMetadataStore.class, store);
		
		return def;
	}
	
    private MetadataStore buildDynamicMetadataStore(VFSDeploymentUnit unit, VDBMetaData vdb, ModelMetaData model) throws DeploymentException{
    	if (model.getSourceNames().isEmpty()) {
    		throw new DeploymentException(vdb.getName()+"-"+vdb.getVersion()+" Can not be deployed because model {"+model.getName()+"} is not fully configured.");
    	}
    	
    	boolean cache = "cached".equalsIgnoreCase(vdb.getPropertyValue("UseConnectorMetadata"));
    	File cacheFile = null;
    	if (cache) {
    		 try {
    			cacheFile = buildCachedFileName(unit, vdb,model.getName());
    			if (cacheFile.exists()) {
    				return this.serializer.loadAttachment(cacheFile, MetadataStore.class);
    			}
			} catch (IOException e) {
				log.warn("invalid metadata in file = "+cacheFile.getAbsolutePath());
			} catch (ClassNotFoundException e) {
				log.warn("invalid metadata in file = "+cacheFile.getAbsolutePath());
			} 
    	}
    	
    	
    	Exception exception = null;
    	for (String connectorName: model.getSourceNames()) {
    		ConnectorManager cm = this.connectorManagerRepository.getConnectorManager(connectorName);
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
    	throw new DeploymentException(vdb.getName()+"-"+vdb.getVersion()+" Can not be deployed because model {"+model.getName()+"} can not retrive metadata", exception);
	}	
    
	public void setVDBRepository(VDBRepository repo) {
		this.vdbRepository = repo;
	}
	
	public void setConnectorManagerRepository(ConnectorManagerRepository repo) {
		this.connectorManagerRepository = repo;
	}    
	
	public void setObjectSerializer(ObjectSerializer serializer) {
		this.serializer = serializer;
	}	
	
	private File buildCachedFileName(VFSDeploymentUnit unit, VDBMetaData vdb, String modelName) throws IOException {
		return this.serializer.getAttachmentPath(unit, vdb.getName()+"_"+vdb.getVersion()+"_"+modelName);
	}
}
