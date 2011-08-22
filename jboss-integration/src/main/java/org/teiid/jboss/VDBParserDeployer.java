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
package org.teiid.jboss;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.jboss.as.server.deployment.*;
import org.jboss.vfs.VirtualFile;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.*;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.VdbConstants;
import org.teiid.metadata.index.IndexMetadataFactory;
import org.teiid.runtime.RuntimePlugin;
import org.xml.sax.SAXException;


/**
 * This file loads the "vdb.xml" file inside a ".vdb" file, along with all the metadata in the .INDEX files
 */
public class VDBParserDeployer implements DeploymentUnitProcessor {
	private ObjectSerializer serializer;
	private VDBRepository vdbRepository;
	
	public VDBParserDeployer(VDBRepository repo, ObjectSerializer serializer) {
		this.vdbRepository = repo;
		this.serializer = serializer;
	}
	
	public void deploy(final DeploymentPhaseContext phaseContext)  throws DeploymentUnitProcessingException {
		DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
		if (!TeiidAttachments.isVDBDeployment(deploymentUnit)) {
			return;
		}

		VirtualFile file = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT).getRoot();

		if (TeiidAttachments.isDynamicVDB(deploymentUnit)) {
			parseVDBXML(file, deploymentUnit);			
		}
		else {
			// scan for different files 
			List<VirtualFile> childFiles = file.getChildren();
			for (VirtualFile childFile:childFiles) {
				scanVDB(childFile, deploymentUnit);
			}
			
			mergeMetaData(deploymentUnit);
		}
	}
	
	private void scanVDB(VirtualFile file, DeploymentUnit deploymentUnit) throws DeploymentUnitProcessingException {
		if (file.isDirectory()) {
			List<VirtualFile> childFiles = file.getChildren();
			for (VirtualFile childFile:childFiles) {
				scanVDB(childFile, deploymentUnit);
			}
		}
		else {
			if (file.getLowerCaseName().equals(VdbConstants.DEPLOYMENT_FILE)) {
				parseVDBXML(file, deploymentUnit);
			}
			else if (file.getLowerCaseName().endsWith(VdbConstants.INDEX_EXT)) {
				IndexMetadataFactory imf = deploymentUnit.getAttachment(TeiidAttachments.INDEX_METADATA);
				if (imf == null) {
					imf = new IndexMetadataFactory();
					deploymentUnit.putAttachment(TeiidAttachments.INDEX_METADATA, imf);
				}
				imf.addIndexFile(file);
			}
			else if (file.getLowerCaseName().endsWith(VdbConstants.MODEL_EXT)) {
				UDFMetaData udf = deploymentUnit.getAttachment(TeiidAttachments.UDF_METADATA);
				if (udf == null) {
					udf = new UDFMetaData();
					deploymentUnit.putAttachment(TeiidAttachments.UDF_METADATA, udf);
				}
				udf.addModelFile(file);				
			}
			
		}
	}

	private void parseVDBXML(VirtualFile file, DeploymentUnit deploymentUnit)
			throws DeploymentUnitProcessingException {
		try {
			Unmarshaller un = getUnMarsheller();
			VDBMetaData vdb = (VDBMetaData)un.unmarshal(file.openStream());
			deploymentUnit.putAttachment(TeiidAttachments.VDB_METADATA, vdb);
			LogManager.logDetail(LogConstants.CTX_RUNTIME,"VDB "+file.getName()+" has been parsed.");  //$NON-NLS-1$ //$NON-NLS-2$
		} catch (JAXBException e) {
			throw new DeploymentUnitProcessingException(e);
		} catch (SAXException e) {
			throw new DeploymentUnitProcessingException(e);
		} catch (IOException e) {
			throw new DeploymentUnitProcessingException(e);
		}
	}
	
    public void undeploy(final DeploymentUnit context) {
    }	
    

	static Unmarshaller getUnMarsheller() throws JAXBException, SAXException {
		JAXBContext jc = JAXBContext.newInstance(new Class<?>[] {VDBMetaData.class});
		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Schema schema = schemaFactory.newSchema(VDBMetaData.class.getResource("/vdb-deployer.xsd")); //$NON-NLS-1$
		Unmarshaller un = jc.createUnmarshaller();
		un.setSchema(schema);
		return un;
	}
	
	protected VDBMetaData mergeMetaData(DeploymentUnit deploymentUnit) throws DeploymentUnitProcessingException {
		VDBMetaData vdb = deploymentUnit.getAttachment(TeiidAttachments.VDB_METADATA);
		UDFMetaData udf = deploymentUnit.getAttachment(TeiidAttachments.UDF_METADATA);
		IndexMetadataFactory imf = deploymentUnit.getAttachment(TeiidAttachments.INDEX_METADATA);
		
		VirtualFile file = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT).getRoot();
		if (vdb == null) {
			LogManager.logError(LogConstants.CTX_RUNTIME, RuntimePlugin.Util.getString("invlaid_vdb_file",file.getName())); //$NON-NLS-1$
			return null;
		}
		
		try {
			vdb.setUrl(file.toURL());		
			
			// build the metadata store
			if (imf != null) {
				imf.addEntriesPlusVisibilities(file, vdb);
								
				// add the cached store.
				File cacheFile = VDBDeployer.buildCachedVDBFileName(this.serializer, file, vdb);
				// check to see if the vdb has been modified when server is down; if it is then clear the old files
				if (this.serializer.isStale(cacheFile, file.getLastModified())) {
					this.serializer.removeAttachments(file);
					LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB", file.getName(), "old cached metadata has been removed"); //$NON-NLS-1$ //$NON-NLS-2$				
				}
				MetadataStoreGroup stores = this.serializer.loadSafe(cacheFile, MetadataStoreGroup.class);
				if (stores == null) {				
					// start to build the new metadata 
					stores = new MetadataStoreGroup();
					stores.addStore(imf.getMetadataStore(vdbRepository.getSystemStore().getDatatypes()));
				}
				else {
					LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB", file.getName(), "was loaded from cached metadata"); //$NON-NLS-1$ //$NON-NLS-2$
				}
				deploymentUnit.putAttachment(TeiidAttachments.METADATA_STORE, stores);				
			}
			
			if (udf != null) {
				// load the UDF
				for(Model model:vdb.getModels()) {
					if (model.getModelType().equals(Model.Type.FUNCTION)) {
						String path = ((ModelMetaData)model).getPath();
						if (path == null) {
							throw new DeploymentUnitProcessingException(RuntimePlugin.Util.getString("invalid_udf_file", model.getName())); //$NON-NLS-1$
						}
						udf.buildFunctionModelFile(model.getName(), path);
					}
				}		
			}
		} catch(IOException e) {
			throw new DeploymentUnitProcessingException(e); 
		} catch (JAXBException e) {
			throw new DeploymentUnitProcessingException(e);
		}
				
		LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB", file.getName(), "has been parsed."); //$NON-NLS-1$ //$NON-NLS-2$
		return vdb;
	}
}
