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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.metadata.property.PropertyReplacer;
import org.jboss.metadata.property.PropertyReplacers;
import org.jboss.metadata.property.PropertyResolver;
import org.jboss.msc.service.ServiceController;
import org.jboss.vfs.VirtualFile;
import org.jboss.vfs.VirtualFileFilter;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.deployers.UDFMetaData;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.metadata.VDBResources;
import org.xml.sax.SAXException;


/**
 * This file loads the "vdb.xml" file inside a ".vdb" file, along with all the metadata in the .INDEX files
 */
class VDBParserDeployer implements DeploymentUnitProcessor {
	
	public VDBParserDeployer() {
	}
	
	public void deploy(final DeploymentPhaseContext phaseContext)  throws DeploymentUnitProcessingException {
		final DeploymentUnit deploymentUnit = phaseContext.getDeploymentUnit();
		if (!TeiidAttachments.isVDBDeployment(deploymentUnit)) {
			return;
		}

		VirtualFile file = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT).getRoot();
		
		if (TeiidAttachments.isVDBXMLDeployment(deploymentUnit)) {
			parseVDBXML(file, deploymentUnit, phaseContext, true);			
		}
		else {
			// scan for different files 
			try {
				List<VirtualFile> childFiles = file.getChildrenRecursively(new VirtualFileFilter() {
					
					@Override
					public boolean accepts(VirtualFile file) {
						if (file.isDirectory() && file.getName().toLowerCase().endsWith(VDBResources.MODEL_EXT)) {
							UDFMetaData udf = deploymentUnit.getAttachment(TeiidAttachments.UDF_METADATA);
							if (udf == null) {
								udf = new FileUDFMetaData();
								deploymentUnit.putAttachment(TeiidAttachments.UDF_METADATA, udf);
							}
							((FileUDFMetaData)udf).addModelFile(file);
							return false;
						}
						return !file.isDirectory() && file.getName().toLowerCase().equals(VDBResources.DEPLOYMENT_FILE);
					}
				});
				
				if (childFiles.size() != 1) {
					throw new DeploymentUnitProcessingException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50101, deploymentUnit, childFiles.size()));
				}
				parseVDBXML(childFiles.get(0), deploymentUnit, phaseContext, false);
				
				mergeMetaData(deploymentUnit);

			} catch (IOException e) {
				throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50017.name(), e);	
			}
		}
	}

	private VDBMetaData parseVDBXML(VirtualFile file, DeploymentUnit deploymentUnit, DeploymentPhaseContext phaseContext, boolean xmlDeployment) throws DeploymentUnitProcessingException {
		try {
			VDBMetadataParser.validate(file.openStream());
            PropertyResolver propertyResolver = deploymentUnit.getAttachment(org.jboss.as.ee.metadata.property.Attachments.FINAL_PROPERTY_RESOLVER);
            PropertyReplacer replacer = PropertyReplacers.resolvingReplacer(propertyResolver);
            String vdbContents = replacer.replaceProperties(ObjectConverterUtil.convertToString(file.openStream()));
			VDBMetaData vdb = VDBMetadataParser.unmarshell(new ByteArrayInputStream(vdbContents.getBytes("UTF-8"))); //$NON-NLS-1$
			ServiceController<?> sc = phaseContext.getServiceRegistry().getService(TeiidServiceNames.OBJECT_SERIALIZER);
			ObjectSerializer serializer = ObjectSerializer.class.cast(sc.getValue());
			if (serializer.buildVdbXml(vdb).exists()) {
				vdb = VDBMetadataParser.unmarshell(new FileInputStream(serializer.buildVdbXml(vdb)));
			}
			vdb.setStatus(Status.LOADING);
			if (xmlDeployment) {
				vdb.setXmlDeployment(true);
			} else {
				String name = deploymentUnit.getName();
				String fileName = StringUtil.getLastToken(name, "/"); //$NON-NLS-1$
				String[] parts = fileName.split("\\."); //$NON-NLS-1$
				if (parts[0].equalsIgnoreCase(vdb.getName()) && parts.length >= 3) {
					try {
						int fileVersion = Integer.parseInt(parts[parts.length - 2]);
						vdb.setVersion(fileVersion);
					} catch (NumberFormatException e) {
						
					}
				}
			}
			deploymentUnit.putAttachment(TeiidAttachments.VDB_METADATA, vdb);
			LogManager.logDetail(LogConstants.CTX_RUNTIME,"VDB "+file.getName()+" has been parsed.");  //$NON-NLS-1$ //$NON-NLS-2$
			return vdb;
		} catch (XMLStreamException e) {
			throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50017.name(), e);
		} catch (IOException e) {
			throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50017.name(), e);
		} catch (SAXException e) {
			throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50017.name(), e);
		}
	}
	
    public void undeploy(final DeploymentUnit context) {
    }	
    
	protected VDBMetaData mergeMetaData(DeploymentUnit deploymentUnit) throws DeploymentUnitProcessingException {
		VDBMetaData vdb = deploymentUnit.getAttachment(TeiidAttachments.VDB_METADATA);
		UDFMetaData udf = deploymentUnit.getAttachment(TeiidAttachments.UDF_METADATA);
		
		VirtualFile file = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT).getRoot();
		if (vdb == null) {
			LogManager.logError(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50016,file.getName())); 
			return null;
		}
		
		try {
			if (udf != null) {
				// load the UDF
				for(Model model:vdb.getModels()) {
					if (model.getModelType().equals(Model.Type.FUNCTION)) {
						String path = ((ModelMetaData)model).getPath();
						if (path == null) {
							throw new DeploymentUnitProcessingException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50075, model.getName()));
						}
						((FileUDFMetaData)udf).buildFunctionModelFile(model.getName(), path);
					}
				}		
			}
		} catch(IOException e) {
			throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50017.name(), e); 
		} catch (XMLStreamException e) {
			throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50017.name(), e);
		}
				
		LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB", file.getName(), "has been parsed."); //$NON-NLS-1$ //$NON-NLS-2$
		return vdb;
	}
}
