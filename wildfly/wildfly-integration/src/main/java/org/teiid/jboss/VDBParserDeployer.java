/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.jboss;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;

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
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.StringUtil;
import org.teiid.deployers.VDBRepository;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadatastore.DeploymentBasedDatabaseStore;
import org.xml.sax.SAXException;


/**
 * This file loads the "vdb.xml" file inside a ".vdb" file, along with all the metadata in the .INDEX files
 */
class VDBParserDeployer implements DeploymentUnitProcessor {
    private VDBRepository vdbRepo;

    public VDBParserDeployer(VDBRepository vdbRepo) {
        this.vdbRepo = vdbRepo;
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
        else if (TeiidAttachments.isVDBDDLDeployment(deploymentUnit)) {
            parseVDBDDL(file, deploymentUnit, phaseContext, true);
        }
        else {
            // scan for different files
            try {
                file.getChildrenRecursively(new VirtualFileFilter() {

                    @Override
                    public boolean accepts(VirtualFile file) {
                        if (file.isDirectory()) {
                            return false;
                        }
                        return false;
                    }
                });

                VirtualFile vdbXml = file.getChild("/META-INF/vdb.xml"); //$NON-NLS-1$
                VirtualFile vdbDDL = file.getChild("/META-INF/vdb.ddl"); //$NON-NLS-1$

                if (!vdbXml.exists() && !vdbDDL.exists()) {
                    throw new DeploymentUnitProcessingException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50101, deploymentUnit));
                }

                if (vdbXml.exists()) {
                    parseVDBXML(vdbXml, deploymentUnit, phaseContext, false);
                } else {
                    parseVDBDDL(vdbDDL, deploymentUnit, phaseContext, false);
                }

                mergeMetaData(deploymentUnit);

            } catch (IOException e) {
                throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50017.name(), e);
            }
        }
    }

    private VDBMetaData parseVDBXML(VirtualFile file, DeploymentUnit deploymentUnit,
            DeploymentPhaseContext phaseContext, boolean xmlDeployment) throws DeploymentUnitProcessingException {
        try {
            VDBMetadataParser.validate(file.openStream());
            PropertyResolver propertyResolver = deploymentUnit.getAttachment(org.jboss.as.ee.metadata.property.Attachments.FINAL_PROPERTY_RESOLVER);
            PropertyReplacer replacer = PropertyReplacers.resolvingReplacer(propertyResolver);
            String vdbContents = replacer.replaceProperties(ObjectConverterUtil.convertToString(file.openStream()));
            VDBMetaData vdb = VDBMetadataParser.unmarshall(new ByteArrayInputStream(vdbContents.getBytes("UTF-8"))); //$NON-NLS-1$
            ServiceController<?> sc = phaseContext.getServiceRegistry().getService(TeiidServiceNames.OBJECT_SERIALIZER);
            ObjectSerializer serializer = ObjectSerializer.class.cast(sc.getValue());
            if (serializer.buildVdbXml(vdb).exists()) {
                vdb = VDBMetadataParser.unmarshall(new FileInputStream(serializer.buildVdbXml(vdb)));
            }
            vdb.setStatus(Status.LOADING);
            if (xmlDeployment) {
                vdb.setXmlDeployment(true);
            } else {
                String name = deploymentUnit.getName();
                String fileName = StringUtil.getLastToken(name, "/"); //$NON-NLS-1$
                int index = fileName.indexOf('.');
                int lastIndex = fileName.lastIndexOf('.');
                if (index > 0 && index != lastIndex && fileName.substring(0, index).equals(vdb.getName())) {
                    vdb.setVersion(name.substring(index+1, lastIndex));
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

    private VDBMetaData parseVDBDDL(VirtualFile file, DeploymentUnit deploymentUnit,
            DeploymentPhaseContext phaseContext, boolean xmlDeployment) throws DeploymentUnitProcessingException {
        try {
            PropertyReplacer replacer = deploymentUnit.getAttachment(org.jboss.as.ee.metadata.property.Attachments.FINAL_PROPERTY_REPLACER);
            String vdbContents = replacer.replaceProperties(ObjectConverterUtil.convertToString(file.openStream()));

            ObjectSerializer serializer = ObjectSerializer.class
                    .cast(phaseContext.getServiceRegistry().getService(TeiidServiceNames.OBJECT_SERIALIZER).getValue());

            DeploymentBasedDatabaseStore store = new DeploymentBasedDatabaseStore(vdbRepo);
            VDBMetaData vdb = store.getVDBMetadata(vdbContents);

            // if there is persisted one, let that be XML version for now.
            if (serializer.buildVdbXml(vdb).exists()) {
                vdb = VDBMetadataParser.unmarshall(new FileInputStream(serializer.buildVdbXml(vdb)));
            }

            vdb.setStatus(Status.LOADING);
            vdb.setXmlDeployment(xmlDeployment);
            deploymentUnit.putAttachment(TeiidAttachments.VDB_METADATA, vdb);
            LogManager.logDetail(LogConstants.CTX_RUNTIME,"VDB "+file.getName()+" has been parsed.");  //$NON-NLS-1$ //$NON-NLS-2$
            return vdb;
        } catch (XMLStreamException e) {
            throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50017.name(), e);
        } catch (IOException e) {
            throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50017.name(), e);
        }
    }

    public void undeploy(final DeploymentUnit context) {
    }

    protected VDBMetaData mergeMetaData(DeploymentUnit deploymentUnit) {
        VDBMetaData vdb = deploymentUnit.getAttachment(TeiidAttachments.VDB_METADATA);

        VirtualFile file = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT).getRoot();
        if (vdb == null) {
            LogManager.logError(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50016,file.getName()));
            return null;
        }

        LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB", file.getName(), "has been parsed."); //$NON-NLS-1$ //$NON-NLS-2$
        return vdb;
    }
}
