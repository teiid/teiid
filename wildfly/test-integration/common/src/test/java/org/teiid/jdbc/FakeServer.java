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
package org.teiid.jdbc;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.transaction.TransactionManager;
import javax.xml.stream.XMLStreamException;

import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBImportMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataParser;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.SimpleMock;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.service.BufferService;
import org.teiid.dqp.service.FakeBufferService;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.metadata.VDBResource;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.metadata.index.VDBMetadataFactory.IndexVDB;
import org.teiid.query.metadata.VDBResources.Resource;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.SourceCapabilities;
import org.teiid.resource.api.XAImporter;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.services.SessionServiceImpl;
import org.teiid.translator.TranslatorException;
import org.teiid.transport.ClientServiceRegistryImpl;

@SuppressWarnings({"nls"})
public class FakeServer extends EmbeddedServer {

    public static class DeployVDBParameter {
        public Map<String, Collection<FunctionMethod>> udfs;
        public MetadataRepository<?, ?> metadataRepo;
        public List<VDBImportMetadata> vdbImports;
        public LinkedHashMap<String, Resource> vdbResources;
        public boolean useVdbXml;
        public boolean useDataRoles;

        public DeployVDBParameter(Map<String, Collection<FunctionMethod>> udfs,
                MetadataRepository<?, ?> metadataRepo) {
            this.udfs = udfs;
            this.metadataRepo = metadataRepo;
        }
    }

    private boolean realBufferManager;

    @SuppressWarnings("serial")
    public FakeServer(boolean start) {
        waitForLoad = true;
        cmr = new ProviderAwareConnectorManagerRepository() {
            @Override
            public ConnectorManager getConnectorManager(String connectorName) {
                ConnectorManager cm = super.getConnectorManager(connectorName);
                if (cm != null) {
                    return cm;
                }
                if (connectorName.equalsIgnoreCase("source")) {
                    return new ConnectorManager("x", "x") {
                        @Override
                        public SourceCapabilities getCapabilities() {
                            return new BasicSourceCapabilities();
                        }
                    };
                }
                return null;
            }
        };
        if (start) {
            start(new EmbeddedConfiguration(), false);
        }
    }

    public void start(EmbeddedConfiguration config, boolean realBufferMangaer) {
        boolean detectTxn = true;
        if (config.getTransactionManager() == null) {
            config.setTransactionManager(SimpleMock.createSimpleMock(TransactionManager.class));
            this.transactionService.setXaImporter(SimpleMock.createSimpleMock(XAImporter.class));
            detectTxn = false;
        }
        this.realBufferManager = realBufferMangaer;
        start(config);
        this.transactionService.setDetectTransactions(detectTxn);
    }

    @Override
    protected BufferService getBufferService() {
        if (!realBufferManager) {
            FakeBufferService fbs = new FakeBufferService(false);
            this.bufferService.setBufferManager(fbs.getBufferManager());
            return bufferService;
        }
        bufferService.setDiskDirectory(UnitTestUtil.getTestScratchPath());
        return super.getBufferService();
    }

    public DQPCore getDqp() {
        return dqp;
    }

    public ConnectorManagerRepository getConnectorManagerRepository() {
        return cmr;
    }

    public void setConnectorManagerRepository(ConnectorManagerRepository cmr) {
        this.cmr = cmr;
    }

    public void setUseCallingThread(boolean useCallingThread) {
        this.useCallingThread = useCallingThread;
    }

    public void deployVDB(String vdbName, String vdbPath) throws Exception {
        deployVDB(vdbName, vdbPath, new DeployVDBParameter(null, null));
    }

    public void deployVDB(String vdbName, String vdbPath, DeployVDBParameter parameterObject) throws Exception {
        IndexVDB imf = VDBMetadataFactory.loadMetadata(vdbName, new File(vdbPath).toURI().toURL());
        parameterObject.vdbResources = imf.resources.getEntriesPlusVisibilities();
        deployVDB(vdbName, imf.store, parameterObject);
    }

    public void deployVDB(String vdbName, MetadataStore metadata) {
        deployVDB(vdbName, metadata, new DeployVDBParameter(null, null));
    }

    public void deployVDB(String vdbName, MetadataStore metadata, DeployVDBParameter parameterObject) {
        VDBMetaData vdbMetaData = null;
        try {
            if (parameterObject.vdbResources != null && parameterObject.useVdbXml) {
                VDBResource resource = parameterObject.vdbResources.get("/META-INF/vdb.xml");
                if (resource !=null) {
                    vdbMetaData = VDBMetadataParser.unmarshall(resource.openStream());
                }
            }
            if (vdbMetaData == null) {
                vdbMetaData = new VDBMetaData();
                vdbMetaData.setName(vdbName);

                for (Schema schema : metadata.getSchemas().values()) {
                    ModelMetaData model = addModel(vdbMetaData, schema);
                    if (parameterObject.metadataRepo != null) {
                        model.addAttachment(MetadataRepository.class, parameterObject.metadataRepo);
                        //fakeserver does not load through the repository framework, so call load after the fact here.
                        MetadataFactory mf = createMetadataFactory(vdbMetaData, metadata, model, parameterObject.vdbResources);
                        mf.setSchema(schema);
                        try {
                            parameterObject.metadataRepo.loadMetadata(mf, null, null);
                        } catch (TranslatorException e) {
                            throw new TeiidRuntimeException(e);
                        }
                    }
                }
            } else {
                if (!parameterObject.useDataRoles) {
                    vdbMetaData.setDataPolicies(Collections.EMPTY_LIST);
                }
                cmr.createConnectorManagers(vdbMetaData, this);
            }

            UDFMetaData udfMetaData = null;
            if (parameterObject.udfs != null) {
                udfMetaData = new UDFMetaData();
                for (Map.Entry<String, Collection<FunctionMethod>> entry : parameterObject.udfs.entrySet()) {
                    udfMetaData.addFunctions(entry.getKey(), entry.getValue());
                }
            }

            if (parameterObject.vdbImports != null) {
                for (VDBImportMetadata vdbImport : parameterObject.vdbImports) {
                    vdbMetaData.getVDBImports().add(vdbImport);
                }
            }

            vdbMetaData.setStatus(VDB.Status.ACTIVE);
            this.repo.addVDB(vdbMetaData, metadata, parameterObject.vdbResources, udfMetaData, cmr);
            this.repo.finishDeployment(vdbMetaData.getName(), vdbMetaData.getVersion());
            this.repo.getLiveVDB(vdbMetaData.getName(), vdbMetaData.getVersion()).setStatus(VDB.Status.ACTIVE);
        } catch (VirtualDatabaseException e) {
            throw new RuntimeException(e);
        } catch (ConnectorManagerException e) {
            throw new RuntimeException(e);
        } catch (XMLStreamException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void deployVDB(VDBMetaData vdb) throws ConnectorManagerException,
            VirtualDatabaseException, TranslatorException {
        super.deployVDB(vdb, null);
    }

    public void removeVDB(String vdbName) {
        undeployVDB(vdbName);
    }

    private ModelMetaData addModel(VDBMetaData vdbMetaData, Schema schema) {
        ModelMetaData model = new ModelMetaData();
        model.setModelType(schema.isPhysical()?Type.PHYSICAL:Type.VIRTUAL);
        model.setName(schema.getName());
        vdbMetaData.addModel(model);
        model.addSourceMapping("source", "translator", "jndi:source");
        return model;
    }

    public VDBMetaData getVDB(String vdbName) {
        return this.repo.getLiveVDB(vdbName);
    }

    public ConnectionImpl createConnection(String embeddedURL) throws Exception {
        return getDriver().connect(embeddedURL, null);
    }

    public ConnectionImpl createConnection(String embeddedURL, Properties prop) throws Exception {
        return getDriver().connect(embeddedURL, prop);
    }

    public ClientServiceRegistryImpl getClientServiceRegistry() {
        return services;
    }

    public void setThrowMetadataErrors(boolean throwMetadataErrors) {
        this.throwMetadataErrors = throwMetadataErrors;
    }

    public SessionServiceImpl getSessionService() {
        return this.sessionService;
    }

}
