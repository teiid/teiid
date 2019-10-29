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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.script.ScriptEngineManager;

import org.jboss.as.naming.deployment.ContextNames;
import org.jboss.as.server.deployment.Attachments;
import org.jboss.as.server.deployment.DeploymentPhaseContext;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.as.server.deployment.DeploymentUnitProcessingException;
import org.jboss.as.server.deployment.DeploymentUnitProcessor;
import org.jboss.modules.ModuleClassLoader;
import org.jboss.msc.service.AbstractServiceListener;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceController.Mode;
import org.jboss.msc.service.ServiceController.State;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceTarget;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;
import org.jboss.vfs.VirtualFile;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDBImport;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.core.TeiidException;
import org.teiid.deployers.RuntimeVDB;
import org.teiid.deployers.TranslatorUtil;
import org.teiid.deployers.UDFMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.jboss.TeiidServiceNames.InvalidServiceNameException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.JBossVirtualFile;
import org.teiid.metadata.index.IndexMetadataRepository;
import org.teiid.query.metadata.VDBResources;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.vdb.runtime.VDBKey;


class VDBDeployer implements DeploymentUnitProcessor {
    private static final String JAVA_CONTEXT = "java:/"; //$NON-NLS-1$
    private TranslatorRepository translatorRepository;
    private VDBRepository vdbRepository;
    JBossLifeCycleListener shutdownListener;

    public VDBDeployer(TranslatorRepository translatorRepo,
            VDBRepository vdbRepo, JBossLifeCycleListener shutdownListener) {
        this.translatorRepository = translatorRepo;
        this.vdbRepository = vdbRepo;
        this.shutdownListener = shutdownListener;
    }

    public void deploy(final DeploymentPhaseContext context)  throws DeploymentUnitProcessingException {
        final DeploymentUnit deploymentUnit = context.getDeploymentUnit();
        if (!TeiidAttachments.isVDBDeployment(deploymentUnit)) {
            return;
        }
        final VDBMetaData deployment = deploymentUnit.getAttachment(TeiidAttachments.VDB_METADATA);

        VDBMetaData other = this.vdbRepository.getVDB(deployment.getName(), deployment.getVersion());
        if (other != null) {
            String deploymentName = other.getPropertyValue(TranslatorUtil.DEPLOYMENT_NAME);
            throw new DeploymentUnitProcessingException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50106, deployment, deploymentName));
        }

        deployment.addProperty(TranslatorUtil.DEPLOYMENT_NAME, deploymentUnit.getName());
        // check to see if there is old vdb already deployed.
        final ServiceController<?> controller = context.getServiceRegistry().getService(TeiidServiceNames.vdbServiceName(deployment.getName(), deployment.getVersion()));
        if (controller != null) {
            LogManager.logInfo(LogConstants.CTX_RUNTIME,  IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50019, deployment));
            controller.setMode(ServiceController.Mode.REMOVE);
        }

        boolean preview = deployment.isPreview();
        if (!preview && deployment.hasErrors()) {
            throw new DeploymentUnitProcessingException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50074, deployment));
        }

        // make sure the translator defined exists in configuration; otherwise add as error
        for (ModelMetaData model:deployment.getModelMetaDatas().values()) {
            if (!model.isSource() || model.getSourceNames().isEmpty()) {
                continue;
            }
            for (String source:model.getSourceNames()) {

                String translatorName = model.getSourceTranslatorName(source);
                if (deployment.isOverideTranslator(translatorName)) {
                    VDBTranslatorMetaData parent = deployment.getTranslator(translatorName);
                    translatorName = parent.getType();
                }

                Translator translator = this.translatorRepository.getTranslatorMetaData(translatorName);
                if ( translator == null) {
                    String msg = IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50077, translatorName, deployment.getName(), deployment.getVersion());
                    LogManager.logWarning(LogConstants.CTX_RUNTIME, msg);
                }
            }
        }

        // add VDB module's classloader as an attachment
        ModuleClassLoader classLoader = deploymentUnit.getAttachment(Attachments.MODULE).getClassLoader();
        deployment.addAttachment(ClassLoader.class, classLoader);
        deployment.addAttachment(ScriptEngineManager.class, new ScriptEngineManager(classLoader));

        try {
            EmbeddedServer.createPreParser(deployment);
        } catch (TeiidException e1) {
            throw new DeploymentUnitProcessingException(e1);
        }

        UDFMetaData udf = new UDFMetaData();
        udf.setFunctionClassLoader(classLoader);
        deployment.addAttachment(UDFMetaData.class, udf);

        VirtualFile file = deploymentUnit.getAttachment(Attachments.DEPLOYMENT_ROOT).getRoot();
        VDBResources resources;
        try {
            resources = new VDBResources(new JBossVirtualFile(file));
        } catch (IOException e) {
            throw new DeploymentUnitProcessingException(IntegrationPlugin.Event.TEIID50017.name(), e);
        }

        this.vdbRepository.addPendingDeployment(deployment);
        // build a VDB service
        final VDBService vdb = new VDBService(deployment, resources, shutdownListener);
        vdb.addMetadataRepository("index", new IndexMetadataRepository()); //$NON-NLS-1$

        final ServiceBuilder<RuntimeVDB> vdbService = context.getServiceTarget().addService(TeiidServiceNames.vdbServiceName(deployment.getName(), deployment.getVersion()), vdb);

        // add dependencies to data-sources
        dataSourceDependencies(deployment, context.getServiceTarget());

        for (VDBImport vdbImport : deployment.getVDBImports()) {
            VDBKey vdbKey = new VDBKey(vdbImport.getName(), vdbImport.getVersion());
            if (vdbKey.isAtMost()) {
                //TODO: could allow partial versions here if we canonicalize
                throw new DeploymentUnitProcessingException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40144, deployment, vdbKey));
            }
            LogManager.logInfo(LogConstants.CTX_RUNTIME,  IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50115, deployment, vdbKey));
            vdbService.addDependency(TeiidServiceNames.vdbFinishedServiceName(vdbImport.getName(), vdbKey.getVersion()));
        }

        // adding the translator services is redundant, however if one is removed then it is an issue.
        for (Model model:deployment.getModels()) {
            List<String> sourceNames = model.getSourceNames();
            for (String sourceName:sourceNames) {
                String translatorName = model.getSourceTranslatorName(sourceName);
                if (deployment.isOverideTranslator(translatorName)) {
                    VDBTranslatorMetaData translator = deployment.getTranslator(translatorName);
                    translatorName = translator.getType();
                }
                vdbService.addDependency(TeiidServiceNames.translatorServiceName(translatorName));
            }
        }

        ServiceName vdbSwitchServiceName = TeiidServiceNames.vdbSwitchServiceName(deployment.getName(), deployment.getVersion());
        vdbService.addDependency(TeiidServiceNames.VDB_REPO, VDBRepository.class,  vdb.vdbRepositoryInjector);
        vdbService.addDependency(TeiidServiceNames.TRANSLATOR_REPO, TranslatorRepository.class,  vdb.translatorRepositoryInjector);
        vdbService.addDependency(TeiidServiceNames.THREAD_POOL_SERVICE, Executor.class,  vdb.executorInjector);
        vdbService.addDependency(TeiidServiceNames.OBJECT_SERIALIZER, ObjectSerializer.class, vdb.serializerInjector);
        vdbService.addDependency(TeiidServiceNames.VDB_STATUS_CHECKER, VDBStatusChecker.class, vdb.vdbStatusCheckInjector);
        vdbService.addDependency(vdbSwitchServiceName, CountDownLatch.class, new InjectedValue<CountDownLatch>());
        //ensure that the secondary services have started
        vdbService.addDependency(TeiidServiceNames.MATVIEW_SERVICE);
        vdbService.addDependency(TeiidServiceNames.REST_WAR_SERVICE);

        // VDB restart switch, control the vdbservice by adding removing the switch service. If you
        // remove the service by setting status remove, there is no way start it back up if vdbservice used alone
        installVDBSwitchService(context.getServiceTarget(), vdbSwitchServiceName);

        vdbService.addListener(new AbstractServiceListener<Object>() {
            @Override
            public void transition(final ServiceController controller, final ServiceController.Transition transition) {
                if (transition.equals(ServiceController.Transition.DOWN_to_WAITING)) {
                    RuntimeVDB runtimeVDB = RuntimeVDB.class.cast(controller.getValue());
                    if (runtimeVDB != null && runtimeVDB.isRestartInProgress()) {
                        ServiceName vdbSwitchServiceName = TeiidServiceNames.vdbSwitchServiceName(deployment.getName(), deployment.getVersion());
                        ServiceController<?> switchSvc =  controller.getServiceContainer().getService(vdbSwitchServiceName);
                        if (switchSvc != null) {
                            CountDownLatch latch = CountDownLatch.class.cast(switchSvc.getValue());
                            try {
                                latch.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                // todo:log it?
                            }
                        }
                        installVDBSwitchService(controller.getServiceContainer(), vdbSwitchServiceName);
                    }
                }
            }
        });
        vdbService.setInitialMode(Mode.PASSIVE).install();
    }

    private void installVDBSwitchService(final ServiceTarget serviceTarget, ServiceName vdbSwitchServiceName) {
        // install switch service now.
        ServiceBuilder<CountDownLatch> svc = serviceTarget.addService(vdbSwitchServiceName, new Service<CountDownLatch>() {
            private CountDownLatch latch = new CountDownLatch(1);
            @Override
            public CountDownLatch getValue() throws IllegalStateException,IllegalArgumentException {
                return this.latch;
            }
            @Override
            public void start(StartContext context) throws StartException {
            }
            @Override
            public void stop(StopContext context) {
            }
        });
        svc.addListener(new AbstractServiceListener<Object>() {
            @Override
            public void transition(final ServiceController controller, final ServiceController.Transition transition) {
                if (transition.equals(ServiceController.Transition.REMOVING_to_REMOVED)) {
                    CountDownLatch latch = CountDownLatch.class.cast(controller.getValue());
                    latch.countDown();
                }
            }
        });
        svc.install();
    }

    static void addDataSourceListener(
            final ServiceTarget serviceTarget,
            final VDBKey vdbKey,
            final String dsName) {
        final String jndiName = getJndiName(dsName);
        ServiceName dsListenerServiceName;
        try {
            dsListenerServiceName = TeiidServiceNames.dsListenerServiceName(vdbKey.getName(), vdbKey.getVersion(), dsName);
        } catch (InvalidServiceNameException e) {
            LogManager.logWarning(LogConstants.CTX_RUNTIME, e, e.getMessage());
            return;
        }
        ContextNames.BindInfo bindInfo = ContextNames.bindInfoFor(jndiName);
        final ServiceName svcName = bindInfo.getBinderServiceName();
        DataSourceListener dsl = new DataSourceListener(dsName, svcName, vdbKey);
        ServiceBuilder<DataSourceListener> sb = serviceTarget.addService(dsListenerServiceName, dsl);
        sb.addDependency(svcName);
        sb.addDependency(TeiidServiceNames.VDB_STATUS_CHECKER, VDBStatusChecker.class, dsl.vdbStatusCheckInjector);
        sb.setInitialMode(Mode.PASSIVE).install();
    }

    private void dataSourceDependencies(VDBMetaData deployment, ServiceTarget serviceTarget) {
        final VDBKey vdbKey = new VDBKey(deployment.getName(), deployment.getVersion());
        Set<String> dataSources = new HashSet<String>();
        for (ModelMetaData model:deployment.getModelMetaDatas().values()) {
            for (String sourceName:model.getSourceNames()) {
                // Need to make the data source service as dependency; otherwise dynamic vdbs will not work correctly.
                String dsName = model.getSourceConnectionJndiName(sourceName);
                if (dsName == null) {
                    continue;
                }
                if (!dataSources.add(VDBStatusChecker.stripContext(dsName))) {
                    continue; //already listening
                }
                addDataSourceListener(serviceTarget, vdbKey, dsName);
            }
        }
    }

    static class DataSourceListener implements Service<DataSourceListener>{
        private String dsName;
        private ServiceName svcName;
        private VDBKey vdb;
        InjectedValue<VDBStatusChecker> vdbStatusCheckInjector = new InjectedValue<VDBStatusChecker>();

        public DataSourceListener(String dsName, ServiceName svcName, VDBKey vdb) {
            this.dsName = dsName;
            this.svcName = svcName;
            this.vdb = vdb;
        }

        public DataSourceListener getValue() throws IllegalStateException,IllegalArgumentException {
            return this;
        }

        @Override
        public void start(StartContext context) throws StartException {
            ServiceController<?> s = context.getController().getServiceContainer().getService(this.svcName);
            if (s != null) {
                this.vdbStatusCheckInjector.getValue().dataSourceAdded(this.dsName, vdb);
            }
        }

        @Override
        public void stop(StopContext context) {
            ServiceController<?> s = context.getController().getServiceContainer().getService(this.svcName);
            if (s.getMode().equals(Mode.REMOVE) || s.getState().equals(State.STOPPING)) {
                this.vdbStatusCheckInjector.getValue().dataSourceRemoved(this.dsName, vdb);
            }
        }
    }

    public static String getJndiName(String name) {
        String jndiName = name;
        if (!name.startsWith(JAVA_CONTEXT)) {
            jndiName = JAVA_CONTEXT + jndiName;
        }
        return jndiName;
    }

    @Override
    public void undeploy(final DeploymentUnit deploymentUnit) {
        if (!TeiidAttachments.isVDBDeployment(deploymentUnit)) {
            return;
        }

        final VDBMetaData deployment = deploymentUnit.getAttachment(TeiidAttachments.VDB_METADATA);
        if (!this.shutdownListener.isShutdownInProgress()) {
            final VDBMetaData vdb = deploymentUnit.getAttachment(TeiidAttachments.VDB_METADATA);

            ServiceController<?> sc = deploymentUnit.getServiceRegistry().getService(TeiidServiceNames.OBJECT_SERIALIZER);
            if (sc != null) {
                ObjectSerializer serilalizer = ObjectSerializer.class.cast(sc.getValue());
                serilalizer.removeAttachments(vdb);
                LogManager.logTrace(LogConstants.CTX_RUNTIME, "VDB "+vdb.getName()+" metadata removed"); //$NON-NLS-1$ //$NON-NLS-2$
            }
        }
        this.vdbRepository.removeVDB(deployment.getName(), deployment.getVersion());

        ServiceController<?> switchSvc = deploymentUnit.getServiceRegistry().getService(TeiidServiceNames.vdbSwitchServiceName(deployment.getName(), deployment.getVersion()));
        if (switchSvc != null) {
            switchSvc.setMode(ServiceController.Mode.REMOVE);
        }

        for (ModelMetaData model:deployment.getModelMetaDatas().values()) {
            for (SourceMappingMetadata smm:model.getSources().values()) {
                String dsName = smm.getConnectionJndiName();
                if (dsName == null) {
                    continue;
                }

                ServiceController<?> dsService;
                try {
                    dsService = deploymentUnit.getServiceRegistry().getService(TeiidServiceNames.dsListenerServiceName(deployment.getName(), deployment.getVersion(), dsName));
                } catch (InvalidServiceNameException e) {
                    continue;
                }
                if (dsService != null) {
                    dsService.setMode(ServiceController.Mode.REMOVE);
                }
            }
        }

        final ServiceController<?> controller = deploymentUnit.getServiceRegistry().getService(TeiidServiceNames.vdbServiceName(deployment.getName(), deployment.getVersion()));
        if (controller != null) {
            controller.setMode(ServiceController.Mode.REMOVE);
        }
    }
}
