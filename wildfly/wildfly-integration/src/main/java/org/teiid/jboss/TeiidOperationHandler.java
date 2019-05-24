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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Future;

import org.jboss.as.connector.metadata.xmldescriptors.ConnectorXmlDescriptor;
import org.jboss.as.connector.services.resourceadapters.deployment.InactiveResourceAdapterDeploymentService.InactiveResourceAdapterDeployment;
import org.jboss.as.connector.util.ConnectorServices;
import org.jboss.as.controller.AbstractWriteAttributeHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.jca.common.api.metadata.spec.ConfigProperty;
import org.jboss.jca.common.api.metadata.spec.ConnectionDefinition;
import org.jboss.jca.common.api.metadata.spec.ResourceAdapter;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceRegistry;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.Admin.SchemaObjectType;
import org.teiid.adminapi.Admin.TranlatorPropertyType;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.CacheStatisticsMetadata;
import org.teiid.adminapi.impl.EngineStatisticsMetadata;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.TransactionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.adminapi.impl.WorkerPoolStatisticsMetadata;
import org.teiid.adminapi.jboss.VDBMetadataMapper;
import org.teiid.adminapi.jboss.VDBMetadataMapper.TransactionMetadataMapper;
import org.teiid.adminapi.jboss.VDBMetadataMapper.VDBTranslatorMetaDataMapper;
import org.teiid.client.plan.PlanNode;
import org.teiid.core.TeiidComponentException;
import org.teiid.deployers.ExtendedPropertyMetadata;
import org.teiid.deployers.ExtendedPropertyMetadataList;
import org.teiid.deployers.RuntimeVDB;
import org.teiid.deployers.RuntimeVDB.ReplaceResult;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.dqp.service.SessionService;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.jboss.TeiidServiceNames.InvalidServiceNameException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.Database;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.runtime.EmbeddedAdminFactory;
import org.teiid.translator.TranslatorProperty.PropertyType;
import org.teiid.vdb.runtime.VDBKey;

/**
 * Keep this the class and all the extended classes stateless as there is single instance.
 */
abstract class TeiidOperationHandler extends BaseOperationHandler<DQPCore> {
    protected TeiidOperationHandler(String operationName){
        super(operationName);
    }

    protected TeiidOperationHandler(String operationName,boolean changesRuntime){
        super(operationName, changesRuntime);
    }

    static VDBMetaData checkVDB(OperationContext context, String vdbName,
            String vdbVersion) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_REPO);
        VDBRepository repo = VDBRepository.class.cast(sc.getValue());
        VDBMetaData vdb = repo.getLiveVDB(vdbName, vdbVersion);
        if (vdb == null) {
            throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50102, vdbName, vdbVersion));
        }
        Status status = vdb.getStatus();
        if (status != VDB.Status.ACTIVE) {
            throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50096, vdbName, vdbVersion));
        }
        return vdb;
    }

    @Override
    protected DQPCore getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
        ServiceController<?> repo = context.getServiceRegistry(isChangesRuntimes()).getRequiredService(TeiidServiceNames.ENGINE);
        if (repo != null) {
            return  DQPCore.class.cast(repo.getValue());
        }
        return null;
    }

    protected BufferManagerService getBufferManager(OperationContext context) {
        ServiceController<?> repo = context.getServiceRegistry(isChangesRuntimes()).getRequiredService(TeiidServiceNames.BUFFER_MGR);
        if (repo != null) {
            return BufferManagerService.class.cast(repo.getService());
        }
        return null;
    }

    protected VDBRepository getVDBrepository(OperationContext context) {
        ServiceController<?> repo = context.getServiceRegistry(isChangesRuntimes()).getRequiredService(TeiidServiceNames.VDB_REPO);
        if (repo != null) {
            return VDBRepository.class.cast(repo.getValue());
        }
        return null;
    }

    protected int getSessionCount(OperationContext context) throws AdminException {
        try {
            return getSessionService(context).getActiveSessionsCount();
        } catch (SessionServiceException e) {
             throw new AdminComponentException(IntegrationPlugin.Event.TEIID50056, e);
        }
    }

    protected SessionService getSessionService(OperationContext context){
        return (SessionService) context.getServiceRegistry(false).getService(TeiidServiceNames.SESSION).getValue();
    }

    public static ModelNode executeQuery(final VDBMetaData vdb,  final DQPCore engine, final String command, final long timoutInMilli, final ModelNode resultsNode, final boolean timeAsString) throws OperationFailedException {
        String user = "CLI ADMIN"; //$NON-NLS-1$
        LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("admin_executing", user, command)); //$NON-NLS-1$

        try {
            Future<?> f = DQPCore.executeQuery(command, vdb, user, "admin-console", timoutInMilli, engine, new DQPCore.ResultsListener() { //$NON-NLS-1$
                @Override
                public void onResults(List<String> columns, List<? extends List<?>> results) throws Exception {
                    writeResults(resultsNode, columns, results, timeAsString);
                }
            });
            f.get();
        } catch (Throwable e) {
            throw new OperationFailedException(e);
        }
        return resultsNode;
    }

    private static void writeResults(ModelNode resultsNode, List<String> columns,  List<? extends List<?>> results, boolean timeAsString) throws SQLException {
        for (List<?> row:results) {
            ModelNode rowNode = new ModelNode();

            for (int colNum = 0; colNum < columns.size(); colNum++) {

                Object aValue = row.get(colNum);
                if (aValue != null) {
                    if (aValue instanceof Integer) {
                        rowNode.get(columns.get(colNum)).set((Integer)aValue);
                    }
                    else if (aValue instanceof Long) {
                        rowNode.get(columns.get(colNum)).set((Long)aValue);
                    }
                    else if (aValue instanceof Double) {
                        rowNode.get(columns.get(colNum)).set((Double)aValue);
                    }
                    else if (aValue instanceof Boolean) {
                        rowNode.get(columns.get(colNum)).set((Boolean)aValue);
                    }
                    else if (aValue instanceof BigInteger) {
                        rowNode.get(columns.get(colNum)).set((BigInteger)aValue);
                    }
                    else if (aValue instanceof BigDecimal) {
                        rowNode.get(columns.get(colNum)).set((BigDecimal)aValue);
                    }
                    else if (aValue instanceof java.sql.Timestamp && !timeAsString) {
                        rowNode.get(columns.get(colNum)).set(((java.sql.Timestamp)aValue).getTime());
                    }
                    else if (aValue instanceof java.sql.Date && !timeAsString) {
                        rowNode.get(columns.get(colNum)).set(((java.sql.Date)aValue).getTime());
                    }
                    else if (aValue instanceof java.sql.Time && !timeAsString) {
                        rowNode.get(columns.get(colNum)).set(((java.sql.Time)aValue).getTime());
                    }
                    else if (aValue instanceof String) {
                        rowNode.get(columns.get(colNum), TYPE).set(ModelType.STRING);
                        rowNode.get(columns.get(colNum)).set((String)aValue);
                    }
                    else if (aValue instanceof Blob) {
                        rowNode.get(columns.get(colNum), TYPE).set(ModelType.OBJECT);
                        rowNode.get(columns.get(colNum)).set("blob"); //$NON-NLS-1$
                    }
                    else if (aValue instanceof Clob) {
                        rowNode.get(columns.get(colNum), TYPE).set(ModelType.OBJECT);
                        rowNode.get(columns.get(colNum)).set("clob"); //$NON-NLS-1$
                    }
                    else if (aValue instanceof SQLXML) {
                        SQLXML xml = (SQLXML)aValue;
                        rowNode.get(columns.get(colNum), TYPE).set(ModelType.STRING);
                        rowNode.get(columns.get(colNum)).set(xml.getString());
                    }
                    else {
                        rowNode.get(columns.get(colNum), TYPE).set(ModelType.STRING);
                        rowNode.get(columns.get(colNum)).set(aValue.toString());
                    }
                }
            }
            resultsNode.add(rowNode);
        }
    }
}

abstract class TranslatorOperationHandler extends BaseOperationHandler<TranslatorRepository> {

    protected TranslatorOperationHandler(String operationName){
        super(operationName);
    }

    @Override
    public TranslatorRepository getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.TRANSLATOR_REPO);
        return TranslatorRepository.class.cast(sc.getValue());
    }
}

class GetRuntimeVersion extends TeiidOperationHandler{
    protected GetRuntimeVersion(String operationName) {
        super(operationName);
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        context.getResult().set(engine.getRuntimeVersion());
    }
    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.setReplyType(ModelType.STRING);
    }
}

/**
 * Since all the properties in the DQP/Buffer Manager etc needs restart, just save it to the configuration
 * then restart will apply correctly to the buffer manager.
 */
class AttributeWrite extends AbstractWriteAttributeHandler<Void> {

    public AttributeWrite(AttributeDefinition... attr) {
        super(attr);
    }

    @Override
    public void execute(OperationContext context, ModelNode operation)
            throws OperationFailedException {
        final String attributeName = operation.require(NAME).asString();
        System.out.println(attributeName);
        if (attributeName.startsWith("buffer-service")) { //$NON-NLS-1$
            final AttributeDefinition attributeDefinition = getAttributeDefinition(attributeName);
            String newAttribute = "buffer-manager-" + attributeDefinition.getXmlName(); //$NON-NLS-1$
            boolean defined = operation.hasDefined(VALUE);
            ModelNode newValue = defined ? operation.get(VALUE) : new ModelNode();

            if (defined &&
                    (newAttribute.equals(Element.BUFFER_MANAGER_MAX_STORAGE_OBJECT_SIZE_ATTRIBUTE.getModelName())
                            || newAttribute.equals(Element.BUFFER_MANAGER_MAX_RESERVED_MB_ATTRIBUTE.getModelName()))) {
                int value = newValue.asInt();
                if (value > 0) {
                    value = value/1024;
                }
                newValue = new ModelNode(value);
            }

            PathAddress currentAddress = context.getCurrentAddress();
            operation = Util.createOperation(ModelDescriptionConstants.WRITE_ATTRIBUTE_OPERATION, currentAddress);
            operation.get(ModelDescriptionConstants.NAME).set(newAttribute);
            operation.get(ModelDescriptionConstants.VALUE).set(newValue);
            super.execute(context, operation);
            return;
        }
        super.execute(context, operation);
    }

    @Override
    protected boolean applyUpdateToRuntime(OperationContext context,ModelNode operation,String attributeName,ModelNode resolvedValue,
            ModelNode currentValue, org.jboss.as.controller.AbstractWriteAttributeHandler.HandbackHolder<Void> handbackHolder)
            throws OperationFailedException {
        return true;
    }

    @Override
    protected void revertUpdateToRuntime(OperationContext context, ModelNode operation, String attributeName,
            ModelNode valueToRestore, ModelNode valueToRevert, Void handback)
            throws OperationFailedException {
    }
}

class GetActiveSessionsCount extends TeiidOperationHandler{
    protected GetActiveSessionsCount(String operationName) {
        super(operationName);
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        try {
            context.getResult().set(getSessionCount(context));
        } catch (AdminException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.setReplyType(ModelType.INT);
    }
}

class ListSessions extends TeiidOperationHandler{
    protected ListSessions() {
        super("list-sessions"); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        String vdbName = null;
        String version = null;
        VDBKey vdbKey = null;

        if (operation.hasDefined(OperationsConstants.OPTIONAL_VDB_VERSION.getName()) && operation.hasDefined(OperationsConstants.OPTIONAL_VDB_NAME.getName())) {
            vdbName = operation.get(OperationsConstants.OPTIONAL_VDB_NAME.getName()).asString();
            version = operation.get(OperationsConstants.OPTIONAL_VDB_VERSION.getName()).asString();
            VDBMetaData metadata = checkVDB(context, vdbName, version);
            vdbKey = metadata.getAttachment(VDBKey.class);
            if (vdbKey == null) {
                vdbKey = new VDBKey(vdbName, version);
            }
        }

        ModelNode result = context.getResult();
        Collection<SessionMetadata> sessions = null;
        if (vdbKey != null) {
            sessions = getSessionService(context).getSessionsLoggedInToVDB(vdbKey);
        } else {
            sessions = getSessionService(context).getActiveSessions();
        }
        for (SessionMetadata session:sessions) {
            VDBMetadataMapper.SessionMetadataMapper.INSTANCE.wrap(session, result.add());
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.OPTIONAL_VDB_NAME);
        builder.addParameter(OperationsConstants.OPTIONAL_VDB_VERSION);
        builder.setReplyType(ModelType.LIST);
        builder.setReplyParameters(VDBMetadataMapper.SessionMetadataMapper.INSTANCE.getAttributeDefinitions());
    }
}

class ListRequestsPerSession extends TeiidOperationHandler{
    protected ListRequestsPerSession() {
        super("list-requests-per-session"); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        if (!operation.hasDefined(OperationsConstants.SESSION.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.SESSION.getName()+MISSING));
        }
        boolean includeSourceQueries = true;
        if (operation.hasDefined(OperationsConstants.INCLUDE_SOURCE.getName())) {
            includeSourceQueries = operation.get(OperationsConstants.INCLUDE_SOURCE.getName()).asBoolean();
        }
        ModelNode result = context.getResult();
        List<RequestMetadata> requests = engine.getRequestsForSession(operation.get(OperationsConstants.SESSION.getName()).asString());
        for (RequestMetadata request:requests) {
            if (request.sourceRequest()) {
                if (includeSourceQueries) {
                    VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
                }
            }
            else {
                VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
            }
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.SESSION);
        builder.addParameter(OperationsConstants.INCLUDE_SOURCE);
        builder.setReplyType(ModelType.LIST);
        builder.setReplyParameters(VDBMetadataMapper.RequestMetadataMapper.INSTANCE.getAttributeDefinitions());
    }
}

class ListRequests extends TeiidOperationHandler{
    protected ListRequests() {
        super("list-requests"); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        boolean includeSourceQueries = true;
        if (operation.hasDefined(OperationsConstants.INCLUDE_SOURCE.getName())) {
            includeSourceQueries = operation.get(OperationsConstants.INCLUDE_SOURCE.getName()).asBoolean();
        }

        ModelNode result = context.getResult();
        List<RequestMetadata> requests = engine.getRequests();
        for (RequestMetadata request:requests) {
            if (request.sourceRequest()) {
                if (includeSourceQueries) {
                    VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
                }
            }
            else {
                VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
            }
        }
    }
    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.INCLUDE_SOURCE);
        builder.setReplyType(ModelType.LIST);
        builder.setReplyParameters(VDBMetadataMapper.RequestMetadataMapper.INSTANCE.getAttributeDefinitions());
    }
}

class ListRequestsPerVDB extends TeiidOperationHandler{
    protected ListRequestsPerVDB() {
        super("list-requests-per-vdb"); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        if (!operation.hasDefined(OperationsConstants.VDB_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME.getName()+MISSING));
        }
        if (!operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION.getName()+MISSING));
        }

        boolean includeSourceQueries = true;
        if (operation.hasDefined(OperationsConstants.INCLUDE_SOURCE.getName())) {
            includeSourceQueries = operation.get(OperationsConstants.INCLUDE_SOURCE.getName()).asBoolean();
        }

        ModelNode result = context.getResult();
        String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
        String vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asString();
        checkVDB(context, vdbName, vdbVersion);
        List<RequestMetadata> requests = new ArrayList<RequestMetadata>();
        Collection<SessionMetadata> sessions = getSessionService(context).getActiveSessions();
        for (SessionMetadata session:sessions) {
            if (vdbName.equals(session.getVDBName()) && session.getVDBVersion().equals(vdbVersion)) {
                requests.addAll(engine.getRequestsForSession(session.getSessionId()));
            }
        }
        for (RequestMetadata request:requests) {
            if (request.sourceRequest()) {
                if (includeSourceQueries) {
                    VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
                }
            }
            else {
                VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
            }
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.VDB_NAME);
        builder.addParameter(OperationsConstants.VDB_VERSION);
        builder.addParameter(OperationsConstants.INCLUDE_SOURCE);

        builder.setReplyType(ModelType.LIST);
        builder.setReplyParameters(VDBMetadataMapper.RequestMetadataMapper.INSTANCE.getAttributeDefinitions());
    }
}

class ListLongRunningRequests extends TeiidOperationHandler{
    protected ListLongRunningRequests() {
        super("list-long-running-requests"); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        ModelNode result = context.getResult();
        List<RequestMetadata> requests = engine.getLongRunningRequests();
        for (RequestMetadata request:requests) {
            VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
        }
    }
    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.setReplyType(ModelType.LIST);
        builder.setReplyParameters(VDBMetadataMapper.RequestMetadataMapper.INSTANCE.getAttributeDefinitions());
    }
}

class TerminateSession extends TeiidOperationHandler{
    protected TerminateSession() {
        super("terminate-session", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        if (!operation.hasDefined(OperationsConstants.SESSION.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.SESSION.getName()+MISSING));
        }
        getSessionService(context).terminateSession(operation.get(OperationsConstants.SESSION.getName()).asString(), null);
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.SESSION);
    }
}

class CancelRequest extends TeiidOperationHandler{
    protected CancelRequest() {
        super("cancel-request", true); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        try {
            if (!operation.hasDefined(OperationsConstants.SESSION.getName())) {
                throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.SESSION.getName()+MISSING));
            }
            if (!operation.hasDefined(OperationsConstants.EXECUTION_ID.getName())) {
                throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.EXECUTION_ID.getName()+MISSING));
            }
            boolean pass = engine.cancelRequest(operation.get(OperationsConstants.SESSION.getName()).asString(), operation.get(OperationsConstants.EXECUTION_ID.getName()).asLong());
            ModelNode result = context.getResult();

            result.set(pass);
        } catch (TeiidComponentException e) {
            throw new OperationFailedException(e, new ModelNode().set(e.getMessage()));
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.SESSION);
        builder.addParameter(OperationsConstants.EXECUTION_ID);
        builder.setReplyType(ModelType.BOOLEAN);
    }
}

class GetPlan extends TeiidOperationHandler{
    protected GetPlan() {
        super("get-query-plan"); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        if (!operation.hasDefined(OperationsConstants.SESSION.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.SESSION.getName()+MISSING));
        }
        if (!operation.hasDefined(OperationsConstants.EXECUTION_ID.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.EXECUTION_ID.getName()+MISSING));
        }
        PlanNode plan = engine.getPlan(operation.get(OperationsConstants.SESSION.getName()).asString(), operation.get(OperationsConstants.EXECUTION_ID.getName()).asLong());
        ModelNode result = context.getResult();

        if (plan != null) {
            result.set(plan.toXml());
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.SESSION);
        builder.addParameter(OperationsConstants.EXECUTION_ID);
        builder.setReplyType(ModelType.STRING);
    }
}

abstract class BaseCachehandler extends BaseOperationHandler<SessionAwareCache>{
    BaseCachehandler(String operationName){
        super(operationName);
    }

    BaseCachehandler(String operationName, boolean changesRuntimeState){
        super(operationName, changesRuntimeState);
    }

    @Override
    protected SessionAwareCache getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
        String cacheType = Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name();

        if (operation.hasDefined(OperationsConstants.CACHE_TYPE.getName())) {
            cacheType = operation.get(OperationsConstants.CACHE_TYPE.getName()).asString();
        }

        ServiceController<?> sc;
        try {
            if (Admin.Cache.valueOf(cacheType) == Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE) {
                sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.CACHE_RESULTSET);
            }
            else {
                sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.CACHE_PREPAREDPLAN);
            }
        } catch (IllegalArgumentException e) {
            throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50071, cacheType));
        }

        return SessionAwareCache.class.cast(sc.getValue());
    }
}


class CacheTypes extends BaseOperationHandler<Void> {
    protected CacheTypes() {
        super("cache-types"); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, Void cache, ModelNode operation) throws OperationFailedException {
        ModelNode result = context.getResult();
        Collection<String> types = SessionAwareCache.getCacheTypes();
        for (String type:types) {
            result.add(type);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.setReplyType(ModelType.LIST);
        builder.setReplyValueType(ModelType.STRING);
    }
}

class ClearCache extends BaseCachehandler {

    protected ClearCache() {
        super("clear-cache", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, SessionAwareCache cache, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.CACHE_TYPE.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.CACHE_TYPE.getName()+MISSING));
        }

        String cacheType = operation.get(OperationsConstants.CACHE_TYPE.getName()).asString();

        if (operation.hasDefined(OperationsConstants.VDB_NAME.getName()) && operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
            String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
            String vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asString();
            TeiidOperationHandler.checkVDB(context, vdbName, vdbVersion);
            LogManager.logInfo(LogConstants.CTX_DQP, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50005, cacheType, vdbName, vdbVersion));
            cache.clearForVDB(vdbName, vdbVersion);
        }
        else {
            LogManager.logInfo(LogConstants.CTX_DQP, IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50098, cacheType));
            cache.clearAll();
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.CACHE_TYPE);
        builder.addParameter(OperationsConstants.OPTIONAL_VDB_NAME);
        builder.addParameter(OperationsConstants.OPTIONAL_VDB_VERSION);
    }
}

class CacheStatistics extends BaseCachehandler {

    protected CacheStatistics() {
        super("cache-statistics"); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, SessionAwareCache cache, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.CACHE_TYPE.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.CACHE_TYPE.getName()+MISSING));
        }
        String cacheType = operation.get(OperationsConstants.CACHE_TYPE.getName()).asString();

        ModelNode result = context.getResult();
        CacheStatisticsMetadata stats = cache.buildCacheStats(cacheType);
        VDBMetadataMapper.CacheStatisticsMetadataMapper.INSTANCE.wrap(stats, result);
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.CACHE_TYPE);
        builder.setReplyType(ModelType.OBJECT);
        builder.setReplyParameters(VDBMetadataMapper.CacheStatisticsMetadataMapper.INSTANCE.getAttributeDefinitions());
    }
}

class MarkDataSourceAvailable extends TeiidOperationHandler{
    protected MarkDataSourceAvailable() {
        super("mark-datasource-available", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.DS_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.DS_NAME.getName()+MISSING));
        }
        String dsName = operation.get(OperationsConstants.DS_NAME.getName()).asString();
        ServiceController<?> sc = context.getServiceRegistry(isChangesRuntimes()).getRequiredService(TeiidServiceNames.VDB_STATUS_CHECKER);
        VDBStatusChecker vsc = VDBStatusChecker.class.cast(sc.getValue());
        vsc.dataSourceAdded(dsName, null);
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.DS_NAME);
    }
}

class WorkerPoolStatistics extends TeiidOperationHandler{

    protected WorkerPoolStatistics() {
        super("workerpool-statistics"); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {
        ModelNode result = context.getResult();
        WorkerPoolStatisticsMetadata stats = engine.getWorkerPoolStatistics();
        VDBMetadataMapper.WorkerPoolStatisticsMetadataMapper.INSTANCE.wrap(stats, result);
    }
    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.setReplyType(ModelType.OBJECT);
        builder.setReplyParameters(VDBMetadataMapper.WorkerPoolStatisticsMetadataMapper.INSTANCE.getAttributeDefinitions());
    }
}

class ListTransactions extends TeiidOperationHandler{

    protected ListTransactions() {
        super("list-transactions"); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {
        ModelNode result = context.getResult();
        Collection<TransactionMetadata> txns = engine.getTransactions();
        for (TransactionMetadata txn:txns) {
            VDBMetadataMapper.TransactionMetadataMapper.INSTANCE.wrap(txn, result.add());
        }
    }
    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.setReplyType(ModelType.LIST);
        builder.setReplyParameters(TransactionMetadataMapper.INSTANCE.getAttributeDefinitions());

    }
}

class TerminateTransaction extends TeiidOperationHandler{

    protected TerminateTransaction() {
        super("terminate-transaction", true); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {

        if (!operation.hasDefined(OperationsConstants.XID.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.XID.getName()+MISSING));
        }

        String xid = operation.get(OperationsConstants.XID.getName()).asString();
        try {
            engine.terminateTransaction(xid);
        } catch (AdminException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.XID);
    }
}

class ExecuteQuery extends TeiidOperationHandler{

    protected ExecuteQuery() {
        super("execute-query", true); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {

        if (!operation.hasDefined(OperationsConstants.VDB_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME.getName()+MISSING));
        }
        if (!operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION.getName()+MISSING));
        }
        if (!operation.hasDefined(OperationsConstants.SQL_QUERY.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.SQL_QUERY.getName()+MISSING));
        }
        if (!operation.hasDefined(OperationsConstants.TIMEOUT_IN_MILLI.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.TIMEOUT_IN_MILLI.getName()+MISSING));
        }

        ModelNode result = context.getResult();
        String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
        String vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asString();
        String sql = operation.get(OperationsConstants.SQL_QUERY.getName()).asString();
        int timeout = operation.get(OperationsConstants.TIMEOUT_IN_MILLI.getName()).asInt();

        VDBMetaData vdb = checkVDB(context, vdbName, vdbVersion);

        result.set(executeQuery(vdb, engine, sql, timeout, new ModelNode(), true));
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.VDB_NAME);
        builder.addParameter(OperationsConstants.VDB_VERSION);
        builder.addParameter(OperationsConstants.SQL_QUERY);
        builder.addParameter(OperationsConstants.TIMEOUT_IN_MILLI);

        builder.setReplyType(ModelType.LIST);
        builder.setReplyValueType(ModelType.STRING);
    }
}

class GetVDB extends BaseOperationHandler<VDBRepository>{

    protected GetVDB() {
        super("get-vdb"); //$NON-NLS-1$
    }

    @Override
    protected VDBRepository getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_REPO);
        return VDBRepository.class.cast(sc.getValue());
    }

    @Override
    protected void executeOperation(OperationContext context, VDBRepository repo, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.VDB_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME.getName()+MISSING));
        }
        if (!operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION.getName()+MISSING));
        }

        boolean includeSchema = true;
        if (operation.hasDefined(OperationsConstants.INCLUDE_SCHEMA.getName())) {
            includeSchema = operation.get(OperationsConstants.INCLUDE_SCHEMA.getName()).asBoolean();
        }

        ModelNode result = context.getResult();
        String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
        String vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asString();

        VDBMetaData vdb = repo.getVDB(vdbName, vdbVersion);
        if (vdb != null) {
            VDBMetadataMapper.INSTANCE.wrap(vdb, result, includeSchema);
        }
    }


    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.VDB_NAME);
        builder.addParameter(OperationsConstants.VDB_VERSION);
        builder.addParameter(OperationsConstants.INCLUDE_SCHEMA);
        builder.setReplyType(ModelType.OBJECT);
        builder.setReplyParameters(VDBMetadataMapper.INSTANCE.getAttributeDefinitions());
    }
}

class GetSchema extends BaseOperationHandler<VDBRepository>{

    protected GetSchema() {
        // even though this is read-only operation schema may be a protected
        // resource, should not visible to every one
        super("get-schema", true); //$NON-NLS-1$
    }

    @Override
    protected VDBRepository getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(isChangesRuntimes()).getRequiredService(TeiidServiceNames.VDB_REPO);
        return VDBRepository.class.cast(sc.getValue());
    }

    @Override
    protected void executeOperation(OperationContext context, VDBRepository repo, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.VDB_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME.getName()+MISSING));
        }
        if (!operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION.getName()+MISSING));
        }

        String modelName = null;
        if (operation.hasDefined(OperationsConstants.MODEL_NAME.getName())) {
            modelName = operation.get(OperationsConstants.MODEL_NAME.getName()).asString();
        }

        ModelNode result = context.getResult();
        String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
        String vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asString();


        VDBMetaData vdb = repo.getLiveVDB(vdbName, vdbVersion);
        if (vdb == null || (vdb.getStatus() != VDB.Status.ACTIVE)) {
            throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50096, vdbName, vdbVersion));
        }

        EnumSet<SchemaObjectType> schemaTypes = null;
        if (modelName != null && vdb.getModel(modelName) == null){
            throw new OperationFailedException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50097, vdbName, vdbVersion, modelName));
        }

        if (operation.hasDefined(OperationsConstants.ENTITY_TYPE.getName())) {
            String[] types = operation.get(OperationsConstants.ENTITY_TYPE.getName()).asString().toUpperCase().split(","); //$NON-NLS-1$
            if (types.length > 0) {
                ArrayList<SchemaObjectType> sot = new ArrayList<Admin.SchemaObjectType>();
                for (int i = 1; i < types.length; i++) {
                    sot.add(SchemaObjectType.valueOf(types[i]));
                }
                schemaTypes =  EnumSet.of(SchemaObjectType.valueOf(types[0]), sot.toArray(new SchemaObjectType[sot.size()]));
            }
            else {
                schemaTypes = EnumSet.of(SchemaObjectType.valueOf(types[0]));
            }
        }

        String regEx = null;
        if (operation.hasDefined(OperationsConstants.ENTITY_PATTERN.getName())) {
            regEx = operation.get(OperationsConstants.ENTITY_PATTERN.getName()).asString();
        }

        String ddl = null;
        MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
        if (modelName != null) {
            Schema schema = metadataStore.getSchema(modelName);
            ddl = DDLStringVisitor.getDDLString(schema, schemaTypes, regEx);
        } else {
            Database db = DatabaseUtil.convert(vdb, metadataStore);
            ddl = DDLStringVisitor.getDDLString(db);
        }
        result.set(ddl);
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.VDB_NAME);
        builder.addParameter(OperationsConstants.VDB_VERSION);
        builder.addParameter(OperationsConstants.MODEL_NAME);
        builder.addParameter(OperationsConstants.ENTITY_TYPE);
        builder.addParameter(OperationsConstants.ENTITY_PATTERN);
        builder.setReplyType(ModelType.STRING);
    }
}

class ListVDBs extends BaseOperationHandler<VDBRepository>{

    protected ListVDBs() {
        super("list-vdbs"); //$NON-NLS-1$
    }

    @Override
    protected VDBRepository getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_REPO);
        return VDBRepository.class.cast(sc.getValue());
    }

    @Override
    protected void executeOperation(OperationContext context, VDBRepository repo, ModelNode operation) throws OperationFailedException {
        boolean includeSchema = true;
        if (operation.hasDefined(OperationsConstants.INCLUDE_SCHEMA.getName())) {
            includeSchema = operation.get(OperationsConstants.INCLUDE_SCHEMA.getName()).asBoolean();
        }

        ModelNode result = context.getResult();
        List<VDBMetaData> vdbs = repo.getVDBs();
        for (VDBMetaData vdb:vdbs) {
            VDBMetadataMapper.INSTANCE.wrap(vdb, result.add(), includeSchema);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.INCLUDE_SCHEMA);
        builder.setReplyType(ModelType.LIST);
        builder.setReplyParameters(VDBMetadataMapper.INSTANCE.getAttributeDefinitions());
    }
}

class ListTranslators extends TranslatorOperationHandler{

    protected ListTranslators() {
        super("list-translators"); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, TranslatorRepository repo, ModelNode operation) throws OperationFailedException {
        ModelNode result = context.getResult();
        List<VDBTranslatorMetaData> translators = repo.getTranslators();
        for (VDBTranslatorMetaData t:translators) {
            VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE.wrap(t, result.add());
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.setReplyType(ModelType.LIST);
        builder.setReplyParameters(VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE.getAttributeDefinitions());
    }
}

class GetTranslator extends TranslatorOperationHandler{

    protected GetTranslator() {
        super("get-translator"); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, TranslatorRepository repo, ModelNode operation) throws OperationFailedException {

        if (!operation.hasDefined(OperationsConstants.TRANSLATOR_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME.getName()+MISSING));
        }

        ModelNode result = context.getResult();
        String translatorName = operation.get(OperationsConstants.TRANSLATOR_NAME.getName()).asString();
        VDBTranslatorMetaData translator = repo.getTranslatorMetaData(translatorName);
        if (translator != null) {
            VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE.wrap(translator, result);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.TRANSLATOR_NAME);
        builder.setReplyType(ModelType.OBJECT);
        builder.setReplyParameters(VDBTranslatorMetaDataMapper.INSTANCE.getAttributeDefinitions());
    }
}

abstract class VDBOperations extends BaseOperationHandler<RuntimeVDB>{
    boolean changesRuntimeState;
    public VDBOperations(String operationName) {
        super(operationName);
    }

    public VDBOperations(String operationName, boolean changesRuntimeState) {
        super(operationName, changesRuntimeState);
        this.changesRuntimeState = changesRuntimeState;
    }

    @Override
    public RuntimeVDB getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.VDB_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME.getName()+MISSING));
        }

        if (!operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION.getName()+MISSING));
        }

        String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
        String vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asString();

        TeiidOperationHandler.checkVDB(context, vdbName, vdbVersion);

        ServiceController<?> sc = context.getServiceRegistry(this.changesRuntimeState).getRequiredService(TeiidServiceNames.vdbServiceName(vdbName, vdbVersion));
        return RuntimeVDB.class.cast(sc.getValue());
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.VDB_NAME);
        builder.addParameter(OperationsConstants.VDB_VERSION);
    }

     static void updateServices(OperationContext context, RuntimeVDB vdb,
                String dsName, ReplaceResult rr) {
        if (rr.isNew) {
            VDBDeployer.addDataSourceListener(context.getServiceTarget(), new VDBKey(vdb.getVdb().getName(), vdb.getVdb().getVersion()), dsName);
        }
        if (rr.removedDs != null) {
            final ServiceRegistry registry = context.getServiceRegistry(true);
            ServiceName serviceName;
            try {
                serviceName = TeiidServiceNames.dsListenerServiceName(vdb.getVdb().getName(), vdb.getVdb().getVersion(), rr.removedDs);
            } catch (InvalidServiceNameException e) {
                return; //the old isn't valid
            }
            final ServiceController<?> controller = registry.getService(serviceName);
            if (controller != null) {
                context.removeService(serviceName);
            }
        }
    }
}

class AddDataRole extends VDBOperations {

    public AddDataRole() {
        super("add-data-role", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.DATA_ROLE.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE.getName()+MISSING));
        }

        if (!operation.hasDefined(OperationsConstants.MAPPED_ROLE.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.MAPPED_ROLE.getName()+MISSING));
        }

        String policyName = operation.get(OperationsConstants.DATA_ROLE.getName()).asString();
        String mappedRole = operation.get(OperationsConstants.MAPPED_ROLE.getName()).asString();

        try {
            vdb.addDataRole(policyName, mappedRole);
        } catch (AdminProcessingException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        super.describeParameters(builder);
        builder.addParameter(OperationsConstants.DATA_ROLE);
        builder.addParameter(OperationsConstants.MAPPED_ROLE);
    }
}

class RemoveDataRole extends VDBOperations {

    public RemoveDataRole() {
        super("remove-data-role", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.DATA_ROLE.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE.getName()+MISSING));
        }

        if (!operation.hasDefined(OperationsConstants.MAPPED_ROLE.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.MAPPED_ROLE.getName()+MISSING));
        }

        String policyName = operation.get(OperationsConstants.DATA_ROLE.getName()).asString();
        String mappedRole = operation.get(OperationsConstants.MAPPED_ROLE.getName()).asString();

        try {
            vdb.remoteDataRole(policyName, mappedRole);
        } catch (AdminProcessingException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        super.describeParameters(builder);
        builder.addParameter(OperationsConstants.DATA_ROLE);
        builder.addParameter(OperationsConstants.MAPPED_ROLE);
    }
}

class AddAnyAuthenticatedDataRole extends VDBOperations {

    public AddAnyAuthenticatedDataRole() {
        super("add-anyauthenticated-role", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.DATA_ROLE.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE.getName()+MISSING));
        }

        String policyName = operation.get(OperationsConstants.DATA_ROLE.getName()).asString();
        try {
            vdb.addAnyAuthenticated(policyName);
        } catch (AdminProcessingException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        super.describeParameters(builder);
        builder.addParameter(OperationsConstants.DATA_ROLE);
    }

}

class RemoveAnyAuthenticatedDataRole extends VDBOperations {

    public RemoveAnyAuthenticatedDataRole() {
        super("remove-anyauthenticated-role", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.DATA_ROLE.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE.getName()+MISSING));
        }

        String policyName = operation.get(OperationsConstants.DATA_ROLE.getName()).asString();
        try {
            vdb.removeAnyAuthenticated(policyName);
        } catch (AdminProcessingException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        super.describeParameters(builder);
        builder.addParameter(OperationsConstants.DATA_ROLE);
    }
}

class ChangeVDBConnectionType extends VDBOperations {

    public ChangeVDBConnectionType() {
        super("change-vdb-connection-type", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.CONNECTION_TYPE.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.CONNECTION_TYPE.getName()+MISSING));
        }

        String connectionType = operation.get(OperationsConstants.CONNECTION_TYPE.getName()).asString();
        try {
            vdb.changeConnectionType(ConnectionType.valueOf(connectionType));
        } catch (AdminProcessingException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        super.describeParameters(builder);
        builder.addParameter(OperationsConstants.CONNECTION_TYPE);
    }
}

class RestartVDB extends VDBOperations {

    public RestartVDB() {
        super("restart-vdb", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
        List<String> models = new ArrayList<String>();
        if (operation.hasDefined(OperationsConstants.MODEL_NAMES.getName())) {
            String modelNames = operation.get(OperationsConstants.MODEL_NAMES.getName()).asString();
            for (String model:modelNames.split(",")) { //$NON-NLS-1$
                models.add(model.trim());
            }
        }

        try {
            vdb.restart(models);
        } catch (AdminProcessingException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        super.describeParameters(builder);
        builder.addParameter(OperationsConstants.MODEL_NAMES);
    }
}

class AssignDataSource extends VDBOperations {

    boolean modelNameParam;

    public AssignDataSource() {
        this("assign-datasource", true, true); //$NON-NLS-1$
    }

    protected AssignDataSource(String operation, boolean modelNameParam, boolean changesRuntimeState) {
        super(operation, changesRuntimeState);
        this.modelNameParam = modelNameParam;
    }

    @Override
    protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.SOURCE_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.SOURCE_NAME.getName()+MISSING));
        }

        if (!operation.hasDefined(OperationsConstants.TRANSLATOR_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME.getName()+MISSING));
        }

        if (!operation.hasDefined(OperationsConstants.DS_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.DS_NAME.getName()+MISSING));
        }

        String sourceName = operation.get(OperationsConstants.SOURCE_NAME.getName()).asString();
        String translatorName = operation.get(OperationsConstants.TRANSLATOR_NAME.getName()).asString();
        String dsName = operation.get(OperationsConstants.DS_NAME.getName()).asString();

        try {
            synchronized (vdb.getVdb()) {
                ReplaceResult rr = vdb.updateSource(sourceName, translatorName, dsName);
                updateServices(context, vdb, dsName, rr);
            }
        } catch (AdminProcessingException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        super.describeParameters(builder);
        if (modelNameParam) {
            builder.addParameter(OperationsConstants.MODEL_NAME);
        }
        builder.addParameter(OperationsConstants.SOURCE_NAME);
        builder.addParameter(OperationsConstants.TRANSLATOR_NAME);
        builder.addParameter(OperationsConstants.DS_NAME);
    }
}

class UpdateSource extends AssignDataSource {

    public UpdateSource() {
        super("update-source", false, true); //$NON-NLS-1$
    }

}

class AddSource extends VDBOperations {

    public AddSource() {
        super("add-source", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.MODEL_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.MODEL_NAME.getName()+MISSING));
        }

        if (!operation.hasDefined(OperationsConstants.SOURCE_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.SOURCE_NAME.getName()+MISSING));
        }

        if (!operation.hasDefined(OperationsConstants.TRANSLATOR_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME.getName()+MISSING));
        }

        if (!operation.hasDefined(OperationsConstants.DS_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.DS_NAME.getName()+MISSING));
        }


        String modelName = operation.get(OperationsConstants.MODEL_NAME.getName()).asString();
        String sourceName = operation.get(OperationsConstants.SOURCE_NAME.getName()).asString();
        String translatorName = operation.get(OperationsConstants.TRANSLATOR_NAME.getName()).asString();
        String dsName = operation.get(OperationsConstants.DS_NAME.getName()).asString();

        try {
            synchronized (vdb.getVdb()) {
                ReplaceResult rr = vdb.addSource(modelName, sourceName, translatorName, dsName);
                updateServices(context, vdb, dsName, rr);
            }
        } catch (AdminProcessingException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        super.describeParameters(builder);
        builder.addParameter(OperationsConstants.MODEL_NAME);
        builder.addParameter(OperationsConstants.SOURCE_NAME);
        builder.addParameter(OperationsConstants.TRANSLATOR_NAME);
        builder.addParameter(OperationsConstants.DS_NAME);
    }
}

class RemoveSource extends VDBOperations {

    public RemoveSource() {
        super("remove-source", true); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
        if (!operation.hasDefined(OperationsConstants.MODEL_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.MODEL_NAME.getName()+MISSING));
        }

        if (!operation.hasDefined(OperationsConstants.SOURCE_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.SOURCE_NAME.getName()+MISSING));
        }

        String modelName = operation.get(OperationsConstants.MODEL_NAME.getName()).asString();
        String sourceName = operation.get(OperationsConstants.SOURCE_NAME.getName()).asString();

        try {
            synchronized (vdb.getVdb()) {
                ReplaceResult rr = vdb.removeSource(modelName, sourceName);
                updateServices(context, vdb, null, rr);
            }
        } catch (AdminProcessingException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        super.describeParameters(builder);
        builder.addParameter(OperationsConstants.MODEL_NAME);
        builder.addParameter(OperationsConstants.SOURCE_NAME);
    }
}

class ReadTranslatorProperties extends TranslatorOperationHandler {
    protected ReadTranslatorProperties() {
        super("read-translator-properties"); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, TranslatorRepository repo, ModelNode operation) throws OperationFailedException {

        if (!operation.hasDefined(OperationsConstants.TRANSLATOR_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME.getName()+MISSING));
        }

        if (!operation.hasDefined(OperationsConstants.PROPERTY_TYPE.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.PROPERTY_TYPE.getName() + MISSING));
        }

        ModelNode result = context.getResult();
        String translatorName = operation.get(OperationsConstants.TRANSLATOR_NAME.getName()).asString();

        String propertyType = operation.get(OperationsConstants.PROPERTY_TYPE.getName()).asString().toUpperCase();
        TranlatorPropertyType translatorPropertyType = TranlatorPropertyType.valueOf(propertyType);

        VDBTranslatorMetaData translator = repo.getTranslatorMetaData(translatorName);
        if (translator != null) {
            ExtendedPropertyMetadataList properties = translator.getAttachment(ExtendedPropertyMetadataList.class);
            if (translatorPropertyType.equals(TranlatorPropertyType.ALL)) {
                for (ExtendedPropertyMetadata epm:properties) {
                    result.add(buildNode(epm));
                }
            } else {
                PropertyType type = PropertyType.valueOf(propertyType);
                for (ExtendedPropertyMetadata epm:properties) {
                    if (PropertyType.valueOf(epm.category()).equals(type)) {
                        result.add(buildNode(epm));
                    }
                }
            }
        }
    }

    static ModelNode buildNode(ExtendedPropertyMetadata prop) {
        ModelNode node = new ModelNode();
        String name = prop.name();
        String type = prop.datatype();

        if ("java.lang.String".equals(type)) { //$NON-NLS-1$
            node.get(name, TYPE).set(ModelType.STRING);
        }
        else if ("java.lang.Integer".equals(type) || "int".equals(type)) { //$NON-NLS-1$ //$NON-NLS-2$
            node.get(name, TYPE).set(ModelType.INT);
        }
        else if ("java.lang.Long".equals(type) || "long".equals(type)) { //$NON-NLS-1$ //$NON-NLS-2$
            node.get(name, TYPE).set(ModelType.LONG);
        }
        else if ("java.lang.Boolean".equals(type) || "boolean".equals(type)) { //$NON-NLS-1$ //$NON-NLS-2$
            node.get(name, TYPE).set(ModelType.BOOLEAN);
        }

        node.get(name, REQUIRED).set(prop.required());

        if (prop.description() != null) {
            node.get(name, DESCRIPTION).set(prop.description());
        }
        if (prop.display() != null) {
            node.get(name, "display").set(prop.display()); //$NON-NLS-1$
        }

        node.get(name, READ_ONLY).set(prop.readOnly());
        node.get(name, "advanced").set(prop.advanced()); //$NON-NLS-1$

        if (prop.allowed() != null) {
            for (String s:prop.allowed()) {
                node.get(name, ALLOWED).add(s);
            }
        }

        node.get(name, "masked").set(prop.masked()); //$NON-NLS-1$

        if (prop.owner() != null) {
            node.get(name, "owner").set(prop.owner()); //$NON-NLS-1$
        }

        if (prop.defaultValue() != null) {
            node.get(name, DEFAULT).set(prop.defaultValue());
        }

        if (prop.category() != null) {
            node.get(name, "category").set(prop.category()); //$NON-NLS-1$
        }
        return node;
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.TRANSLATOR_NAME);
        builder.addParameter(OperationsConstants.PROPERTY_TYPE);
        builder.setReplyType(ModelType.LIST);
        builder.setReplyValueType(ModelType.PROPERTY);
    }
}

class ReadRARDescription extends TeiidOperationHandler {

    protected ReadRARDescription() {
        super("read-rar-description"); //$NON-NLS-1$
    }
    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {
        ModelNode result = context.getResult();

        if (!operation.hasDefined(OperationsConstants.RAR_NAME.getName())) {
            throw new OperationFailedException(IntegrationPlugin.Util.getString(OperationsConstants.RAR_NAME.getName()+MISSING));
        }
        String rarName = operation.get(OperationsConstants.RAR_NAME.getName()).asString();

        ResourceAdapter ra = null;
        ServiceName svcName = ConnectorServices.INACTIVE_RESOURCE_ADAPTER_SERVICE.append(rarName);
        ServiceController<?> sc = context.getServiceRegistry(false).getService(svcName);
        if (sc != null) {
            InactiveResourceAdapterDeployment deployment = InactiveResourceAdapterDeployment.class.cast(sc.getValue());
            ConnectorXmlDescriptor descriptor = deployment.getConnectorXmlDescriptor();
            ra = descriptor.getConnector().getResourceadapter();
        }
        else {
            svcName = ServiceName.JBOSS.append("deployment", "unit").append(rarName); //$NON-NLS-1$ //$NON-NLS-2$
            sc = context.getServiceRegistry(false).getService(svcName);
            if (sc == null) {
                throw new OperationFailedException(IntegrationPlugin.Util.getString("RAR_notfound"));  //$NON-NLS-1$
            }
            DeploymentUnit du = DeploymentUnit.class.cast(sc.getValue());
            ConnectorXmlDescriptor cd = du.getAttachment(ConnectorXmlDescriptor.ATTACHMENT_KEY);
            ra = cd.getConnector().getResourceadapter();
        }
        result.add(buildReadOnlyNode("resourceadapter-class", ra.getResourceadapterClass())); //$NON-NLS-1$
        List<ConnectionDefinition> connDefinitions = ra.getOutboundResourceadapter().getConnectionDefinitions();
        for (ConnectionDefinition p:connDefinitions) {
            result.add(buildReadOnlyNode("managedconnectionfactory-class", p.getManagedConnectionFactoryClass().getValue())); //$NON-NLS-1$
            List<? extends ConfigProperty> props = p.getConfigProperties();
            for (ConfigProperty prop:props) {
                result.add(buildNode(prop));
            }
        }
    }

    private ModelNode buildReadOnlyNode(String name, String value) {
        ModelNode node = new ModelNode();
        node.get(name, TYPE).set(ModelType.STRING);
        node.get(name, "display").set(name); //$NON-NLS-1$
        node.get(name, READ_ONLY).set(Boolean.toString(Boolean.TRUE));
        node.get(name, "advanced").set(Boolean.toString(Boolean.TRUE)); //$NON-NLS-1$
        node.get(name, DEFAULT).set(value);
        return node;
    }

    private ModelNode buildNode(ConfigProperty prop) {
        String name = prop.getConfigPropertyName().getValue();
        String type = prop.getConfigPropertyType().getValue();

        String defaltValue = null;
        if (prop.getConfigPropertyValue() != null) {
            defaltValue = prop.getConfigPropertyValue().getValue();
        }

        String description = null;
        if (prop.getDescriptions() != null) {
            description = prop.getDescriptions().get(0).getValue();
        }

        ExtendedPropertyMetadata extended = new ExtendedPropertyMetadata(name, type, description, defaltValue);
        return ReadTranslatorProperties.buildNode(extended);
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.addParameter(OperationsConstants.RAR_NAME);
        builder.setReplyType(ModelType.LIST);
        builder.setReplyValueType(ModelType.PROPERTY);
    }
}

//TEIID-2404
class EngineStatistics extends TeiidOperationHandler {
    protected EngineStatistics() {
        super("engine-statistics"); //$NON-NLS-1$
    }

    @Override
    protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
        try {
            BufferManagerService bufferMgrSvc = getBufferManager(context);
            EngineStatisticsMetadata stats = EmbeddedAdminFactory.createEngineStats(getSessionCount(context), bufferMgrSvc, engine);
            VDBMetadataMapper.EngineStatisticsMetadataMapper.INSTANCE.wrap(stats, context.getResult());
        } catch (AdminException e) {
            throw new OperationFailedException(e);
        }
    }

    @Override
    protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
        builder.setReplyType(ModelType.OBJECT);
        builder.setReplyParameters(VDBMetadataMapper.EngineStatisticsMetadataMapper.INSTANCE.getAttributeDefinitions());
    }
}
