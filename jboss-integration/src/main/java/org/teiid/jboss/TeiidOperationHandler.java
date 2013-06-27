/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jboss.as.connector.metadata.xmldescriptors.ConnectorXmlDescriptor;
import org.jboss.as.connector.services.resourceadapters.deployment.InactiveResourceAdapterDeploymentService.InactiveResourceAdapterDeployment;
import org.jboss.as.connector.util.ConnectorServices;
import org.jboss.as.controller.AbstractWriteAttributeHandler;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.descriptions.DefaultOperationDescriptionProvider;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.registry.OperationEntry;
import org.jboss.as.server.deployment.DeploymentUnit;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.jca.common.api.metadata.ra.ConfigProperty;
import org.jboss.jca.common.api.metadata.ra.ConnectionDefinition;
import org.jboss.jca.common.api.metadata.ra.ResourceAdapter;
import org.jboss.jca.common.api.metadata.ra.ResourceAdapter1516;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceRegistry;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.Admin.SchemaObjectType;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.*;
import org.teiid.adminapi.impl.VDBMetadataMapper.TransactionMetadataMapper;
import org.teiid.adminapi.impl.VDBMetadataMapper.VDBTranslatorMetaDataMapper;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.plan.PlanNode;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidComponentException;
import org.teiid.deployers.ExtendedPropertyMetadata;
import org.teiid.deployers.RuntimeVDB;
import org.teiid.deployers.RuntimeVDB.ReplaceResult;
import org.teiid.deployers.VDBRepository;
import org.teiid.deployers.VDBStatusChecker;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Schema;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.tempdata.TempTableDataManager;
import org.teiid.vdb.runtime.VDBKey;

/**
 * Keep this the class and all the extended classes stateless as there is single instance.
 */
abstract class TeiidOperationHandler extends BaseOperationHandler<DQPCore> {

	protected TeiidOperationHandler(String operationName){
		super(operationName);
	}

	@Override
	protected DQPCore getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
        ServiceController<?> repo = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.ENGINE);
        if (repo != null) {
        	return  DQPCore.class.cast(repo.getValue());
        }
        return null;
	}

	protected BufferManagerService getBufferManager(OperationContext context) {
		ServiceController<?> repo = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.BUFFER_MGR);
        if (repo != null) {
        	return BufferManagerService.class.cast(repo.getService());
        }
        return null;
	}

	protected VDBRepository getVDBrepository(OperationContext context) {
		ServiceController<?> repo = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_REPO);
        if (repo != null) {
        	return VDBRepository.class.cast(repo.getValue());
        }
        return null;
	}

	protected int getSessionCount(OperationContext context) throws AdminException {
		int count = 0;
		List<TransportService> transportServices = getTransportServices(context);
		for (TransportService t: transportServices) {
			count += t.getActiveSessionsCount();
		}
		return count;
	}

	protected List<TransportService> getTransportServices(OperationContext context){
		List<TransportService> transports = new ArrayList<TransportService>();
		List<ServiceName> services = context.getServiceRegistry(false).getServiceNames();
        for (ServiceName name:services) {
        	if (TeiidServiceNames.TRANSPORT_BASE.isParentOf(name)) {
        		ServiceController<?> transport = context.getServiceRegistry(false).getService(name);
        		if (transport != null) {
        			transports.add(TransportService.class.cast(transport.getValue()));
        		}
        	}
        }
        return transports;
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
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
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
		int version = -1;
		boolean filter = false;

		if (operation.hasDefined(OperationsConstants.OPTIONAL_VDB_VERSION.getName()) && operation.hasDefined(OperationsConstants.OPTIONAL_VDB_NAME.getName())) {
			vdbName = operation.get(OperationsConstants.OPTIONAL_VDB_NAME.getName()).asString();
			version = operation.get(OperationsConstants.OPTIONAL_VDB_VERSION.getName()).asInt();
			if (!isValidVDB(context, vdbName, version)) {
				throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50096, vdbName, version)));
			}
			filter = true;
		}

		ModelNode result = context.getResult();
		for (TransportService t: getTransportServices(context)) {
			Collection<SessionMetadata> sessions = t.getActiveSessions();
			for (SessionMetadata session:sessions) {
				if (filter) {
					if (session.getVDBName().equals(vdbName) && session.getVDBVersion() == version) {
						VDBMetadataMapper.SessionMetadataMapper.INSTANCE.wrap(session, result.add());
					}
				}
				else {
					VDBMetadataMapper.SessionMetadataMapper.INSTANCE.wrap(session, result.add());
				}
			}
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
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SESSION.getName()+MISSING)));
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
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME.getName()+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION.getName()+MISSING)));
		}

		boolean includeSourceQueries = true;
		if (operation.hasDefined(OperationsConstants.INCLUDE_SOURCE.getName())) {
			includeSourceQueries = operation.get(OperationsConstants.INCLUDE_SOURCE.getName()).asBoolean();
		}

		ModelNode result = context.getResult();
		String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asInt();
		if (!isValidVDB(context, vdbName, vdbVersion)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50096, vdbName, vdbVersion)));
		}
		for (TransportService t: getTransportServices(context)) {
			List<RequestMetadata> requests = t.getRequestsUsingVDB(vdbName,vdbVersion);
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
		super("terminate-session"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		if (!operation.hasDefined(OperationsConstants.SESSION.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SESSION.getName()+MISSING)));
		}
		for (TransportService t: getTransportServices(context)) {
			t.terminateSession(operation.get(OperationsConstants.SESSION.getName()).asString());
		}
	}

	@Override
	protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
		builder.addParameter(OperationsConstants.SESSION);
	}
}

class CancelRequest extends TeiidOperationHandler{
	protected CancelRequest() {
		super("cancel-request"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		try {
			if (!operation.hasDefined(OperationsConstants.SESSION.getName())) {
				throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SESSION.getName()+MISSING)));
			}
			if (!operation.hasDefined(OperationsConstants.EXECUTION_ID.getName())) {
				throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.EXECUTION_ID.getName()+MISSING)));
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
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SESSION.getName()+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.EXECUTION_ID.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.EXECUTION_ID.getName()+MISSING)));
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

	@Override
	protected SessionAwareCache getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
		String cacheType = Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name();

		if (operation.hasDefined(OperationsConstants.CACHE_TYPE.getName())) {
			cacheType = operation.get(OperationsConstants.CACHE_TYPE.getName()).asString();
		}

		ServiceController<?> sc;
		if (SessionAwareCache.isResultsetCache(cacheType)) {
			sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.CACHE_RESULTSET);
		}
		else {
			sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.CACHE_PREPAREDPLAN);
		}

		if (sc != null) {
			return SessionAwareCache.class.cast(sc.getValue());
		}
		return null;
	}
}


class CacheTypes extends BaseCachehandler {
	protected CacheTypes() {
		super("cache-types"); //$NON-NLS-1$
	}

	@Override
	protected void executeOperation(OperationContext context, SessionAwareCache cache, ModelNode operation) throws OperationFailedException {
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
		super("clear-cache"); //$NON-NLS-1$
	}

	@Override
	protected void executeOperation(OperationContext context, SessionAwareCache cache, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.CACHE_TYPE.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.CACHE_TYPE.getName()+MISSING)));
		}

		String cacheType = operation.get(OperationsConstants.CACHE_TYPE.getName()).asString();
		if (cache == null) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50071, cacheType)));
		}

		if (operation.hasDefined(OperationsConstants.VDB_NAME.getName()) && operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
			String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
			int vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asInt();
			if (!isValidVDB(context, vdbName, vdbVersion)) {
				throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50096, vdbName, vdbVersion)));
			}
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
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.CACHE_TYPE.getName()+MISSING)));
		}
		String cacheType = operation.get(OperationsConstants.CACHE_TYPE.getName()).asString();
		if (cache == null) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50071, cacheType)));
		}

		ModelNode result = context.getResult();
		CacheStatisticsMetadata stats = buildCacheStats(cacheType, cache);
		VDBMetadataMapper.CacheStatisticsMetadataMapper.INSTANCE.wrap(stats, result);
	}

	private CacheStatisticsMetadata buildCacheStats(String name, SessionAwareCache cache) {
		CacheStatisticsMetadata stats = new CacheStatisticsMetadata();
		stats.setName(name);
		stats.setHitRatio(cache.getRequestCount() == 0?0:((double)cache.getCacheHitCount()/cache.getRequestCount())*100);
		stats.setTotalEntries(cache.getTotalCacheEntries());
		stats.setRequestCount(cache.getRequestCount());
		return stats;
	}

	@Override
	protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
		builder.addParameter(OperationsConstants.CACHE_TYPE);
		builder.setReplyType(ModelType.LIST);
		builder.setReplyParameters(VDBMetadataMapper.CacheStatisticsMetadataMapper.INSTANCE.getAttributeDefinitions());
	}
}

class MarkDataSourceAvailable extends TeiidOperationHandler{
	protected MarkDataSourceAvailable() {
		super("mark-datasource-available"); //$NON-NLS-1$
	}

	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.DS_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DS_NAME.getName()+MISSING)));
		}
		String dsName = operation.get(OperationsConstants.DS_NAME.getName()).asString();
		ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_STATUS_CHECKER);
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
		builder.setReplyType(ModelType.LIST);
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
		super("terminate-transaction"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {

		if (!operation.hasDefined(OperationsConstants.XID.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.XID.getName()+MISSING)));
		}

		String xid = operation.get(OperationsConstants.XID.getName()).asString();
		try {
			engine.terminateTransaction(xid);
		} catch (AdminException e) {
			throw new OperationFailedException(e, new ModelNode().set(e.getMessage()));
		}
	}

	@Override
	protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
		builder.addParameter(OperationsConstants.XID);
	}
}

class ExecuteQuery extends TeiidOperationHandler{

	protected ExecuteQuery() {
		super("execute-query"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {

		if (!operation.hasDefined(OperationsConstants.VDB_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME.getName()+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION.getName()+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.SQL_QUERY.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SQL_QUERY.getName()+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.TIMEOUT_IN_MILLI.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TIMEOUT_IN_MILLI.getName()+MISSING)));
		}

		ModelNode result = context.getResult();
		String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asInt();
		String sql = operation.get(OperationsConstants.SQL_QUERY.getName()).asString();
		int timeout = operation.get(OperationsConstants.TIMEOUT_IN_MILLI.getName()).asInt();

		if (!isValidVDB(context, vdbName, vdbVersion)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50096, vdbName, vdbVersion)));
		}

		result.set(executeQuery(context, engine, vdbName, vdbVersion, sql, timeout, new ModelNode()));
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

	public ModelNode executeQuery(final OperationContext context,  final DQPCore engine, final String vdbName, final int version, final String command, final long timoutInMilli, final ModelNode resultsNode) throws OperationFailedException {
		String user = "CLI ADMIN"; //$NON-NLS-1$
		LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("admin_executing", user, command)); //$NON-NLS-1$

        VDBMetaData vdb = getVDBrepository(context).getLiveVDB(vdbName, version);
        if (vdb == null) {
        	throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString("wrong_vdb")));//$NON-NLS-1$
        }
        final SessionMetadata session = TempTableDataManager.createTemporarySession(user, "admin-console", vdb); //$NON-NLS-1$

		final long requestID =  0L;

		DQPWorkContext workContext = new DQPWorkContext();
		workContext.setUseCallingThread(true);
		workContext.setSession(session);

		try {
			return workContext.runInContext(new Callable<ModelNode>() {
				@Override
				public ModelNode call() throws Exception {

					long start = System.currentTimeMillis();
					RequestMessage request = new RequestMessage(command);
					request.setExecutionId(requestID);
					request.setRowLimit(engine.getMaxRowsFetchSize()); // this would limit the number of rows that are returned.
					Future<ResultsMessage> message = engine.executeRequest(requestID, request);
					ResultsMessage rm = null;
					if (timoutInMilli < 0) {
						rm = message.get();
					} else {
						rm = message.get(timoutInMilli, TimeUnit.MILLISECONDS);
					}
			        if (rm.getException() != null) {
			             throw new AdminProcessingException(IntegrationPlugin.Event.TEIID50047, rm.getException());
			        }

			        if (rm.isUpdateResult()) {
			        	writeResults(resultsNode, Arrays.asList("update-count"), rm.getResultsList()); //$NON-NLS-1$
			        }
			        else {
			        	writeResults(resultsNode, Arrays.asList(rm.getColumnNames()), rm.getResultsList());

				        while (rm.getFinalRow() == -1 || rm.getLastRow() < rm.getFinalRow()) {
				        	long elapsed = System.currentTimeMillis() - start;
							message = engine.processCursorRequest(requestID, rm.getLastRow()+1, 1024);
							rm = message.get(timoutInMilli-elapsed, TimeUnit.MILLISECONDS);
							writeResults(resultsNode, Arrays.asList(rm.getColumnNames()), rm.getResultsList());
				        }
			        }

			        long elapsed = System.currentTimeMillis() - start;
			        ResultsFuture<?> response = engine.closeRequest(requestID);
			        response.get(timoutInMilli-elapsed, TimeUnit.MILLISECONDS);
					return resultsNode;
				}
			});
		} catch (Throwable t) {
			throw new OperationFailedException(new ModelNode().set(t.getMessage()));
		} finally {
			try {
				workContext.runInContext(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						engine.terminateSession(session.getSessionId());
						return null;
					}
				});
			} catch (Throwable e) {
				throw new OperationFailedException(new ModelNode().set(e.getMessage()));
			}
		}
	}

	private void writeResults(ModelNode resultsNode, List<String> columns,  List<? extends List<?>> results) throws SQLException {
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
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME.getName()+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION.getName()+MISSING)));
		}

		ModelNode result = context.getResult();
		String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asInt();

		VDBMetaData vdb = repo.getVDB(vdbName, vdbVersion);
		if (vdb != null) {
			VDBMetadataMapper.INSTANCE.wrap(vdb, result);
		}
		else {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50096, vdbName, vdbVersion)));
		}
	}


	@Override
    public OperationDefinition getOperationDefinition() {
    	final AttributeDefinition[] parameters = new AttributeDefinition[] {OperationsConstants.VDB_NAME, OperationsConstants.VDB_VERSION};
    	final ResourceDescriptionResolver resolver = new TeiidResourceDescriptionResolver(name());
        return new OperationDefinition(name(), OperationEntry.EntryType.PUBLIC, EnumSet.noneOf(OperationEntry.Flag.class), ModelType.OBJECT, null, true, null, null, parameters) {
			@Override
			public DescriptionProvider getDescriptionProvider() {
				return new DefaultOperationDescriptionProvider(name(), resolver, resolver,  ModelType.OBJECT, ModelType.OBJECT, null, null, this.parameters) {
					@Override
				    protected ModelNode getReplyValueTypeDescription(ResourceDescriptionResolver descriptionResolver, Locale locale, ResourceBundle bundle) {
						return VDBMetadataMapper.INSTANCE.describe( new ModelNode());
				    }
				};
			}
        };
    }
}

class GetSchema extends BaseOperationHandler<VDBRepository>{

	protected GetSchema() {
		super("get-schema"); //$NON-NLS-1$
	}

	@Override
	protected VDBRepository getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_REPO);
        return VDBRepository.class.cast(sc.getValue());
	}

	@Override
	protected void executeOperation(OperationContext context, VDBRepository repo, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.VDB_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME.getName()+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION.getName()+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.MODEL_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.MODEL_NAME.getName()+MISSING)));
		}

		ModelNode result = context.getResult();
		String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asInt();
		String modelName = operation.get(OperationsConstants.MODEL_NAME.getName()).asString();

		VDBMetaData vdb = repo.getLiveVDB(vdbName, vdbVersion);
		if (vdb == null || (vdb.getStatus() != VDB.Status.ACTIVE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50096, vdbName, vdbVersion)));
		}

		EnumSet<SchemaObjectType> schemaTypes = null;
		if (vdb.getModel(modelName) == null){
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50097, vdbName, vdbVersion, modelName)));
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

		MetadataStore metadataStore = vdb.getAttachment(TransformationMetadata.class).getMetadataStore();
		Schema schema = metadataStore.getSchema(modelName);
		String ddl = DDLStringVisitor.getDDLString(schema, schemaTypes, regEx);
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
		ModelNode result = context.getResult();
		List<VDBMetaData> vdbs = repo.getVDBs();
		for (VDBMetaData vdb:vdbs) {
			VDBMetadataMapper.INSTANCE.wrap(vdb, result.add());
		}
	}

	@Override
    public OperationDefinition getOperationDefinition() {
    	final AttributeDefinition[] parameters = new AttributeDefinition[0];
    	final ResourceDescriptionResolver resolver = new TeiidResourceDescriptionResolver(name());
        return new OperationDefinition(name(), OperationEntry.EntryType.PUBLIC, EnumSet.noneOf(OperationEntry.Flag.class), ModelType.OBJECT, null, true, null, null, parameters) {
			@Override
			public DescriptionProvider getDescriptionProvider() {
				return new DefaultOperationDescriptionProvider(name(), resolver, resolver,  ModelType.LIST, ModelType.OBJECT, null, null, this.parameters) {
					@Override
				    protected ModelNode getReplyValueTypeDescription(ResourceDescriptionResolver descriptionResolver, Locale locale, ResourceBundle bundle) {
						return VDBMetadataMapper.INSTANCE.describe( new ModelNode());
				    }
				};
			}
        };
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
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME.getName()+MISSING)));
		}

		ModelNode result = context.getResult();
		String translatorName = operation.get(OperationsConstants.TRANSLATOR_NAME.getName()).asString();
		VDBTranslatorMetaData translator = repo.getTranslatorMetaData(translatorName);
		VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE.wrap(translator, result);
	}

	@Override
	protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
		builder.addParameter(OperationsConstants.TRANSLATOR_NAME);
		builder.setReplyType(ModelType.OBJECT);
		builder.setReplyParameters(VDBTranslatorMetaDataMapper.INSTANCE.getAttributeDefinitions());
	}
}

abstract class VDBOperations extends BaseOperationHandler<RuntimeVDB>{

	public VDBOperations(String operationName) {
		super(operationName);
	}

	@Override
	public RuntimeVDB getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.VDB_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME.getName()+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.VDB_VERSION.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION.getName()+MISSING)));
		}

		String vdbName = operation.get(OperationsConstants.VDB_NAME.getName()).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION.getName()).asInt();

		if (!isValidVDB(context, vdbName, vdbVersion)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50096, vdbName, vdbVersion)));
		}

		ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.vdbServiceName(vdbName, vdbVersion));
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
		    final ServiceName serviceName = TeiidServiceNames.dsListenerServiceName(vdb.getVdb().getName(), vdb.getVdb().getVersion(), rr.removedDs);
		    final ServiceController<?> controller = registry.getService(serviceName);
		    if (controller != null) {
		    	context.removeService(serviceName);
		    }
		}
	}
}

class AddDataRole extends VDBOperations {

	public AddDataRole() {
		super("add-data-role"); //$NON-NLS-1$
	}

	@Override
	protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.DATA_ROLE.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE.getName()+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.MAPPED_ROLE.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.MAPPED_ROLE.getName()+MISSING)));
		}

		String policyName = operation.get(OperationsConstants.DATA_ROLE.getName()).asString();
		String mappedRole = operation.get(OperationsConstants.MAPPED_ROLE.getName()).asString();

		try {
			vdb.addDataRole(policyName, mappedRole);
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
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
		super("remove-data-role"); //$NON-NLS-1$
	}

	@Override
	protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.DATA_ROLE.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE.getName()+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.MAPPED_ROLE.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.MAPPED_ROLE.getName()+MISSING)));
		}

		String policyName = operation.get(OperationsConstants.DATA_ROLE.getName()).asString();
		String mappedRole = operation.get(OperationsConstants.MAPPED_ROLE.getName()).asString();

		try {
			vdb.remoteDataRole(policyName, mappedRole);
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
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
		super("add-anyauthenticated-role"); //$NON-NLS-1$
	}

	@Override
	protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.DATA_ROLE.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE.getName()+MISSING)));
		}

		String policyName = operation.get(OperationsConstants.DATA_ROLE.getName()).asString();
		try {
			vdb.addAnyAuthenticated(policyName);
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
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
		super("remove-anyauthenticated-role"); //$NON-NLS-1$
	}

	@Override
	protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.DATA_ROLE.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE.getName()+MISSING)));
		}

		String policyName = operation.get(OperationsConstants.DATA_ROLE.getName()).asString();
		try {
			vdb.removeAnyAuthenticated(policyName);
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
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
		super("change-vdb-connection-type"); //$NON-NLS-1$
	}

	@Override
	protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.CONNECTION_TYPE.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.CONNECTION_TYPE.getName()+MISSING)));
		}

		String connectionType = operation.get(OperationsConstants.CONNECTION_TYPE.getName()).asString();
		try {
			vdb.changeConnectionType(ConnectionType.valueOf(connectionType));
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
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
		super("restart-vdb"); //$NON-NLS-1$
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
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
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
		this("assign-datasource", true); //$NON-NLS-1$
	}
	
	protected AssignDataSource(String operation, boolean modelNameParam) {
		super(operation);
		this.modelNameParam = modelNameParam;
	}

	@Override
	protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.SOURCE_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SOURCE_NAME.getName()+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.TRANSLATOR_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME.getName()+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.DS_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DS_NAME.getName()+MISSING)));
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
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
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
		super("update-source", false); //$NON-NLS-1$
	}
	
}

class AddSource extends VDBOperations {
	
	public AddSource() {
		super("add-source"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.MODEL_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.MODEL_NAME.getName()+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.SOURCE_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SOURCE_NAME.getName()+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.TRANSLATOR_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME.getName()+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.DS_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DS_NAME.getName()+MISSING)));
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
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
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
		super("remove-source"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, RuntimeVDB vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.MODEL_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.MODEL_NAME.getName()+MISSING)));
		}
		
		if (!operation.hasDefined(OperationsConstants.SOURCE_NAME.getName())) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SOURCE_NAME.getName()+MISSING)));
		}

		String modelName = operation.get(OperationsConstants.MODEL_NAME.getName()).asString();
		String sourceName = operation.get(OperationsConstants.SOURCE_NAME.getName()).asString();

		try {
			synchronized (vdb.getVdb()) {
				ReplaceResult rr = vdb.removeSource(modelName, sourceName);
				updateServices(context, vdb, null, rr);
			}
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
	}

	@Override
	protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
		super.describeParameters(builder);
		builder.addParameter(OperationsConstants.MODEL_NAME);
		builder.addParameter(OperationsConstants.SOURCE_NAME);
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
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.RAR_NAME.getName()+MISSING)));
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
				throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString("RAR_notfound")));  //$NON-NLS-1$
			}
			DeploymentUnit du = DeploymentUnit.class.cast(sc.getValue());
			ConnectorXmlDescriptor cd = du.getAttachment(ConnectorXmlDescriptor.ATTACHMENT_KEY);
			ra = cd.getConnector().getResourceadapter();
		}
		if (ra instanceof ResourceAdapter1516) {
			ResourceAdapter1516 ra1516 = (ResourceAdapter1516)ra;
			result.add(buildReadOnlyNode("resourceadapter-class", ra1516.getResourceadapterClass())); //$NON-NLS-1$
			List<ConnectionDefinition> connDefinitions = ra1516.getOutboundResourceadapter().getConnectionDefinitions();
			for (ConnectionDefinition p:connDefinitions) {
				result.add(buildReadOnlyNode("managedconnectionfactory-class", p.getManagedConnectionFactoryClass().getValue())); //$NON-NLS-1$
				List<? extends ConfigProperty> props = p.getConfigProperties();
				for (ConfigProperty prop:props) {
					result.add(buildNode(prop));
				}
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
		ModelNode node = new ModelNode();
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

		if ("java.lang.String".equals(type)) { //$NON-NLS-1$
			node.get(name, TYPE).set(ModelType.STRING);
		}
		else if ("java.lang.Integer".equals(type)) { //$NON-NLS-1$
			node.get(name, TYPE).set(ModelType.INT);
		}
		else if ("java.lang.Long".equals(type)) { //$NON-NLS-1$
			node.get(name, TYPE).set(ModelType.LONG);
		}
		else if ("java.lang.Boolean".equals(type)) { //$NON-NLS-1$
			node.get(name, TYPE).set(ModelType.BOOLEAN);
		}

		node.get(name, REQUIRED).set(extended.required());

		if (extended.description() != null) {
			node.get(name, DESCRIPTION).set(extended.description());
		}
		node.get(name, "display").set(extended.display()); //$NON-NLS-1$
        node.get(name, READ_ONLY).set(extended.readOnly());
        node.get(name, "advanced").set(extended.advanced()); //$NON-NLS-1$

        if (extended.allowed() != null) {
        	for (String s:extended.allowed()) {
        		node.get(name, ALLOWED).add(s);
        	}
        }

        node.get(name, "masked").set(extended.masked()); //$NON-NLS-1$

        if (extended.defaultValue() != null) {
        	node.get(name, DEFAULT).set(extended.defaultValue());
        }
		return node;
	}

	@Override
	protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
		builder.addParameter(OperationsConstants.RAR_NAME);
		builder.setReplyType(ModelType.LIST);
		builder.setReplyValueType(ModelType.STRING);
	}
}

//TEIID-2404
class EngineStatistics extends TeiidOperationHandler {
	protected EngineStatistics() {
		super("engine-statistics"); //$NON-NLS-1$
	}

	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		EngineStatisticsMetadata stats = new EngineStatisticsMetadata();
		try {
			BufferManagerService bufferMgrSvc = getBufferManager(context);
			stats.setSessionCount(getSessionCount(context));
			stats.setTotalMemoryUsedInKB(bufferMgrSvc.getHeapCacheMemoryInUseKB());
			stats.setMemoryUsedByActivePlansInKB(bufferMgrSvc.getHeapMemoryInUseByActivePlansKB());
			stats.setDiskWriteCount(bufferMgrSvc.getDiskWriteCount());
			stats.setDiskReadCount(bufferMgrSvc.getDiskReadCount());
			stats.setCacheReadCount(bufferMgrSvc.getCacheReadCount());
			stats.setCacheWriteCount(bufferMgrSvc.getCacheWriteCount());
			stats.setDiskSpaceUsedInMB(bufferMgrSvc.getUsedDiskBufferSpaceMB());
			stats.setActivePlanCount(engine.getActivePlanCount());
			stats.setWaitPlanCount(engine.getWaitingPlanCount());
			stats.setMaxWaitPlanWaterMark(engine.getMaxWaitingPlanWatermark());
			VDBMetadataMapper.EngineStatisticsMetadataMapper.INSTANCE.wrap(stats, context.getResult());
		} catch (AdminException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
	}

	@Override
	protected void describeParameters(SimpleOperationDefinitionBuilder builder) {
		builder.setReplyType(ModelType.LIST);
		builder.setReplyParameters(VDBMetadataMapper.EngineStatisticsMetadataMapper.INSTANCE.getAttributeDefinitions());
	}
}
