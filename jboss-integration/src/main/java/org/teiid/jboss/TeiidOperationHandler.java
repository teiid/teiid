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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.DESCRIPTION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REPLY_PROPERTIES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REQUEST_PROPERTIES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.REQUIRED;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.TYPE;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLStreamException;

import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.impl.*;
import org.teiid.adminapi.impl.VDBMetadataMapper.TransactionMetadataMapper;
import org.teiid.adminapi.impl.VDBMetadataMapper.VDBTranslatorMetaDataMapper;
import org.teiid.client.RequestMessage;
import org.teiid.client.ResultsMessage;
import org.teiid.client.security.SessionToken;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidComponentException;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

abstract class TeiidOperationHandler extends BaseOperationHandler<DQPCore> {
	List<Transport> transports = new ArrayList<Transport>();
	protected VDBRepository vdbRepo;
	protected DQPCore engine;
	
	protected TeiidOperationHandler(String operationName){
		super(operationName);
	}
	
	@Override
	protected DQPCore getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {        
        List<ServiceName> services = context.getServiceRegistry(false).getServiceNames();
        for (ServiceName name:services) {
        	if (name.isParentOf(TeiidServiceNames.TRANSPORT_BASE)) {
        		ServiceController<?> transport = context.getServiceRegistry(false).getService(name);
        		if (transport != null) {
        			this.transports.add(Transport.class.cast(transport.getValue()));
        		}
        	}
        }
        ServiceController<?> repo = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_REPO);
        if (repo != null) {
        	this.vdbRepo = VDBRepository.class.cast(repo.getValue());
        }
        
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.ENGINE);
        if (sc != null) {
        	this.engine = DQPCore.class.cast(sc.getValue());
        }
        return this.engine;	
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
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).set(ModelType.STRING);
	}	
}

class GetActiveSessionsCount extends TeiidOperationHandler{
	protected GetActiveSessionsCount(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		try {
			int count = 0;
			for (Transport t: this.transports) {
				count += t.getActiveSessionsCount();
			}
			context.getResult().set(String.valueOf(count));
		} catch (AdminException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
	}
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).set(ModelType.INT);
	}		
}

class ListSessions extends TeiidOperationHandler{
	protected ListSessions() {
		super("list-sessions"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		ModelNode result = context.getResult();
		for (Transport t: this.transports) {
			Collection<SessionMetadata> sessions = t.getActiveSessions();
			for (SessionMetadata session:sessions) {
				VDBMetadataMapper.SessionMetadataMapper.INSTANCE.wrap(session, result.add());
			}
		}			
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).add(VDBMetadataMapper.SessionMetadataMapper.INSTANCE.describe(new ModelNode()));
	}	
}

class RequestsPerSession extends TeiidOperationHandler{
	protected RequestsPerSession() {
		super("requests-per-session"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		if (!operation.hasDefined(OperationsConstants.SESSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SESSION+MISSING)));
		}
		ModelNode result = context.getResult();
		List<RequestMetadata> requests = engine.getRequestsForSession(operation.get(OperationsConstants.SESSION).asString());
		for (RequestMetadata request:requests) {
			VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SESSION));
		
		operationNode.get(REPLY_PROPERTIES).add(VDBMetadataMapper.RequestMetadataMapper.INSTANCE.describe(new ModelNode()));
	}	
}

class ListRequests extends TeiidOperationHandler{
	protected ListRequests() {
		super("list-requests"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		ModelNode result = context.getResult();
		List<RequestMetadata> requests = engine.getRequests();
		for (RequestMetadata request:requests) {
			VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
		}
	}
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).add(VDBMetadataMapper.RequestMetadataMapper.INSTANCE.describe(new ModelNode()));
	}	
}

class RequestsPerVDB extends TeiidOperationHandler{
	protected RequestsPerVDB() {
		super("requests-per-vdb"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		if (!operation.hasDefined(OperationsConstants.VDB_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.VDB_VERSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION+MISSING)));
		}
		
		ModelNode result = context.getResult();
		String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
			for (Transport t: this.transports) {
			List<RequestMetadata> requests = t.getRequestsUsingVDB(vdbName,vdbVersion);
			for (RequestMetadata request:requests) {
				VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
			}
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_NAME));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, TYPE).set(ModelType.INT);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_VERSION)); 
		
		operationNode.get(REPLY_PROPERTIES).add(VDBMetadataMapper.RequestMetadataMapper.INSTANCE.describe(new ModelNode()));
	}	
}

class GetLongRunningQueries extends TeiidOperationHandler{
	protected GetLongRunningQueries() {
		super("long-running-queries"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		ModelNode result = context.getResult();
		List<RequestMetadata> requests = engine.getLongRunningRequests();
		for (RequestMetadata request:requests) {
			VDBMetadataMapper.RequestMetadataMapper.INSTANCE.wrap(request, result.add());
		}
	}
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).add(VDBMetadataMapper.RequestMetadataMapper.INSTANCE.describe(new ModelNode()));
	}	
}

class TerminateSession extends TeiidOperationHandler{
	protected TerminateSession() {
		super("terminate-session"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		if (!operation.hasDefined(OperationsConstants.SESSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SESSION+MISSING)));
		}		
		for (Transport t: this.transports) {
			t.terminateSession(operation.get(OperationsConstants.SESSION).asString());
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SESSION));
	}		
}

class CancelRequest extends TeiidOperationHandler{
	protected CancelRequest() {
		super("cancel-request"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException{
		try {
			if (!operation.hasDefined(OperationsConstants.SESSION)) {
				throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SESSION+MISSING)));
			}
			if (!operation.hasDefined(OperationsConstants.EXECUTION_ID)) {
				throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.EXECUTION_ID+MISSING)));
			}			
			boolean pass = engine.cancelRequest(operation.get(OperationsConstants.SESSION).asString(), operation.get(OperationsConstants.EXECUTION_ID).asLong());
			ModelNode result = context.getResult();
			result.set(pass);
		} catch (TeiidComponentException e) {
			throw new OperationFailedException(e, new ModelNode().set(e.getMessage()));
		} 
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SESSION));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.EXECUTION_ID, TYPE).set(ModelType.LONG);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.EXECUTION_ID, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.EXECUTION_ID, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.EXECUTION_ID));
		
		operationNode.get(REPLY_PROPERTIES).set(ModelType.BOOLEAN);
	}		
}

abstract class BaseCachehandler extends BaseOperationHandler<SessionAwareCache>{
	BaseCachehandler(String operationName){
		super(operationName);
	}
	
	@Override
	protected SessionAwareCache getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
		String cacheType = Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE.name();
		
		if (operation.hasDefined(OperationsConstants.CACHE_TYPE)) {
			cacheType = operation.get(OperationsConstants.CACHE_TYPE).asString();
			//throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.CACHE_TYPE+MISSING)));
		}
		
		ServiceController<?> sc;
		if (SessionAwareCache.isResultsetCache(cacheType)) {
			sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.CACHE_RESULTSET); 
		}
		else {
			sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.CACHE_PREPAREDPLAN);
		}
		
        return SessionAwareCache.class.cast(sc.getValue());	
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
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		ModelNode node = new ModelNode();
		node.get(OperationsConstants.CACHE_TYPE, TYPE).set(ModelType.STRING);
		node.get(OperationsConstants.CACHE_TYPE, REQUIRED).set(true);
		node.get(OperationsConstants.CACHE_TYPE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.CACHE_TYPE));
		operationNode.get(REPLY_PROPERTIES).add(node);
	}	
}

class ClearCache extends BaseCachehandler {
	
	protected ClearCache() {
		super("clear-cache"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, SessionAwareCache cache, ModelNode operation) throws OperationFailedException {
		if (operation.hasDefined(OperationsConstants.CACHE_TYPE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.CACHE_TYPE+MISSING)));
		}

		String cacheType = operation.get(OperationsConstants.CACHE_TYPE).asString();
		if (operation.hasDefined(OperationsConstants.VDB_NAME) && operation.hasDefined(OperationsConstants.VDB_VERSION)) {
			String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
			int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
			LogManager.logInfo(LogConstants.CTX_DQP, IntegrationPlugin.Util.getString("clearing_cache_vdb", cacheType, vdbName, vdbVersion)); //$NON-NLS-1$
			cache.clearForVDB(vdbName, vdbVersion);
		}
		else {
			LogManager.logInfo(LogConstants.CTX_DQP, IntegrationPlugin.Util.getString("clearing_cache", cacheType)); //$NON-NLS-1$
			cache.clearAll();
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.CACHE_TYPE));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, REQUIRED).set(false);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_NAME));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, TYPE).set(ModelType.INT);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, REQUIRED).set(false);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_VERSION)); 
		
	}	
}

class CacheStatistics extends BaseCachehandler {
	
	protected CacheStatistics() {
		super("cache-statistics"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, SessionAwareCache cache, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.CACHE_TYPE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.CACHE_TYPE+MISSING)));
		}
		ModelNode result = context.getResult();
		String cacheType = operation.get(OperationsConstants.CACHE_TYPE).asString();
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
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.CACHE_TYPE));
		
		ModelNode node = new ModelNode();
		operationNode.get(REPLY_PROPERTIES).add(VDBMetadataMapper.CacheStatisticsMetadataMapper.INSTANCE.describe(node));
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
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).add(VDBMetadataMapper.WorkerPoolStatisticsMetadataMapper.INSTANCE.describe(new ModelNode()));
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
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		ModelNode node = new ModelNode();
		operationNode.get(REPLY_PROPERTIES).add(TransactionMetadataMapper.INSTANCE.describe(node));
	}	
}

class TerminateTransaction extends TeiidOperationHandler{
	
	protected TerminateTransaction() {
		super("terminate-transaction"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {
		
		if (!operation.hasDefined(OperationsConstants.XID)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.XID+MISSING)));
		}		
		
		String xid = operation.get(OperationsConstants.XID).asString();
		try {
			engine.terminateTransaction(xid);
		} catch (AdminException e) {
			throw new OperationFailedException(e, new ModelNode().set(e.getMessage()));
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.XID, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.XID, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.XID, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));
	}	
}

class MergeVDBs extends BaseOperationHandler<VDBRepository>{
	
	protected MergeVDBs() {
		super("merge-vdbs"); //$NON-NLS-1$
	}
	
	@Override
	protected VDBRepository getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_REPO);
        return VDBRepository.class.cast(sc.getValue());	
	}
	
	@Override
	protected void executeOperation(OperationContext context, VDBRepository repo, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.SOURCE_VDBNAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SOURCE_VDBNAME+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.SOURCE_VDBVERSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SOURCE_VDBVERSION+MISSING)));
		}
		
		if (!operation.hasDefined(OperationsConstants.TARGET_VDBNAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TARGET_VDBNAME+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.TARGET_VDBVERSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TARGET_VDBVERSION+MISSING)));
		}
				
		String sourceVDBName = operation.get(OperationsConstants.SOURCE_VDBNAME).asString();
		int sourceVDBversion = operation.get(OperationsConstants.SOURCE_VDBVERSION).asInt();
		String targetVDBName = operation.get(OperationsConstants.TARGET_VDBNAME).asString();
		int targetVDBversion = operation.get(OperationsConstants.TARGET_VDBVERSION).asInt();
		try {
			repo.mergeVDBs(sourceVDBName, sourceVDBversion, targetVDBName, targetVDBversion);
		} catch (AdminException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBNAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBNAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBNAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SOURCE_VDBNAME));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBVERSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBVERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBVERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SOURCE_VDBVERSION));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBNAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBNAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBNAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.TARGET_VDBNAME));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBVERSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBVERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBVERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.TARGET_VDBVERSION));
	}	
}

class ExecuteQuery extends TeiidOperationHandler{
	
	protected ExecuteQuery() {
		super("execute-query"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(OperationContext context, DQPCore engine, ModelNode operation) throws OperationFailedException {
		
		if (!operation.hasDefined(OperationsConstants.VDB_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.VDB_VERSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION+MISSING)));
		}	
		if (!operation.hasDefined(OperationsConstants.SQL_QUERY)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SQL_QUERY+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.TIMEOUT_IN_MILLI)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TIMEOUT_IN_MILLI+MISSING)));
		}		
		
		ModelNode result = context.getResult();
		String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
		String sql = operation.get(OperationsConstants.SQL_QUERY).asString();
		int timeout = operation.get(OperationsConstants.TIMEOUT_IN_MILLI).asInt();
		
		result.set(executeQuery(vdbName, vdbVersion, sql, timeout, new ModelNode()));
	}

	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_NAME));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_VERSION));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SQL_QUERY, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SQL_QUERY, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SQL_QUERY, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SQL_QUERY));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TIMEOUT_IN_MILLI, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TIMEOUT_IN_MILLI, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TIMEOUT_IN_MILLI, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.TIMEOUT_IN_MILLI));
		
		operationNode.get(REPLY_PROPERTIES).set(ModelType.LIST);
	}	
	
	public ModelNode executeQuery(final String vdbName, final int version, final String command, final long timoutInMilli, final ModelNode resultsNode) throws OperationFailedException {
		String user = "CLI ADMIN"; //$NON-NLS-1$
		LogManager.logDetail(LogConstants.CTX_RUNTIME, IntegrationPlugin.Util.getString("admin_executing", user, command)); //$NON-NLS-1$
		
		SessionMetadata session = createTemporarySession(vdbName, version, user);

		final long requestID =  0L;
		
		DQPWorkContext context = new DQPWorkContext();
		context.setSession(session);
		
		try {
			return context.runInContext(new Callable<ModelNode>() {
				@Override
				public ModelNode call() throws Exception {
					
					long start = System.currentTimeMillis();
					RequestMessage request = new RequestMessage(command);
					request.setExecutionId(0L);
					request.setRowLimit(engine.getMaxRowsFetchSize()); // this would limit the number of rows that are returned.
					Future<ResultsMessage> message = engine.executeRequest(requestID, request);
					ResultsMessage rm = null;
					if (timoutInMilli < 0) {
						rm = message.get();
					} else {
						rm = message.get(timoutInMilli, TimeUnit.MILLISECONDS);
					}
			        if (rm.getException() != null) {
			            throw new AdminProcessingException(rm.getException());
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
	
	private SessionMetadata createTemporarySession(final String vdbName, final int version, final String userName) {
		
        long creationTime = System.currentTimeMillis();

        // Return a new session info object
        SessionMetadata newSession = new SessionMetadata();
        newSession.setSessionToken(new SessionToken(userName));
        newSession.setSessionId(newSession.getSessionToken().getSessionID());
        newSession.setUserName(userName);
        newSession.setCreatedTime(creationTime);
        newSession.setApplicationName("admin-console"); //$NON-NLS-1$
        newSession.setVDBName(vdbName);
        newSession.setVDBVersion(version);
        
        newSession.setVdb(this.vdbRepo.getVDB(vdbName, version));
		return newSession;
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
		if (!operation.hasDefined(OperationsConstants.VDB_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.VDB_VERSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION+MISSING)));
		}
		
		ModelNode result = context.getResult();
		String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();

		VDBMetaData vdb = repo.getVDB(vdbName, vdbVersion);
		VDBMetadataMapper.INSTANCE.wrap(vdb, result);
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_NAME));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_VERSION));

		operationNode.get(REPLY_PROPERTIES).set(VDBMetadataMapper.INSTANCE.describe(new ModelNode()));
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
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).add(VDBMetadataMapper.INSTANCE.describe(new ModelNode()));
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
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).add(VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE.describe(new ModelNode()));
	}	
}

class GetTranslator extends TranslatorOperationHandler{
	
	protected GetTranslator() {
		super("get-translator"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, TranslatorRepository repo, ModelNode operation) throws OperationFailedException {
		
		if (!operation.hasDefined(OperationsConstants.TRANSLATOR_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME+MISSING)));
		}
		
		ModelNode result = context.getResult();
		String translatorName = operation.get(OperationsConstants.TRANSLATOR_NAME).asString();
		VDBTranslatorMetaData translator = repo.getTranslatorMetaData(translatorName);
		VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE.wrap(translator, result);
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.TRANSLATOR_NAME));
		
		operationNode.get(REPLY_PROPERTIES).set(VDBTranslatorMetaDataMapper.INSTANCE.describe(new ModelNode()));
	}	
}

abstract class VDBOperations extends BaseOperationHandler<VDBMetaData>{
	private ObjectSerializer serializer;
	
	public VDBOperations(String operationName) {
		super(operationName);
	}
	
	@Override
	public VDBMetaData getService(OperationContext context, PathAddress pathAddress, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.VDB_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME+MISSING)));
		}
		
		if (!operation.hasDefined(OperationsConstants.VDB_VERSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION+MISSING)));
		}

		String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();

		ServiceController<?> osSvc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.OBJECT_SERIALIZER);
		this.serializer = ObjectSerializer.class.cast(osSvc.getValue());
		
		ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.vdbServiceName(vdbName, vdbVersion));
        return VDBMetaData.class.cast(sc.getValue());	
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_NAME));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_VERSION));
	}	
	
	protected void save(VDBMetaData vdb) throws AdminProcessingException{
		try {
			VDBMetadataParser.marshell(vdb, this.serializer.getVdbXmlOutputStream(vdb));
		} catch (IOException e) {
			throw new AdminProcessingException(e);
		} catch (XMLStreamException e) {
			throw new AdminProcessingException(e);
		}
	}	
}

class AddDataRole extends VDBOperations {

	public AddDataRole() {
		super("add-data-role"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, VDBMetaData vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.DATA_ROLE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.MAPPED_ROLE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.MAPPED_ROLE+MISSING)));
		}

		String policyName = operation.get(OperationsConstants.DATA_ROLE).asString();
		String mappedRole = operation.get(OperationsConstants.MAPPED_ROLE).asString();
		
		try {
			DataPolicyMetadata policy = vdb.getDataPolicy(policyName);
			
			if (policy == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("policy_not_found", policyName, vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
			}		
			
			policy.addMappedRoleName(mappedRole);
			save(vdb);
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
	}
	
	@Override
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		super.describeParameters(operationNode, bundle);

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.DATA_ROLE));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.MAPPED_ROLE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.MAPPED_ROLE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.MAPPED_ROLE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.MAPPED_ROLE));
	}		
}

class RemoveDataRole extends VDBOperations {

	public RemoveDataRole() {
		super("remove-data-role"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, VDBMetaData vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.DATA_ROLE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.MAPPED_ROLE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.MAPPED_ROLE+MISSING)));
		}

		String policyName = operation.get(OperationsConstants.DATA_ROLE).asString();
		String mappedRole = operation.get(OperationsConstants.MAPPED_ROLE).asString();
		
		try {
			DataPolicyMetadata policy = vdb.getDataPolicy(policyName);
			
			if (policy == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("policy_not_found", policyName, vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
			}		
			
			policy.removeMappedRoleName(mappedRole);
			save(vdb);
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
	}
	
	@Override
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		super.describeParameters(operationNode, bundle);

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.DATA_ROLE));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.MAPPED_ROLE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.MAPPED_ROLE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.MAPPED_ROLE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.MAPPED_ROLE));
	}		
}

class AddAnyAuthenticatedDataRole extends VDBOperations {

	public AddAnyAuthenticatedDataRole() {
		super("add-anyauthenticated-role"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, VDBMetaData vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.DATA_ROLE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE+MISSING)));
		}

		String policyName = operation.get(OperationsConstants.DATA_ROLE).asString();
		
		try {
			DataPolicyMetadata policy = vdb.getDataPolicy(policyName);
			
			if (policy == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("policy_not_found", policyName, vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
			}		
			
			policy.setAnyAuthenticated(true);
			save(vdb);
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
	}
	
	@Override
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		super.describeParameters(operationNode, bundle);

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.DATA_ROLE));
	}		
	
}

class RemoveAnyAuthenticatedDataRole extends VDBOperations {

	public RemoveAnyAuthenticatedDataRole() {
		super("remove-anyauthenticated-role"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, VDBMetaData vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.DATA_ROLE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DATA_ROLE+MISSING)));
		}

		String policyName = operation.get(OperationsConstants.DATA_ROLE).asString();
		
		try {
			DataPolicyMetadata policy = vdb.getDataPolicy(policyName);
			
			if (policy == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("policy_not_found", policyName, vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
			}		
			
			policy.setAnyAuthenticated(false);
			save(vdb);
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
	}
	
	@Override
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		super.describeParameters(operationNode, bundle);

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DATA_ROLE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.DATA_ROLE));
	}			
}

class ChangeVDBConnectionType extends VDBOperations {

	public ChangeVDBConnectionType() {
		super("change-vdb-connection-type"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, VDBMetaData vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.CONNECTION_TYPE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.CONNECTION_TYPE+MISSING)));
		}

		String connectionType = operation.get(OperationsConstants.CONNECTION_TYPE).asString();
		try {
			vdb.setConnectionType(connectionType);
			save(vdb);
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
	}
	
	@Override
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		super.describeParameters(operationNode, bundle);
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CONNECTION_TYPE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CONNECTION_TYPE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CONNECTION_TYPE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.CONNECTION_TYPE));
	}		
}

class AssignDataSource extends VDBOperations {

	public AssignDataSource() {
		super("assign-datasource"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(OperationContext context, VDBMetaData vdb, ModelNode operation) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.MODEL_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.MODEL_NAME+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.SOURCE_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SOURCE_NAME+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.TRANSLATOR_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME+MISSING)));
		}

		if (!operation.hasDefined(OperationsConstants.DS_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.DS_NAME+MISSING)));
		}
		
		
		String modelName = operation.get(OperationsConstants.MODEL_NAME).asString();
		String sourceName = operation.get(OperationsConstants.SOURCE_NAME).asString();
		String translatorName = operation.get(OperationsConstants.TRANSLATOR_NAME).asString();
		String dsName = operation.get(OperationsConstants.DS_NAME).asString();
		
		try {
			ModelMetaData model = vdb.getModel(modelName);
			
			if (model == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("model_not_found", modelName, vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
			}
			
			SourceMappingMetadata source = model.getSourceMapping(sourceName);
			if(source == null) {
				throw new AdminProcessingException(IntegrationPlugin.Util.getString("source_not_found", sourceName, modelName, vdb.getName(), vdb.getVersion())); //$NON-NLS-1$
			}
			source.setTranslatorName(translatorName);
			source.setConnectionJndiName(dsName);
			save(vdb);
		} catch (AdminProcessingException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
	}
	
	@Override
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		super.describeParameters(operationNode, bundle);

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.MODEL_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.MODEL_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.MODEL_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.MODEL_NAME));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.MODEL_NAME));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.TRANSLATOR_NAME));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DS_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DS_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.DS_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.DS_NAME));
		
	}		
}