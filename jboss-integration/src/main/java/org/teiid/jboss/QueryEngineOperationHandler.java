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

import java.util.Collection;
import java.util.List;
import java.util.ResourceBundle;

import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.impl.*;
import org.teiid.adminapi.impl.MetadataMapper.TransactionMetadataMapper;
import org.teiid.adminapi.impl.MetadataMapper.VDBTranslatorMetaDataMapper;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.datamgr.TranslatorRepository;
import org.teiid.jboss.deployers.RuntimeEngineDeployer;

abstract class QueryEngineOperationHandler extends BaseOperationHandler<RuntimeEngineDeployer> {
	
	protected QueryEngineOperationHandler(String operationName){
		super(operationName);
	}
	
	@Override
	protected RuntimeEngineDeployer getService(OperationContext context, PathAddress pathAddress) throws OperationFailedException {
		String serviceName = pathAddress.getLastElement().getValue();
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.engineServiceName(serviceName));
        return RuntimeEngineDeployer.class.cast(sc.getValue());	
	}
}

abstract class TranslatorOperationHandler extends BaseOperationHandler<TranslatorRepository> {
	
	protected TranslatorOperationHandler(String operationName){
		super(operationName);
	}
	
	@Override
	public TranslatorRepository getService(OperationContext context, PathAddress pathAddress) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.TRANSLATOR_REPO);
        return TranslatorRepository.class.cast(sc.getValue());	
	}
}

class GetRuntimeVersion extends QueryEngineOperationHandler{
	protected GetRuntimeVersion(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		node.set(engine.getRuntimeVersion());
	}
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).set(ModelType.STRING);
		operationNode.get(REPLY_PROPERTIES, DESCRIBE).set(bundle.getString(getReplyName()));
	}	
}

class GetActiveSessionsCount extends QueryEngineOperationHandler{
	protected GetActiveSessionsCount(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		try {
			node.set(String.valueOf(engine.getActiveSessionsCount()));
		} catch (AdminException e) {
			// TODO: handle exception in model node terms 
		}
	}
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).set(ModelType.INT);
		operationNode.get(REPLY_PROPERTIES, DESCRIBE).set(bundle.getString(getReplyName()));
	}		
}

class GetActiveSessions extends QueryEngineOperationHandler{
	protected GetActiveSessions() {
		super("active-sessions"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		try {
			Collection<SessionMetadata> sessions = engine.getActiveSessions();
			for (SessionMetadata session:sessions) {
				MetadataMapper.SessionMetadataMapper.wrap(session, node.add());
			}
		} catch (AdminException e) {
			// TODO: handle exception in model node terms 
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).add(MetadataMapper.SessionMetadataMapper.describe(new ModelNode()));
	}	
}

class GetRequestsPerSession extends QueryEngineOperationHandler{
	protected GetRequestsPerSession() {
		super("requests-per-session"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		if (!operation.hasDefined(OperationsConstants.SESSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SESSION+MISSING)));
		}
		List<RequestMetadata> requests = engine.getRequestsForSession(operation.get(OperationsConstants.SESSION).asString());
		for (RequestMetadata request:requests) {
			MetadataMapper.RequestMetadataMapper.wrap(request, node.add());
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SESSION));
		
		operationNode.get(REPLY_PROPERTIES).add(MetadataMapper.RequestMetadataMapper.describe(new ModelNode()));
	}	
}

class GetRequestsPerVDB extends QueryEngineOperationHandler{
	protected GetRequestsPerVDB() {
		super("requests-per-vdb"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		try {
			
			if (!operation.hasDefined(OperationsConstants.VDB_NAME)) {
				throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME+MISSING)));
			}
			if (!operation.hasDefined(OperationsConstants.VDB_VERSION)) {
				throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION+MISSING)));
			}
						
			String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
			int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
			List<RequestMetadata> requests = engine.getRequestsUsingVDB(vdbName,vdbVersion);
			for (RequestMetadata request:requests) {
				MetadataMapper.RequestMetadataMapper.wrap(request, node.add());
			}
		} catch (AdminException e) {
			throw new OperationFailedException(e, new ModelNode().set(e.getMessage()));
		} 
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_NAME));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, TYPE).set(ModelType.INT);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_VERSION)); 
		
		operationNode.get(REPLY_PROPERTIES).add(MetadataMapper.RequestMetadataMapper.describe(new ModelNode()));
	}	
}

class GetLongRunningQueries extends QueryEngineOperationHandler{
	protected GetLongRunningQueries() {
		super("long-running-queries"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		List<RequestMetadata> requests = engine.getLongRunningRequests();
		for (RequestMetadata request:requests) {
			MetadataMapper.RequestMetadataMapper.wrap(request, node.add());
		}
	}
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).add(MetadataMapper.RequestMetadataMapper.describe(new ModelNode()));
	}	
}

class TerminateSession extends QueryEngineOperationHandler{
	protected TerminateSession() {
		super("terminate-session"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		if (!operation.hasDefined(OperationsConstants.SESSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SESSION+MISSING)));
		}		
		engine.terminateSession(operation.get(OperationsConstants.SESSION).asString());
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SESSION));
	}		
}

class CancelQuery extends QueryEngineOperationHandler{
	protected CancelQuery() {
		super("cancel-query"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		try {
			if (!operation.hasDefined(OperationsConstants.SESSION)) {
				throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.SESSION+MISSING)));
			}
			boolean pass = engine.cancelRequest(operation.get(OperationsConstants.SESSION).asString(), operation.get(OperationsConstants.EXECUTION_ID).asLong());
			node.set(pass);
		} catch (AdminException e) {
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
		operationNode.get(REPLY_PROPERTIES, DESCRIBE).set(bundle.getString(getReplyName()));
	}		
}

class CacheTypes extends QueryEngineOperationHandler{
	protected CacheTypes() {
		super("cache-types"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		Collection<String> types = engine.getCacheTypes();
		for (String type:types) {
			node.add(type);
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

class ClearCache extends QueryEngineOperationHandler{
	
	protected ClearCache() {
		super("clear-cache"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		
		if (!operation.hasDefined(OperationsConstants.CACHE_TYPE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.CACHE_TYPE+MISSING)));
		}
		
		String cacheType = operation.get(OperationsConstants.CACHE_TYPE).asString();
		
		if (operation.get(OperationsConstants.VDB_NAME) != null && operation.get(OperationsConstants.VDB_VERSION) != null) {
			String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
			int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
			engine.clearCache(cacheType, vdbName, vdbVersion);
		}
		else {
			engine.clearCache(cacheType);
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

class CacheStatistics extends QueryEngineOperationHandler{
	
	protected CacheStatistics() {
		super("cache-statistics"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.CACHE_TYPE)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.CACHE_TYPE+MISSING)));
		}
		String cacheType = operation.get(OperationsConstants.CACHE_TYPE).asString();
		CacheStatisticsMetadata stats = engine.getCacheStatistics(cacheType);
		MetadataMapper.CacheStatisticsMetadataMapper.wrap(stats, node);
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.CACHE_TYPE));
		
		ModelNode node = new ModelNode();
		operationNode.get(REPLY_PROPERTIES).add(MetadataMapper.CacheStatisticsMetadataMapper.describe(node));
	}	
}

class WorkerPoolStatistics extends QueryEngineOperationHandler{
	
	protected WorkerPoolStatistics() {
		super("workerpool-statistics"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		WorkerPoolStatisticsMetadata stats = engine.getWorkerPoolStatistics();
		MetadataMapper.WorkerPoolStatisticsMetadataMapper.wrap(stats, node);
	}
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).add(MetadataMapper.WorkerPoolStatisticsMetadataMapper.describe(new ModelNode()));
	}		
}

class ActiveTransactions extends QueryEngineOperationHandler{
	
	protected ActiveTransactions() {
		super("active-transactions"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		Collection<TransactionMetadata> txns = engine.getTransactions();
		for (TransactionMetadata txn:txns) {
			MetadataMapper.TransactionMetadataMapper.wrap(txn, node.add());
		}
	}
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		ModelNode node = new ModelNode();
		operationNode.get(REPLY_PROPERTIES).add(TransactionMetadataMapper.describe(node));
	}	
}

class TerminateTransaction extends QueryEngineOperationHandler{
	
	protected TerminateTransaction() {
		super("terminate-transaction"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		
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
	protected VDBRepository getService(OperationContext context, PathAddress pathAddress) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_REPO);
        return VDBRepository.class.cast(sc.getValue());	
	}
	
	@Override
	protected void executeOperation(VDBRepository repo, ModelNode operation, ModelNode node) throws OperationFailedException {
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

class ExecuteQuery extends QueryEngineOperationHandler{
	
	protected ExecuteQuery() {
		super("execute-query"); //$NON-NLS-1$
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		
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
		
		String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
		String sql = operation.get(OperationsConstants.SQL_QUERY).asString();
		int timeout = operation.get(OperationsConstants.TIMEOUT_IN_MILLI).asInt();
		try {
			List<List> results = engine.executeQuery(vdbName, vdbVersion, sql, timeout);
			List colNames = results.get(0);
			for (int rowNum = 1; rowNum < results.size(); rowNum++) {
				
				List row = results.get(rowNum);
				ModelNode rowNode = new ModelNode();
				rowNode.get(TYPE).set(ModelType.OBJECT);
				
				for (int colNum = 0; colNum < colNames.size(); colNum++) {
					//TODO: support in native types instead of string here.
					rowNode.get(ATTRIBUTES, colNames.get(colNum).toString()).set(row.get(colNum).toString());
				}
				node.add(rowNode);
			}
		} catch (AdminException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
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
}

class GetVDB extends BaseOperationHandler<VDBRepository>{
	
	protected GetVDB() {
		super("get-vdb"); //$NON-NLS-1$
	}
	
	@Override
	protected VDBRepository getService(OperationContext context, PathAddress pathAddress) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_REPO);
        return VDBRepository.class.cast(sc.getValue());	
	}
	
	@Override
	protected void executeOperation(VDBRepository repo, ModelNode operation, ModelNode node) throws OperationFailedException {
		if (!operation.hasDefined(OperationsConstants.VDB_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_NAME+MISSING)));
		}
		if (!operation.hasDefined(OperationsConstants.VDB_VERSION)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.VDB_VERSION+MISSING)));
		}
		
		String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();

		VDBMetaData vdb = repo.getVDB(vdbName, vdbVersion);
		MetadataMapper.wrap(vdb, node);
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_NAME));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_VERSION));

		operationNode.get(REPLY_PROPERTIES).set(MetadataMapper.describe(new ModelNode()));
	}	
}

class ListVDBs extends BaseOperationHandler<VDBRepository>{
	
	protected ListVDBs() {
		super("list-vdbs"); //$NON-NLS-1$
	}
	
	@Override
	protected VDBRepository getService(OperationContext context, PathAddress pathAddress) throws OperationFailedException {
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.VDB_REPO);
        return VDBRepository.class.cast(sc.getValue());	
	}	
	
	@Override
	protected void executeOperation(VDBRepository repo, ModelNode operation, ModelNode node) throws OperationFailedException {
		List<VDBMetaData> vdbs = repo.getVDBs();
		for (VDBMetaData vdb:vdbs) {
			node.add(MetadataMapper.wrap(vdb, node.add()));
		}
	}
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES).add(MetadataMapper.describe(new ModelNode()));
	}	
}

class GetTranslators extends TranslatorOperationHandler{
	
	protected GetTranslators() {
		super("list-translators"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(TranslatorRepository repo, ModelNode operation, ModelNode node) throws OperationFailedException {
		List<VDBTranslatorMetaData> translators = repo.getTranslators();
		for (VDBTranslatorMetaData t:translators) {
			node.add(MetadataMapper.VDBTranslatorMetaDataMapper.wrap(t, node.add()));
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES, DESCRIPTION).set(bundle.getString(getReplyName()));
		operationNode.get(REPLY_PROPERTIES).add(MetadataMapper.VDBTranslatorMetaDataMapper.describe(new ModelNode()));
	}	
}

class GetTranslator extends TranslatorOperationHandler{
	
	protected GetTranslator() {
		super("get-translator"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(TranslatorRepository repo, ModelNode operation, ModelNode node) throws OperationFailedException {
		
		if (!operation.hasDefined(OperationsConstants.TRANSLATOR_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME+MISSING)));
		}
		
		String translatorName = operation.get(OperationsConstants.TRANSLATOR_NAME).asString();
		
		VDBTranslatorMetaData translator = repo.getTranslatorMetaData(translatorName);
		MetadataMapper.VDBTranslatorMetaDataMapper.wrap(translator, node);
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.TRANSLATOR_NAME));
		
		operationNode.get(REPLY_PROPERTIES).set(VDBTranslatorMetaDataMapper.describe(new ModelNode()));
		operationNode.get(REPLY_PROPERTIES, DESCRIPTION).set(bundle.getString(getReplyName()));
	}	
}

class ListQueryEngines extends TranslatorOperationHandler{
	
	protected ListQueryEngines() {
		super("list-engines"); //$NON-NLS-1$
	}
	
	@Override
	protected void executeOperation(TranslatorRepository repo, ModelNode operation, ModelNode node) throws OperationFailedException {
		
		if (!operation.hasDefined(OperationsConstants.TRANSLATOR_NAME)) {
			throw new OperationFailedException(new ModelNode().set(IntegrationPlugin.Util.getString(OperationsConstants.TRANSLATOR_NAME+MISSING)));
		}
		
		String translatorName = operation.get(OperationsConstants.TRANSLATOR_NAME).asString();
		
		VDBTranslatorMetaData translator = repo.getTranslatorMetaData(translatorName);
		MetadataMapper.VDBTranslatorMetaDataMapper.wrap(translator, node);
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.TRANSLATOR_NAME));
		
		operationNode.get(REPLY_PROPERTIES).set(VDBTranslatorMetaDataMapper.describe(new ModelNode()));
		operationNode.get(REPLY_PROPERTIES, DESCRIPTION).set(bundle.getString(getReplyName()));
	}	
}