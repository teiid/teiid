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
import java.util.Locale;
import java.util.ResourceBundle;

import org.jboss.as.controller.*;
import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.jboss.msc.service.ServiceController;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.impl.*;
import org.teiid.jboss.deployers.RuntimeEngineDeployer;

abstract class QueryEngineModelHandler implements OperationHandler, DescriptionProvider {
	private static final String DESCRIBE = ".describe"; //$NON-NLS-1$
	
	private String operationName; 
	
	protected QueryEngineModelHandler(String operationName){
		this.operationName = operationName;
	}
	
	@Override
	public OperationResult execute(final OperationContext context, final ModelNode operation, final ResultHandler resultHandler) throws OperationFailedException {
        
        final RuntimeOperationContext runtimeContext = context.getRuntimeContext();
        if (runtimeContext != null) {
            runtimeContext.setRuntimeTask(new RuntimeTask() {

                @Override
                public void execute(RuntimeTaskContext context) throws OperationFailedException {
                    ServiceController<?> sc = context.getServiceRegistry().getRequiredService(RuntimeEngineDeployer.SERVICE_NAME);
                    RuntimeEngineDeployer engine = RuntimeEngineDeployer.class.cast(sc.getValue());
                    
                    resultHandler.handleResultFragment(ResultHandler.EMPTY_LOCATION, executeOperation(engine, operation));
                }});
        }

        resultHandler.handleResultComplete();
        return new BasicOperationResult();
	}
	
    @Override
    public ModelNode getModelDescription(final Locale locale) {
        final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
        final ModelNode operation = new ModelNode();
        operation.get(OPERATION_NAME).set(this.operationName);
        operation.get(DESCRIPTION).set(bundle.getString(getBundleOperationName()+DESCRIBE));
        describeParameters(operation, bundle);
        return operation;
    }	
    
    protected String getBundleOperationName() {
    	return RuntimeEngineDeployer.class.getSimpleName()+"."+this.operationName; //$NON-NLS-1$
    }
	
    protected String getParameterDescription(ResourceBundle bundle, String parmName) {
    	return bundle.getString(RuntimeEngineDeployer.class.getSimpleName()+"."+this.operationName+"."+parmName+DESCRIBE); //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
	abstract protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException;
	
	protected void describeParameters(@SuppressWarnings("unused") ModelNode operationNode, @SuppressWarnings("unused")ResourceBundle bundle) {
	}
}

class GetRuntimeVersion extends QueryEngineModelHandler{
	protected GetRuntimeVersion(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException{
		ModelNode node = new ModelNode();
		node.set(engine.getRuntimeVersion());
		return node;
	}
}

class GetActiveSessionsCount extends QueryEngineModelHandler{
	protected GetActiveSessionsCount(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException{
		ModelNode node = new ModelNode();
		try {
			node.set(String.valueOf(engine.getActiveSessionsCount()));
		} catch (AdminException e) {
			// TODO: handle exception in model node terms 
		}
		return node;
	}
}

class GetActiveSessions extends QueryEngineModelHandler{
	protected GetActiveSessions(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException{
		ModelNode node = new ModelNode();
		node.get(TYPE).set(ModelType.LIST);
		
		try {
			Collection<SessionMetadata> sessions = engine.getActiveSessions();
			for (SessionMetadata session:sessions) {
				node.add(SessionMetadataMapper.wrap(session));
			}
		} catch (AdminException e) {
			// TODO: handle exception in model node terms 
		}
		return node;
	}
}

class GetRequestsPerSession extends QueryEngineModelHandler{
	protected GetRequestsPerSession(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException{
		ModelNode node = new ModelNode();
		node.get(TYPE).set(ModelType.LIST);
		
		List<RequestMetadata> requests = engine.getRequestsForSession(operation.get(OperationsConstants.SESSION).asString());
		for (RequestMetadata request:requests) {
			node.add(RequestMetadataMapper.wrap(request));
		}
		return node;
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SESSION));
		
		//TODO: define response??
	}	
}

class GetRequestsPerVDB extends QueryEngineModelHandler{
	protected GetRequestsPerVDB(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException{
		ModelNode node = new ModelNode();
		node.get(TYPE).set(ModelType.LIST);
		
		try {
			String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
			int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
			List<RequestMetadata> requests = engine.getRequestsUsingVDB(vdbName,vdbVersion);
			for (RequestMetadata request:requests) {
				node.add(RequestMetadataMapper.wrap(request));
			}
		} catch (AdminException e) {
			// TODO: handle exception in model node terms 
		} 
		return node;
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_NAME));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, TYPE).set(ModelType.INT);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_VERSION)); 
		
		//TODO: define response??
	}	
}

class GetLongRunningQueries extends QueryEngineModelHandler{
	protected GetLongRunningQueries(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException{
		ModelNode node = new ModelNode();
		node.get(TYPE).set(ModelType.LIST);
		
		List<RequestMetadata> requests = engine.getLongRunningRequests();
		for (RequestMetadata request:requests) {
			node.add(RequestMetadataMapper.wrap(request));
		}
		return node;
	}
}

class TerminateSession extends QueryEngineModelHandler{
	protected TerminateSession(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException{
		ModelNode node = new ModelNode();
		engine.terminateSession(operation.get(OperationsConstants.SESSION).asString());
		return node;
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SESSION));
	}		
}

class CancelQuery extends QueryEngineModelHandler{
	protected CancelQuery(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException{
		ModelNode node = new ModelNode();
		try {
			engine.cancelRequest(operation.get(OperationsConstants.SESSION).asString(), operation.get(OperationsConstants.EXECUTION_ID).asLong());
		} catch (AdminException e) {
			// TODO: handle exception in model node terms 
		} 
		return node;
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SESSION));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.EXECUTION_ID, TYPE).set(ModelType.LONG);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.EXECUTION_ID, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.EXECUTION_ID, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.EXECUTION_ID));
	}		
}

class CacheTypes extends QueryEngineModelHandler{
	protected CacheTypes(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException {
		ModelNode node = new ModelNode();
		node.get(TYPE).set(ModelType.LIST);
		Collection<String> types = engine.getCacheTypes();
		for (String type:types) {
			node.add(type);
		}
		return node;
	}
}

class ClearCache extends QueryEngineModelHandler{
	
	protected ClearCache(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException {
		ModelNode node = new ModelNode();
		String cacheType = operation.get(OperationsConstants.CACHE_TYPE).asString();
		
		if (operation.get(OperationsConstants.VDB_NAME) != null && operation.get(OperationsConstants.VDB_VERSION) != null) {
			String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
			int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
			engine.clearCache(cacheType, vdbName, vdbVersion);
		}
		else {
			engine.clearCache(cacheType);
		}
		return node;
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

class CacheStatistics extends QueryEngineModelHandler{
	
	protected CacheStatistics(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException {
		String cacheType = operation.get(OperationsConstants.CACHE_TYPE).asString();
		CacheStatisticsMetadata stats = engine.getCacheStatistics(cacheType);
		return CacheStatisticsMetadataMapper.wrap(stats);
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.CACHE_TYPE));
	}	
}

class WorkerPoolStatistics extends QueryEngineModelHandler{
	
	protected WorkerPoolStatistics(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException {
		WorkerPoolStatisticsMetadata stats = engine.getWorkerPoolStatistics();
		return WorkerPoolStatisticsMetadataMapper.wrap(stats);
	}
}

class ActiveTransactions extends QueryEngineModelHandler{
	
	protected ActiveTransactions(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException {
		Collection<TransactionMetadata> txns = engine.getTransactions();
		
		ModelNode node = new ModelNode();
		node.get(TYPE).set(ModelType.LIST);
		
		for (TransactionMetadata txn:txns) {
			node.add(TransactionMetadataMapper.wrap(txn));
		}
		
		return node;
	}
}

class TerminateTransaction extends QueryEngineModelHandler{
	
	protected TerminateTransaction(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException {
		String xid = operation.get(OperationsConstants.XID).asString();
		try {
			engine.terminateTransaction(xid);
		} catch (AdminException e) {
			// TODO: Handle exception
		}
		return new ModelNode();
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.XID, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.XID, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.XID, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));
	}	
}

class MergeVDBs extends QueryEngineModelHandler{
	
	protected MergeVDBs(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException {
		String sourceVDBName = operation.get(OperationsConstants.SOURCE_VDBNAME).asString();
		int sourceVDBversion = operation.get(OperationsConstants.SOURCE_VDBVERSION).asInt();
		String targetVDBName = operation.get(OperationsConstants.TARGET_VDBNAME).asString();
		int targetVDBversion = operation.get(OperationsConstants.TARGET_VDBVERSION).asInt();
		try {
			engine.mergeVDBs(sourceVDBName, sourceVDBversion, targetVDBName, targetVDBversion);
		} catch (AdminException e) {
			throw new OperationFailedException(new ModelNode().set(e.getMessage()));
		}
		return new ModelNode();
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBNAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBNAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBNAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBVERSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBVERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SOURCE_VDBVERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBNAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBNAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBNAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBVERSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBVERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TARGET_VDBVERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));
	}	
}

class ExecuteQuery extends QueryEngineModelHandler{
	
	protected ExecuteQuery(String operationName) {
		super(operationName);
	}
	@Override
	protected ModelNode executeOperation(RuntimeEngineDeployer engine, ModelNode operation) throws OperationFailedException {
		String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
		String sql = operation.get(OperationsConstants.SQL_QUERY).asString();
		int timeout = operation.get(OperationsConstants.TIMEOUT_IN_MILLI).asInt();
		ModelNode node = new ModelNode();
		try {
			node.get(TYPE).set(ModelType.LIST);
			
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
		return node;
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SQL_QUERY, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SQL_QUERY, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SQL_QUERY, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));

		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TIMEOUT_IN_MILLI, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TIMEOUT_IN_MILLI, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TIMEOUT_IN_MILLI, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));
	}	
}