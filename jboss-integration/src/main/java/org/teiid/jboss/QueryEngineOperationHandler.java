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
import static org.teiid.jboss.Configuration.addAttribute;

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
import org.teiid.adminapi.impl.MetadataMapper.VDBTranslatorMetaDataMapper;
import org.teiid.jboss.deployers.RuntimeEngineDeployer;

abstract class QueryEngineOperationHandler extends AbstractAddStepHandler implements DescriptionProvider {
	private static final String DESCRIBE = ".describe"; //$NON-NLS-1$
	
	private String operationName; 
	
	protected QueryEngineOperationHandler(String operationName){
		this.operationName = operationName;
	}
	
	@Override
	protected void populateModel(final ModelNode operation, final ModelNode model) throws OperationFailedException{
		
	}
	
	@Override
    protected void performRuntime(final OperationContext context, final ModelNode operation, final ModelNode model,
            final ServiceVerificationHandler verificationHandler, final List<ServiceController<?>> newControllers) throws OperationFailedException {
		
        ServiceController<?> sc = context.getServiceRegistry(false).getRequiredService(TeiidServiceNames.engineServiceName(operation.require(Configuration.ENGINE_NAME).asString()));
        RuntimeEngineDeployer engine = RuntimeEngineDeployer.class.cast(sc.getValue());
        executeOperation(engine, operation, model);
    }
	
		
    @Override
    public ModelNode getModelDescription(final Locale locale) {
        final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
        final ModelNode operation = new ModelNode();
        operation.get(OPERATION_NAME).set(this.operationName);
        operation.get(DESCRIPTION).set(bundle.getString(getBundleOperationName()+DESCRIBE));
        addAttribute(operation, Configuration.ENGINE_NAME, REQUEST_PROPERTIES, bundle.getString(Configuration.ENGINE_NAME+Configuration.DESC), ModelType.STRING, true, null);
        describeParameters(operation, bundle);
        return operation;
    }	
    
    protected String getBundleOperationName() {
    	return RuntimeEngineDeployer.class.getSimpleName()+"."+this.operationName; //$NON-NLS-1$
    }
	
    protected String getReplyName() {
    	return getBundleOperationName()+".reply"+DESCRIBE; //$NON-NLS-1$
    }
    
    protected String getParameterDescription(ResourceBundle bundle, String parmName) {
    	return bundle.getString(RuntimeEngineDeployer.class.getSimpleName()+"."+this.operationName+"."+parmName+DESCRIBE); //$NON-NLS-1$ //$NON-NLS-2$
    }    
    
	abstract protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException;
	
	protected void describeParameters(@SuppressWarnings("unused") ModelNode operationNode, @SuppressWarnings("unused")ResourceBundle bundle) {
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
}

class GetActiveSessions extends QueryEngineOperationHandler{
	protected GetActiveSessions(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		node.get(TYPE).set(ModelType.LIST);
		
		try {
			Collection<SessionMetadata> sessions = engine.getActiveSessions();
			for (SessionMetadata session:sessions) {
				node.add(MetadataMapper.SessionMetadataMapper.wrap(session, node.add()));
			}
		} catch (AdminException e) {
			// TODO: handle exception in model node terms 
		}
	}
}

class GetRequestsPerSession extends QueryEngineOperationHandler{
	protected GetRequestsPerSession(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		node.get(TYPE).set(ModelType.LIST);
		
		List<RequestMetadata> requests = engine.getRequestsForSession(operation.get(OperationsConstants.SESSION).asString());
		for (RequestMetadata request:requests) {
			node.add(MetadataMapper.RequestMetadataMapper.wrap(request, node.add()));
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SESSION));
		
		//TODO: define response??
	}	
}

class GetRequestsPerVDB extends QueryEngineOperationHandler{
	protected GetRequestsPerVDB(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		node.get(TYPE).set(ModelType.LIST);
		
		try {
			String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
			int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
			List<RequestMetadata> requests = engine.getRequestsUsingVDB(vdbName,vdbVersion);
			for (RequestMetadata request:requests) {
				node.add(MetadataMapper.RequestMetadataMapper.wrap(request, node.add()));
			}
		} catch (AdminException e) {
			// TODO: handle exception in model node terms 
		} 
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

class GetLongRunningQueries extends QueryEngineOperationHandler{
	protected GetLongRunningQueries(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		node.get(TYPE).set(ModelType.LIST);
		
		List<RequestMetadata> requests = engine.getLongRunningRequests();
		for (RequestMetadata request:requests) {
			node.add(MetadataMapper.RequestMetadataMapper.wrap(request, node.add()));
		}
	}
}

class TerminateSession extends QueryEngineOperationHandler{
	protected TerminateSession(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		engine.terminateSession(operation.get(OperationsConstants.SESSION).asString());
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.SESSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.SESSION));
	}		
}

class CancelQuery extends QueryEngineOperationHandler{
	protected CancelQuery(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException{
		try {
			engine.cancelRequest(operation.get(OperationsConstants.SESSION).asString(), operation.get(OperationsConstants.EXECUTION_ID).asLong());
		} catch (AdminException e) {
			// TODO: handle exception in model node terms 
		} 
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

class CacheTypes extends QueryEngineOperationHandler{
	protected CacheTypes(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		node.get(TYPE).set(ModelType.LIST);
		Collection<String> types = engine.getCacheTypes();
		for (String type:types) {
			node.add(type);
		}
	}
}

class ClearCache extends QueryEngineOperationHandler{
	
	protected ClearCache(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
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
	
	protected CacheStatistics(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		String cacheType = operation.get(OperationsConstants.CACHE_TYPE).asString();
		CacheStatisticsMetadata stats = engine.getCacheStatistics(cacheType);
		MetadataMapper.CacheStatisticsMetadataMapper.wrap(stats, node);
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.CACHE_TYPE, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.CACHE_TYPE));
	}	
}

class WorkerPoolStatistics extends QueryEngineOperationHandler{
	
	protected WorkerPoolStatistics(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		WorkerPoolStatisticsMetadata stats = engine.getWorkerPoolStatistics();
		MetadataMapper.WorkerPoolStatisticsMetadataMapper.wrap(stats, node);
	}
}

class ActiveTransactions extends QueryEngineOperationHandler{
	
	protected ActiveTransactions(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		Collection<TransactionMetadata> txns = engine.getTransactions();
		
		node.get(TYPE).set(ModelType.LIST);
		
		for (TransactionMetadata txn:txns) {
			node.add(MetadataMapper.TransactionMetadataMapper.wrap(txn, node.add()));
		}
		
	}
}

class TerminateTransaction extends QueryEngineOperationHandler{
	
	protected TerminateTransaction(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		String xid = operation.get(OperationsConstants.XID).asString();
		try {
			engine.terminateTransaction(xid);
		} catch (AdminException e) {
			// TODO: Handle exception
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.XID, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.XID, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.XID, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.XID));
	}	
}

class MergeVDBs extends QueryEngineOperationHandler{
	
	protected MergeVDBs(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		String sourceVDBName = operation.get(OperationsConstants.SOURCE_VDBNAME).asString();
		int sourceVDBversion = operation.get(OperationsConstants.SOURCE_VDBVERSION).asInt();
		String targetVDBName = operation.get(OperationsConstants.TARGET_VDBNAME).asString();
		int targetVDBversion = operation.get(OperationsConstants.TARGET_VDBVERSION).asInt();
		try {
			engine.mergeVDBs(sourceVDBName, sourceVDBversion, targetVDBName, targetVDBversion);
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
	
	protected ExecuteQuery(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();
		String sql = operation.get(OperationsConstants.SQL_QUERY).asString();
		int timeout = operation.get(OperationsConstants.TIMEOUT_IN_MILLI).asInt();
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
	}	
}

class GetVDB extends QueryEngineOperationHandler{
	
	protected GetVDB(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		String vdbName = operation.get(OperationsConstants.VDB_NAME).asString();
		int vdbVersion = operation.get(OperationsConstants.VDB_VERSION).asInt();

		VDBMetaData vdb = engine.getVDB(vdbName, vdbVersion);
		MetadataMapper.wrap(vdb, node);
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_NAME));
		
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.VDB_VERSION, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.VDB_VERSION));
	}	
}

class GetVDBs extends QueryEngineOperationHandler{
	
	protected GetVDBs(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		List<VDBMetaData> vdbs = engine.getVDBs();
		node.get(TYPE).set(ModelType.LIST);
		
		for (VDBMetaData vdb:vdbs) {
			node.add(MetadataMapper.wrap(vdb, node.add()));
		}
	}
	
}

class GetTranslators extends QueryEngineOperationHandler{
	
	protected GetTranslators(String operationName) {
		super(operationName);
	}
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		List<VDBTranslatorMetaData> translators = engine.getTranslators();
		node.get(TYPE).set(ModelType.OBJECT);
		
		for (VDBTranslatorMetaData t:translators) {
			node.add(MetadataMapper.VDBTranslatorMetaDataMapper.wrap(t, node.add()));
		}
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REPLY_PROPERTIES, TYPE).set(ModelType.LIST);
		operationNode.get(REPLY_PROPERTIES, VALUE_TYPE).set(ModelType.OBJECT);
		operationNode.get(REPLY_PROPERTIES, DESCRIPTION).set(bundle.getString(getReplyName()));
	}	
}

class GetTranslator extends QueryEngineOperationHandler{
	
	protected GetTranslator(String operationName) {
		super(operationName);
	}
	
	@Override
	protected void executeOperation(RuntimeEngineDeployer engine, ModelNode operation, ModelNode node) throws OperationFailedException {
		String translatorName = operation.get(OperationsConstants.TRANSLATOR_NAME).asString();
		
		VDBTranslatorMetaData translator = engine.getTranslator(translatorName);
		MetadataMapper.VDBTranslatorMetaDataMapper.wrap(translator, node);
	}
	
	protected void describeParameters(ModelNode operationNode, ResourceBundle bundle) {
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, TYPE).set(ModelType.STRING);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, REQUIRED).set(true);
		operationNode.get(REQUEST_PROPERTIES, OperationsConstants.TRANSLATOR_NAME, DESCRIPTION).set(getParameterDescription(bundle, OperationsConstants.TRANSLATOR_NAME));
		
		VDBTranslatorMetaDataMapper.describe(operationNode.get(REPLY_PROPERTIES));
		operationNode.get(REPLY_PROPERTIES, DESCRIPTION).set(bundle.getString(getReplyName()));
	}	
}