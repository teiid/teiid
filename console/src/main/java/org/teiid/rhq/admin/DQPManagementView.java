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
package org.teiid.rhq.admin;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedOperation;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.ManagedObjectImpl;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.CompositeValue;
import org.jboss.metatype.api.values.CompositeValueSupport;
import org.jboss.metatype.api.values.GenericValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.rhq.plugins.jbossas5.connection.ProfileServiceConnection;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.Request.ProcessingState;
import org.teiid.adminapi.VDB.Status;
import org.teiid.rhq.plugin.objects.ExecutedResult;
import org.teiid.rhq.plugin.objects.RequestMetadata;
import org.teiid.rhq.plugin.objects.SessionMetadata;
import org.teiid.rhq.plugin.objects.TransactionMetadata;
import org.teiid.rhq.plugin.util.DeploymentUtils;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.Platform;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.VDB;

public class DQPManagementView implements PluginConstants {

	private static ManagedComponent mc = null;
	private static final Log LOG = LogFactory.getLog(PluginConstants.DEFAULT_LOGGER_CATEGORY);

	private static final String VDB_EXT = ".vdb"; //$NON-NLS-1$
	
	//Session metadata fields
	private static final String SECURITY_DOMAIN = "securityDomain"; //$NON-NLS-1$
	private static final String VDB_VERSION = "VDBVersion"; //$NON-NLS-1$
	private static final String VDB_NAME = "VDBName"; //$NON-NLS-1$
	private static final String USER_NAME = "userName"; //$NON-NLS-1$
	private static final String SESSION_ID = "sessionId"; //$NON-NLS-1$
	private static final String LAST_PING_TIME = "lastPingTime"; //$NON-NLS-1$
	private static final String IP_ADDRESS = "IPAddress"; //$NON-NLS-1$
	private static final String CLIENT_HOST_NAME = "clientHostName"; //$NON-NLS-1$
	private static final String CREATED_TIME = "createdTime"; //$NON-NLS-1$
	private static final String APPLICATION_NAME = "applicationName"; //$NON-NLS-1$

	//Request metadata fields
	private static final String TRANSACTION_ID = "transactionId"; //$NON-NLS-1$
	private static final String NODE_ID = "nodeId"; //$NON-NLS-1$
	private static final String SOURCE_REQUEST = "sourceRequest"; //$NON-NLS-1$
	private static final String COMMAND = "command"; //$NON-NLS-1$
	private static final String START_TIME = "startTime"; //$NON-NLS-1$
	private static final String EXECUTION_ID = "executionId"; //$NON-NLS-1$
	private static final String STATE = "processingState"; //$NON-NLS-1$
	
	//Transaction metadata fields
	private static final String SCOPE = "scope"; //$NON-NLS-1$
	private static final String ASSOCIATED_SESSION = "associatedSession"; //$NON-NLS-1$
	
	public DQPManagementView() {
	}

	/*
	 * Metric methods
	 */
	public Object getMetric(ProfileServiceConnection connection,
			String componentType, String identifier, String metric,
			Map<String, Object> valueMap) throws Exception {
		Object resultObject = new Object();

		if (componentType.equals(PluginConstants.ComponentType.Platform.NAME)) {
			resultObject = getPlatformMetric(connection, componentType, metric,	valueMap);
		} else if (componentType.equals(PluginConstants.ComponentType.VDB.NAME)) {
			resultObject = getVdbMetric(connection, componentType, identifier,metric, valueMap);
		}
		return resultObject;
	}

	private Object getPlatformMetric(ProfileServiceConnection connection,
			String componentType, String metric, Map<String, Object> valueMap) throws Exception {

		Object resultObject = new Object();

		if (metric.equals(PluginConstants.ComponentType.Platform.Metrics.QUERY_COUNT)) {
			resultObject = new Double(getQueryCount(connection).doubleValue());
		} else if (metric.equals(PluginConstants.ComponentType.Platform.Metrics.SESSION_COUNT)) {
			resultObject = new Double(getSessionCount(connection).doubleValue());
		} else if (metric.equals(PluginConstants.ComponentType.Platform.Metrics.LONG_RUNNING_QUERIES)) {
			Collection<RequestMetadata> longRunningQueries = new ArrayList<RequestMetadata>();
			getRequestCollectionValue(getLongRunningQueries(connection),	longRunningQueries);
			resultObject = new Double(longRunningQueries.size());
		} else if (metric.equals(PluginConstants.ComponentType.Platform.Metrics.BUFFER_USAGE)) {
			try {
				resultObject = ProfileServiceUtil.doubleValue(getUsedBufferSpace(connection));
			} catch (Exception e) {
				final String msg = "Exception executing operation: " + Platform.Operations.GET_BUFFER_USAGE; //$NON-NLS-1$
				LOG.error(msg, e);
			}
		} else if (metric.startsWith(Admin.Cache.PREPARED_PLAN_CACHE.toString() + ".") //$NON-NLS-1$
				|| metric.startsWith(Admin.Cache.QUERY_SERVICE_RESULT_SET_CACHE	.toString()+ ".")) { //$NON-NLS-1$
			return getCacheProperty(connection, metric);
		}
		return resultObject;
	}

	private Object getCacheProperty(ProfileServiceConnection connection,String metric) {
		int dotIndex = metric.indexOf('.');
		String cacheType = metric.substring(0, dotIndex);
		String property = metric.substring(dotIndex + 1);
		CompositeValueSupport mv = (CompositeValueSupport) getCacheStats(connection, cacheType);
		MetaValue v = mv.get(property);
		return ((SimpleValue) v).getValue();
	}

	private Object getVdbMetric(ProfileServiceConnection connection,
			String componentType, String identifier, String metric,
			Map<String, Object> valueMap) throws Exception {

		Object resultObject = new Object();

		if (metric.equals(PluginConstants.ComponentType.VDB.Metrics.ERROR_COUNT)) {
			// TODO remove version parameter after AdminAPI is changed
			resultObject = getErrorCount(connection, (String) valueMap.get(VDB.NAME));
		} else if (metric.equals(PluginConstants.ComponentType.VDB.Metrics.STATUS)) {
			// TODO remove version parameter after AdminAPI is changed
			resultObject = getVDBStatus(connection, (String) valueMap.get(VDB.NAME));
		} else if (metric.equals(PluginConstants.ComponentType.VDB.Metrics.QUERY_COUNT)) {
			resultObject = new Double(getQueryCount(connection).doubleValue());
		} else if (metric.equals(PluginConstants.ComponentType.VDB.Metrics.SESSION_COUNT)) {
			resultObject = new Double(getSessionCount(connection).doubleValue());
		} else if (metric.equals(PluginConstants.ComponentType.VDB.Metrics.LONG_RUNNING_QUERIES)) {
			Collection<RequestMetadata> longRunningQueries = new ArrayList<RequestMetadata>();
			getRequestCollectionValue(getLongRunningQueries(connection),	longRunningQueries);
			resultObject = new Double(longRunningQueries.size());
		}
		return resultObject;
	}

	/*
	 * Operation methods
	 */

	public void executeOperation(ProfileServiceConnection connection,
			ExecutedResult operationResult, final Map<String, Object> valueMap) throws Exception {

		if (operationResult.getComponentType().equals(PluginConstants.ComponentType.Platform.NAME)) {
			executePlatformOperation(connection, operationResult,	operationResult.getOperationName(), valueMap);
		} else if (operationResult.getComponentType().equals(	PluginConstants.ComponentType.VDB.NAME)) {
			executeVdbOperation(connection, operationResult, operationResult	.getOperationName(), valueMap);
		}
	}

	private void executePlatformOperation(ProfileServiceConnection connection,
			ExecutedResult operationResult, final String operationName,
			final Map<String, Object> valueMap) throws Exception {
		Collection<RequestMetadata> resultObject = new ArrayList<RequestMetadata>();
		Collection<SessionMetadata> activeSessionsCollection = new ArrayList<SessionMetadata>();
		Collection<TransactionMetadata> transactionsCollection = new ArrayList<TransactionMetadata>();

		if (operationName.equals(Platform.Operations.GET_LONGRUNNINGQUERIES)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			getRequestCollectionValue(getLongRunningQueries(connection),	resultObject);
			operationResult.setContent(createReportResultList(fieldNameList,	resultObject.iterator()));
		} else if (operationName.equals(Platform.Operations.GET_SESSIONS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue sessionMetaValue = getSessions(connection);
			getSessionCollectionValue(sessionMetaValue,activeSessionsCollection);
			operationResult.setContent(createReportResultList(fieldNameList,	activeSessionsCollection.iterator()));
		} else if (operationName.equals(Platform.Operations.GET_REQUESTS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue requestMetaValue = getRequests(connection);
			getRequestCollectionValue(requestMetaValue, resultObject);
			operationResult.setContent(createReportResultList(fieldNameList,	resultObject.iterator()));
		} else if (operationName.equals(Platform.Operations.GET_TRANSACTIONS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue transactionMetaValue = getTransactions(connection);
			getTransactionCollectionValue(transactionMetaValue,transactionsCollection);
			operationResult.setContent(createReportResultList(fieldNameList,	resultObject.iterator()));
		} else if (operationName.equals(Platform.Operations.KILL_TRANSACTION)) {
			String sessionID = (String) valueMap.get(Operation.Value.TRANSACTION_ID);
			MetaValue[] args = new MetaValue[] { SimpleValueSupport.wrap(sessionID) };
			try {
				executeManagedOperation(connection, getRuntimeEngineDeployer(connection, mc), Platform.Operations.KILL_TRANSACTION, args);
			} catch (Exception e) {
				final String msg = "Exception executing operation: " + Platform.Operations.KILL_TRANSACTION; //$NON-NLS-1$
				LOG.error(msg, e);
			}
		} else if (operationName.equals(Platform.Operations.KILL_SESSION)) {
			String sessionID = (String) valueMap.get(Operation.Value.SESSION_ID);
			MetaValue[] args = new MetaValue[] { SimpleValueSupport.wrap(sessionID) };
			try {
				executeManagedOperation(connection, getRuntimeEngineDeployer(connection, mc), Platform.Operations.KILL_SESSION, args);
			} catch (Exception e) {
				final String msg = "Exception executing operation: " + Platform.Operations.KILL_SESSION; //$NON-NLS-1$
				LOG.error(msg, e);
			}
		} else if (operationName.equals(Platform.Operations.KILL_REQUEST)) {
			Long requestID = (Long) valueMap.get(Operation.Value.REQUEST_ID);
			String sessionID = (String) valueMap.get(Operation.Value.SESSION_ID);
			MetaValue[] args = new MetaValue[] {
					SimpleValueSupport.wrap(requestID),
					SimpleValueSupport.wrap(sessionID) };
			try {
				executeManagedOperation(connection, getRuntimeEngineDeployer(connection, mc), Platform.Operations.KILL_REQUEST, args);
			} catch (Exception e) {
				final String msg = "Exception executing operation: " + Platform.Operations.KILL_REQUEST; //$NON-NLS-1$
				LOG.error(msg, e);
			}
		} else if (operationName.equals(Platform.Operations.DEPLOY_VDB_BY_URL)) {
			String vdbUrl = (String) valueMap.get(Operation.Value.VDB_URL);
			String deployName = (String) valueMap.get(Operation.Value.VDB_DEPLOY_NAME);
		
			// add vdb extension if missing
			if (!deployName.endsWith(VDB_EXT)) {
				deployName = deployName + VDB_EXT;
			}

			try {
				URL url = new URL(vdbUrl);
				DeploymentUtils.deployArchive(deployName, connection.getDeploymentManager(), url, false);
			} catch (Exception e) {
				final String msg = "Exception executing operation: " + Platform.Operations.DEPLOY_VDB_BY_URL; //$NON-NLS-1$
				LOG.error(msg, e);
				throw new RuntimeException(e);
			}
		}
	}

	private void executeVdbOperation(ProfileServiceConnection connection,
			ExecutedResult operationResult, final String operationName,
			final Map<String, Object> valueMap) throws Exception {
		Collection<ArrayList<String>> sqlResultsObject = new ArrayList<ArrayList<String>>();
		Collection<RequestMetadata> resultObject = new ArrayList<RequestMetadata>();
		Collection<SessionMetadata> activeSessionsCollection = new ArrayList<SessionMetadata>();
		String vdbName = (String) valueMap.get(PluginConstants.ComponentType.VDB.NAME);
		vdbName = formatVdbName(vdbName);
		String vdbVersion = (String) valueMap.get(PluginConstants.ComponentType.VDB.VERSION);

		if (operationName.equals(VDB.Operations.GET_PROPERTIES)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			getProperties(connection, PluginConstants.ComponentType.VDB.NAME);
			operationResult.setContent(createReportResultList(fieldNameList,	resultObject.iterator()));
		} else if (operationName.equals(VDB.Operations.GET_SESSIONS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue sessionMetaValue = getSessions(connection);
			getSessionCollectionValueForVDB(sessionMetaValue, activeSessionsCollection, vdbName);
			operationResult.setContent(createReportResultList(fieldNameList,	activeSessionsCollection.iterator()));
		} else if (operationName.equals(VDB.Operations.GET_REQUESTS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue requestMetaValue = getRequestsForVDB(connection, vdbName,	Integer.parseInt(vdbVersion));
			getRequestCollectionValue(requestMetaValue, resultObject);
			operationResult.setContent(createReportResultList(fieldNameList,	resultObject.iterator()));
		} else if (operationName.equals(VDB.Operations.GET_MATVIEWS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue resultsMetaValue = executeMaterializedViewQuery(	connection, vdbName, Integer.parseInt(vdbVersion));
			getResultsCollectionValue(resultsMetaValue, sqlResultsObject);
			operationResult.setContent(createReportResultListForMatViewQuery(fieldNameList, sqlResultsObject.iterator()));
		} else if (operationName.equals(VDB.Operations.CLEAR_CACHE)) {
			
			try {
			executeClearCache(	connection, vdbName, Integer.parseInt(vdbVersion), 
					(String) valueMap.get(Operation.Value.CACHE_TYPE));
				
			}catch(Exception e){
				//Some failure during Clear Cache. Set message here since it has already been logged.
				operationResult.setContent("failure - see log for details"); //$NON-NLS-1$
			}

			//If no exceptions, we assume the clear cache worked
			operationResult.setContent("cache successfully cleared!"); //$NON-NLS-1$
		
		} else if (operationName.equals(VDB.Operations.RELOAD_MATVIEW)) {
			MetaValue resultsMetaValue = reloadMaterializedView(connection,	vdbName, Integer.parseInt(vdbVersion),
					(String) valueMap.get(Operation.Value.MATVIEW_SCHEMA),
					(String) valueMap.get(Operation.Value.MATVIEW_TABLE),
					(Boolean) valueMap.get(Operation.Value.INVALIDATE_MATVIEW));
			if (resultsMetaValue==null) {
				operationResult.setContent("failure - see log for details"); //$NON-NLS-1$
			} else {
				operationResult.setContent("data successfully refreshed!"); //$NON-NLS-1$
			}
		}

	}

	/*
	 * Helper methods
	 */

	private String formatVdbName(String vdbName) {

		return vdbName.substring(0, vdbName.lastIndexOf(".")); //$NON-NLS-1$
	}

	public MetaValue getProperties(ProfileServiceConnection connection,	final String component) {

		MetaValue propertyValue = null;
		MetaValue args = null;

		try {
			propertyValue = executeManagedOperation(connection,	getRuntimeEngineDeployer(connection, mc),
					PluginConstants.Operation.GET_PROPERTIES, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_PROPERTIES; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return propertyValue;

	}

	protected MetaValue getRequests(ProfileServiceConnection connection) {

		MetaValue requestsCollection = null;
		MetaValue args = null;

		try {
			requestsCollection = executeManagedOperation(connection,	getRuntimeEngineDeployer(connection, mc),	PluginConstants.Operation.GET_REQUESTS, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_REQUESTS; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return requestsCollection;

	}

	protected void executeClearCache(
			ProfileServiceConnection connection, String vdbName, int vdbVersion, String cacheType) throws Exception {

		MetaValue[] args = new MetaValue[] {SimpleValueSupport.wrap(cacheType),
				SimpleValueSupport.wrap(vdbName),
				SimpleValueSupport.wrap(vdbVersion) }; 

		try {
			executeManagedOperation(connection,	getRuntimeEngineDeployer(connection, mc),	VDB.Operations.CLEAR_CACHE, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + VDB.Operations.EXECUTE_QUERIES; //$NON-NLS-1$
			LOG.error(msg, e);
			throw e;
		}
	}
	
	protected MetaValue executeMaterializedViewQuery(
			ProfileServiceConnection connection, String vdbName, int vdbVersion) {

		MetaValue resultsCollection = null;
		MetaValue[] args = new MetaValue[] {
				SimpleValueSupport.wrap(vdbName),
				SimpleValueSupport.wrap(vdbVersion),
				SimpleValueSupport.wrap(Operation.Value.MAT_VIEW_QUERY),	
				SimpleValueSupport.wrap(Long.parseLong("9999999")) }; //$NON-NLS-1$

		try {
			resultsCollection = executeManagedOperation(connection,	getRuntimeEngineDeployer(connection, mc),	VDB.Operations.EXECUTE_QUERIES, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + VDB.Operations.EXECUTE_QUERIES; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return resultsCollection;

	}

	protected MetaValue reloadMaterializedView(
			ProfileServiceConnection connection, String vdbName,
			int vdbVersion, String schema, String table, Boolean invalidate) {

		MetaValue result = null;
		String matView = schema + "." + table; //$NON-NLS-1$
		String query = PluginConstants.Operation.Value.MAT_VIEW_REFRESH;
		query = query.replace("param1", matView); //$NON-NLS-1$
		query = query.replace("param2", invalidate.toString()); //$NON-NLS-1$
		MetaValue[] args = new MetaValue[] {
				SimpleValueSupport.wrap(vdbName),
				SimpleValueSupport.wrap(vdbVersion),
				SimpleValueSupport.wrap(query),
				SimpleValueSupport.wrap(Long.parseLong("9999999")) }; //$NON-NLS-1$

		try {
			result = executeManagedOperation(connection,	getRuntimeEngineDeployer(connection, mc),
					VDB.Operations.EXECUTE_QUERIES, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + VDB.Operations.RELOAD_MATVIEW; //$NON-NLS-1$
			LOG.error(msg, e);
			
		}

		return result;

	}

	protected MetaValue getRequestsForVDB(ProfileServiceConnection connection,
			String vdbName, int vdbVersion) {

		MetaValue requestsCollection = null;
		MetaValue[] args = new MetaValue[] {
				SimpleValueSupport.wrap(vdbName),
				SimpleValueSupport.wrap(vdbVersion) };

		try {
			requestsCollection = executeManagedOperation(connection,
					getRuntimeEngineDeployer(connection, mc),
					PluginConstants.ComponentType.VDB.Operations.GET_REQUESTS,
					args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_REQUESTS; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return requestsCollection;

	}

	protected MetaValue getTransactions(ProfileServiceConnection connection) {

		MetaValue transactionsCollection = null;
		MetaValue args = null;

		try {
			transactionsCollection = executeManagedOperation(connection,
					getRuntimeEngineDeployer(connection, mc),
					Platform.Operations.GET_TRANSACTIONS, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_TRANSACTIONS; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return transactionsCollection;

	}

	public MetaValue getSessions(ProfileServiceConnection connection) {

		MetaValue sessionCollection = null;
		MetaValue args = null;

		try {
			sessionCollection = executeManagedOperation(connection,
					getRuntimeEngineDeployer(connection, mc),
					PluginConstants.Operation.GET_SESSIONS, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_SESSIONS; //$NON-NLS-1$
			LOG.error(msg, e);
		}
		return sessionCollection;

	}

	public static String getVDBStatus(ProfileServiceConnection connection,
			String vdbName) {

		ManagedComponent mcVdb = null;
		try {
			mcVdb = ProfileServiceUtil.getManagedComponent(connection,
							new org.jboss.managed.api.ComponentType(
									PluginConstants.ComponentType.VDB.TYPE,
									PluginConstants.ComponentType.VDB.SUBTYPE),	vdbName);
		} catch (NamingException e) {
			final String msg = "NamingException in getVDBStatus(): " + e.getExplanation(); //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e) {
			final String msg = "Exception in getVDBStatus(): " + e.getMessage(); //$NON-NLS-1$
			LOG.error(msg, e);
		}

		if (mcVdb == null) {
			return Status.INACTIVE.toString();
		}

		return ProfileServiceUtil.getSimpleValue(mcVdb, "status", String.class); //$NON-NLS-1$
	}

	public static MetaValue executeManagedOperation(
			ProfileServiceConnection connection, ManagedComponent mc,
			String operation, MetaValue... args) throws Exception {

		for (ManagedOperation mo : mc.getOperations()) {
			String opName = mo.getName();
			if (opName.equals(operation)) {
				try {
					if (args.length == 1 && args[0] == null) {
						return mo.invoke();
					}
					return mo.invoke(args);
				} catch (Exception e) {
					final String msg = "Exception getting the AdminApi in " + operation; //$NON-NLS-1$
					LOG.error(msg, e);
					throw new RuntimeException(e);
				}
			}
		}
		throw new Exception("No operation found with given name = " + operation); //$NON-NLS-1$

	}

	/**
	 * @param mc
	 * @return
	 */
	private static ManagedComponent getRuntimeEngineDeployer(
			ProfileServiceConnection connection, ManagedComponent mc) {
		try {
			mc = ProfileServiceUtil.getRuntimeEngineDeployer(connection);
		} catch (NamingException e) {
			final String msg = "NamingException getting the DQPManagementView"; //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e1) {
			final String msg = "Exception getting the DQPManagementView"; //$NON-NLS-1$
			LOG.error(msg, e1);
		}
		return mc;
	}

	/**
	 * @param mc
	 * @return
	 */
	private static ManagedComponent getBufferService(ProfileServiceConnection connection, ManagedComponent mc) {
		try {
			mc = ProfileServiceUtil.getBufferService(connection);
		} catch (NamingException e) {
			final String msg = "NamingException getting the SessionService"; //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e1) {
			final String msg = "Exception getting the SessionService"; //$NON-NLS-1$
			LOG.error(msg, e1);
		}
		return mc;
	}

	public static MetaValue getManagedProperty(ProfileServiceConnection connection, ManagedComponent mc, String property) throws Exception {

		ManagedProperty managedProperty = null;
		try {
			managedProperty = mc.getProperty(property);
		} catch (Exception e) {
			final String msg = "Exception getting the AdminApi in " + property; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		if (managedProperty != null) {
			return managedProperty.getValue();
		}

		throw new Exception("No property found with given name =" + property); //$NON-NLS-1$
	}

	private Integer getQueryCount(ProfileServiceConnection connection) throws Exception {

		Integer count = new Integer(0);

		MetaValue requests = null;
		Collection<RequestMetadata> requestsCollection = new ArrayList<RequestMetadata>();

		requests = getRequests(connection);

		getRequestCollectionValue(requests, requestsCollection);

		if (!requestsCollection.isEmpty()) {
			count = requestsCollection.size();
		}

		return count;
	}

	private Integer getSessionCount(ProfileServiceConnection connection) throws Exception {

		Collection<SessionMetadata> activeSessionsCollection = new ArrayList<SessionMetadata>();
		MetaValue sessionMetaValue = getSessions(connection);
		getSessionCollectionValue(sessionMetaValue, activeSessionsCollection);
		return activeSessionsCollection.size();
	}

	/**
	 * @param mcVdb
	 * @return count
	 * @throws Exception
	 */
	private int getErrorCount(ProfileServiceConnection connection,String vdbName) {

		ManagedComponent mcVdb = null;
		try {
			mcVdb = ProfileServiceUtil.getManagedComponent(connection,
							new org.jboss.managed.api.ComponentType(
									PluginConstants.ComponentType.VDB.TYPE,
									PluginConstants.ComponentType.VDB.SUBTYPE),vdbName);
		} catch (NamingException e) {
			final String msg = "NamingException in getVDBStatus(): " + e.getExplanation(); //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e) {
			final String msg = "Exception in getVDBStatus(): " + e.getMessage(); //$NON-NLS-1$
			LOG.error(msg, e);
		}

		// Get models from VDB
		int count = 0;
		ManagedProperty property = mcVdb.getProperty("models"); //$NON-NLS-1$
		CollectionValueSupport valueSupport = (CollectionValueSupport) property.getValue();
		MetaValue[] metaValues = valueSupport.getElements();

		for (MetaValue value : metaValues) {
			GenericValueSupport genValueSupport = (GenericValueSupport) value;
			ManagedObjectImpl managedObject = (ManagedObjectImpl) genValueSupport.getValue();

			// Get any model errors/warnings
			MetaValue errors = managedObject.getProperty("errors").getValue(); //$NON-NLS-1$
			if (errors != null) {
				CollectionValueSupport errorValueSupport = (CollectionValueSupport) errors;
				MetaValue[] errorArray = errorValueSupport.getElements();
				count += errorArray.length;
			}
		}
		return count;
	}

	protected MetaValue getCacheStats(ProfileServiceConnection connection,
			String type) {
		try {
			return executeManagedOperation(connection,getRuntimeEngineDeployer(connection, mc),
					Platform.Operations.GET_CACHE_STATS, SimpleValueSupport.wrap(type));
		} catch (Exception e) {
			LOG.error("Exception executing operation: " + Platform.Operations.GET_CACHE_STATS, e); //$NON-NLS-1$
		}
		return null;
	}

	protected MetaValue getLongRunningQueries(
			ProfileServiceConnection connection) {

		MetaValue requestsCollection = null;
		MetaValue args = null;

		try {
			requestsCollection = executeManagedOperation(connection,
					getRuntimeEngineDeployer(connection, mc),
					Platform.Operations.GET_LONGRUNNINGQUERIES, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_LONGRUNNINGQUERIES; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return requestsCollection;
	}

	protected MetaValue getUsedBufferSpace(ProfileServiceConnection connection) {

		MetaValue usedBufferSpace = null;

		try {
			usedBufferSpace = getManagedProperty(connection, getBufferService(
					connection, mc), Platform.Operations.GET_BUFFER_USAGE);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_BUFFER_USAGE; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return usedBufferSpace;
	}

	private void getRequestCollectionValue(MetaValue pValue, Collection<RequestMetadata> list) throws Exception {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue).getElements()) {
				if (value.getMetaType().isComposite()) {
					RequestMetadata request = unwrapRequestMetaValue(value);
					list.add(request);
				} else {
					throw new IllegalStateException(pValue + " is not a Composite type"); //$NON-NLS-1$
				}
			}
		}
	}

	private void getResultsCollectionValue(MetaValue pValue, Collection<ArrayList<String>> list) throws Exception {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue).getElements()) {
				if (value.getMetaType().isCollection()) {
					ArrayList<String> row = new ArrayList<String>();
					MetaValue[] metaValueArray = ((CollectionValueSupport)value).getElements();
					for (MetaValue cell : metaValueArray){
						row.add(ProfileServiceUtil.stringValue(cell));
					}
					list.add(row);
				}
			}
		}
	}

	private void getResultsCollectionValueForMatViewRefresh(MetaValue pValue, Collection<ArrayList<String>> list) throws Exception {
		MetaType metaType = pValue.getMetaType();
		for (MetaValue value : ((CollectionValueSupport) pValue).getElements()) {
			if (value.getMetaType().isCollection()) {
				ArrayList<String> row = new ArrayList<String>();
				MetaValue[] metaValueArray = ((CollectionValueSupport)value).getElements();
				for (MetaValue cell : metaValueArray){
					row.add(ProfileServiceUtil.stringValue(cell));
				}
				list.add(row);
			}
		}
	}

	public <T> void getTransactionCollectionValue(MetaValue pValue, Collection<TransactionMetadata> list) throws Exception {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue).getElements()) {
				if (value.getMetaType().isComposite()) {
					TransactionMetadata transaction = unwrapTransactionMetaValue(value);
					list.add(transaction);
				} else {
					throw new IllegalStateException(pValue
							+ " is not a Composite type"); //$NON-NLS-1$
				}
			}
		}
	}

	public <T> void getSessionCollectionValue(MetaValue pValue,Collection<SessionMetadata> list) throws Exception {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue).getElements()) {
				if (value.getMetaType().isComposite()) {
					SessionMetadata session = unwrapSessionMetaValue(value);
					list.add(session);
				} else {
					throw new IllegalStateException(pValue
							+ " is not a Composite type"); //$NON-NLS-1$
				}
			}
		}
	}

	public <T> void getSessionCollectionValueForVDB(MetaValue pValue,Collection<SessionMetadata> list, String vdbName) throws Exception {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue).getElements()) {
				if (value.getMetaType().isComposite()) {
					if (ProfileServiceUtil.stringValue(((CompositeValueSupport)value).get("VDBName")).equals(vdbName)) { //$NON-NLS-1$
						SessionMetadata session = unwrapSessionMetaValue(value);
						list.add(session);
					}
				} else {
					throw new IllegalStateException(pValue+ " is not a Composite type"); //$NON-NLS-1$
				}
			}
		}
	}

	private Collection createReportResultList(List fieldNameList, Iterator objectIter) {
		Collection reportResultList = new ArrayList();

		while (objectIter.hasNext()) {
			Object object = objectIter.next();

			Class cls = null;
			try {
				cls = object.getClass();
				Iterator methodIter = fieldNameList.iterator();
				Map reportValueMap = new HashMap<String, String>();
				while (methodIter.hasNext()) {
					String fieldName = (String) methodIter.next();
					String methodName = fieldName;
					Method meth = cls.getMethod(methodName, (Class[]) null);
					Object retObj = meth.invoke(object, (Object[]) null);
					reportValueMap.put(fieldName, retObj);
				}
				reportResultList.add(reportValueMap);
			} catch (Throwable e) {
				System.err.println(e);
			}
		}
		return reportResultList;
	}

	private Collection createReportResultListForMatViewQuery(List fieldNameList, Iterator objectIter) {
		Collection reportResultList = new ArrayList();

		// Iterate through rows
		while (objectIter.hasNext()) {
			ArrayList<Object> columnValues = (ArrayList<Object>) objectIter.next();

			try {
				Iterator fieldIter = fieldNameList.iterator();
				Map reportValueMap = new HashMap<String, Object>();
				// Iterate through columns with a row
				for (Object columnValue : columnValues) {
					String fieldName = (String) fieldIter.next();
					reportValueMap.put(fieldName, columnValue);
				}
				reportResultList.add(reportValueMap);
			} catch (Throwable e) {
				System.err.println(e);
			}
		}
		return reportResultList;
	}
	
	public SessionMetadata unwrapSessionMetaValue(MetaValue metaValue) throws Exception {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValueSupport compositeValue = (CompositeValueSupport) metaValue;
			
			SessionMetadata session = new SessionMetadata();
			session.setApplicationName((String) ProfileServiceUtil.stringValue(compositeValue.get(APPLICATION_NAME)));
			session.setCreatedTime((Long) ProfileServiceUtil.longValue(compositeValue.get(CREATED_TIME)));
			session.setClientHostName((String) ProfileServiceUtil.stringValue(compositeValue.get(CLIENT_HOST_NAME)));
			session.setIPAddress((String) ProfileServiceUtil.stringValue(compositeValue.get(IP_ADDRESS)));
			session.setLastPingTime((Long) ProfileServiceUtil.longValue(compositeValue.get(LAST_PING_TIME)));
			session.setSessionId((String) ProfileServiceUtil.stringValue(compositeValue.get(SESSION_ID)));
			session.setUserName((String) ProfileServiceUtil.stringValue(compositeValue.get(USER_NAME)));
			session.setVDBName((String) ProfileServiceUtil.stringValue(compositeValue.get(VDB_NAME)));
			session.setVDBVersion((Integer) ProfileServiceUtil.integerValue(compositeValue.get(VDB_VERSION)));
			session.setSecurityDomain((String) ProfileServiceUtil.stringValue(compositeValue.get(SECURITY_DOMAIN)));
			return session;
		}
		throw new IllegalStateException("Unable to unwrap session " + metaValue); //$NON-NLS-1$
	}

	public RequestMetadata unwrapRequestMetaValue(MetaValue metaValue) throws Exception {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValue compositeValue = (CompositeValue) metaValue;
			
			RequestMetadata request = new RequestMetadata();
			request.setExecutionId((Long) ProfileServiceUtil.longValue(compositeValue.get(EXECUTION_ID)));
			request.setSessionId((String) ProfileServiceUtil.stringValue(compositeValue.get(SESSION_ID)));
			request.setStartTime((Long) ProfileServiceUtil.longValue(compositeValue.get(START_TIME)));
			request.setCommand((String) ProfileServiceUtil.stringValue(compositeValue.get(COMMAND)));
			request.setSourceRequest((Boolean) ProfileServiceUtil.booleanValue(compositeValue.get(SOURCE_REQUEST)));
			request.setNodeId((Integer) ProfileServiceUtil.integerValue(compositeValue.get(NODE_ID)));
			request.setTransactionId((String) ProfileServiceUtil.stringValue(compositeValue.get(TRANSACTION_ID)));
			request.setState((ProcessingState) ProfileServiceUtil.getSimpleValue(compositeValue.get(STATE), ProcessingState.class));
			return request;
		}
		throw new IllegalStateException("Unable to unwrap RequestMetadata " + metaValue); //$NON-NLS-1$
	}
	
	public TransactionMetadata unwrapTransactionMetaValue(MetaValue metaValue) throws Exception {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValue compositeValue = (CompositeValue) metaValue;
			
			TransactionMetadata transaction = new TransactionMetadata();
			transaction.setAssociatedSession((String) ProfileServiceUtil.stringValue(compositeValue.get(ASSOCIATED_SESSION)));
			transaction.setCreatedTime((Long) ProfileServiceUtil.longValue(compositeValue.get(CREATED_TIME)));
			transaction.setScope((String) ProfileServiceUtil.stringValue(compositeValue.get(SCOPE)));
			transaction.setId((String) ProfileServiceUtil.stringValue(compositeValue.get("id"))); //$NON-NLS-1$
			return transaction;
		}
		throw new IllegalStateException("Unable to unwrap TransactionMetadata " + metaValue); //$NON-NLS-1$
	}
	
}
