package org.teiid.rhq.admin;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.managed.api.ComponentType;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedOperation;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.values.CollectionValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.RequestMetadataMapper;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.SessionMetadataMapper;
import org.teiid.rhq.comm.ExecutedResult;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.Platform;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.Platform.Metrics;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.Platform.Operations;

import com.metamatrix.core.MetaMatrixRuntimeException;

public class DQPManagementView implements PluginConstants{

	private static ManagedComponent mc = null;
	private static final Log LOG = LogFactory.getLog(DQPManagementView.class);

	public DQPManagementView() {

	}

	/*
	 * Metric methods
	 */
	public Object getMetric(String componentType, String identifier,
			String metric, Map<String, Object> valueMap) {
		Object resultObject = new Object();

		if (componentType.equals(PluginConstants.ComponentType.Platform.NAME)) {
			resultObject = getPlatformMetric(componentType, metric, valueMap);
		} else if (componentType.equals(PluginConstants.ComponentType.VDB.NAME)) {
			resultObject = getVdbMetric(componentType, identifier, metric,
					valueMap);
		}

		return resultObject;
	}

	private Object getPlatformMetric(String componentType, String metric,
			Map valueMap) {

		Object resultObject = new Object();

		if (metric
				.equals(PluginConstants.ComponentType.Platform.Metrics.QUERY_COUNT)) {
			resultObject = new Double(getQueryCount().doubleValue());
		} else {
			if (metric
					.equals(PluginConstants.ComponentType.Platform.Metrics.SESSION_COUNT)) {
				resultObject = new Double(getSessionCount().doubleValue());
			} else {
				if (metric
						.equals(PluginConstants.ComponentType.Platform.Metrics.LONG_RUNNING_QUERIES)) {
					Integer longRunningQueryLimit = (Integer) valueMap
							.get(PluginConstants.Operation.Value.LONG_RUNNING_QUERY_LIMIT);
					Collection<RequestMetadata> longRunningQueries = getLongRunningQueries(
							longRunningQueryLimit);
					resultObject = new Double(longRunningQueries.size());
				}
			}
		}

		return resultObject;
	}

	private Object getVdbMetric(String componentType, String identifier,
			String metric, Map valueMap) {

		Object resultObject = new Object();

		// if (metric.equals(ComponentType.Metric.HIGH_WATER_MARK)) {
		// resultObject = new Double(getHighWatermark(identifier));
		// }

		return resultObject;
	}

	/*
	 * Operation methods
	 */

	public void executeOperation(ExecutedResult operationResult,
			final Map valueMap) {

		if (operationResult.getComponentType().equals(
				PluginConstants.ComponentType.Platform.NAME)) {
			executePlatformOperation(operationResult, operationResult
					.getOperationName(), valueMap);
		}

		// else if
		// (operationResult.getComponentType().equals(ConnectionConstants.ComponentType.Runtime.System.TYPE))
		// {
		// executeSystemOperation(operationResult,
		// operationResult.getOperationName(), valueMap);
		// } else if (operationResult.getComponentType().equals(
		// Runtime.Process.TYPE)) {
		// executeProcessOperation(operationResult,
		// operationResult.getOperationName(), valueMap);
		// } else if
		// (operationResult.getComponentType().equals(com.metamatrix.rhq.comm.ConnectionConstants.ComponentType.Runtime.Host.TYPE))
		// {
		// executeHostOperation(operationResult,
		// operationResult.getOperationName(), valueMap);
		// } else if
		// (operationResult.getComponentType().equals(com.metamatrix.rhq.comm.ConnectionConstants.ComponentType.Runtime.Session.TYPE))
		// {
		// executeSessionOperation(operationResult,
		// operationResult.getOperationName(), valueMap);
		// } else if
		// (operationResult.getComponentType().equals(com.metamatrix.rhq.comm.ConnectionConstants.ComponentType.Runtime.Queries.TYPE))
		// {
		// executeQueriesOperation(operationResult,
		// operationResult.getOperationName(), valueMap);
		// }
	}

	private void executePlatformOperation(ExecutedResult operationResult,
			final String operationName, final Map<String, Object> valueMap) {
		Collection<RequestMetadata> resultObject = new ArrayList<RequestMetadata>();

		if (operationName.equals(Platform.Operations.GET_LONGRUNNINGQUERIES)) {
			Integer longRunningValue = (Integer) valueMap
					.get(Operation.Value.LONG_RUNNING_QUERY_LIMIT);
			List<String> fieldNameList = operationResult.getFieldNameList();
			resultObject = getLongRunningQueries(longRunningValue);
			operationResult.setContent(createReportResultList(fieldNameList, resultObject.iterator()));
		} 
		
//		else if (operationName.equals(ComponentType.Operation.KILL_REQUEST)) {
//			String requestID = (String) valueMap
//					.get(ConnectionConstants.ComponentType.Operation.Value.REQUEST_ID);
//			cancelRequest(requestID);
//		} else if (operationName.equals(ComponentType.Operation.GET_VDBS)) {
//			List fieldNameList = operationResult.getFieldNameList();
//			resultObject = getVDBs(fieldNameList);
//			operationResult.setContent((List) resultObject);
//		} else if (operationName.equals(ComponentType.Operation.GET_PROPERTIES)) {
//			String identifier = (String) valueMap
//					.get(ConnectionConstants.IDENTIFIER);
//			Properties props = getProperties(
//					ConnectionConstants.ComponentType.Runtime.System.TYPE,
//					identifier);
//			resultObject = createReportResultList(props);
//			operationResult.setContent((List) resultObject);
//		}

	}

	/*
	 * Helper methods
	 */

	protected MetaValue getRequests(List<String> fieldNameList) {

		MetaValue requestsCollection = null;
		MetaValue args = null;

		requestsCollection = executeManagedOperation(mc,
				PluginConstants.Operation.GET_REQUESTS, args);

		return requestsCollection;

		// if (fieldNameList != null) {
		// Collection reportResultCollection = createReportResultList(
		// fieldNameList, requestsCollection.iterator());
		// return reportResultCollection;
		// } else {
		// return requestsCollection;
		// }
	}

	public MetaValue getSessions(List<String> fieldNameList) {

		MetaValue sessionCollection = null;
		MetaValue args = null;

		sessionCollection = executeManagedOperation(mc,
				PluginConstants.Operation.GET_SESSIONS, args);
		return sessionCollection;

		// if (fieldNameList != null) {
		// Collection reportResultCollection = createReportResultList(
		// fieldNameList, requestsCollection.iterator());
		// return reportResultCollection;
		// } else {
		// return requestsCollection;
		// }
	}

	public static MetaValue executeManagedOperation(ManagedComponent mc,
			String operation, MetaValue... args) {

		try {
			mc = ProfileServiceUtil.getDQPManagementView();
		} catch (NamingException e) {
			LOG.error(e);
		} catch (Exception e1) {
			LOG.error(e1);
		}

		for (ManagedOperation mo : mc.getOperations()) {
			String opName = mo.getName();
			if (opName.equals(operation)) {
				try {
					if (args.length == 1 && args[0] == null) {
						return mo.invoke();
					} else {
						return mo.invoke(args);
					}
				} catch (Exception e) {
					final String msg = "Exception getting the AdminApi in " + operation; //$NON-NLS-1$
					LOG.error(msg, e);
				}
			}
		}
		throw new MetaMatrixRuntimeException(
				"No operation found with given name =" + operation);

	}

	private Integer getQueryCount() {

		Integer count = new Integer(0);

		MetaValue requests = null;
		Collection<RequestMetadata> requestsCollection = new ArrayList();

		requests = getRequests(null);

		getRequestCollectionValue(requests, requestsCollection);

		if (requestsCollection != null && !requestsCollection.isEmpty()) {
			count = requestsCollection.size();
		}

		return count;
	}

	private Integer getSessionCount() {

		Collection<SessionMetadata> activeSessionsCollection = new ArrayList<SessionMetadata>();
		MetaValue sessionMetaValue = getSessions(null);
		getSessionCollectionValue(sessionMetaValue, activeSessionsCollection);
		return activeSessionsCollection.size();
	}

	protected Collection<RequestMetadata> getLongRunningQueries(
			int longRunningValue) {

		MetaValue requestsCollection = null;
		Collection<RequestMetadata> list = new ArrayList<RequestMetadata>();

		double longRunningQueryTimeDouble = new Double(longRunningValue);

		requestsCollection = getRequests(null);

		getRequestCollectionValue(requestsCollection, list);

		Iterator<RequestMetadata> requestsIter = list.iterator();
		while (requestsIter.hasNext()) {
			RequestMetadata request = requestsIter.next();
			long startTime = request.getProcessingTime();
			// Get msec from each, and subtract.
			long runningTime = Calendar.getInstance().getTimeInMillis()
					- startTime;

			if (runningTime < longRunningQueryTimeDouble) {
				requestsIter.remove();
			}
		}
		 
		return list;
	}
	
	public static <T> void getRequestCollectionValue(MetaValue pValue,
			Collection<RequestMetadata> list) {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue)
					.getElements()) {
				if (value.getMetaType().isComposite()) {
					RequestMetadataMapper requestMapper = new RequestMetadataMapper();
					RequestMetadata requestMetaData = requestMapper
							.unwrapMetaValue(value);
					list.add(requestMetaData);
				} else {
					throw new IllegalStateException(pValue
							+ " is not a Composite type");
				}
			}
		}
	}

	public static <T> void getSessionCollectionValue(MetaValue pValue,
			Collection<SessionMetadata> list) {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue)
					.getElements()) {
				if (value.getMetaType().isComposite()) {
					SessionMetadataMapper sessionMapper = new SessionMetadataMapper();
					SessionMetadata sessionMetaData = sessionMapper
							.unwrapMetaValue(value);
					list.add(sessionMetaData);
				} else {
					throw new IllegalStateException(pValue
							+ " is not a Composite type");
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

}
