package org.teiid.rhq.admin;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.jboss.metatype.api.values.GenericValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.Transaction;
import org.teiid.adminapi.impl.RequestMetadata;
import org.teiid.adminapi.impl.RequestMetadataMapper;
import org.teiid.rhq.comm.ExecutedResult;
import org.teiid.rhq.plugin.util.PluginConstants;
import org.teiid.rhq.plugin.util.ProfileServiceUtil;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.Platform;
import org.teiid.rhq.plugin.util.PluginConstants.ComponentType.VDB;

public class DQPManagementView implements PluginConstants {

	private static ManagedComponent mc = null;
	private static final Log LOG = LogFactory.getLog(DQPManagementView.class);
	private static final MetaValueFactory metaValueFactory = MetaValueFactory
			.getInstance();

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
			Map<String, Object> valueMap) {

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
					Collection<Request> longRunningQueries = new ArrayList<Request>();
					getRequestCollectionValue(getLongRunningQueries(),
							longRunningQueries);
					resultObject = new Double(longRunningQueries.size());
				}
			}
		}

		return resultObject;
	}

	private Object getVdbMetric(String componentType, String identifier,
			String metric, Map<String, Object> valueMap) {

		Object resultObject = new Object();

		if (metric
				.equals(PluginConstants.ComponentType.VDB.Metrics.ERROR_COUNT)) {
			// TODO remove version parameter after AdminAPI is changed
			resultObject = getErrorCount((String) valueMap.get(VDB.NAME));
		} else if (metric
				.equals(PluginConstants.ComponentType.VDB.Metrics.STATUS)) {
			// TODO remove version parameter after AdminAPI is changed
			resultObject = getVDBStatus((String) valueMap.get(VDB.NAME), 1);
		} else if (metric
				.equals(PluginConstants.ComponentType.VDB.Metrics.QUERY_COUNT)) {
			resultObject = new Double(getQueryCount().doubleValue());
		} else if (metric
				.equals(PluginConstants.ComponentType.VDB.Metrics.SESSION_COUNT)) {
			resultObject = new Double(getSessionCount().doubleValue());
		} else if (metric
				.equals(PluginConstants.ComponentType.VDB.Metrics.LONG_RUNNING_QUERIES)) {
			Collection<Request> longRunningQueries = new ArrayList<Request>();
			getRequestCollectionValue(getLongRunningQueries(),
					longRunningQueries);
			resultObject = new Double(longRunningQueries.size());

		}

		return resultObject;
	}

	/*
	 * Operation methods
	 */

	public void executeOperation(ExecutedResult operationResult,
			final Map<String, Object> valueMap) {

		if (operationResult.getComponentType().equals(
				PluginConstants.ComponentType.Platform.NAME)) {
			executePlatformOperation(operationResult, operationResult
					.getOperationName(), valueMap);
		} else if (operationResult.getComponentType().equals(
				PluginConstants.ComponentType.VDB.NAME)) {
			executeVdbOperation(operationResult, operationResult
					.getOperationName(), valueMap);
		}

	}

	private void executePlatformOperation(ExecutedResult operationResult,
			final String operationName, final Map<String, Object> valueMap) {
		Collection<Request> resultObject = new ArrayList<Request>();
		Collection<Session> activeSessionsCollection = new ArrayList<Session>();
		Collection<Transaction> transactionsCollection = new ArrayList<Transaction>();

		if (operationName.equals(Platform.Operations.GET_LONGRUNNINGQUERIES)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			getRequestCollectionValue(getLongRunningQueries(), resultObject);
			operationResult.setContent(createReportResultList(fieldNameList,
					resultObject.iterator()));
		} else if (operationName.equals(Platform.Operations.GET_SESSIONS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue sessionMetaValue = getSessions();
			getSessionCollectionValue(sessionMetaValue,
					activeSessionsCollection);
			operationResult.setContent(createReportResultList(fieldNameList,
					activeSessionsCollection.iterator()));
		} else if (operationName.equals(Platform.Operations.GET_REQUESTS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue requestMetaValue = getRequests();
			getRequestCollectionValue(requestMetaValue, resultObject);
			operationResult.setContent(createReportResultList(fieldNameList,
					resultObject.iterator()));
		} else if (operationName.equals(Platform.Operations.GET_TRANSACTIONS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue transactionMetaValue = getTransactions();
			getTransactionCollectionValue(transactionMetaValue,
					transactionsCollection);
			operationResult.setContent(createReportResultList(fieldNameList,
					resultObject.iterator()));
		} else if (operationName.equals(Platform.Operations.KILL_TRANSACTION)) {
			Long sessionID = (Long) valueMap
					.get(Operation.Value.TRANSACTION_ID);
			MetaValue[] args = new MetaValue[] { metaValueFactory
					.create(sessionID) };
			try {
				executeManagedOperation(mc,
						Platform.Operations.KILL_TRANSACTION, args);
			} catch (Exception e) {
				final String msg = "Exception executing operation: " + Platform.Operations.KILL_TRANSACTION; //$NON-NLS-1$
				LOG.error(msg, e);
			}
		} else if (operationName.equals(Platform.Operations.KILL_SESSION)) {
			Long sessionID = (Long) valueMap.get(Operation.Value.SESSION_ID);
			MetaValue[] args = new MetaValue[] { metaValueFactory
					.create(sessionID) };
			try {
				executeManagedOperation(mc, Platform.Operations.KILL_SESSION,
						args);
			} catch (Exception e) {
				final String msg = "Exception executing operation: " + Platform.Operations.KILL_SESSION; //$NON-NLS-1$
				LOG.error(msg, e);
			}
		} else if (operationName.equals(Platform.Operations.KILL_REQUEST)) {
			Long requestID = (Long) valueMap.get(Operation.Value.REQUEST_ID);
			Long sessionID = (Long) valueMap.get(Operation.Value.SESSION_ID);
			MetaValue[] args = new MetaValue[] {
					metaValueFactory.create(requestID),
					metaValueFactory.create(sessionID) };
			try {
				executeManagedOperation(mc, Platform.Operations.KILL_REQUEST,
						args);
			} catch (Exception e) {
				final String msg = "Exception executing operation: " + Platform.Operations.KILL_REQUEST; //$NON-NLS-1$
				LOG.error(msg, e);
			}
		}
	}

	private void executeVdbOperation(ExecutedResult operationResult,
			final String operationName, final Map<String, Object> valueMap) {
		Collection<Request> resultObject = new ArrayList<Request>();
		Collection<Session> activeSessionsCollection = new ArrayList<Session>();
		String vdbName = (String) valueMap
				.get(PluginConstants.ComponentType.VDB.NAME);

		if (operationName.equals(VDB.Operations.GET_PROPERTIES)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			getProperties(PluginConstants.ComponentType.VDB.NAME);
			operationResult.setContent(createReportResultList(fieldNameList,
					resultObject.iterator()));
		} else if (operationName.equals(VDB.Operations.GET_SESSIONS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue sessionMetaValue = getSessions();
			getSessionCollectionValueForVDB(sessionMetaValue,
					activeSessionsCollection, vdbName);
			operationResult.setContent(createReportResultList(fieldNameList,
					activeSessionsCollection.iterator()));
		} else if (operationName.equals(VDB.Operations.GET_REQUESTS)) {
			List<String> fieldNameList = operationResult.getFieldNameList();
			MetaValue requestMetaValue = getRequests();
			getRequestCollectionValueForVDB(requestMetaValue, resultObject, vdbName);
			operationResult.setContent(createReportResultList(fieldNameList,
					resultObject.iterator()));
		}

	}

	/*
	 * Helper methods
	 */

	public MetaValue getProperties(final String component) {

		MetaValue propertyValue = null;
		MetaValue args = null;

		try {
			propertyValue = executeManagedOperation(mc,
					PluginConstants.Operation.GET_PROPERTIES, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_PROPERTIES; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return propertyValue;

	}

	protected MetaValue getRequests() {

		MetaValue requestsCollection = null;
		MetaValue args = null;

		try {
			requestsCollection = executeManagedOperation(mc,
					PluginConstants.Operation.GET_REQUESTS, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_REQUESTS; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return requestsCollection;

	}

	protected MetaValue getTransactions() {

		MetaValue transactionsCollection = null;
		MetaValue args = null;

		try {
			transactionsCollection = executeManagedOperation(mc,
					Platform.Operations.GET_TRANSACTIONS, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_TRANSACTIONS; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return transactionsCollection;

	}

	public MetaValue getSessions() {

		MetaValue sessionCollection = null;
		MetaValue args = null;

		try {
			sessionCollection = executeManagedOperation(mc,
					PluginConstants.Operation.GET_SESSIONS, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_SESSIONS; //$NON-NLS-1$
			LOG.error(msg, e);
		}
		return sessionCollection;

	}

	public static String getVDBStatus(String vdbName, int version) {

		ManagedComponent mcVdb = null;
		try {
			mcVdb = ProfileServiceUtil
					.getManagedComponent(
							new org.jboss.managed.api.ComponentType(
									PluginConstants.ComponentType.VDB.TYPE,
									PluginConstants.ComponentType.VDB.SUBTYPE),
							vdbName);
		} catch (NamingException e) {
			final String msg = "NamingException in getVDBStatus(): " + e.getExplanation(); //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e) {
			final String msg = "Exception in getVDBStatus(): " + e.getMessage(); //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return ProfileServiceUtil.getSimpleValue(mcVdb, "status", String.class);
	}

	public static MetaValue executeManagedOperation(ManagedComponent mc,
			String operation, MetaValue... args) throws Exception {

		mc = getDQPManagementView(mc);

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
		throw new Exception("No operation found with given name =" + operation);

	}

	/**
	 * @param mc
	 * @return
	 */
	private static ManagedComponent getDQPManagementView(ManagedComponent mc) {
		try {
			mc = ProfileServiceUtil.getDQPManagementView();
		} catch (NamingException e) {
			final String msg = "NamingException getting the DQPManagementView"; //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e1) {
			final String msg = "Exception getting the DQPManagementView"; //$NON-NLS-1$
			LOG.error(msg, e1);
		}
		return mc;
	}

	public static MetaValue getManagedProperty(ManagedComponent mc,
			String property, MetaValue... args) throws Exception {

		mc = getDQPManagementView(mc);

		try {
			mc.getProperty(property);
		} catch (Exception e) {
			final String msg = "Exception getting the AdminApi in " + property; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		throw new Exception("No property found with given name =" + property);
	}

	private Integer getQueryCount() {

		Integer count = new Integer(0);

		MetaValue requests = null;
		Collection<Request> requestsCollection = new ArrayList<Request>();

		requests = getRequests();

		getRequestCollectionValue(requests, requestsCollection);

		if (requestsCollection != null && !requestsCollection.isEmpty()) {
			count = requestsCollection.size();
		}

		return count;
	}

	private Integer getSessionCount() {

		Collection<Session> activeSessionsCollection = new ArrayList<Session>();
		MetaValue sessionMetaValue = getSessions();
		getSessionCollectionValue(sessionMetaValue, activeSessionsCollection);
		return activeSessionsCollection.size();
	}

	/**
	 * @param mcVdb
	 * @return count
	 * @throws Exception
	 */
	private int getErrorCount(String vdbName) {

		ManagedComponent mcVdb = null;
		try {
			mcVdb = ProfileServiceUtil
					.getManagedComponent(
							new org.jboss.managed.api.ComponentType(
									PluginConstants.ComponentType.VDB.TYPE,
									PluginConstants.ComponentType.VDB.SUBTYPE),
							vdbName);
		} catch (NamingException e) {
			final String msg = "NamingException in getVDBStatus(): " + e.getExplanation(); //$NON-NLS-1$
			LOG.error(msg, e);
		} catch (Exception e) {
			final String msg = "Exception in getVDBStatus(): " + e.getMessage(); //$NON-NLS-1$
			LOG.error(msg, e);
		}

		// Get models from VDB
		int count = 0;
		ManagedProperty property = mcVdb.getProperty("models");
		CollectionValueSupport valueSupport = (CollectionValueSupport) property
				.getValue();
		MetaValue[] metaValues = valueSupport.getElements();

		for (MetaValue value : metaValues) {
			GenericValueSupport genValueSupport = (GenericValueSupport) value;
			ManagedObjectImpl managedObject = (ManagedObjectImpl) genValueSupport
					.getValue();

			// Get any model errors/warnings
			MetaValue errors = managedObject.getProperty("errors").getValue();
			if (errors != null) {
				CollectionValueSupport errorValueSupport = (CollectionValueSupport) errors;
				MetaValue[] errorArray = errorValueSupport.getElements();
				count += errorArray.length;
			}
		}
		return count;
	}

	protected MetaValue getLongRunningQueries() {

		MetaValue requestsCollection = null;
		MetaValue args = null;

		try {
			requestsCollection = executeManagedOperation(mc,
					Platform.Operations.GET_LONGRUNNINGQUERIES, args);
		} catch (Exception e) {
			final String msg = "Exception executing operation: " + Platform.Operations.GET_LONGRUNNINGQUERIES; //$NON-NLS-1$
			LOG.error(msg, e);
		}

		return requestsCollection;
	}

	public static <T> void getRequestCollectionValue(MetaValue pValue,
			Collection<Request> list) {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue)
					.getElements()) {
				if (value.getMetaType().isComposite()) {
					Request Request = (Request) MetaValueFactory.getInstance()
							.unwrap(value);
					list.add(Request);
				} else {
					throw new IllegalStateException(pValue
							+ " is not a Composite type");
				}
			}
		}
	}

	private void getRequestCollectionValueForVDB(MetaValue pValue,
			Collection<Request> list, String vdbName) {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue)
					.getElements()) {
				if (value.getMetaType().isComposite()) {
					RequestMetadataMapper rmm = new RequestMetadataMapper();
					RequestMetadata request = (RequestMetadata) rmm
							.unwrapMetaValue(value);
						list.add(request);
				} else {
					throw new IllegalStateException(pValue
							+ " is not a Composite type");
				}
			}
		}
	}

	private Collection<Session> getSessionsForVDB(String vdbName) {
		Collection<Session> activeSessionsCollection = Collections.emptyList();
		MetaValue sessionMetaValue = getSessions();
		getSessionCollectionValueForVDB(sessionMetaValue,
				activeSessionsCollection, vdbName);
		return activeSessionsCollection;
	}

	public static <T> void getTransactionCollectionValue(MetaValue pValue,
			Collection<Transaction> list) {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue)
					.getElements()) {
				if (value.getMetaType().isComposite()) {
					Transaction transaction = (Transaction) MetaValueFactory
							.getInstance().unwrap(value);
					list.add(transaction);
				} else {
					throw new IllegalStateException(pValue
							+ " is not a Composite type");
				}
			}
		}
	}

	public static <T> void getSessionCollectionValue(MetaValue pValue,
			Collection<Session> list) {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue)
					.getElements()) {
				if (value.getMetaType().isComposite()) {
					Session Session = (Session) MetaValueFactory.getInstance()
							.unwrap(value);
					list.add(Session);
				} else {
					throw new IllegalStateException(pValue
							+ " is not a Composite type");
				}
			}
		}
	}

	public static <T> void getSessionCollectionValueForVDB(MetaValue pValue,
			Collection<Session> list, String vdbName) {
		MetaType metaType = pValue.getMetaType();
		if (metaType.isCollection()) {
			for (MetaValue value : ((CollectionValueSupport) pValue)
					.getElements()) {
				if (value.getMetaType().isComposite()) {
					Session session = (Session) MetaValueFactory.getInstance()
							.unwrap(value);
					if (session.getVDBName().equals(vdbName)) {
						list.add(session);
					}
				} else {
					throw new IllegalStateException(pValue
							+ " is not a Composite type");
				}
			}
		}
	}

	private Collection createReportResultList(List fieldNameList,
			Iterator objectIter) {
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
