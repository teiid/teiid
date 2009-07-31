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
package org.teiid.rhq.comm.impl;

import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminException;
import org.teiid.adminapi.AdminObject;
import org.teiid.adminapi.ConnectorBinding;
import org.teiid.adminapi.Host;
import org.teiid.adminapi.ProcessObject;
import org.teiid.adminapi.QueueWorkerPool;
import org.teiid.adminapi.Request;
import org.teiid.adminapi.Session;
import org.teiid.adminapi.SystemObject;
import org.teiid.rhq.comm.Component;
import org.teiid.rhq.comm.Connection;
import org.teiid.rhq.comm.ConnectionConstants;
import org.teiid.rhq.comm.ConnectionException;
import org.teiid.rhq.comm.ConnectionPool;
import org.teiid.rhq.comm.ExecutedResult;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Queries.Query;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.System.Metrics;
import org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.System.Operations;
import org.teiid.rhq.plugin.HostComponent;

import com.metamatrix.jdbc.MMConnection;
import com.metamatrix.platform.security.api.MetaMatrixSessionState;

public class ConnectionImpl implements Connection, TeiidConnectionConstants {
 
	// TODO:
	// 1. Change from using LogManager. This writes to the MM Server, the
	// messages should be
	// written to the agent log
	// 2. Need to support connecting via ssl
	// 3. Need to understand the life cycle of a connection
	// a. during discovery
	// b. during monitoring, operations, etc.
	// 4. add post processing for calculated fields in operation results
	

	private static final Log log = LogFactory.getLog(ConnectionImpl.class);

	public String key = "";

	private Properties environmentProps = null;

	private Admin adminApi = null;
	private MMConnection mmconn = null;
    
    private ConnectionPool connectionPool = null;
    
    // set when an exception is thrown by the admin api
    // and indicates when close is called, to tell the pool
    // to remove the connection;
    private boolean invalidConnection = false;
    
    protected ConnectionImpl (final String key, 
                                final Properties envProps,
                                final ConnectionPool pool,
                                final MMConnection connection) throws SQLException {
        this.connectionPool = pool;
        this.mmconn = connection;
        this.adminApi = this.mmconn.getAdminAPI();
        this.key = key;
        this.environmentProps = envProps;        
        
    }

    public boolean isValid() {
        return (! this.invalidConnection);
    }
    /** 
     * @see org.teiid.rhq.comm.Connection#isAlive()
     */
    public boolean isAlive() {
        try {
            if (adminApi != null) {
                adminApi.getSystem();
                this.invalidConnection=false;
                return true;
            }
        } catch (Throwable e) {
            invalidConnection = true;
            log.error("Error: admin connection for " + key + " is not alive", e); //$NON-NLS-1$ //$NON-NLS-2$
        } 
        return false;
    }    

	/**
	 * @see org.teiid.rhq.comm.Connection#close()
	 */
	public void close() {
		try {
			if (connectionPool != null) {
                  connectionPool.close(this);
            }
                
		} catch (Exception e) {
			log.error("Error returning connection to the connection pool", e); //$NON-NLS-1$
		} 
		
		invalidConnection = true;

	}
    

    /**
     * Called by the factory to close the source connection 
     * 
     */
    
    protected void closeSource() {
        try {
            if (mmconn != null) {
            	mmconn.close();
            }
        } catch (Exception e) {
            log.error("Error closing the admin connection", e); //$NON-NLS-1$
        } finally {
            adminApi = null;
            mmconn = null;
            connectionPool = null;
            environmentProps.clear();
            environmentProps = null; 
            this.invalidConnection = true;
            
        }        
    }

//	public Properties getEnvironment() throws ConnectionException {
//		Properties systemProperties = new Properties();
//		try {
//    		Collection processCollection = ConnectionUtil.getAllProcesses(
//    				getConnection(), this.getHost().getIdentifier());
//    
//    		Iterator processIter = processCollection.iterator();
//    		String portList = null;
//    
//    		boolean first = true;
//    		while (processIter.hasNext()) {
//    			ProcessObject process = (ProcessObject) processIter.next();
//    			if (first) {
//    				first = false;
//    				portList = String.valueOf(process.getPort());
//    			} else {
//    				portList += PORT_DELIM + process.getPort(); //$NON-NLS-1$
//    			}
//    
//    		}
//    
//    		systemProperties.setProperty(ConnectionConstants.USERNAME,
//    				environmentProps.getProperty(ConnectionConstants.USERNAME));
//    		systemProperties.setProperty(ConnectionConstants.PASSWORD,
//    				environmentProps.getProperty(ConnectionConstants.PASSWORD));
//    		systemProperties.setProperty(ConnectionConstants.PORTS, portList);
//    		environmentProps = (Properties) systemProperties.clone();
//    
//    		return systemProperties;
//        } catch (AdminException ae) {
//            invalidConnection = true;
//            throw new ConnectionException(ae.getMessage());            
//        } finally {
//            this.resetConnection();
//        }
//	}
    
    public Collection<Component> discoverComponents(String componentType,
			String identifier) throws ConnectionException {
		if (componentType
				.equals(Runtime.Connector.TYPE)) {
			return getConnectors(identifier);

//		} else if (componentType
//				.equals(Runtime.Service.TYPE)) {
//			return getServices(identifier);

		} else if (componentType
				.equals(Runtime.Process.TYPE)) {
			return getVMs(identifier);

		} else if (componentType
				.equals(Runtime.Host.TYPE)) {
			return getAllHosts();

//		} else if (componentType
//				.equals(ComponentType.Resource.Service.TYPE)) {
//			return getServicesForConfig(identifier);
//
//		} else if (componentType
//				.equals(ComponentType.Resource.Connector.TYPE)) {
//			return getConnectorsForConfig(identifier);

		}
//		else if (componentType
//				.equals(ComponentType.Runtime.Session.TYPE)) {
//			return getConnectorsForConfig(identifier);
//
//		}
		return Collections.EMPTY_LIST;
	}  
    


	public void executeOperation(ExecutedResult operationResult,
            final Map valueMap) throws ConnectionException {

        if (operationResult.getComponentType().equals(Runtime.Connector.TYPE)) {
            executeConnectorOperation(operationResult, operationResult.getOperationName(), valueMap);
        } else  if (operationResult.getComponentType().equals(ConnectionConstants.ComponentType.Runtime.System.TYPE)) {
        	executeSystemOperation(operationResult, operationResult.getOperationName(), valueMap);
        } else if (operationResult.getComponentType().equals( Runtime.Process.TYPE)) {
        	executeProcessOperation(operationResult, operationResult.getOperationName(), valueMap);
        } else if (operationResult.getComponentType().equals(org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Host.TYPE)) {
        	executeHostOperation(operationResult, operationResult.getOperationName(), valueMap);
        }  else if (operationResult.getComponentType().equals(org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Session.TYPE)) {
        	executeSessionOperation(operationResult, operationResult.getOperationName(), valueMap);
        }  else if (operationResult.getComponentType().equals(org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Queries.TYPE)) {
        	executeQueriesOperation(operationResult, operationResult.getOperationName(), valueMap);
        }         
    }
    


	private void executeSystemOperation(ExecutedResult operationResult, final String operationName, final Map valueMap)
    throws ConnectionException {
        Object resultObject = new Object();
        
        if (operationName.equals(Operations.BOUNCE_SYSTEM)) {
        	Boolean waitUntilFinished = (Boolean)valueMap.get(ConnectionConstants.ComponentType.Operation.Value.WAIT_UNTIL_FINISHED);
            bounceSystem(waitUntilFinished);
        }else if (operationName.equals(Operations.GET_LONGRUNNINGQUERIES)) {
        	Boolean includeSourceQueries = (Boolean)valueMap.get(ConnectionConstants.ComponentType.Operation.Value.INCLUDE_SOURCE_QUERIES);
        	Integer longRunningValue = (Integer)valueMap.get(ConnectionConstants.ComponentType.Operation.Value.LONG_RUNNING_QUERY_LIMIT);
			List fieldNameList = operationResult.getFieldNameList();
        	resultObject = getLongRunningQueries(includeSourceQueries, longRunningValue, fieldNameList);
        	operationResult.setContent((List)resultObject); 
        }else if (operationName.equals(ComponentType.Operation.KILL_REQUEST)) {
        	String requestID = (String)valueMap.get(ConnectionConstants.ComponentType.Operation.Value.REQUEST_ID);
    		cancelRequest(requestID); 
        }else if (operationName.equals(ComponentType.Operation.GET_VDBS)) {
        	List fieldNameList = operationResult.getFieldNameList();
        	resultObject = getVDBs(fieldNameList);
        	operationResult.setContent((List)resultObject); 
        }else if (operationName.equals(ComponentType.Operation.GET_PROPERTIES)) {
           	String identifier = (String)valueMap.get(ConnectionConstants.IDENTIFIER);
           	Properties props = getProperties(ConnectionConstants.ComponentType.Runtime.System.TYPE, identifier); 
           	resultObject = createReportResultList(props);
           	operationResult.setContent((List)resultObject); 
        }

    }    

    private void executeProcessOperation(ExecutedResult operationResult, final String operationName, final Map valueMap)
    throws ConnectionException {
        Object resultObject = new Object();
        
        if (operationName.equals(ComponentType.Operation.GET_PROPERTIES)) {
           	String identifier = (String)valueMap.get(ConnectionConstants.IDENTIFIER);
           	Properties props = getProperties(Runtime.Process.TYPE, identifier); 
           	resultObject = createReportResultList(props);
           	operationResult.setContent((List)resultObject); 
        }
    }   
    
    private void executeHostOperation(ExecutedResult operationResult, final String operationName, final Map valueMap)
    throws ConnectionException {
        Object resultObject = new Object();
        
        if (operationName.equals(org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Host.Operations.GET_HOSTS)) {
           	String identifier = (String)valueMap.get(ConnectionConstants.IDENTIFIER);
           	Properties props = getProperties(org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Host.TYPE, identifier); 
           	resultObject = createReportResultList(props);
           	operationResult.setContent((List)resultObject); 
        }
    }    
    
    private void executeConnectorOperation(ExecutedResult operationResult, final String operationName, final Map valueMap)
    throws ConnectionException {
        Object resultObject = new Object();
        String identifier = (String)valueMap.get(ConnectionConstants.IDENTIFIER);
        
        if (operationName.equals(Runtime.Connector.Operations.RESTART_CONNECTOR)) {
            startConnector(identifier);
        }else if (operationName.equals(Runtime.Connector.Operations.STOP_CONNECTOR)) { 
            Boolean stopNow = (Boolean)valueMap.get(ConnectionConstants.ComponentType.Operation.Value.STOP_NOW);
            stopConnector(identifier, stopNow);
        }else if (operationName.equals(ComponentType.Operation.GET_PROPERTIES)) {
               	Properties props = getProperties(Runtime.Connector.TYPE, identifier); 
               	resultObject = createReportResultList(props);
               	operationResult.setContent((List)resultObject); 
        }
    }
    
    private void executeSessionOperation(ExecutedResult operationResult, final String operationName, final Map valueMap)
    throws ConnectionException {
    	if (operationName.equals(org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Session.Query.GET_SESSIONS)) {
            Object resultObject = new Object();

	    	List fieldNameList = operationResult.getFieldNameList();
	        resultObject = getSessions(false, fieldNameList);
	        operationResult.setContent((List)resultObject); 
    	}

    	
    }
    
    private void executeQueriesOperation(ExecutedResult operationResult, final String operationName, final Map valueMap)
    throws ConnectionException {
   
    	if (operationName.equals(Query.GET_QUERIES)) {
    		Object resultObject = new Object();
    		
 //   		Boolean includeSourceQueries = (Boolean)valueMap.get(ConnectionConstants.ComponentType.Operation.Value.INCLUDE_SOURCE_QUERIES);
    		List fieldNameList = operationResult.getFieldNameList();
    		resultObject = getRequests(false, fieldNameList);
    		operationResult.setContent((List)resultObject); 
    	}
    }
        

    public Object getMetric(String componentType, String identifier, String metric, Map valueMap)
    throws ConnectionException {
        Object resultObject = new Object();
        
        if (componentType.equals(ComponentType.Runtime.System.TYPE)){
            resultObject = getSystemMetric(componentType, metric, valueMap);
        }else if (componentType.equals(Runtime.Process.TYPE)){
            resultObject = getProcessMetric(componentType, identifier, metric, valueMap);
        }
        
        
        return resultObject;
    }

    private Object getSystemMetric(String componentType, String metric,
                Map valueMap) throws ConnectionException {

            Object resultObject = new Object();

            if (metric.equals(Metrics.QUERY_COUNT)) {
                resultObject = new Double(getQueryCount().doubleValue());
            } else {
                if (metric.equals(Metrics.SESSION_COUNT)) {
                    resultObject = new Double(getSessionCount().doubleValue());
                } else {
                    if (metric.equals(Metrics.LONG_RUNNING_QUERIES)) {
                    	Integer longRunningQueryLimit = (Integer)valueMap.get(ConnectionConstants.ComponentType.Operation.Value.LONG_RUNNING_QUERY_LIMIT);
                        Collection<Request> longRunningQueries = getLongRunningQueries(false, longRunningQueryLimit, null);
                        resultObject = new Double(longRunningQueries.size());
                    }
                }
            }

            return resultObject;
        }
            

    private Object getProcessMetric(String componentType, String identifier, String metric,
                Map valueMap) throws ConnectionException {

            Object resultObject = new Object();

            if (metric.equals(ComponentType.Metric.HIGH_WATER_MARK)) {
                resultObject = new Double(getHighWatermark(identifier));
            } 

            return resultObject;
        }

	public Boolean isAvailable(String componentType, String identifier)
			throws ConnectionException {

		try {
			
			Admin conn = this.getConnection();
			if (componentType
					.equalsIgnoreCase(Runtime.Connector.TYPE)) {
	                ConnectorBinding cb = ConnectionUtil.getConnector(conn, identifier);
						if (cb.getState() == ConnectorBinding.STATE_OPEN) {
							return true;
						}	
//			} else if (componentType
//					.equalsIgnoreCase(Runtime.Service.TYPE)) {
//                Service svc = ConnectionUtil.getService(conn, identifier);
//				if (svc.getState() == Service.STATE_OPEN) {
//					return true;
//				}
			
			} else if (componentType
					.equalsIgnoreCase(Runtime.Process.TYPE)) {
	                ProcessObject vm = ConnectionUtil.getProcess(conn, identifier);
					if (vm.isRunning()) {
						return true;
					}	
			} else if (componentType
					.equalsIgnoreCase(Runtime.Host.TYPE)) {
	                Host host=ConnectionUtil.getHost(identifier, conn);
					if (host.isRunning()) {
						return true;
					}	
			} else if (componentType
					.equalsIgnoreCase(Runtime.System.TYPE)) {
					if (conn.getSystem() != null) {
						return true;
					}	
			} 
        } catch (AdminException ae) {
            invalidConnection = true;
                          
            log.error(ae.getMessage());
        } 
		

		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.metamatrix.admin.api.rhq.connection.Connection#getProperty(java.lang.String,
	 *      java.lang.String)
	 */
	public String getProperty(final String identifier, final String property)
			throws ConnectionException {

		String propertyValue = null;


//		if (identifier.equals(ConnectionConstants.SYSTEM_NAME_IDENTIFIER)) {
//			Resource mmResource = (Resource) getResource(identifier,
//                                                             getConnection());
//			propertyValue = mmResource.getPropertyValue(property);
//		}

		return propertyValue;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.metamatrix.admin.api.rhq.connection.Connection#getInstallationDirectory()
	 */
	public String getKey() {
		return key;
	}

	/**
	 * @see org.teiid.rhq.comm.Connection#getHost()
	 */
	public Component getHost(String identifier) throws ConnectionException {
        
        try {
        	
			return  mapHost( ConnectionUtil.getHost(identifier, getConnection()) );
		} catch (AdminException e) {
	           invalidConnection = true;
	            throw new ConnectionException(e.getMessage());            
		} 

	}
	
	public Collection getAllHosts() throws ConnectionException {
		Collection<Component> hostObjs = null;
//		try {
//			Collection<Host> hosts = getConnection().getHosts("*"); 
//			hostObjs = new ArrayList(hosts.size());
//			
//			for (Iterator<Host>it=hosts.iterator(); it.hasNext();) {
//				Host h = it.next();
//
//				ComponentImpl chost = mapHost(h);
//
//				hostObjs.add(chost);
//			}
//		} catch (AdminException e) {
//	           invalidConnection = true;
//	            throw new ConnectionException(e.getMessage());            
//		}  
		
		return hostObjs;
	}
	
	private ComponentImpl mapHost(Host h) throws ConnectionException {
		ComponentImpl chost = createComponent(h);
		
		chost.addProperty(HostComponent.INSTALL_DIR, h.getPropertyValue(Host.HOST_DIRECTORY));
		return chost;
	
	}
	
	
	/**
	 * @see org.teiid.rhq.comm.Connection#getHost()
	 */
//	public Collection<Component> getServices(String vmIdentifier) throws ConnectionException {
//        
//        try {
//        	
//            Collection<Service> servicesCollection =  getConnection().getServices(vmIdentifier +"|*");
//
//       		Iterator<Service> iterSvc = servicesCollection.iterator();
//
//      		Collection<Component> svccomponents = new ArrayList<Component>(servicesCollection.size());
//    		while (iterSvc.hasNext()) {
//     			Service svc = iterSvc.next();   			
//    			
//    			Component comp = mapService(svc);
//     
//    			svccomponents.add(comp);
//    		}
//    		return svccomponents;
//           	
//		} catch (AdminException e) {
//	           invalidConnection = true;
//	            throw new ConnectionException(e.getMessage());            
//		} 
//
//	}	
//	
	
//	public Collection<Component> getServicesForConfig(String identifier)
//			throws ConnectionException {
//	       try {
//	        	
//	            Collection<Service> servicesCollection =  getConnection().getServicesToConfigure(identifier);
//
//	       		Iterator<Service> iterSvc = servicesCollection.iterator();
//
//	      		Collection<Component> svccomponents = new ArrayList<Component>(servicesCollection.size());
//	    		while (iterSvc.hasNext()) {
//	     			Service svc = iterSvc.next();   			
//	    			
//	    			Component comp = mapService(svc);
//	     
//	    			svccomponents.add(comp);
//	    		}
//	    		return svccomponents;
//	           	
//			} catch (AdminException e) {
//		           invalidConnection = true;
//		            throw new ConnectionException(e.getMessage());            
//			} 
//
//	}

	public Collection<Component> getVMs(String hostIdentifier) throws ConnectionException {
        
        try {
             
    		Collection processes = getConnection().getProcesses(hostIdentifier + "|*");   
    
    		Iterator<ProcessObject> iterVMs = processes.iterator();
    		log.info("Processing processes..."); //$NON-NLS-1$
    
    		Collection vmcomponents = new ArrayList(processes.size());
    		while (iterVMs.hasNext()) {
    			// ProcessObject processObject = iterHostProcesses.next();
    			ProcessObject vm = iterVMs.next();   			
    			
    			Component comp = mapProcess(vm);
     
    			vmcomponents.add(comp);
    		}

    		return vmcomponents;
        } catch (AdminException ae) {
            invalidConnection = true;
            throw new ConnectionException(ae.getMessage());            
            
        } 
	}	
	
	private Component mapProcess(ProcessObject vm) throws ConnectionException {
		if (vm == null) return null;
		
			ComponentImpl comp = createComponent(vm);
            comp.setPort(vm.getPropertyValue(ProcessObject.SERVER_PORT));
 		
		return comp;

	}

	/**
	 * @see com.metamatrix.rhq.comm.Connection#getVMs()
	 */
//	public Collection<Component> getVMs() throws ConnectionException {
//        
//		return getVMs(this.getHost().getIdentifier());
// 
//	}
	
	/**
     * Returns a collection of all VDBs in the system.
     * @param fieldNameList - operaration result fields if required. May be null. 
     * 
     * @return Collection   
     */
	public Collection getVDBs(List fieldNameList)
			throws ConnectionException {

		Collection vdbCollection = Collections.EMPTY_LIST;
		
		try {

			vdbCollection = getConnection().getVDBs("*");
		} catch (AdminException ae) {
			invalidConnection = true;
			throw new ConnectionException(ae.getMessage());

		} 

		if (fieldNameList != null) {
			Collection reportResultCollection = createReportResultList(
					fieldNameList, vdbCollection.iterator());
			return reportResultCollection;
		} else {
			return vdbCollection;
		}
	}
	
	

//	public Collection<Component> getConnectorsForConfig(String identifier)
//			throws ConnectionException {
//		
//		Collection<Component> connectors = Collections.EMPTY_LIST;
//		
//	   try {
//		Collection connectorsCollection =  getConnection().getConnectorBindingsToConfigure(identifier);
//		
//		Iterator<ConnectorBinding> iterConnectors = connectorsCollection.iterator();
//		log.info("Getting connector bindings for configuration...");//$NON-NLS-1$
//
//		while (iterConnectors.hasNext()) {
//			// ProcessObject processObject = iterHostProcesses.next();
//			ConnectorBinding connectorBinding = iterConnectors.next();
//
//			log.debug("Found connector binding " + connectorBinding.getName());//$NON-NLS-1$
//
//			Component comp = mapConnector(connectorBinding);
//
//			connectors.add(comp);
//		}		
//		return connectors;
//	} catch (AdminException e) {
//        invalidConnection = true;
//        throw new ConnectionException(e.getMessage());            
//	}
//
//	}

	public Collection<Component> getConnectors(String vmIdentifier) throws ConnectionException {

		Collection<Component> connectors = Collections.EMPTY_LIST;
		try {

			Collection mmConnectors = getConnection().getConnectorBindings(vmIdentifier +"|*");

			connectors = new ArrayList(mmConnectors != null ? mmConnectors
					.size() : 0);

			Iterator<ConnectorBinding> iterConnectors = mmConnectors.iterator();
			log.info("Processing connector bindings...");//$NON-NLS-1$

			while (iterConnectors.hasNext()) {
				// ProcessObject processObject = iterHostProcesses.next();
				ConnectorBinding connectorBinding = iterConnectors.next();

				log
						.info("Found connector binding " + connectorBinding.getName());//$NON-NLS-1$

				Component comp = mapConnector(connectorBinding);

				connectors.add(comp);
			}
        } catch (AdminException ae) {
            invalidConnection = true;
            throw new ConnectionException(ae.getMessage());            
		} 
		return connectors;
	}
	
	private Component getConnector(String connectorIdentifier) throws ConnectionException {
		
			
			try {
				ConnectorBinding connectorBinding = ConnectionUtil.getConnector(getConnection(), connectorIdentifier);
				return mapConnector(connectorBinding);
				
			} catch (AdminException ae) {
	            invalidConnection = true;
	            throw new ConnectionException(ae.getMessage());            
			}
	}
	
//	private Component getService(String svcIdentifier) throws ConnectionException {
//		
//		
//		try {
//			Service svc = ConnectionUtil.getService(getConnection(), svcIdentifier);
//			return mapService(svc);
//			
//		} catch (AdminException ae) {
//            invalidConnection = true;
//            throw new ConnectionException(ae.getMessage());            
//		}
//}
	
	private Component mapConnector(ConnectorBinding connectorBinding) throws ConnectionException {
		ComponentImpl comp = createComponent(connectorBinding);

		comp.setDescription(connectorBinding.getDescription());
		
		return comp;

	}	
	
//	private Component mapService(Service service) throws ConnectionException {
//		ComponentImpl comp = createComponent(service);
//
//		comp.setDescription(service.getDescription());
//		
//		return comp;
//
//	}
	
	private Component mapSession(Session session) throws ConnectionException {
		ComponentImpl comp = null;
		
			try {
				Class clzz = Class.forName(ComponentImpl.class.getName(), true, this.connectionPool.getClassLoader());
				comp = (ComponentImpl) clzz.newInstance();
				
						comp.setIdentifier(session.getIdentifier());
					comp.setName(session.getSessionID());
					comp.setSystemKey(getKey());
					comp.setVersion("1.0"); //$NON-NLS-1$
					
					comp.setProperties(session.getProperties());


			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new ConnectionException(e.getMessage());
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new ConnectionException(e.getMessage());
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new ConnectionException(e.getMessage());
			}
	        return comp;


	}	

	// *******************
	// Support Methods
	// *******************

	public Boolean isSystemAvailable() throws ConnectionException {

        return this.isAlive();

	}

	private String getSystemName(final Admin adminApi) throws ConnectionException {
		return "NoSystemName";
//		Resource mmJGroups = (Resource) getResource("JGroups", adminApi); //$NON-NLS-1$
//		return mmJGroups.getPropertyValue(SYSTEM_NAME_PROPERTY);
	}


	/**
	 * @throws Exception
	 */
	// static
//	private Integer getQueuedThreadCount(Admin adminApi) throws Exception {
//		
//		ProcessObject process = ConnectionUtil.getProcess(getConnection(), processIdentifier);
//
//		return process.getQueueWorkerPool().getQueued();
//
//	}

    /**
     * @throws Exception
     */
    private void startConnector(final String connectorBindingIdentifier) throws ConnectionException {
        try {
            (getConnection())
                    .startConnectorBinding(connectorBindingIdentifier);
        } catch (AdminException ae) {
            invalidConnection = true;
            throw new ConnectionException(ae.getMessage());            

        } 
    }

    /**
     * @throws Exception
     */
    private void bounceSystem(final boolean waitUntilFinished) throws ConnectionException {
        try {
            getConnection().restart();
        } catch (AdminException err) {
            throw new ConnectionException(err.getMessage());
        } 
    }
    
    /**
     * @throws Exception
     */
    private void stopConnector(String connectorBindingIdentifier, boolean hardStop)
            throws ConnectionException {
        try {
            getConnection().stopConnectorBinding(
                    connectorBindingIdentifier, hardStop);
        } catch (AdminException ae) {
            invalidConnection = true;
            throw new ConnectionException(ae.getMessage());            

        } 
    }

	/**
	 * @throws Exception
	 */
	private static boolean isReachable(final String host, int port)
			throws UnknownHostException {
		return NetUtils.getInstance().isPortAvailable(host, port);
	}

	/**
	 * @throws Exception
	 */
	// static
//	private Integer getThreadPoolThreadCount(final Admin adminApi)
//			throws ConnectionException {
//		Integer threadCount = new Integer(0);
//		
//		ProcessObject process = ConnectionUtil.getProcess(getConnection(), processIdentifier);
//
//		QueueWorkerPool pool = null;
//			pool = process.getQueueWorkerPool();
//			threadCount = pool.getThreads();
//		return threadCount;
//	}

	private Integer getQueryCount() throws ConnectionException {

		Integer count = new Integer(0);

		Collection<Request> requestsCollection = null;
		requestsCollection = getRequests(false, null);
        
		if (requestsCollection != null && !requestsCollection.isEmpty()) {
			count = requestsCollection.size();
		}

		return count;
	}
	
	protected Collection<Request> getRequests(boolean includeSourceQueries, List fieldNameList) throws ConnectionException {

		Collection<Request> requestsCollection = null;;
		
		try {
			Admin conn = getConnection();
			requestsCollection = conn.getRequests(WILDCARD);
			if (includeSourceQueries){
				Collection<Request> sourceRequestsCollection = Collections.EMPTY_LIST;
				sourceRequestsCollection = conn.getSourceRequests(WILDCARD);
				requestsCollection.addAll(sourceRequestsCollection);
			}
        } catch (AdminException ae) {
            invalidConnection = true;
            throw new ConnectionException(ae.getMessage());            
            
		} catch (Exception e) {
			final String msg = "Exception getting the AdminApi in getRequests: "; //$NON-NLS-1$
            log.error(msg, e);
            throw new ConnectionException(msg);
		} 
		
		if (fieldNameList!=null){
			Collection reportResultCollection = createReportResultList(fieldNameList, requestsCollection.iterator());
			return reportResultCollection;
		}else{
			return requestsCollection;
		}
		
	}
	
	protected void cancelRequest(String requestID) throws ConnectionException {
		
		try {
			getConnection().cancelRequest(requestID);
        } catch (AdminException ae) {
            invalidConnection = true;
            throw new ConnectionException(ae.getMessage());            
            
		} catch (Exception e) {
			final String msg = "Exception getting the AdminApi in getRequests: "; //$NON-NLS-1$
            log.error(msg, e);
            throw new ConnectionException(msg);
		} 
		
	}
	
//	public Collection<Request> postProcessing(String operationName, Collection reportResultCollection) throws ConnectionException {
//
//		if (operationName.equals(ConnectionConstants.ComponentType.Operation.GET_QUERIES)){
//			Iterator resultIter = reportResultCollection.iterator();
//			while (resultIter.hasNext()){
//				Map row = (Map)resultIter.next();
//				String createDateString = row.get("Createdate");
//				Date createDate = new Date(createDateString);
//				
//				Date current = new Date(System.nanoTime());
//				GregorianCalendar cal = new GregorianCalendar();
//				cal.add()
//				Date elapsedTime = Calendar.getInstance().add( createDate)
//			}
//			String createDateString = reportResultList.			
//		}
//		return reportResultList;
//	}

	protected Collection<Request> getLongRunningQueries(boolean includeSourceQueries, int longRunningValue, List fieldNameList) throws ConnectionException {

		Collection<Request> requestsCollection = null;

		double longRunningQueryTimeDouble = new Double(longRunningValue);

		try {
			requestsCollection = getRequests(includeSourceQueries, null);
        } catch (Exception e) {
			final String msg = "AdminException getting the AdminApi in getLongRunningQueries: "; //$NON-NLS-1$
            log.error(msg, e);
            throw new ConnectionException(msg);
		} 

		Iterator<Request> requestsIter = requestsCollection.iterator();
		while (requestsIter.hasNext()) {
			Request request = requestsIter.next();
			Date startTime = request.getProcessingDate();
			// Get msec from each, and subtract.
			long runningTime = Calendar.getInstance().getTimeInMillis() - startTime.getTime();
			
			if (runningTime < longRunningQueryTimeDouble) {
				requestsIter.remove();
			}
		}

		if (fieldNameList!=null){
			Collection reportResultCollection = createReportResultList(fieldNameList, requestsCollection.iterator());
			return reportResultCollection;
		}else{
			return requestsCollection;
		}
	}
	
	private Integer getSessionCount() throws ConnectionException {

		Collection<Session> activeSessionsCollection = Collections.EMPTY_LIST;
		try {
			activeSessionsCollection = getActiveSessions();
        } catch (AdminException ae) {
            invalidConnection = true;
            throw new ConnectionException(ae.getMessage());            
            
		} catch (Exception e) {
			final String msg = "AdminException getting the AdminApi in getSessionCount: "; //$NON-NLS-1$
            log.error(msg, e);
            throw new ConnectionException(e.getMessage());
		} 

		return activeSessionsCollection.size();
	}
	
	private Collection<Session> getActiveSessions(String identifier) throws AdminException {
		Collection allSessionsCollection = getConnection().getSessions(identifier);
		Collection activeSessionsCollection = new ArrayList();

		Iterator<Session> allSessionsIter = allSessionsCollection
				.iterator();
		while (allSessionsIter.hasNext()) {
			Session session = allSessionsIter.next();
			if (session.getState() == MetaMatrixSessionState.ACTIVE) {
				activeSessionsCollection.add(session);
			}
		}
		
		return activeSessionsCollection;
	}

	private Collection<Session> getActiveSessions() throws AdminException {
		Collection allSessionsCollection = getConnection().getSessions(WILDCARD);
		Collection activeSessionsCollection = new ArrayList();

		Iterator<Session> allSessionsIter = allSessionsCollection
				.iterator();
		while (allSessionsIter.hasNext()) {
			Session session = allSessionsIter.next();
			if (session.getState() == MetaMatrixSessionState.ACTIVE) {
				activeSessionsCollection.add(session);
			}
		}
		
		return activeSessionsCollection;
	}

	public Collection getSessions(boolean activeOnly, List fieldNameList) throws ConnectionException {

		Collection sessionsCollection  = Collections.EMPTY_LIST;
		try {
			if (activeOnly){
				sessionsCollection = getActiveSessions();
			}else{
				sessionsCollection = getConnection().getSessions(WILDCARD);
			}
        } catch (AdminException ae) {
            invalidConnection = true;
            throw new ConnectionException(ae.getMessage());            
            
		} catch (Exception e) {
			final String msg = "AdminException getting the AdminApi in getSessions: "; //$NON-NLS-1$
            log.error(msg, e);
            throw new ConnectionException(e.getMessage());
		} 
		
		if (sessionsCollection != null && sessionsCollection.size() > 0) {
			Iterator sessionsIter = sessionsCollection.iterator();
		
			return createReportResultList(fieldNameList, sessionsIter);
		}

		return Collections.EMPTY_LIST;
	}

	private Collection createReportResultList(Properties props) {
		Collection reportResultList = new ArrayList();
		
		//Create list from properties and sort
		Enumeration<Object> keys = props.keys();
		List<String> elementList = new ArrayList();
		while (keys.hasMoreElements()) {
			elementList.add((String) keys.nextElement());
		}

		Collections.sort(elementList);
		
		Iterator propsKeySetIter = elementList.iterator();
		
		while (propsKeySetIter.hasNext()) {
			Map reportValueMap = new HashMap();
			String name = (String)propsKeySetIter.next();
			Object value = props.get(name);
			reportValueMap.put(ConnectionConstants.ComponentType.Operation.Value.NAME, name);
			reportValueMap.put(ConnectionConstants.ComponentType.Operation.Value.VALUE, value);
			reportResultList.add(reportValueMap);
		}
		return reportResultList;
	}
	
	private Collection createReportResultList(List fieldNameList, Iterator sessionsIter) {
		Collection reportResultList = new ArrayList();
		
		while (sessionsIter.hasNext()) {
			Object object = sessionsIter.next();

			Class cls = null;
			try {
				cls = object.getClass();
				Iterator methodIter = fieldNameList.iterator();
				Map reportValueMap = new HashMap();
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
	
	private Integer getHighWatermark(String processIdentifier) throws ConnectionException {

		try {
			
			ProcessObject process = ConnectionUtil.getProcess(getConnection(), processIdentifier);
			QueueWorkerPool pool = null;
				pool = process.getQueueWorkerPool();
				return pool.getTotalHighwaterMark();
				
		} catch (AdminException e) {
			// TODO Auto-generated catch block
			throw new ConnectionException(e.getMessage());
				
		} 

	}

	/**
	 * @param resourcePrefix
	 * @return
	 */
//	private  Object getResource(final String resourceIdentifier,
//			final Admin adminApi) throws ConnectionException {
//
//		Object resource = null;
//		try {
//
//			Collection<Object> resourceCollection = getConnection()adminApi
//					.getResources(resourceIdentifier);
//			// Collection<Object> processCollection = ((ServerMonitoringAdmin)
//			// adminApi)
//			// .getProcesses(AdminObject.WILDCARD);
//			Iterator<Object> resourceIter = resourceCollection.iterator();
//
//			while (resourceIter.hasNext()) {
//				resource = resourceIter.next();
//			}
//        } catch (AdminException ae) {
//            invalidConnection = true;
//            throw new ConnectionException(ae.getMessage());            
//           
//
//		} catch (Exception e) {
//			final String msg = "LogonException getting the AdminApi in getResource: "; //$NON-NLS-1$
//            log.error(msg, e);
//		}
//
//		return resource;
//	}


	/**
	 * @param resourcePrefix
	 * @return
	 */
	private Component getProcess(final String processIdentifier) throws ConnectionException {

		ProcessObject process = null;
		try {

			process = ConnectionUtil.getProcess(getConnection(), processIdentifier);
			
			return mapProcess(process);

		} catch (Exception e) {
			final String msg = "AdminException in getProcess: "; //$NON-NLS-1$
            log.error(msg, e);
            throw new ConnectionException(e.getMessage());
		} 

	}

    /**
     *  Return the properties for component of a specified resource type  
     * @param resourceType
     * @param identifier
     * @return
     * @throws ConnectionException
     * @since 5.5.3
     */
    
    public Properties getProperties(String resourceType, String identifier) 
        throws ConnectionException { 
    	String className = null;
    	Component ao = null;
        if (resourceType.equalsIgnoreCase(org.teiid.rhq.comm.ConnectionConstants.ComponentType.Runtime.Host.TYPE) ) {
            className = Host.class.getName();
            ao = this.getHost(identifier);
          
       } else if (resourceType.equalsIgnoreCase(ConnectionConstants.ComponentType.Runtime.System.TYPE) ) {
           className = SystemObject.class.getName(); 
           			
                   
       } else if (resourceType.equalsIgnoreCase(Runtime.Process.TYPE) ) {
           className = ProcessObject.class.getName();   
           
           ao = this.getProcess(identifier);
                   
       } else if (resourceType.equalsIgnoreCase(Runtime.Connector.TYPE) ) {
           className = ConnectorBinding.class.getName();                   
                   
           ao = this.getConnector(identifier);
//       } else if (resourceType.equalsIgnoreCase(Runtime.Service.TYPE) ) {
//           className = Service.class.getName();                   
//                   
//           ao = this.getService(identifier);
       } 
        
       if (ao == null) {
           throw new ConnectionException("Unable to get properties for invalid resource " + identifier + " of resource type " + resourceType);  //$NON-NLS-1$
       }
        
        Properties props = null;
        try {
            Map defaults = ConnectionUtil.getPropertiesDefinitions(getConnection(), resourceType, className, identifier);
            
            props = new Properties();
            props.putAll(defaults);
            
            // overlay the defined properties for the component object over the defaults
            props.putAll(ao.getProperties());
        } catch (AdminException ae) {
            invalidConnection = true;
            throw new ConnectionException(ae.getMessage());            

        } 
        
        return props;
        
    }
    /*    

	/**
	 * Returns the system name for the current connection
	 * 
	 * @param componentName
	 * @param operationName
	 * @param parameters
	 * @return systemName
	 */
//	private String getProperty() throws Exception {
//
//		String systemName = "NoSystemName";


//        Resource mmJGroups = (Resource) getResource("JGroups",  getConnection()); //$NON-NLS-1$
//        systemName = mmJGroups.getPropertyValue("metamatrix.cluster.name"); //$NON-NLS-1$
//
//        return systemName;
//
//	}

	private Admin getConnection()  {
         return adminApi;
        

	}
	
   private ComponentImpl createComponent(AdminObject object) throws ConnectionException {
	   if (object != null) {
		   return createComponent(object.getIdentifier(), object.getName(), object.getProperties());
	   }
	   return createComponent("NOTSET", "NAMENOTSET", null);

	   
	   
   }
    
    
    private ComponentImpl createComponent(String identifier, String name, Properties props) throws ConnectionException {

        ComponentImpl comp = null;
		try {
			Class clzz = Class.forName(ComponentImpl.class.getName(), true, this.connectionPool.getClassLoader());
			comp = (ComponentImpl) clzz.newInstance();
			
			comp.setIdentifier(identifier);
			comp.setName(name);
			comp.setSystemKey(getKey());
			comp.setVersion("1.0"); //$NON-NLS-1$
			
			if (props != null) {
				comp.setProperties(props);
			}
			

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new ConnectionException(e.getMessage());
		}
        return comp;

    }


}
