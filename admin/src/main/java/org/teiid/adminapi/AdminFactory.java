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

package org.teiid.adminapi;

import static org.jboss.as.controller.client.helpers.ClientConstants.DEPLOYMENT_REMOVE_OPERATION;
import static org.jboss.as.controller.client.helpers.ClientConstants.DEPLOYMENT_UNDEPLOY_OPERATION;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;

import org.jboss.as.cli.Util;
import org.jboss.as.cli.operation.OperationFormatException;
import org.jboss.as.cli.operation.impl.DefaultOperationRequestAddress;
import org.jboss.as.cli.operation.impl.DefaultOperationRequestBuilder;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
import org.teiid.adminapi.PropertyDefinition.RestartType;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.MetadataMapper;
import org.teiid.adminapi.impl.PropertyDefinitionMetadata;
import org.teiid.adminapi.impl.VDBMetadataMapper;
import org.teiid.adminapi.impl.VDBMetadataMapper.RequestMetadataMapper;
import org.teiid.adminapi.impl.VDBMetadataMapper.SessionMetadataMapper;
import org.teiid.adminapi.impl.VDBMetadataMapper.TransactionMetadataMapper;
import org.teiid.core.util.ObjectConverterUtil;


/** 
 * Singleton factory for class for creating Admin connections to the Teiid
 */
@SuppressWarnings("nls")
public class AdminFactory {
	private static AdminFactory INSTANCE = new AdminFactory();
	
	public static AdminFactory getInstance() {
		return INSTANCE;
	}
    /**
     * Creates a ServerAdmin with the specified connection properties. 
     * @param userName
     * @param password
     * @param serverURL
     * @param applicationName
     * @return
     * @throws AdminException
     */
    public Admin createAdmin(String host, int port, String userName, char[] password) throws AdminException {
        if(host == null) {
            host = "localhost"; //$NON-NLS-1$
        }

        if(port < 0) {
            port = 9999;
        }

        try {
            CallbackHandler cbh = new AuthenticationCallbackHandler(userName, password);
            ModelControllerClient newClient = ModelControllerClient.Factory.create(host, port, cbh);

            List<String> nodeTypes = Util.getNodeTypes(newClient, new DefaultOperationRequestAddress());
            if (!nodeTypes.isEmpty()) {
                boolean domainMode = nodeTypes.contains("server-group"); //$NON-NLS-1$ 
                System.out.println("Connected to " //$NON-NLS-1$ 
                        + (domainMode ? "domain controller at " : "standalone controller at ") //$NON-NLS-1$ //$NON-NLS-2$
                        + host + ":" + port); //$NON-NLS-1$ 
                return new AdminImpl(newClient);
            } 
            System.out.println("The controller is not available at " + host + ":" + port); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (UnknownHostException e) {
        	throw new AdminProcessingException("Failed to resolve host '" + host + "': " + e.getLocalizedMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }
        return null;
    }
    
    private class AuthenticationCallbackHandler implements CallbackHandler {
        private boolean realmShown = false;
        private String userName = null;
        private char[] password = null;

        public AuthenticationCallbackHandler(String user, char[] password) {
        	this.userName = user;
        	this.password = password;
        }
        
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            // Special case for anonymous authentication to avoid prompting user for their name.
            if (callbacks.length == 1 && callbacks[0] instanceof NameCallback) {
                ((NameCallback)callbacks[0]).setName("anonymous CLI user"); //$NON-NLS-1$
                return;
            }

            for (Callback current : callbacks) {
                if (current instanceof RealmCallback) {
                    RealmCallback rcb = (RealmCallback) current;
                    String defaultText = rcb.getDefaultText();
                    rcb.setText(defaultText); // For now just use the realm suggested.
                    if (realmShown == false) {
                        realmShown = true;
                    }
                } else if (current instanceof RealmChoiceCallback) {
                    throw new UnsupportedCallbackException(current, "Realm choice not currently supported."); //$NON-NLS-1$
                } else if (current instanceof NameCallback) {
                    NameCallback ncb = (NameCallback) current;
                    ncb.setName(userName);
                } else if (current instanceof PasswordCallback) {
                    PasswordCallback pcb = (PasswordCallback) current;
                    pcb.setPassword(password);
                } else {
                    throw new UnsupportedCallbackException(current);
                }
            }
        }

    }    
    
    private class AdminImpl implements Admin{
    	private static final String JAVA_CONTEXT = "java:/";
		private ModelControllerClient connection;
    	private boolean domainMode = false;
    	
    	public AdminImpl (ModelControllerClient connection) {
    		this.connection = connection;
            List<String> nodeTypes = Util.getNodeTypes(connection, new DefaultOperationRequestAddress());
            if (!nodeTypes.isEmpty()) {
                domainMode = nodeTypes.contains("server-group"); //$NON-NLS-1$
            }     		
    	}
    	
		@Override
		public void clearCache(String cacheType) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "clear-cache", "cache-type", cacheType);//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }
		}

		@Override
		public void clearCache(String cacheType, String vdbName, int vdbVersion) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "clear-cache", 
	        		"cache-type", cacheType,
	        		"vdb-name", vdbName,
	        		"vdb-version", String.valueOf(vdbVersion));//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }
		}

		@Override
		public void close() {
			if (this.connection != null) {
		        try {
		        	connection.close();
		        } catch (Throwable t) {
		        	//ignore
		        }				
		        this.connection = null;
				this.domainMode = false;
			}
		}
		
		private void createConnectionFactory(String deploymentName,	String templateName, Properties properties)	throws AdminException {
			Set<String> resourceAdapters = getDeployedResourceAdapterNames();
			if (!resourceAdapters.contains(templateName)) {
				addResourceAdapter(templateName);
			}
			
			///subsystem=resource-adapters/resource-adapter=teiid-connector-file.rar/connection-definitions=fooDS:add(class-name=org.teiid.resource.adapter.file.FileManagedConnectionFactory, jndi-name=java\:\/fooDS, pool-name=foo-pool)
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;

	        try {
	            builder.addNode("subsystem", "resource-adapters"); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.addNode("resource-adapter", templateName); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.addNode("connection-definitions", deploymentName); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.setOperationName("add"); 
	            builder.addProperty("jndi-name", addJavaContext(deploymentName));
	            builder.addProperty("pool-name", deploymentName);
	            request = builder.buildRequest();
	        } catch (OperationFormatException e) {
	            throw new IllegalStateException("Failed to build operation", e); //$NON-NLS-1$
	        }
			
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	throw new AdminProcessingException(e);
	        }
	        
	        // add all the config properties
            Enumeration keys = properties.propertyNames();
            while (keys.hasMoreElements()) {
            	String key = (String)keys.nextElement();
            	addConfigProperty(templateName, deploymentName, key, properties.getProperty(key));
            }
		}

		// /subsystem=resource-adapters/resource-adapter=teiid-connector-file.rar/connection-definitions=fooDS/config-properties=ParentDirectory2:add(value=/home/rareddy/testing)
		private void addConfigProperty(String templateName, String deploymentName, String key, String value) throws AdminProcessingException {
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;
	        try {
	            builder.addNode("subsystem", "resource-adapters"); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.addNode("resource-adapter", templateName); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.addNode("connection-definitions", deploymentName); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.addNode("config-properties", key); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.setOperationName("add"); 
	            builder.addProperty("value", value);
	            request = builder.buildRequest();
	        } catch (OperationFormatException e) {
	            throw new IllegalStateException("Failed to build operation", e); //$NON-NLS-1$
	        }
			
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	throw new AdminProcessingException(e);
	        }
		}

		// /subsystem=resource-adapters/resource-adapter=teiid-connector-ws.rar:add(archive=teiid-connector-ws.rar, transaction-support=NoTransaction)
		private void addResourceAdapter(String rarName) throws AdminProcessingException {
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;

	        try {
	            builder.addNode("subsystem", "resource-adapters"); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.addNode("resource-adapter", rarName); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.setOperationName("add"); 
	            request = builder.buildRequest();
	            request.get("archive").set(rarName);
	            request.get("transaction-support").set("NoTransaction");
	        } catch (OperationFormatException e) {
	            throw new IllegalStateException("Failed to build operation", e); //$NON-NLS-1$
	        }
			
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	throw new AdminProcessingException(e);
	        }				
		}
		
		class AbstractMetadatMapper implements MetadataMapper<String>{
			@Override
			public ModelNode wrap(String obj, ModelNode node) {
				return null;
			}
			@Override
			public String unwrap(ModelNode node) {
				return null;
			}
			@Override
			public ModelNode describe(ModelNode node) {
				return null;
			}
		}
		
		public List<String> getInstalledJDBCDrivers() throws AdminException {
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;

	        try {
	            builder.addNode("subsystem", "datasources"); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.setOperationName("installed-drivers-list"); 
	            request = builder.buildRequest();

	        } catch (OperationFormatException e) {
	            throw new IllegalStateException("Failed to build operation", e); //$NON-NLS-1$
	        }
			
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	            List<String> drivers = getList(outcome, new AbstractMetadatMapper() {
					@Override
					public String unwrap(ModelNode node) {
						if (node.hasDefined("driver-name")) {
							return node.get("driver-name").asString();
						}
						return null;
					}
				});
	            return drivers;
	        } catch (IOException e) {
	        	throw new AdminProcessingException(e);
	        }	
		}
		
		@Override
		public void createDataSource(String deploymentName,	String templateName, Properties properties)	throws AdminException {
			deploymentName = removeJavaContext(deploymentName);
			
			Collection<String> dsNames = getDataSourceNames();
			if (dsNames.contains(deploymentName)) {
				throw new AdminProcessingException(AdminPlugin.Util.getString("datasource_exists", deploymentName));
			}
			
			Set<String> resourceAdapters = getAvailableResourceAdapterNames();
        	if (resourceAdapters.contains(templateName)) {
	            createConnectionFactory(deploymentName, templateName, properties);
	            return;
        	}
			
        	List<String> drivers = getInstalledJDBCDrivers();
        	if (!drivers.contains(templateName)) {
        		throw new AdminProcessingException(AdminPlugin.Util.getString("driver_not_defined", templateName));
        	}
        	
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;
	        try {
	            builder.addNode("subsystem", "datasources"); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.addNode("data-source", deploymentName); //$NON-NLS-1$	        		
	        	
	            builder.setOperationName("add"); 
	            
	            builder.addProperty("jndi-name", addJavaContext(deploymentName));
	            builder.addProperty("driver-name", templateName);
	            
	            builder.addProperty("pool-name", deploymentName);
	            builder.addProperty("pool-prefill", "false");
	            builder.addProperty("max-pool-size", "20");
	            builder.addProperty("min-pool-size", "10");
	            
	            if (properties != null) {
		            builder.addProperty("connection-url", properties.getProperty("connection-url"));
		            if (properties.getProperty("user-name") != null) {
		            	builder.addProperty("user-name", properties.getProperty("user-name"));
		            }
		            if (properties.getProperty("password") != null) {
		            	builder.addProperty("password", properties.getProperty("password"));
		            }
		            if (properties.getProperty("check-valid-connection-sql") != null) {
		            	builder.addProperty("check-valid-connection-sql", properties.getProperty("check-valid-connection-sql"));
		            }
	            }
	            else {
	            	throw new AdminProcessingException(AdminPlugin.Util.getString("connection_url_required"));
	            }
	            
	            request = builder.buildRequest();
	        } catch (OperationFormatException e) {
	            throw new IllegalStateException("Failed to build operation", e); //$NON-NLS-1$
	        }
	        
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	throw new AdminProcessingException(e);
	        }	        
		}

		@Override
		public void deleteDataSource(String deployedName) throws AdminException {
			deployedName = removeJavaContext(deployedName);
			
			Collection<String> dsNames = getDataSourceNames();
			if (!dsNames.contains(deployedName)) {
				throw new AdminProcessingException(AdminPlugin.Util.getString("datasource_doesnot_exists", deployedName));
			}
			
			boolean deleted = deleteDS(deployedName, false, "datasources", "data-source");
			
			// check xa connections
			if (!deleted) {
				deleted = deleteDS(deployedName, false, "datasources", "xa-data-source");
			}
			
			// check connection factories
			if (!deleted) {
				Map<String, String> raDSMap = getResourceAdapterDataSources();
				String rarName = raDSMap.get(deployedName);
				if (rarName != null) {
					deleted = deleteDS(rarName, true, "resource-adapters", "resource-adapter", deployedName);	
				}
			}
		}

		private String removeJavaContext(String deployedName) {
			if (deployedName.startsWith(JAVA_CONTEXT)) {
				deployedName = deployedName.substring(6);
			}
			return deployedName;
		}
		
		private String addJavaContext(String deployedName) {
			if (!deployedName.startsWith(JAVA_CONTEXT)) {
				deployedName = JAVA_CONTEXT+deployedName;
			}
			return deployedName;
		}		

		private boolean deleteDS(String deployedName, boolean connFactory, String... subsystem) throws AdminProcessingException {
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;

	        try {
	            builder.addNode("subsystem", subsystem[0]); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.addNode(subsystem[1], deployedName);
	            if (connFactory) {
	            	builder.addNode("connection-definitions", subsystem[2]);
	            }
	            builder.setOperationName("remove"); 
	            request = builder.buildRequest();
	        } catch (OperationFormatException e) {
	            throw new IllegalStateException("Failed to build operation", e); //$NON-NLS-1$
	        }
			
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                return false;
	            }
	            return true;
	        } catch (IOException e) {
	        	throw new AdminProcessingException(e);
	        }
		}

		@Override
		public void undeploy(String deployedName) throws AdminException {
	        try {			
				ModelNode request = buildUndeployRequest(deployedName);

	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (OperationFormatException e) {
	        	throw new AdminProcessingException(e);
	        } catch (IOException e) {
	        	throw new AdminProcessingException(e);
	        }
		}
		
	    public ModelNode buildUndeployRequest(String name) throws OperationFormatException {
	        ModelNode composite = new ModelNode();
	        composite.get("operation").set("composite");
	        composite.get("address").setEmptyList();
	        ModelNode steps = composite.get("steps");

	        DefaultOperationRequestBuilder builder;

	        if(this.domainMode) {
            	final List<String> serverGroups = Util.getServerGroups(this.connection);

                for (String group : serverGroups){
                    ModelNode groupStep = Util.configureDeploymentOperation(DEPLOYMENT_UNDEPLOY_OPERATION, name, group);
                    steps.add(groupStep);
                }

                for (String group : serverGroups) {
                    ModelNode groupStep = Util.configureDeploymentOperation(DEPLOYMENT_REMOVE_OPERATION, name, group);
                    steps.add(groupStep);
                }
	        } else if(Util.isDeployedAndEnabledInStandalone(name, this.connection)) {
	            builder = new DefaultOperationRequestBuilder();
	            builder.setOperationName("undeploy");
	            builder.addNode("deployment", name);
	            steps.add(builder.buildRequest());
	        }

	        // remove content
            builder = new DefaultOperationRequestBuilder();
            builder.setOperationName("remove");
            builder.addNode("deployment", name);
            steps.add(builder.buildRequest());
            
	        return composite;
	    }

		@Override
		public void deploy(String deployName, InputStream vdb)	throws AdminException {
			ModelNode request = buildDeployVDBRequest(deployName, vdb);
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }				
		}

		private ModelNode buildDeployVDBRequest(String fileName, InputStream vdb) throws AdminProcessingException {
            try {
				if (Util.isDeploymentInRepository(fileName, this.connection)){
	                // replace
					DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	                builder = new DefaultOperationRequestBuilder();
	                builder.setOperationName("full-replace-deployment");
	                builder.addProperty("name", fileName);
	                byte[] bytes = ObjectConverterUtil.convertToByteArray(vdb);
	                builder.getModelNode().get("content").get(0).get("bytes").set(bytes);
	                return builder.buildRequest();                
				}
				
				//add
		        ModelNode composite = new ModelNode();
		        composite.get("operation").set("composite");
		        composite.get("address").setEmptyList();
		        ModelNode steps = composite.get("steps");			
				
				DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	            builder.setOperationName("add");
	            builder.addNode("deployment", fileName);

				byte[] bytes = ObjectConverterUtil.convertToByteArray(vdb);
				builder.getModelNode().get("content").get(0).get("bytes").set(bytes);
				steps.add(builder.buildRequest());
            
	            // deploy
	            if (this.domainMode) {
	            	List<String> serverGroups = Util.getServerGroups(this.connection);
	                for (String serverGroup : serverGroups) {
	                    steps.add(Util.configureDeploymentOperation("add", fileName, serverGroup));
	                }
	                for (String serverGroup : serverGroups) {
	                    steps.add(Util.configureDeploymentOperation("deploy", fileName, serverGroup));
	                }
	            } else {
	                builder = new DefaultOperationRequestBuilder();
	                builder.setOperationName("deploy");
	                builder.addNode("deployment", fileName);
	                steps.add(builder.buildRequest());
	            }     
	            return composite;
			} catch (OperationFormatException e) {
				throw new AdminProcessingException(e);
			} catch (IOException e) {
				throw new AdminProcessingException(e);
			}      
		}

		@Override
		public CacheStatistics getCacheStats(String cacheType) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "cache-statistics",	"cache-type", cacheType);//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	            	if (outcome.hasDefined("result")) {
	            		ModelNode result = outcome.get("result");
	            		return VDBMetadataMapper.CacheStatisticsMetadataMapper.INSTANCE.unwrap(result);
	            	}	            	
	            	
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }
	        return null;
		}

		@Override
		public Collection<String> getCacheTypes() throws AdminException {
	        final ModelNode request = buildRequest("teiid", "cache-types");//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	            	return Util.getList(outcome);
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }
	        return Collections.emptyList();
		}

		private List<String> getChildNodeNames(String subsystem, String childNode) throws AdminException {
	        final ModelNode request = buildRequest(subsystem, "read-children-names", "child-type", childNode);//$NON-NLS-1$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	                return Util.getList(outcome);
	            }
	        } catch (IOException e) {
	        	throw new AdminProcessingException(e);
	        }
	        return Collections.emptyList();	
			
		}
		
		/**
		 * /subsystem=datasources:read-children-names(child-type=data-source)
		 * /subsystem=resource-adapters/resource-adapter={rar-file}:read-resource
		 * @see org.teiid.adminapi.Admin#getDataSourceNames()
		 */
		@Override
		public Collection<String> getDataSourceNames() throws AdminException {
			Set<String> datasourceNames = new HashSet<String>();
			datasourceNames.addAll(getChildNodeNames("datasources", "data-source"));
			datasourceNames.addAll(getChildNodeNames("datasources", "xa-data-source"));
			datasourceNames.addAll(getResourceAdapterDataSources().keySet());
			
			Set<String> dsNames = new HashSet<String>();
			for (String s:datasourceNames) {
				if (s.startsWith(JAVA_CONTEXT)) {
					dsNames.add(s.substring(6));
				}
				else {
					dsNames.add(s);
				}
			}
	        return dsNames;	
		}

		private Map<String, String> getResourceAdapterDataSources() throws AdminException {
			HashMap<String, String> datasourceNames = new HashMap<String, String>();
			Set<String> resourceAdapters = getDeployedResourceAdapterNames();
			for (String resource:resourceAdapters) {
				DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
				try {
				    builder.addNode("subsystem", "resource-adapters"); //$NON-NLS-1$ //$NON-NLS-2$
				    builder.addNode("resource-adapter", resource); //$NON-NLS-1$ //$NON-NLS-2$
				    builder.setOperationName("read-resource"); 
				    ModelNode request = builder.buildRequest();
				    
				    ModelNode outcome = this.connection.execute(request);
				    if (Util.isSuccess(outcome)) {
				    	if (outcome.hasDefined("result")) {
				    		ModelNode result = outcome.get("result");
				        	if (result.hasDefined("connection-definitions")) {
				        		List<ModelNode> connDefs = result.get("connection-definitions").asList();
				        		for (ModelNode conn:connDefs) {
				        			Iterator<String> it = conn.keys().iterator();
				        			if (it.hasNext()) {
				        				datasourceNames.put(it.next(), resource);
				        			}
				        		}
				        	}
				    	}
				    }
				} catch (OperationFormatException e) {
				    throw new AdminProcessingException("Failed to build operation", e); //$NON-NLS-1$
				} catch (IOException e) {
					throw new AdminProcessingException("Failed to build operation", e); //$NON-NLS-1$
				}
			}
			return datasourceNames;
		}
		
		/**
		 * This will get all deplyed RAR names
		 * /subsystem=resource-adapters:read-children-names(child-type=resource-adapter)
		 * @return
		 * @throws AdminException
		 */
		private Set<String> getDeployedResourceAdapterNames() throws AdminException {
			Set<String> templates = new HashSet<String>();
	        final ModelNode request = buildRequest("resource-adapters", "read-children-names", "child-type", "resource-adapter");//$NON-NLS-1$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	                templates.addAll(Util.getList(outcome));
	                return templates;
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }
	        return Collections.emptySet();					
		}

		// :read-children-names(child-type=deployment)
		private Set<String> getAvailableResourceAdapterNames() throws AdminException {
			Set<String> templates = new HashSet<String>();
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;
	        try {
	            builder.setOperationName("read-children-names"); 
	            builder.addProperty("child-type", "deployment");
	            request = builder.buildRequest();
	        } catch (OperationFormatException e) {
	            throw new IllegalStateException("Failed to build operation", e); //$NON-NLS-1$
	        }
			
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            List<String> deployments = Util.getList(outcome);
	            for (String deployment:deployments) {
	            	if (deployment.endsWith(".rar")) {
	            		templates.add(deployment);
	            	}
	            }
	        } catch (IOException e) {
	        	throw new AdminProcessingException(e);
	        }
	        return templates;
		}

		@Override
		public Set<String> getDataSourceTemplateNames() throws AdminException {
			Set<String> templates = new HashSet<String>();
			templates.addAll(getInstalledJDBCDrivers());
			templates.addAll(getAvailableResourceAdapterNames());
			return templates;
		}
		
		@Override
		public WorkerPoolStatistics getWorkerPoolStats() throws AdminException {
			final ModelNode request = buildRequest("teiid", "workerpool-statistics");//$NON-NLS-1$
			if (request != null) {
		        try {
		            ModelNode outcome = this.connection.execute(request);
		            if (Util.isSuccess(outcome)) {
		            	if (outcome.hasDefined("result")) {
		            		ModelNode result = outcome.get("result");
		            		return VDBMetadataMapper.WorkerPoolStatisticsMetadataMapper.INSTANCE.unwrap(result);
		            	}	            	
		            }		            
		        } catch (Exception e) {
		        	throw new AdminProcessingException(e);
		        }
			}
	        return null;
		}		

		
		@Override
		public void cancelRequest(String sessionId, long executionId) throws AdminException {
			final ModelNode request = buildRequest("teiid", "terminate-session", "session", sessionId, "execution-id", String.valueOf(executionId));//$NON-NLS-1$
			if (request == null) {
				return;
			}
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }		
		}
		
		@Override
		public Collection<? extends Request> getRequests() throws AdminException {
			final ModelNode request = buildRequest("teiid", "list-requests");//$NON-NLS-1$
			if (request != null) {
		        try {
		            ModelNode outcome = this.connection.execute(request);
		            if (Util.isSuccess(outcome)) {
		                return getList(outcome, RequestMetadataMapper.INSTANCE);
		            }
		        } catch (Exception e) {
		        	throw new AdminProcessingException(e);
		        }
			}
	        return Collections.emptyList();
		}

		@Override
		public Collection<? extends Request> getRequestsForSession(String sessionId) throws AdminException {
			final ModelNode request = buildRequest("teiid", "requests-per-session", "session", sessionId);//$NON-NLS-1$
			if (request != null) {
		        try {
		            ModelNode outcome = this.connection.execute(request);
		            if (Util.isSuccess(outcome)) {
		                return getList(outcome, RequestMetadataMapper.INSTANCE);
		            }
		        } catch (Exception e) {
		        	throw new AdminProcessingException(e);
		        }
			}
	        return Collections.emptyList();
		}

		@Override
		public Collection<? extends Session> getSessions() throws AdminException {
			final ModelNode request = buildRequest("teiid", "list-sessions");//$NON-NLS-1$
			if (request != null) {
		        try {
		            ModelNode outcome = this.connection.execute(request);
		            if (Util.isSuccess(outcome)) {
		                return getList(outcome, SessionMetadataMapper.INSTANCE);
		            }
		        } catch (Exception e) {
		        	throw new AdminProcessingException(e);
		        }
			}
	        return Collections.emptyList();
		}

		/**
		 * pattern on CLI
		 * /subsystem=datasources/data-source=foo:read-resource-description
		 */
		@Override		
		public Collection<PropertyDefinition> getTemplatePropertyDefinitions(String templateName) throws AdminException {

			ModelNode request = null;
			ModelNode result = null;
			try {
				Set<String> resourceAdapters = getAvailableResourceAdapterNames();
	        	if (resourceAdapters.contains(templateName)) {
	        		DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
		            builder.addNode("subsystem", "teiid"); //$NON-NLS-1$ //$NON-NLS-2$
		            builder.setOperationName("read-rar-description"); //$NON-NLS-1$
		            builder.addProperty("rar-name", templateName);
		            request = builder.buildRequest();					
			        try {
			            ModelNode outcome = this.connection.execute(request);
			            if (!Util.isSuccess(outcome)) {
			                throw new AdminProcessingException(Util.getFailureDescription(outcome));
			            }
			            result = outcome.get("result");
			        } catch (IOException e) {
			        	throw new AdminProcessingException(e);
			        }			            
	        	}
	        	else {
	        		result = new ModelNode();
	        		result.add(buildProperty("connection-url", "connection URL", ModelType.STRING, "connection url", true));
	        		result.add(buildProperty("user-name", "User Name", ModelType.STRING, "user name", false));
	        		result.add(buildProperty("password", "Password", ModelType.STRING, "password", false));
	        		result.add(buildProperty("check-valid-connection-sql", "Connection Validate SQL", ModelType.STRING, "SQL to be used to validate the connection", false));
	        	}
	        	
	        } catch (OperationFormatException e) {
	            throw new IllegalStateException("Failed to build operation", e); //$NON-NLS-1$
	        }
	        return buildPropertyDefinitions(result.asList());
		}
		
		private ModelNode buildProperty(String name, String displayName, ModelType modelType, String description, boolean required) {
			ModelNode node = new ModelNode();
			node.get(name, "type").set(modelType);
	        node.get(name, "description").set(description);
	        node.get(name, "required").set(required);
	        node.get(name, "display").set(displayName);
	        return node;
		}

		private ArrayList<PropertyDefinition> buildPropertyDefinitions(List<ModelNode> propsNodes) {
			ArrayList<PropertyDefinition> propDefinitions = new ArrayList<PropertyDefinition>();
        	for (ModelNode node:propsNodes) {
        		PropertyDefinitionMetadata def = new PropertyDefinitionMetadata();
        		Set<String> keys = node.keys();
        		
        		String name = keys.iterator().next();
        		def.setName(name);
        		node = node.get(name);

        		if (node.hasDefined("display")) {
        			def.setDisplayName(node.get("display").asString());
        		}
        		
        		if (node.hasDefined("description")) {
        			def.setDescription(node.get("description").asString());
        		}
        		
        		if (node.hasDefined("allowed")) {
        			List<ModelNode> allowed = node.get("allowed").asList();
        			ArrayList<String> list = new ArrayList<String>();
        			for(ModelNode m:allowed) {
        				list.add(m.asString());
        			}
        			def.setAllowedValues(list);
        		}        		
        		
        		if (node.hasDefined("required")) {
        			def.setRequired(node.get("required").asBoolean());
        		}
        		
        		if (node.hasDefined("read-only")) {
        			String access = node.get("read-only").asString();
        			def.setModifiable(Boolean.parseBoolean(access));
        		}
        		
        		if (node.hasDefined("advanced")) {
        			String access = node.get("advanced").asString();
        			def.setAdvanced(Boolean.parseBoolean(access));
        		}        		
        		
        		if (node.hasDefined("masked")) {
        			String access = node.get("masked").asString();
        			def.setAdvanced(Boolean.parseBoolean(access));
        		} 
        		
        		if (node.hasDefined("restart-required")) {
        			def.setRequiresRestart(RestartType.NONE);
        		}

        		String type = node.get("type").asString();
        		if (ModelType.STRING.name().equals(type)) {
        			def.setPropertyTypeClassName(String.class.getName());
        		}
        		else if (ModelType.INT.name().equals(type)) {
        			def.setPropertyTypeClassName(Integer.class.getName());
        		}
        		else if (ModelType.LONG.name().equals(type)) {
        			def.setPropertyTypeClassName(Long.class.getName());
        		}        		
        		else if (ModelType.BOOLEAN.name().equals(type)) {
        			def.setPropertyTypeClassName(Boolean.class.getName());
        		}
        		else if (ModelType.BIG_INTEGER.name().equals(type)) {
        			def.setPropertyTypeClassName(BigInteger.class.getName());
        		}        		
        		else if (ModelType.BIG_DECIMAL.name().equals(type)) {
        			def.setPropertyTypeClassName(BigDecimal.class.getName());
        		}     
        		
        		if (node.hasDefined("default")) {
            		if (ModelType.STRING.name().equals(type)) {
            			def.setDefaultValue(node.get("default").asString());
            		}
            		else if (ModelType.INT.name().equals(type)) {
            			def.setDefaultValue(node.get("default").asInt());
            		}
            		else if (ModelType.LONG.name().equals(type)) {
            			def.setDefaultValue(node.get("default").asLong());
            		}        		
            		else if (ModelType.BOOLEAN.name().equals(type)) {
            			def.setDefaultValue(node.get("default").asBoolean());
            		}
            		else if (ModelType.BIG_INTEGER.name().equals(type)) {
            			def.setDefaultValue(node.get("default").asBigInteger());
            		}        		
            		else if (ModelType.BIG_DECIMAL.name().equals(type)) {
            			def.setDefaultValue(node.get("default").asBigDecimal());
            		}          			
        		}
        		propDefinitions.add(def);
        	}
			return propDefinitions;
		}

		@Override
		public Collection<? extends Transaction> getTransactions() throws AdminException {
			final ModelNode request = buildRequest("teiid", "list-transactions");//$NON-NLS-1$
			if (request != null) {
		        try {
		            ModelNode outcome = this.connection.execute(request);
		            if (Util.isSuccess(outcome)) {
		                return getList(outcome, TransactionMetadataMapper.INSTANCE);
		            }
		        } catch (Exception e) {
		        	throw new AdminProcessingException(e);
		        }
			}
	        return Collections.emptyList();
		}
		
		@Override
		public void terminateSession(String sessionId) throws AdminException {
			final ModelNode request = buildRequest("teiid", "terminate-session", "session", sessionId);//$NON-NLS-1$
			if (request == null) {
				return;
			}
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }			
		}

		@Override
		public void terminateTransaction(String transactionId) throws AdminException {
			final ModelNode request = buildRequest("teiid", "terminate-transaction", "xid", transactionId);//$NON-NLS-1$
			if (request == null) {
				return;
			}
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }			
		}		

		@Override
		public Translator getTranslator(String deployedName) throws AdminException {
			final ModelNode request = buildRequest("teiid", "get-translator", "translator-name", deployedName);//$NON-NLS-1$
			if (request == null) {
				return null;
			}
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	            	if (outcome.hasDefined("result")) {
	            		ModelNode result = outcome.get("result");
	            		return VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE.unwrap(result);
	            	}	            	
	            }
	            
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }			
			return null;
		}

		@Override
		public Collection<? extends Translator> getTranslators() throws AdminException {
	        final ModelNode request = buildRequest("teiid", "list-translators");//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	                return getList(outcome, VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE);
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }

	        return Collections.emptyList();
		}
		
		private ModelNode buildRequest(String subsystem, String operationName, String... params) {
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;
	        try {
	            builder.addNode("subsystem", subsystem); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.setOperationName(operationName); 
	            request = builder.buildRequest();
	            if (params != null && params.length % 2 == 0) {
	            	for (int i = 0; i < params.length; i+=2) {
	            		builder.addProperty(params[i], params[i+1]);
	            	}
	            }
	        } catch (OperationFormatException e) {
	            throw new IllegalStateException("Failed to build operation", e); //$NON-NLS-1$
	        }
			return request;
		}
		
	    private <T> List<T> getList(ModelNode operationResult,  MetadataMapper<T> mapper) {
	        if(!operationResult.hasDefined("result")) //$NON-NLS-1$
	            return Collections.emptyList();

	        List<ModelNode> nodeList = operationResult.get("result").asList(); //$NON-NLS-1$
	        if(nodeList.isEmpty())
	            return Collections.emptyList();

	        List<T> list = new ArrayList<T>(nodeList.size());
	        for(ModelNode node : nodeList) {
        		list.add(mapper.unwrap(node));
	        }
	        return list;
	    }		
	    
	    private <T> Set<T> getSet(ModelNode operationResult,  MetadataMapper<T> mapper) {
	        if(!operationResult.hasDefined("result")) //$NON-NLS-1$
	            return Collections.emptySet();

	        List<ModelNode> nodeList = operationResult.get("result").asList(); //$NON-NLS-1$
	        if(nodeList.isEmpty())
	            return Collections.emptySet();

	        Set<T> list = new HashSet<T>(nodeList.size());
	        for(ModelNode node : nodeList) {
	            list.add(mapper.unwrap(node));
	        }
	        return list;
	    }	    

		@Override
		public VDB getVDB(String vdbName, int vdbVersion) throws AdminException {
			final ModelNode request = buildRequest("teiid", "get-vdb", "vdb-name", vdbName, "vdb-version", String.valueOf(vdbVersion));//$NON-NLS-1$
			if (request == null) {
				return null;
			}
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	            	if (outcome.hasDefined("result")) {
	            		ModelNode result = outcome.get("result");
	            		return VDBMetadataMapper.INSTANCE.unwrap(result);
	            	}	            	
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }			
			return null;
		}

		@Override
		public Set<? extends VDB> getVDBs() throws AdminException {
	        final ModelNode request = buildRequest("teiid", "list-vdbs");//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	                return getSet(outcome, VDBMetadataMapper.INSTANCE);
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }

	        return Collections.emptySet();
		}

		@Override
		public void mergeVDBs(String sourceVDBName, int sourceVDBVersion,
				String targetVDBName, int targetVDBVersion)
				throws AdminException {
			final ModelNode request = buildRequest("teiid", "merge-vdbs", 
					"source-vdb-name", sourceVDBName, 
					"source-vdb-name", String.valueOf(sourceVDBVersion),
					"target-vdb-name", targetVDBName, 
					"target-vdb-version", String.valueOf(targetVDBVersion));//$NON-NLS-1$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }			
		}

		@Override
		public void addDataRoleMapping(String vdbName, int vdbVersion, String dataRole, String mappedRoleName) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "add-data-role", 
	        		"vdb-name", vdbName,
	        		"vdb-version", String.valueOf(vdbVersion),
	        		"data-role", dataRole,
	        		"mapped-role", mappedRoleName);//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }
		}		
		
		@Override
		public void removeDataRoleMapping(String vdbName, int vdbVersion, String dataRole, String mappedRoleName) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "remove-data-role", 
	        		"vdb-name", vdbName,
	        		"vdb-version", String.valueOf(vdbVersion),
	        		"data-role", dataRole,
	        		"mapped-role", mappedRoleName);//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }		
		}

		@Override
		public void setAnyAuthenticatedForDataRole(String vdbName, int vdbVersion, String dataRole, boolean anyAuthenticated) throws AdminException {
	        ModelNode request = buildRequest("teiid", "add-anyauthenticated-role", 
	        		"vdb-name", vdbName,
	        		"vdb-version", String.valueOf(vdbVersion),
	        		"data-role", dataRole); //$NON-NLS-1$ //$NON-NLS-2$
	        
	        if (!anyAuthenticated) {
	        	request = buildRequest("teiid", "remove-anyauthenticated-role", 
		        		"vdb-name", vdbName,
		        		"vdb-version", String.valueOf(vdbVersion),
		        		"data-role", dataRole); //$NON-NLS-1$ //$NON-NLS-2$	        	
	        }
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }			
		}

		@Override
		public void changeVDBConnectionType(String vdbName, int vdbVersion, ConnectionType type) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "change-vdb-connection-type", 
	        		"vdb-name", vdbName,
	        		"vdb-version", String.valueOf(vdbVersion),
	        		"connection-type", type.name());//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }				
		}		
		
		@Override
		public void assignToModel(String vdbName, int vdbVersion, String modelName, String sourceName, String translatorName,
				String dsName) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "assign-datasource", 
	        		"vdb-name", vdbName,
	        		"vdb-version", String.valueOf(vdbVersion),
	        		"model-name", modelName,
	        		"source-name", sourceName,
	        		"translator-name", translatorName,
	        		"ds-name", dsName);//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }				
		}
		
	    @Override
	    public void markDataSourceAvailable(String jndiName) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "mark-datasource-available","ds-name", jndiName);//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	throw new AdminProcessingException(Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	throw new AdminProcessingException(e);
	        }	    	
	    }		
    }
}
