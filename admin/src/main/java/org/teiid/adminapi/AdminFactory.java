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
import static org.jboss.as.controller.client.helpers.ClientConstants.RESULT;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Logger;

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
import org.teiid.adminapi.impl.AdminObjectImpl;
import org.teiid.adminapi.impl.MetadataMapper;
import org.teiid.adminapi.impl.PropertyDefinitionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBMetadataMapper;
import org.teiid.adminapi.impl.VDBMetadataMapper.RequestMetadataMapper;
import org.teiid.adminapi.impl.VDBMetadataMapper.SessionMetadataMapper;
import org.teiid.adminapi.impl.VDBMetadataMapper.TransactionMetadataMapper;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.core.util.ObjectConverterUtil;


/**
 * Singleton factory for class for creating Admin connections to the Teiid
 */
@SuppressWarnings("nls")
public class AdminFactory {
	private static final Logger LOGGER = Logger.getLogger(AdminFactory.class.getName());
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
     * @param profileName - Name of the domain mode profile
     * @return
     * @throws AdminException
     */
    public Admin createAdmin(String host, int port, String userName, char[] password, String profileName) throws AdminException {
    	AdminImpl admin = (AdminImpl)createAdmin(host, port, userName, password);
    	if (admin != null) {
    		admin.setProfileName(profileName);
    	}
    	return admin;
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
                LOGGER.info("Connected to " //$NON-NLS-1$
                        + (domainMode ? "domain controller at " : "standalone controller at ") //$NON-NLS-1$ //$NON-NLS-2$
                        + host + ":" + port); //$NON-NLS-1$
                return new AdminImpl(newClient);
            }
            LOGGER.info(AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70051, host, port)); //$NON-NLS-1$ //$NON-NLS-2$
        } catch (UnknownHostException e) {
        	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70000, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70000, host, e.getLocalizedMessage()));
        }
        return null;
    }

    public Admin createAdmin(ModelControllerClient connection) {
    	return new AdminImpl(connection);
    }

    /**
     * Name of the domain mode profile.
     * @param connection
     * @param profileName
     * @return
     */
    public Admin createAdmin(ModelControllerClient connection, String profileName) {
    	AdminImpl admin = new AdminImpl(connection);
    	admin.setProfileName(profileName);
    	return admin;
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
                    if (this.realmShown == false) {
                        this.realmShown = true;
                    }
                } else if (current instanceof RealmChoiceCallback) {
                    throw new UnsupportedCallbackException(current, "Realm choice not currently supported."); //$NON-NLS-1$
                } else if (current instanceof NameCallback) {
                    NameCallback ncb = (NameCallback) current;
                    ncb.setName(this.userName);
                } else if (current instanceof PasswordCallback) {
                    PasswordCallback pcb = (PasswordCallback) current;
                    pcb.setPassword(this.password);
                } else {
                    throw new UnsupportedCallbackException(current);
                }
            }
        }

    }

	private class ResultCallback {
		@SuppressWarnings("unused")
		void onSuccess(ModelNode outcome, ModelNode result) throws AdminException {
		}
		void onFailure(String msg) throws AdminProcessingException {
			throw new AdminProcessingException(AdminPlugin.Event.TEIID70006, msg);
		}
	}

    public class AdminImpl implements Admin{
    	private static final String CLASS_NAME = "class-name";
		private static final String JAVA_CONTEXT = "java:/";
		private ModelControllerClient connection;
    	private boolean domainMode = false;
    	private String profileName = "ha";

    	public AdminImpl (ModelControllerClient connection) {
    		this.connection = connection;
            List<String> nodeTypes = Util.getNodeTypes(connection, new DefaultOperationRequestAddress());
            if (!nodeTypes.isEmpty()) {
                this.domainMode = nodeTypes.contains("server-group"); //$NON-NLS-1$
            }
    	}

		public void setProfileName(String name) {
			this.profileName = name;
		}

		@Override
		public void clearCache(String cacheType) throws AdminException {
			cliCall("clear-cache",
					new String[] { "subsystem", "teiid" },
					new String[] { "cache-type", cacheType},
					new ResultCallback());
		}

		@Override
		public void clearCache(String cacheType, String vdbName, int vdbVersion) throws AdminException {
			cliCall("clear-cache",
					new String[] { "subsystem", "teiid" },
					new String[] { "cache-type", cacheType, "vdb-name",vdbName, "vdb-version", String.valueOf(vdbVersion) },
					new ResultCallback());
		}

		@Override
		public void close() {
			if (this.connection != null) {
		        try {
		        	this.connection.close();
		        } catch (Throwable t) {
		        	//ignore
		        }
		        this.connection = null;
				this.domainMode = false;
			}
		}

		private void createConnectionFactory(String deploymentName,	String rarName, Properties properties)	throws AdminException {

			if (!getInstalledResourceAdaptorNames().contains(rarName)) {
				///subsystem=resource-adapters/resource-adapter=fileDS:add
				addArchiveResourceAdapter(rarName);
			}

			//AS-4776 HACK - BEGIN
			else {
				// add duplicate resource adapter AS-4776 Workaround
				String moduleName = getResourceAdapterModuleName(rarName);
				addModuleResourceAdapter(deploymentName, moduleName);
				rarName = deploymentName;
			}
			//AS-4776 HACK - END
			
			BuildPropertyDefinitions bpd = new BuildPropertyDefinitions();
			buildResourceAdpaterProperties(rarName, bpd);
			ArrayList<PropertyDefinition> jcaSpecific = bpd.getPropertyDefinitions();

			///subsystem=resource-adapters/resource-adapter=fileDS/connection-definitions=fileDS:add(jndi-name=java\:\/fooDS)
	        ArrayList<String> parameters = new ArrayList<String>();
	        parameters.add("jndi-name");
	        parameters.add(addJavaContext(deploymentName));
	        parameters.add("enabled");
	        parameters.add("true");
            if (properties.getProperty(CLASS_NAME) != null) {
    	        parameters.add(CLASS_NAME);
    	        parameters.add(properties.getProperty(CLASS_NAME));
            }

            // add jca specific proeprties
            for (PropertyDefinition pd:jcaSpecific) {
                if (properties.getProperty(pd.getName()) != null) {
        	        parameters.add(pd.getName());
        	        parameters.add(properties.getProperty(pd.getName()));
                }
            }

			cliCall("add", new String[] { "subsystem", "resource-adapters",
					"resource-adapter", rarName,
					"connection-definitions", deploymentName },
					parameters.toArray(new String[parameters.size()]), new ResultCallback());

	        // add all the config properties
            Enumeration keys = properties.propertyNames();
            while (keys.hasMoreElements()) {
            	boolean add = true;
            	String key = (String)keys.nextElement();
            	if (key.equals(CLASS_NAME)) {
            		add = false;
            	}
            	for (PropertyDefinition pd:jcaSpecific) {
            		if (key.equals(pd.getName())) {
            			add = false;
            			break;
            		}
            	}
            	if (add) {
            		addConfigProperty(rarName, deploymentName, key, properties.getProperty(key));
            	}
            }

            // activate
            activateConnectionFactory(rarName);
		}


		// /subsystem=resource-adapters/resource-adapter=fileDS/connection-definitions=fileDS/config-properties=ParentDirectory2:add(value=/home/rareddy/testing)
		private void addConfigProperty(String rarName, String deploymentName, String key, String value) throws AdminException {
			if (value == null || value.trim().isEmpty()) {
				throw new AdminProcessingException(AdminPlugin.Event.TEIID70054, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70054, key));
			}
			cliCall("add", new String[] { "subsystem", "resource-adapters",
					"resource-adapter", rarName,
					"connection-definitions", deploymentName,
					"config-properties", key},
					new String[] {"value", value}, new ResultCallback());
		}

		// /subsystem=resource-adapters/resource-adapter=fileDS:activate
		private void activateConnectionFactory(String rarName) throws AdminException {
			cliCall("activate", new String[] { "subsystem", "resource-adapters",
					"resource-adapter", rarName },
					null, new ResultCallback());
		}

		private void addProfileNode(DefaultOperationRequestBuilder builder) throws AdminException {
			if (this.domainMode) {
				String profile = getProfileName();
				if (profile != null) {
					builder.addNode("profile",profile);
				}
			}
		}

		// /subsystem=resource-adapters/resource-adapter=teiid-connector-ws.rar:add(archive=teiid-connector-ws.rar, transaction-support=NoTransaction)
		private void addArchiveResourceAdapter(String rarName) throws AdminException {
			cliCall("add", new String[] { "subsystem", "resource-adapters",
					"resource-adapter", rarName },
					new String[] { "archive", rarName, "transaction-support","NoTransaction" },
					new ResultCallback());
		}
		
		private void addModuleResourceAdapter(String rarName, String moduleName) throws AdminException {
			cliCall("add", new String[] { "subsystem", "resource-adapters",
					"resource-adapter", rarName },
					new String[] { "module", moduleName, "transaction-support","NoTransaction" },
					new ResultCallback());			
		}

		private String getResourceAdapterModuleName(String rarName)
				throws AdminException {
			final List<String> props = new ArrayList<String>();
			cliCall("read-resource",
					new String[] { "subsystem", "resource-adapters", "resource-adapter", rarName},
					null, new ResultCallback() {
						@Override
						void onSuccess(ModelNode outcome, ModelNode result) throws AdminException {
				    		List<ModelNode> properties = outcome.get("result").asList();
				    		
			        		for (ModelNode prop:properties) {
			        			if (!prop.getType().equals(ModelType.PROPERTY)) {
			        				continue;
			        			}
			    				org.jboss.dmr.Property p = prop.asProperty();			        			
								if (p.getName().equals("module")) {
									props.add(p.getValue().asString());
								}
			        		}
						}
					});
			return props.get(0);
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

		public Set<String> getInstalledJDBCDrivers() throws AdminException {
			HashSet<String> driverList = new HashSet<String>();
			driverList.addAll(getChildNodeNames("datasources", "jdbc-driver"));

			if (!this.domainMode) {
				//'installed-driver-list' not available in the domain mode.
				final ModelNode request = buildRequest("datasources", "installed-drivers-list");
		        try {
		            ModelNode outcome = this.connection.execute(request);
		            if (Util.isSuccess(outcome)) {
			            List<String> drivers = getList(outcome, new AbstractMetadatMapper() {
							@Override
							public String unwrap(ModelNode node) {
								if (node.hasDefined("driver-name")) {
									return node.get("driver-name").asString();
								}
								return null;
							}
						});
			            driverList.addAll(drivers);
		            }
		        } catch (IOException e) {
		        	throw new AdminComponentException(AdminPlugin.Event.TEIID70052, e);
		        }
			}
			else {
				// TODO: AS7 needs to provide better way to query the deployed JDBC drivers
				List<String> deployments = getChildNodeNames(null, "deployment");
	            for (String deployment:deployments) {
	            	if (!deployment.contains("translator") && deployment.endsWith(".jar")) {
	            		driverList.add(deployment);
	            	}
	            }
			}
	        return driverList;
		}

		public String getProfileName() throws AdminException {
			if (!this.domainMode) {
				return null;
			}
			if (this.profileName == null) {
				this.profileName = getChildNodeNames(null, "profile").get(0);
			}
			return this.profileName;
		}

		@Override
		public void createDataSource(String deploymentName,	String templateName, Properties properties)	throws AdminException {
			deploymentName = removeJavaContext(deploymentName);

			Collection<String> dsNames = getDataSourceNames();
			if (dsNames.contains(deploymentName)) {
				 throw new AdminProcessingException(AdminPlugin.Event.TEIID70003, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70003, deploymentName));
			}

			Set<String> resourceAdapters = getResourceAdapterNames();
        	if (resourceAdapters.contains(templateName)) {
	            createConnectionFactory(deploymentName, templateName, properties);
	            return;
        	}

        	Set<String> drivers = getInstalledJDBCDrivers();
        	if (!drivers.contains(templateName)) {
        		 throw new AdminProcessingException(AdminPlugin.Event.TEIID70004, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70004, templateName));
        	}

        	// build properties
        	Collection<PropertyDefinition> dsProperties = getTemplatePropertyDefinitions(templateName);
        	ArrayList<String> parameters = new ArrayList<String>();
            if (properties != null) {
            	parameters.add("connection-url");
            	parameters.add(properties.getProperty("connection-url"));

	            for (PropertyDefinition prop : dsProperties) {
	            	if (prop.getName().equals("connection-properties")) {
	            		continue;
	            	}
	            	String value = properties.getProperty(prop.getName());
	            	if (value != null) {
	                	parameters.add(prop.getName());
	                	parameters.add(value);
	            	}
	            }
            }
            else {
            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70005, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70005));
            }

        	parameters.add("jndi-name");
        	parameters.add(addJavaContext(deploymentName));
        	parameters.add("driver-name");
        	parameters.add(templateName);
        	parameters.add("pool-name");
        	parameters.add(deploymentName);

        	// add data source
			cliCall("add", new String[] { "subsystem", "datasources","data-source", deploymentName },
					parameters.toArray(new String[parameters.size()]),
					new ResultCallback());

	        // add connection properties that are specific to driver
            String cp = properties.getProperty("connection-properties");
            if (cp != null) {
            	StringTokenizer st = new StringTokenizer(cp, ",");
            	while(st.hasMoreTokens()) {
            		String prop = st.nextToken();
            		String key = prop.substring(0, prop.indexOf('='));
            		String value = prop.substring(prop.indexOf('=')+1);
            		addConnectionProperty(deploymentName, key, value);
            	}
            }

	        // issue the "enable" operation
			cliCall("enable", new String[] { "subsystem", "datasources","data-source", deploymentName },
					null, new ResultCallback());
		}

		// /subsystem=datasources/data-source=DS/connection-properties=foo:add(value=/home/rareddy/testing)
		private void addConnectionProperty(String deploymentName, String key, String value) throws AdminException {
			if (value == null || value.trim().isEmpty()) {
				throw new AdminProcessingException(AdminPlugin.Event.TEIID70054, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70054, key));
			}
			cliCall("add", new String[] { "subsystem", "datasources",
					"data-source", deploymentName,
					"connection-properties", key },
					new String[] {"value", value }, new ResultCallback());
		}

		private void execute(final ModelNode request) throws AdminException {
			try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                 throw new AdminProcessingException(AdminPlugin.Event.TEIID70006, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70007, e);
	        }
		}

		@Override
		public Properties getDataSource(String deployedName) throws AdminException {
			deployedName = removeJavaContext(deployedName);

			Collection<String> dsNames = getDataSourceNames();
			if (!dsNames.contains(deployedName)) {
				 throw new AdminProcessingException(AdminPlugin.Event.TEIID70008, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70008, deployedName));
			}

			Properties dsProperties = new Properties();

			// check regular data-source
			cliCall("read-resource",
					new String[] { "subsystem", "datasources", "data-source", deployedName}, null,
					new DataSourceProperties(dsProperties));

			// check xa connections
			if (dsProperties.isEmpty()) {
				cliCall("read-resource",
						new String[] {"subsystem", "datasources", "xa-data-source", deployedName}, null,
						new DataSourceProperties(dsProperties));
			}

			// check connection factories
			if (dsProperties.isEmpty()) {
				Map<String, String> raDSMap = getConnectionFactoryNames();
				// deployed rar name, may be it is == deployedName or if server restarts it will be rar name or rar->[1..n] name
				String rarName = raDSMap.get(deployedName);
				if (rarName != null) {
					cliCall("read-resource",
							new String[] { "subsystem", "resource-adapters", "resource-adapter", rarName, "connection-definitions", deployedName},
							null, new ConnectionFactoryProperties(dsProperties, rarName, deployedName, null));
				}
			}
			return dsProperties;
		}

		private class DataSourceProperties extends ResultCallback {
			private Properties dsProperties;
			DataSourceProperties(Properties props){
				this.dsProperties = props;
			}
			@Override
			public void onSuccess(ModelNode outcome, ModelNode result) throws AdminProcessingException {
	    		List<ModelNode> props = outcome.get("result").asList();
        		for (ModelNode prop:props) {
        			if (prop.getType().equals(ModelType.PROPERTY)) {
        				org.jboss.dmr.Property p = prop.asProperty();
        				ModelType type = p.getValue().getType();
        				if (p.getValue().isDefined() && !type.equals(ModelType.LIST) && !type.equals(ModelType.OBJECT)) {
							if (p.getName().equals("driver-name")
									|| p.getName().equals("jndi-name")
									|| !excludeProperty(p.getName())) {
        						this.dsProperties.setProperty(p.getName(), p.getValue().asString());
        					}
        				}
        			}
        		}
			}
			@Override
			public void onFailure(String msg) throws AdminProcessingException {
			}
		}

		private class ConnectionFactoryProperties extends ResultCallback {
			private Properties dsProperties;
			private String deployedName;
			private String rarName;
			private String configName;

			ConnectionFactoryProperties(Properties props, String rarName, String deployedName, String configName){
				this.dsProperties = props;
				this.rarName = rarName;
				this.deployedName = deployedName;
				this.configName = configName;
			}

			@Override
			public void onSuccess(ModelNode outcome, ModelNode result) throws AdminException {
	    		List<ModelNode> props = outcome.get("result").asList();
        		for (ModelNode prop:props) {
        			if (!prop.getType().equals(ModelType.PROPERTY)) {
        				continue;
        			}
    				org.jboss.dmr.Property p = prop.asProperty();
					if (p.getName().equals("jndi-name")) {
						this.dsProperties.setProperty("jndi-name", p.getValue().asString());
					}
    				if (!p.getValue().isDefined() || excludeProperty(p.getName())) {
    					continue;
    				}
					if (p.getName().equals("archive")) {
						this.dsProperties.setProperty("driver-name", p.getValue().asString());
					}
					if (p.getName().equals("value")) {
						this.dsProperties.setProperty(this.configName, p.getValue().asString());
					}
					else if (p.getName().equals("config-properties")) {
						List<ModelNode> configs = p.getValue().asList();
						for (ModelNode config:configs) {
							if (config.getType().equals(ModelType.PROPERTY)) {
								org.jboss.dmr.Property p1 = config.asProperty();
								//getConnectionFactoryProperties(rarName, dsProps, subsystem[0], subsystem[1], subsystem[2], subsystem[3], );
								cliCall("read-resource",
										new String[] {"subsystem","resource-adapters",
												"resource-adapter",this.rarName,
												"connection-definitions",this.deployedName,
												"config-properties",p1.getName()}, null,
												new ConnectionFactoryProperties(this.dsProperties, this.rarName, this.deployedName, p1.getName()));
							}
						}
					}
					else {
						this.dsProperties.setProperty(p.getName(), p.getValue().asString());
					}
        		}
			}

			@Override
			public void onFailure(String msg) throws AdminProcessingException {
			}
		}

		@Override
		public void deleteDataSource(String deployedName) throws AdminException {
			deployedName = removeJavaContext(deployedName);

			Collection<String> dsNames = getDataSourceNames();
			if (!dsNames.contains(deployedName)) {
				 throw new AdminProcessingException(AdminPlugin.Event.TEIID70008, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70008, deployedName));
			}

			boolean deleted = deleteDS(deployedName,"datasources", "data-source");

			// check xa connections
			if (!deleted) {
				deleted = deleteDS(deployedName,"datasources", "xa-data-source");
			}

			// check connection factories
			if (!deleted) {
				Map<String, String> raDSMap = getConnectionFactoryNames();
				// deployed rar name, may be it is == deployedName or if server restarts it will be rar name or rar->[1..n] name
				String rarName = raDSMap.get(deployedName);
				if (rarName != null) {
					cliCall("remove", new String[] { "subsystem",
							"resource-adapters", "resource-adapter", rarName,
							"connection-definitions", deployedName }, null,
							new ResultCallback());
					
					//AS-4776 HACK - BEGIN
					if (getInstalledResourceAdaptorNames().contains(deployedName)) {
						cliCall("remove", new String[] { "subsystem",
								"resource-adapters", "resource-adapter", deployedName}, null,
								new ResultCallback());
					}
					//AS-4776 HACK - END
				}
			}

			dsNames = getDataSourceNames();
			if (dsNames.contains(deployedName)) {
				throw new AdminProcessingException(AdminPlugin.Event.TEIID70008, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70008, deployedName));
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

		private boolean deleteDS(String deployedName, String... subsystem) throws AdminException {
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;

	        try {
	        	addProfileNode(builder);

	            builder.addNode("subsystem", subsystem[0]); //$NON-NLS-1$ //$NON-NLS-2$
	            builder.addNode(subsystem[1], deployedName);
	            builder.setOperationName("remove");
	            request = builder.buildRequest();
	        } catch (OperationFormatException e) {
	            throw new AdminComponentException(AdminPlugin.Event.TEIID70010, e, "Failed to build operation"); //$NON-NLS-1$
	        }

	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	                return false;
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70009, e);
	        }
	        return true;
		}

		@Override
		public void undeploy(String deployedName) throws AdminException {
			ModelNode request;
			try {
				request = buildUndeployRequest(deployedName, false);
	        } catch (OperationFormatException e) {
	        	throw new AdminComponentException(AdminPlugin.Event.TEIID70010, e, "Failed to build operation"); //$NON-NLS-1$
	        }
			execute(request);
		}

		public void undeploy(String deployedName, boolean force) throws AdminException {
			ModelNode request;
			try {
				request = buildUndeployRequest(deployedName, force);
	        } catch (OperationFormatException e) {
	        	throw new AdminComponentException(AdminPlugin.Event.TEIID70010, e, "Failed to build operation"); //$NON-NLS-1$
	        }
			execute(request);
		}

	    public ModelNode buildUndeployRequest(String name, boolean force) throws OperationFormatException {
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
	        } else if(Util.isDeployedAndEnabledInStandalone(name, this.connection)||force) {
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
			ModelNode request = buildDeployVDBRequest(deployName, vdb, true);
			execute(request);
		}

		public void deploy(String deployName, InputStream vdb, boolean persist)	throws AdminException {
			ModelNode request = buildDeployVDBRequest(deployName, vdb, persist);
			execute(request);
		}

		private ModelNode buildDeployVDBRequest(String fileName, InputStream vdb, boolean persist) throws AdminException {
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
				ModelNode request = builder.buildRequest();
	            if (!persist) {
	            	request.get("persistent").set(false); // prevents writing this deployment out to standalone.xml
	            }
	            request.get("enabled").set(true);
				steps.add(request);

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
					request = builder.buildRequest();
		            if (!persist) {
		            	request.get("persistent").set(false); // prevents writing this deployment out to standalone.xml
		            }
		            request.get("enabled").set(true);
	                steps.add(request);
	            }
	            return composite;
			} catch (OperationFormatException e) {
				throw new AdminComponentException(AdminPlugin.Event.TEIID70010, e, "Failed to build operation"); //$NON-NLS-1$
			} catch (IOException e) {
				 throw new AdminComponentException(AdminPlugin.Event.TEIID70011, e);
			}
		}

		@Override
		public Collection<? extends CacheStatistics> getCacheStats(String cacheType) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "cache-statistics",	"cache-type", cacheType);//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	            	if (this.domainMode) {
	            		return getDomainAwareList(outcome, VDBMetadataMapper.CacheStatisticsMetadataMapper.INSTANCE);
	            	}
	            	if (outcome.hasDefined("result")) {
	            		ModelNode result = outcome.get("result");
	            		return Arrays.asList(VDBMetadataMapper.CacheStatisticsMetadataMapper.INSTANCE.unwrap(result));
	            	}
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70013, e);
	        }
	        return null;
		}

		@Override
		public Collection<? extends EngineStatistics> getEngineStats() throws AdminException {
	        final ModelNode request = buildRequest("teiid", "engine-statistics");//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	            	if (this.domainMode) {
	            		return getDomainAwareList(outcome, VDBMetadataMapper.EngineStatisticsMetadataMapper.INSTANCE);
	            	}
	            	if (outcome.hasDefined("result")) {
	            		ModelNode result = outcome.get("result");
	            		return Arrays.asList(VDBMetadataMapper.EngineStatisticsMetadataMapper.INSTANCE.unwrap(result));
	            	}
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70013, e);
	        }
	        return null;
		}

		@Override
		public Collection<String> getCacheTypes() throws AdminException {
	        final ModelNode request = buildRequest("teiid", "cache-types");//$NON-NLS-1$ //$NON-NLS-2$
	        return new HashSet<String>(executeList(request));
		}

		private Collection<String> executeList(final ModelNode request)	throws AdminException {
			try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	            	return Util.getList(outcome);
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70014, e);
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
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70015, e);
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
			datasourceNames.addAll(getConnectionFactoryNames().keySet());

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

		private Map<String, String> getConnectionFactoryNames() throws AdminException {
			HashMap<String, String> datasourceNames = new HashMap<String, String>();
			Set<String> resourceAdapters = getInstalledResourceAdaptorNames();
			for (String resource:resourceAdapters) {
				getRAConnections(datasourceNames, resource);
			}
			return datasourceNames;
		}

		private void getRAConnections(final HashMap<String, String> datasourceNames, final String rarName) throws AdminException {
			cliCall("read-resource", new String[] {"subsystem", "resource-adapters", "resource-adapter", rarName}, null, new ResultCallback() {
				@Override
				public void onSuccess(ModelNode outcome, ModelNode result) throws AdminProcessingException {
		        	if (result.hasDefined("connection-definitions")) {
		        		List<ModelNode> connDefs = result.get("connection-definitions").asList();
		        		for (ModelNode conn:connDefs) {
		        			Iterator<String> it = conn.keys().iterator();
		        			if (it.hasNext()) {
		        				datasourceNames.put(it.next(), rarName);
		        			}
		        		}
		        	}
		        }
				@Override
				public void onFailure(String msg) throws AdminProcessingException {
					// no-op
				}
			});
		}

		/**
		 * This will get all deployed RAR names
		 * /subsystem=resource-adapters:read-children-names(child-type=resource-adapter)
		 * @return
		 * @throws AdminException
		 */
		private Set<String> getInstalledResourceAdaptorNames() throws AdminException {
			Set<String> templates = new HashSet<String>();
			templates.addAll(getChildNodeNames("resource-adapters", "resource-adapter"));
	        return templates;
		}

		// :read-children-names(child-type=deployment)
		private Set<String> getResourceAdapterNames() throws AdminException {
			Set<String> templates = getDeployedResourceAdaptorNames();
            templates.addAll(getInstalledResourceAdaptorNames());
            
            //AS-4776 HACK - BEGIN
            Map<String, String> connFactoryMap = getConnectionFactoryNames();
            for (String key:connFactoryMap.keySet()) {
            	templates.remove(key);
            }
            //AS-4776 HACK - END
	        return templates;
		}

		private Set<String> getDeployedResourceAdaptorNames() throws AdminException {
			Set<String> templates = new HashSet<String>();
			List<String> deployments = getChildNodeNames(null, "deployment");
            for (String deployment:deployments) {
            	if (deployment.endsWith(".rar")) {
            		templates.add(deployment);
            	}
            }
			return templates;
		}

		public List<String> getDeployments(){
			return Util.getDeployments(this.connection);
		}

		@Override
		public Set<String> getDataSourceTemplateNames() throws AdminException {
			Set<String> templates = new HashSet<String>();
			templates.addAll(getInstalledJDBCDrivers());
			templates.addAll(getResourceAdapterNames());
			return templates;
		}

		@Override
		public Collection<? extends WorkerPoolStatistics> getWorkerPoolStats() throws AdminException {
			final ModelNode request = buildRequest("teiid", "workerpool-statistics");//$NON-NLS-1$
			if (request != null) {
		        try {
		            ModelNode outcome = this.connection.execute(request);
		            if (Util.isSuccess(outcome)) {
		            	if (this.domainMode) {
		            		return getDomainAwareList(outcome, VDBMetadataMapper.WorkerPoolStatisticsMetadataMapper.INSTANCE);
		            	}
		            	if (outcome.hasDefined("result")) {
		            		ModelNode result = outcome.get("result");
		            		return Arrays.asList(VDBMetadataMapper.WorkerPoolStatisticsMetadataMapper.INSTANCE.unwrap(result));
		            	}
		            }
		        } catch (IOException e) {
		        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70020, e);
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
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70021, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70022, e);
	        }
		}

		@Override
		public Collection<? extends Request> getRequests() throws AdminException {
			final ModelNode request = buildRequest("teiid", "list-requests");//$NON-NLS-1$
			if (request != null) {
		        try {
		            ModelNode outcome = this.connection.execute(request);
		            if (Util.isSuccess(outcome)) {
		                return getDomainAwareList(outcome, RequestMetadataMapper.INSTANCE);
		            }

		        } catch (IOException e) {
		        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70023, e);
		        }
			}
	        return Collections.emptyList();
		}

		@Override
		public Collection<? extends Request> getRequestsForSession(String sessionId) throws AdminException {
			final ModelNode request = buildRequest("teiid", "list-requests-per-session", "session", sessionId);//$NON-NLS-1$
			if (request != null) {
		        try {
		            ModelNode outcome = this.connection.execute(request);
		            if (Util.isSuccess(outcome)) {
		                return getDomainAwareList(outcome, RequestMetadataMapper.INSTANCE);
		            }
		        } catch (IOException e) {
		        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70024, e);
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
		                return getDomainAwareList(outcome, SessionMetadataMapper.INSTANCE);
		            }
		        } catch (IOException e) {
		        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70025, e);
		        }
			}
	        return Collections.emptyList();
		}

		/**
		 * /subsystem=resource-adapters/resource-adapter=teiid-connector-ws.rar/connection-definitions=foo:read-resource-description
		 */
		private void buildResourceAdpaterProperties(String rarName, BuildPropertyDefinitions builder) throws AdminException {
			cliCall("read-resource-description", new String[] { "subsystem","resource-adapters",
					"resource-adapter", rarName,
					"connection-definitions", "any" }, null, builder);
		}

		/**
		 * pattern on CLI
		 * /subsystem=datasources/data-source=foo:read-resource-description
		 */
		@Override
		public Collection<PropertyDefinition> getTemplatePropertyDefinitions(String templateName) throws AdminException {

			BuildPropertyDefinitions builder = new BuildPropertyDefinitions();

			// RAR properties
			Set<String> resourceAdapters = getResourceAdapterNames();
        	if (resourceAdapters.contains(templateName)) {
        		cliCall("read-rar-description", new String[] {"subsystem", "teiid"}, new String[] {"rar-name", templateName}, builder);
        		buildResourceAdpaterProperties(templateName, builder);
		        return builder.getPropertyDefinitions();
        	}

        	// get JDBC properties
    		cliCall("read-resource-description", new String[] {"subsystem", "datasources", "data-source", templateName}, null, builder);

	        // add driver specific properties
	        PropertyDefinitionMetadata cp = new PropertyDefinitionMetadata();
	        cp.setName("connection-properties");
	        cp.setDisplayName("Addtional Driver Properties");
	        cp.setDescription("The connection-properties element allows you to pass in arbitrary connection properties to the Driver.connect(url, props) method. Supply comma separated name-value pairs"); //$NON-NLS-1$
	        cp.setRequired(false);
	        cp.setAdvanced(true);
	        ArrayList<PropertyDefinition> props = builder.getPropertyDefinitions();
	        props.add(cp);
	        return props;
		}
		
		@Override
	    public Collection<? extends PropertyDefinition> getTranslatorPropertyDefinitions(String translatorName) throws AdminException{
			BuildPropertyDefinitions builder = new BuildPropertyDefinitions();
			Collection<? extends Translator> translators = getTranslators();
			for (Translator t:translators) {
				if (t.getName().equalsIgnoreCase(translatorName)) {
	        		cliCall("read-translator-properties", new String[] {"subsystem", "teiid"}, new String[] {"translator-name", translatorName}, builder);
	        		return builder.getPropertyDefinitions();
				}
			}
			throw new AdminProcessingException(AdminPlugin.Event.TEIID70055, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70055, translatorName));
	    }
		

		private class BuildPropertyDefinitions extends ResultCallback{
			private ArrayList<PropertyDefinition> propDefinitions = new ArrayList<PropertyDefinition>();

			@Override
			public void onSuccess(ModelNode outcome, ModelNode result) throws AdminProcessingException {
				if (result.getType().equals(ModelType.LIST)) {
					buildPropertyDefinitions(result.asList());
				}
				else if (result.get("attributes").isDefined()) {
						buildPropertyDefinitions(result.get("attributes").asList());
				}
			}

			@Override
			public void onFailure(String msg) throws AdminProcessingException {
				throw new AdminProcessingException(AdminPlugin.Event.TEIID70026, msg);
			}

			ArrayList<PropertyDefinition> getPropertyDefinitions() {
				return this.propDefinitions;
			}

			private void buildPropertyDefinitions(List<ModelNode> propsNodes) {
	        	for (ModelNode node:propsNodes) {
	        		PropertyDefinitionMetadata def = new PropertyDefinitionMetadata();
	        		Set<String> keys = node.keys();

	        		String name = keys.iterator().next();
	        		if (excludeProperty(name)) {
	        			continue;
	        		}
	        		def.setName(name);
	        		node = node.get(name);

	        		if (node.hasDefined("display")) {
	        			def.setDisplayName(node.get("display").asString());
	        		}
	        		else {
	        			def.setDisplayName(name);
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
	        			def.setModifiable(!Boolean.parseBoolean(access));
	        		}

	        		if (node.hasDefined("access-type")) {
	        			String access = node.get("access-type").asString();
	        			if ("read-write".equals(access)) {
	        				def.setModifiable(true);
	        			}
	        			else {
	        				def.setModifiable(false);
	        			}
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
	        		this.propDefinitions.add(def);
	        	}
			}
		}

		private boolean excludeProperty(String name) {
			String[] names = { "jndi-name",
					"pool-name",
					"driver-name",
					"reauth-plugin-class-name", "enabled",
					"valid-connection-checker-class-name",
					"valid-connection-checker-properties",
					"stale-connection-checker-class-name",
					"stale-connection-checker-properties",
					"exception-sorter-class-name",
					"exception-sorter-properties",
					"use-try-lock",
					"allocation-retry",
					"allocation-retry-wait-millis",
					"jta",
					"use-java-context",
					"url-selector-strategy-class-name",
					"driver-class",
					"datasource-class",
					"use-ccm"};
			for (String n:names) {
				if (n.equalsIgnoreCase(name)) {
					return true;
				}
			}
			return false;
		}

		@Override
		public Collection<? extends Transaction> getTransactions() throws AdminException {
			final ModelNode request = buildRequest("teiid", "list-transactions");//$NON-NLS-1$
			if (request != null) {
		        try {
		            ModelNode outcome = this.connection.execute(request);
		            if (Util.isSuccess(outcome)) {
		                return getDomainAwareList(outcome, TransactionMetadataMapper.INSTANCE);
		            }
		        } catch (IOException e) {
		        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70028, e);
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
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70029, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70030, e);
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
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70031, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70032, e);
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
	            		if (this.domainMode) {
		            		List<VDBTranslatorMetaData> list = getDomainAwareList(outcome, VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE);
		            		if (list != null && !list.isEmpty()) {
		            			return list.get(0);
		            		}
	            		}
	            		else {
		            		ModelNode result = outcome.get("result");
		            		return VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE.unwrap(result);
	            		}
	            	}
	            }

	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70033, e);
	        }
			return null;
		}

		@Override
		public Collection<? extends Translator> getTranslators() throws AdminException {
	        final ModelNode request = buildRequest("teiid", "list-translators");//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	                return getDomainAwareList(outcome, VDBMetadataMapper.VDBTranslatorMetaDataMapper.INSTANCE);
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70034, e);
	        }

	        return Collections.emptyList();
		}
		private ModelNode buildRequest(String subsystem, String operationName, String... params) throws AdminException {
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;
	        try {
	        	if (subsystem != null) {
		        	addProfileNode(builder);
		            builder.addNode("subsystem", subsystem); //$NON-NLS-1$ //$NON-NLS-2$
	        	}
	            builder.setOperationName(operationName);
	            request = builder.buildRequest();
	            if (params != null && params.length % 2 == 0) {
	            	for (int i = 0; i < params.length; i+=2) {
	            		builder.addProperty(params[i], params[i+1]);
	            	}
	            }
	        } catch (OperationFormatException e) {
	        	throw new AdminComponentException(AdminPlugin.Event.TEIID70010, e, "Failed to build operation"); //$NON-NLS-1$
	        }
			return request;
		}

		private void cliCall(String operationName, String[] address, String[] params, ResultCallback callback) throws AdminException {
			DefaultOperationRequestBuilder builder = new DefaultOperationRequestBuilder();
	        final ModelNode request;
	        try {
	        	if (address.length % 2 != 0) {
	        		throw new IllegalArgumentException("Failed to build operation"); //$NON-NLS-1$
	        	}
	        	addProfileNode(builder);
	        	for (int i = 0; i < address.length; i+=2) {
		            builder.addNode(address[i], address[i+1]); //$NON-NLS-1$ //$NON-NLS-2$
	        	}
	            builder.setOperationName(operationName);
	            request = builder.buildRequest();
	            if (params != null && params.length % 2 == 0) {
	            	for (int i = 0; i < params.length; i+=2) {
	            		builder.addProperty(params[i], params[i+1]);
	            	}
	            }
	            ModelNode outcome = this.connection.execute(request);
	            ModelNode result = null;
	            if (Util.isSuccess(outcome)) {
			    	if (outcome.hasDefined("result")) {
			    		result = outcome.get("result");
			    	}
	                callback.onSuccess(outcome, result);
	            }
	            else {
	            	callback.onFailure(Util.getFailureDescription(outcome));
	            }
	        } catch (OperationFormatException e) {
	        	throw new AdminComponentException(AdminPlugin.Event.TEIID70010, e, "Failed to build operation"); //$NON-NLS-1$
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70007, e);
	        }
		}

		private <T> List<T> getDomainAwareList(ModelNode operationResult,  MetadataMapper<T> mapper) {
	    	if (this.domainMode) {
	    		List<T> returnList = new ArrayList<T>();

	    		ModelNode serverGroups = operationResult.get("server-groups");
	    		Set<String> serverGroupNames = serverGroups.keys();
	    		for (String serverGroupName:serverGroupNames) {
	    			ModelNode serverGroup = serverGroups.get(serverGroupName);
	    			ModelNode hostGroups = serverGroup.get("host");
	    			Set<String> hostKeys = hostGroups.keys();
	    			for(String hostName:hostKeys) {
	    				ModelNode hostGroup = hostGroups.get(hostName);
	    	  			Set<String> serverNames = hostGroup.keys();
		    			for (String serverName:serverNames) {
		    				ModelNode server = hostGroup.get(serverName);
		    				if (server.get("response", "outcome").asString().equals(Util.SUCCESS)) {
		    					ModelNode result = server.get("response", "result");
		    					if (result.isDefined()) {
		    				        List<ModelNode> nodeList = result.asList(); //$NON-NLS-1$
		    				        for(ModelNode node : nodeList) {
		    				        	T anObj = mapper.unwrap(node);
		    				        	if (anObj instanceof DomainAware) {
		    				        		((AdminObjectImpl)anObj).setServerGroup(serverGroupName);
		    				        		((AdminObjectImpl)anObj).setServerName(serverName);
		    				        		((AdminObjectImpl)anObj).setHostName(hostName);
		    				        	}
		    				        	returnList.add(anObj);
		    				        }

		    					}
		    				}
		    			}
	    			}
	    		}
	    		return returnList;
	    	}
	    	return getList(operationResult, mapper);
		}

	    private <T> List<T> getList(ModelNode operationResult,  MetadataMapper<T> mapper) {
	        if(!operationResult.hasDefined("result")) {
				return Collections.emptyList();
			}

	        List<ModelNode> nodeList = operationResult.get("result").asList(); //$NON-NLS-1$
	        if(nodeList.isEmpty()) {
				return Collections.emptyList();
			}

	        List<T> list = new ArrayList<T>(nodeList.size());
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
	            	if (this.domainMode) {
		            	List<VDBMetaData> list = getDomainAwareList(outcome, VDBMetadataMapper.INSTANCE);
		            	if (list != null && !list.isEmpty()) {
		            		return list.get(0);
		            	}
	            	}
	            	else {
		            	if (outcome.hasDefined("result")) {
		            		ModelNode result = outcome.get("result");
		            		return VDBMetadataMapper.INSTANCE.unwrap(result);
		            	}
	            	}
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70035, e);
	        }
			return null;
		}

		@Override
		public List<? extends VDB> getVDBs() throws AdminException {
	        final ModelNode request = buildRequest("teiid", "list-vdbs");//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (Util.isSuccess(outcome)) {
	                return getDomainAwareList(outcome, VDBMetadataMapper.INSTANCE);
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70036, e);
	        }

	        return Collections.emptyList();
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
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70039, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70040, e);
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
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70041, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70042, e);
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
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70043, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70044, e);
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
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70045, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70046, e);
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
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70047, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70048, e);
	        }
		}
		
		@Override
		public void updateSource(String vdbName, int vdbVersion, String sourceName, String translatorName,
				String dsName) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "update-source",
	        		"vdb-name", vdbName,
	        		"vdb-version", String.valueOf(vdbVersion),
	        		"source-name", sourceName,
	        		"translator-name", translatorName,
	        		"ds-name", dsName);//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70047, Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70048, e);
	        }
		}
		
		@Override
		public void addSource(String vdbName, int vdbVersion, String modelName, String sourceName, String translatorName,
				String dsName) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "add-source",
	        		"vdb-name", vdbName,
	        		"vdb-version", String.valueOf(vdbVersion),
	        		"model-name", modelName,
	        		"source-name", sourceName,
	        		"translator-name", translatorName,
	        		"ds-name", dsName);//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70047, Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70048, e);
	        }
		}
		
		@Override
		public void removeSource(String vdbName, int vdbVersion, String modelName, String sourceName) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "remove-source",
	        		"vdb-name", vdbName,
	        		"vdb-version", String.valueOf(vdbVersion),
	        		"model-name", modelName,
	        		"source-name", sourceName);//$NON-NLS-1$ //$NON-NLS-2$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70047, Util.getFailureDescription(outcome));
	            }
	        } catch (Exception e) {
	        	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70048, e);
	        }
		}

	    @Override
	    public void markDataSourceAvailable(String jndiName) throws AdminException {
	        final ModelNode request = buildRequest("teiid", "mark-datasource-available","ds-name", jndiName);//$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70049, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70050, e);
	        }
	    }

		@Override
		public void restartVDB(String vdbName, int vdbVersion, String... models) throws AdminException {
			ModelNode request = null;
			String modelNames = null;

			if (models != null && models.length > 0) {
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < models.length-1; i++) {
					sb.append(models[i]).append(",");
				}
				sb.append(models[models.length-1]);
				modelNames = sb.toString();
			}

			if (modelNames != null) {
				request = buildRequest("teiid", "restart-vdb",
		        		"vdb-name", vdbName,
		        		"vdb-version", String.valueOf(vdbVersion),
		        		"model-names", modelNames);//$NON-NLS-1$ //$NON-NLS-2$
			}
			else {
				request = buildRequest("teiid", "restart-vdb",
		        		"vdb-name", vdbName,
		        		"vdb-version", String.valueOf(vdbVersion));//$NON-NLS-1$
			}

	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70045, Util.getFailureDescription(outcome));
	            }
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70046, e);
	        }
		}

		@Override
		public String getSchema(String vdbName, int vdbVersion,
				String modelName, EnumSet<SchemaObjectType> allowedTypes,
				String typeNamePattern) throws AdminException {
			ModelNode request = null;

			ArrayList<String> params = new ArrayList<String>();
			params.add("vdb-name");
			params.add(vdbName);
			params.add("vdb-version");
			params.add(String.valueOf(vdbVersion));
			params.add("model-name");
			params.add(modelName);

			if (allowedTypes != null) {
				params.add("entity-type");
				StringBuilder sb = new StringBuilder();
				for (SchemaObjectType type:allowedTypes) {
					if (sb.length() > 0) {
						sb.append(",");
					}
					sb.append(type.name());
				}
				params.add(sb.toString());
			}

			if (typeNamePattern != null) {
				params.add("entity-pattern");
				params.add(typeNamePattern);
			}

			request = buildRequest("teiid", "get-schema", params.toArray(new String[params.size()]));//$NON-NLS-1$ //$NON-NLS-2$

	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70045, Util.getFailureDescription(outcome));
	            }
	            return outcome.get(RESULT).asString();
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70046, e);
	        }
		}

		@Override
		public String getQueryPlan(String sessionId, int executionId)  throws AdminException {
			final ModelNode request = buildRequest("teiid", "get-plan", "session", sessionId, "execution-id", String.valueOf(executionId));//$NON-NLS-1$
			if (request == null) {
				return null;
			}
	        try {
	            ModelNode outcome = this.connection.execute(request);
	            if (!Util.isSuccess(outcome)) {
	            	 throw new AdminProcessingException(AdminPlugin.Event.TEIID70021, Util.getFailureDescription(outcome));
	            }
	            return outcome.get(RESULT).asString();
	        } catch (IOException e) {
	        	 throw new AdminComponentException(AdminPlugin.Event.TEIID70022, e);
	        }
		}

		@Override
		public void restart() {
			try {
				cliCall("reload", new String[] {}, new String[] {}, new ResultCallback());
			} catch (AdminException e) {
				//ignore
			}
		}
    }
}
