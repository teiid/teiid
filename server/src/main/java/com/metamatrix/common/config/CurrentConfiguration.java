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

package com.metamatrix.common.config;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ComponentType;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ResourceModel;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicHost;
import com.metamatrix.common.config.reader.CurrentConfigurationReader;
import com.metamatrix.common.config.reader.PropertiesConfigurationReader;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.NetUtils;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ReflectionHelper;

/**
 * <p>
 * This class represents a single and universal framework for accessing
 * the current configuration for an application or server VM.  The current
 * configuration contains the specification of runtime properties and
 * component deployment information.  Depending upon the bootstrap information,
 * this framework may access the configuration information from a central
 * repository or simply from a local file.
 * </p>
 * <p>
 * This framework can be configured to periodically refresh the cached
 * configuation information, and a separate thread is used to do this in
 * the background.  As such, the static <code>shutdown()</code> method
 * should be called by applications when exiting.  This method will block until
 * this framework has successfully shutdown and cleaned up all resources,
 * and will return gracefully if the method were already called.
 * Note, however, that any call to this framework to obtain configuration
 * information will restart the framework, requiring another eventual
 * shutdown.
 * </p>
 */
public final class CurrentConfiguration {
	/* 
	 * This property enables the overriding of the default {@link BOOTSTRAP_FILE_NAME}
	 */
	public static final String BOOTSTRAP_FILE_PROPERTY_OVERRIDE = "teiid.bootstrap.file"; //$NON-NLS-1$
	
    public static final String BOOTSTRAP_FILE_NAME = "teiid.properties"; //$NON-NLS-1$
    public static final String CONFIGURATION_READER_CLASS_PROPERTY_NAME = "metamatrix.config.reader"; //$NON-NLS-1$
    public static final String CLUSTER_NAME = "cluster.name"; //$NON-NLS-1$
    public static final String CONFIGURATION_NAME= "configuration.name"; //$NON-NLS-1$
    public static final String CLUSTER_MEMBERS = "cluster.unicast.members"; //$NON-NLS-1$
    
	private CurrentConfigurationReader reader;
    private Properties bootstrapProperties;
    private Properties modifyableBootstrapProperties;
    private Properties systemBootstrapProperties;
	private String bindAddress;
	private InetAddress hostAddress;
	private String configurationName;
	private String processName;
    
    private static CurrentConfiguration INSTANCE = new CurrentConfiguration();
    
    public static CurrentConfiguration getInstance() {
    	return INSTANCE;
    }

    /**
     * Private constructor that prevents instantiation.
     */
    private CurrentConfiguration() {
    	setHostProperties(null);
    }
    
    private void setHostProperties(String bind) {
    	Host host = getDefaultHost();
    	this.configurationName = host.getFullName();
    	String hostName = host.getHostAddress();
    	if (bind == null) {
    		bind = host.getBindAddress();
    	}
    	
    	boolean bindAddressDefined = (bind != null && bind.length() > 0);
    	boolean hostNameDefined = (hostName != null && hostName.length() > 0);

    	try {
	    	if (hostNameDefined) {
				this.hostAddress = NetUtils.resolveHostByName(hostName);
			}
	    	    	
	    	if (bindAddressDefined) {
	    		this.bindAddress = bind;
	    		
	    		if (!hostNameDefined) { 
	    			this.hostAddress = InetAddress.getByName(bindAddress);
	    		}
	    	}
	    	else {
	    		if (!hostNameDefined) {
		    		this.hostAddress = NetUtils.getInstance().getInetAddress();
		    	}
	    		this.bindAddress = this.hostAddress.getHostAddress();
	    	}
    	} catch (UnknownHostException e) {
    		throw new RuntimeException(e);
    	}
    	
    	// these properties will be used to identify the server in TCP based cluster setup.
    	String unicastMembers = bootstrapProperties.getProperty(CLUSTER_MEMBERS);
    	if (unicastMembers == null) {
    		unicastMembers = this.configurationName+"|"+this.hostAddress.getCanonicalHostName(); //$NON-NLS-1$
    	}
    	else {
    		unicastMembers = unicastMembers+","+this.configurationName+"|"+this.hostAddress.getCanonicalHostName(); //$NON-NLS-1$  //$NON-NLS-2$
    	}
    	
    	modifyableBootstrapProperties.setProperty(CLUSTER_MEMBERS, unicastMembers);
    }
    
    public boolean isAvailable() {
    	return this.reader != null;
    }
    
    /**
     * Return the stringified representation of this application information object.
     * @return the string form of this object; never null
     */
    public String getHostInfo() {
        StringBuffer sb = new StringBuffer("Host Information"); //$NON-NLS-1$ 
        sb.append('\n');
        sb.append(" VM Name:               " + processName ); //$NON-NLS-1$
        sb.append('\n');
        sb.append(" Hostname:              " + hostAddress.getCanonicalHostName() ); //$NON-NLS-1$
        sb.append('\n');
        sb.append(" Version:               ").append(ApplicationInfo.getInstance().getReleaseNumber()); //$NON-NLS-1$
        sb.append('\n');
        sb.append(" Build Date:            ").append(ApplicationInfo.getInstance().getBuildDate()); //$NON-NLS-1$
        return sb.toString();
    }
    
    public void setProcessName(String name) {
		VMComponentDefn deployedVM;
		try {
			deployedVM = getConfiguration().getVMForHost(getDefaultHost().getName(), name);
		} catch (ConfigurationException e) {
			throw new MetaMatrixRuntimeException(e);
		}
		
		if (deployedVM == null) {
			throw new MetaMatrixRuntimeException(CommonPlugin.Util.getString("CurrentConfiguration.unknown_process", name)); //$NON-NLS-1$ 
		}
    	this.processName = name;
    	setHostProperties(deployedVM.getBindAddress());
    }
    
    public String getBindAddress() {
		return bindAddress;
	}
    
    public InetAddress getHostAddress() {
		return hostAddress;
	}
    
    public String getConfigurationName() {
		return configurationName;
	}
    
    public String getProcessName() {
		return processName;
	}
    
    public String getClusterName() throws ConfigurationException {
        Properties props = getResourceProperties(ResourceNames.JGROUPS);
        return props.getProperty(CLUSTER_NAME, "Teiid-Cluster"); //$NON-NLS-1$
    }

    /**
     * Get all of the configuration properties.  The properties
     * that are returned have default properties that are the bootstrap properties.
     * @return the immutable properties; never null
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
    public Properties getProperties() {
		try {
			ConfigurationModelContainer cmc = getReader().getConfigurationModel();
			ComponentTypeID id = cmc.getConfiguration().getComponentTypeID();
			Properties result = new Properties(getBootStrapProperties());
			PropertiesUtils.putAll(result, cmc.getDefaultPropertyValues(id));
			PropertiesUtils.putAll(result, cmc.getConfiguration().getProperties());
	        return new UnmodifiableProperties(result);
		} catch (ConfigurationException e) {
			throw new MetaMatrixRuntimeException(e);
		}
    }

    /**
     * Get the connection properties for the specified resource.  The resourse name
     * is dependent on the type of resource.  For services, excluding connectors,
     * the resource name will be the component type name {@link com.metamatrix.common.config.api.ComponentType}.
     * For other types, they will generally have a predefined static variable
     * called RESOURCE_NAME that will be used to ask for its properties.
     * @param resourceName is the name of the resource to obtain properties for.
     * @return the immutable properties; never null
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
    public Properties getResourceProperties(String resourceName) throws ConfigurationException {
        Properties result;

        SharedResource sr = getReader().getConfigurationModel().getResource(resourceName);

        Properties props;
        if (sr != null) {
            props = ResourceModel.getDefaultProperties(resourceName);
            props.putAll(sr.getProperties());
        } else {
            props = new Properties();

        }

        // allow the system properties to override
        PropertiesUtils.setOverrideProperies(props, getSystemBootStrapProperties());

        result =  new UnmodifiableProperties(props);

        return result;
    }
    
    /**
     * Get the current configuration that is to be used for deployment.
     * @return the Configuration used for deployment
     * @throws ConfigurationException if the current configuration could not be obtained
     */
    public Configuration getConfiguration() throws ConfigurationException {
       Configuration config = getReader().getConfigurationModel().getConfiguration();
       if ( config == null ) {
    	   throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0021, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0021));
       }
       return config;
    }


    /**
     * Get the current configuration that is to be used for deployment.
     * @return the Configuration used for deployment
     * @throws ConfigurationException if the current configuration could not be obtained
     */
    public ConfigurationModelContainer getConfigurationModel() throws ConfigurationException {
        ConfigurationModelContainer config = null;
        try {
            config = getReader().getConfigurationModel();
        } catch (UnsupportedOperationException e) {
        }

        if (config == null) {
            throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0022,
                                             CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0022));
        }

        return config;
    }
    
    /**
     * Returns a <code>Collection</code> of type <code>ComponentType</code> that represent
     * all the ComponentTypes defined.
     * @param includeDeprecated true if class names that have been deprecated should be
     *    included in the returned list, or false if only non-deprecated constants should be returned.
     * @return List of type <code>ComponentType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see com.metamatrix.common.api.ComponentType
     */
    public Collection<ComponentType> getComponentTypes(boolean includeDeprecated) throws ConfigurationException {
        return getReader().getConfigurationModel().getComponentTypes().values();
    }
    
     
    /**
     * Returns the Host based on the current running machine. 
     * @return the full Host object
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     */
    public Host getDefaultHost() {
    	String name = getBootStrapProperties().getProperty(CONFIGURATION_NAME, "embedded"); //$NON-NLS-1$
    	BasicHost host = new BasicHost(new ConfigurationID(name), new HostID(name), Host.HOST_COMPONENT_TYPE_ID);
        
    	Properties props = new Properties();
    	props.setProperty(org.teiid.adminapi.Host.INSTALL_DIR, getBootStrapProperties().getProperty(org.teiid.adminapi.Host.INSTALL_DIR, System.getProperty("user.dir"))); //$NON-NLS-1$
    	props.setProperty(org.teiid.adminapi.Host.HOST_DIRECTORY, getBootStrapProperties().getProperty(org.teiid.adminapi.Host.HOST_DIRECTORY, System.getProperty("user.dir"))); //$NON-NLS-1$
    	props.setProperty(org.teiid.adminapi.Host.LOG_DIRECTORY, getBootStrapProperties().getProperty(org.teiid.adminapi.Host.LOG_DIRECTORY, System.getProperty("user.dir"))); //$NON-NLS-1$
    	props.setProperty(org.teiid.adminapi.Host.HOST_BIND_ADDRESS, getBootStrapProperties().getProperty(org.teiid.adminapi.Host.HOST_BIND_ADDRESS, "")); //$NON-NLS-1$
    	props.setProperty(org.teiid.adminapi.Host.HOST_PHYSICAL_ADDRESS, getBootStrapProperties().getProperty(org.teiid.adminapi.Host.HOST_PHYSICAL_ADDRESS, "")); //$NON-NLS-1$
    	
    	host.setProperties(props);
        return host;
    }  

    
    /**
     * Reset causes not just a refresh, but the bootstrapping process
     * to occur again.
     */
    public static void reset() {
    	INSTANCE = new CurrentConfiguration();
    }
    
    public synchronized Properties getBootStrapProperties() {
		if (bootstrapProperties == null) {
			Properties systemBootStrapProps = getSystemBootStrapProperties();
			Properties bootstrapProps = new Properties(systemBootStrapProps);
	        InputStream bootstrapPropStream = null;
	        
	        String bootstrapfile = systemBootStrapProps.getProperty(BOOTSTRAP_FILE_PROPERTY_OVERRIDE, BOOTSTRAP_FILE_NAME);
	        try {
	        	bootstrapPropStream = this.getClass().getClassLoader().getResourceAsStream(bootstrapfile);
	        	if (bootstrapPropStream != null) {
	        		bootstrapProps.load(bootstrapPropStream);
	        	}
	        } catch (IOException e) {
        		throw new MetaMatrixRuntimeException(ErrorMessageKeys.CONFIG_ERR_0069, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0069, BOOTSTRAP_FILE_NAME));
	        } finally {
		        try {
		        	if (bootstrapPropStream != null) {
		        		bootstrapPropStream.close();
		        	}
		        } catch (IOException e ) {
		        }
	        }
	        modifyableBootstrapProperties = bootstrapProps;
			bootstrapProperties = new UnmodifiableProperties(bootstrapProps);
		}
		return bootstrapProperties;
	}
    
    public synchronized final Properties getSystemBootStrapProperties() {
        if (systemBootstrapProperties == null) {
            systemBootstrapProperties = new UnmodifiableProperties(System.getProperties());
        }
        return systemBootstrapProperties;
    }

    synchronized CurrentConfigurationReader getReader() throws ConfigurationException {
    	if (reader == null) {
            // Get the default bootstrap properties from the System properties ...
			Properties bootstrap = getBootStrapProperties();
			
            String readerClassName = bootstrap.getProperty( CONFIGURATION_READER_CLASS_PROPERTY_NAME, PropertiesConfigurationReader.class.getName() );

        	try {
        		reader = (CurrentConfigurationReader)ReflectionHelper.create(readerClassName, null, Thread.currentThread().getContextClassLoader());
        	} catch (Exception e) {
        		throw new ConfigurationException(e);
			}
    	}

    	return reader;
    }
}
