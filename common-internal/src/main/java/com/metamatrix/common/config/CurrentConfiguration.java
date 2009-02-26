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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ResourceModel;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicHost;
import com.metamatrix.common.config.reader.CurrentConfigurationReader;
import com.metamatrix.common.config.reader.PropertiesConfigurationReader;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;

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
 *
 * NOTE: This class should NOT use LogManager because it can cause recursive behavior.
 */
public final class CurrentConfiguration {

    public static final String BOOTSTRAP_FILE_NAME = "metamatrix.properties"; //$NON-NLS-1$
    public static final String CONFIGURATION_READER_CLASS_PROPERTY_NAME = "metamatrix.config.reader"; //$NON-NLS-1$
    public static final String CLUSTER_NAME = "metamatrix.cluster.name"; //$NON-NLS-1$
    public static final String CONFIGURATION_NAME= "configuration.name"; //$NON-NLS-1$
    
	private CurrentConfigurationReader reader;
    private Properties bootstrapProperties;
    private Properties systemBootstrapProperties;
    
    private static CurrentConfiguration INSTANCE = new CurrentConfiguration();
    
    public static CurrentConfiguration getInstance() {
    	return INSTANCE;
    }

    /**
     * Private constructor that prevents instantiation.
     */
    private CurrentConfiguration() {
    }
    
    public String getClusterName() throws ConfigurationException {
        Properties props = getResourceProperties(ResourceNames.JGROUPS);
        return props.getProperty(CLUSTER_NAME, "Federate-Cluster"); //$NON-NLS-1$
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
			Properties result = getReader().getConfigurationModel().getConfiguration().getProperties();
	        Properties copyResult = PropertiesUtils.clone(result,getBootStrapProperties(),false,true);
	        if ( !(copyResult instanceof UnmodifiableProperties) ) {
	        	copyResult = new UnmodifiableProperties(copyResult);
	        }
	        return copyResult;
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
    public Collection getComponentTypes(boolean includeDeprecated) throws ConfigurationException {
        return getReader().getConfigurationModel().getComponentTypes().values();
    }
    
    /**
     * Returns a <code>Collection</code> of type <code>ProductType</code> that represents
     * all the ComponentTypes defined.
     * @return List of type <code>ProductType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ProductType
     */
    public Collection getProductTypes() throws ConfigurationException {
        return getReader().getConfigurationModel().getProductTypes();
    }
      
    /**
     * Returns the Host based on the current running machine. 
     * @return the full Host object
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     */
    public Host getDefaultHost() throws ConfigurationException {
    	String name = getBootStrapProperties().getProperty(CONFIGURATION_NAME);
    	BasicHost host = new BasicHost(new ConfigurationID(name), new HostID(name), Host.HOST_COMPONENT_TYPE_ID);
        
    	Properties props = new Properties();
    	props.setProperty(com.metamatrix.admin.api.objects.Host.INSTALL_DIR, getBootStrapProperties().getProperty(com.metamatrix.admin.api.objects.Host.INSTALL_DIR, "")); //$NON-NLS-1$
    	props.setProperty(com.metamatrix.admin.api.objects.Host.HOST_DIRECTORY, getBootStrapProperties().getProperty(com.metamatrix.admin.api.objects.Host.HOST_DIRECTORY, "")); //$NON-NLS-1$
    	props.setProperty(com.metamatrix.admin.api.objects.Host.LOG_DIRECTORY, getBootStrapProperties().getProperty(com.metamatrix.admin.api.objects.Host.LOG_DIRECTORY, "")); //$NON-NLS-1$
    	props.setProperty(com.metamatrix.admin.api.objects.Host.HOST_BIND_ADDRESS, getBootStrapProperties().getProperty(com.metamatrix.admin.api.objects.Host.HOST_BIND_ADDRESS, "")); //$NON-NLS-1$
    	props.setProperty(com.metamatrix.admin.api.objects.Host.HOST_PHYSICAL_ADDRESS, getBootStrapProperties().getProperty(com.metamatrix.admin.api.objects.Host.HOST_PHYSICAL_ADDRESS, "")); //$NON-NLS-1$
    	
    	host.setProperties(props);
        return host;
    }  

    
    /**
     * Reset causes not just a refresh, but the bootstrapping process
     * to occur again.
     */
    public synchronized final void reset() throws ConfigurationException {
        reader = null;
        bootstrapProperties = null;
        systemBootstrapProperties = null;
    }

    /**
     * This method should be called <i>only</i> by
     * {@link StartupStateController}, which is used by
     * MetaMatrixController to initialize the system configurations during bootstrapping.
     * Once bootstrap properties are verified, this method will use
     * the {@link #reader} to attempt to put the system state into
     * {@link StartupStateController#STATE_STARTING}, and then
     * commence with initialization.  If the state is already
     * {@link StartupStateController#STATE_STARTING}, then another
     * MetaMatrixController is already currently in the process of
     * starting the system, and a {@link StartupStateException}
     * will be thrown.  If this method returns without an
     * exception, then the system state will be in state
     * {@link StartupStateController#STATE_STARTING}, and the calling
     * code should proceed with startup.


     * @param forceInitialization if the system is in a state other than
     * {@link StartupStateController#STATE_STOPPED}, and the
     * administrator thinks the system actually crashed and is
     * not really running, he can choose to force the
     * initialization.  Otherwise, if the system is in one of these states,
     * an exception will be thrown.  This method is package-level so
     * that only StartupStateController can access it.
     * @throws StartupStateException if the system is
     * not in a state in which initialization can proceed.  This
     * exception will indicate the current system state.
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
//JBoss fix - method made public to get around a AccessDenied problem with Jboss30
    public final void performSystemInitialization(boolean forceInitialization) throws StartupStateException, ConfigurationException {

        // this only initializes the persistence of the configuration
        // (i.e., copying NEXTSTARTUP to OPERATIONAL, etc)
    	getReader().performSystemInitialization(forceInitialization);

		// perform reset to reload configuration information
        reset();
    }

    /**
     * This will put the system into a state of {@link #STATE_STOPPED}.
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
//JBoss fix - method made public to get around a AccessDenied problem with Jboss30
    public final void indicateSystemShutdown() throws ConfigurationException {
    	getReader().indicateSystemShutdown();
    }

	public synchronized Properties getBootStrapProperties() throws ConfigurationException {
		if (bootstrapProperties == null) {
			Properties systemBootStrapProps = getSystemBootStrapProperties();
			Properties bootstrapProps = new Properties(systemBootStrapProps);
	        InputStream bootstrapPropStream = null;
	        try {
	        	bootstrapPropStream = this.getClass().getClassLoader().getResourceAsStream(BOOTSTRAP_FILE_NAME);
	        	if (bootstrapPropStream != null) {
	        		bootstrapProps.load(bootstrapPropStream);
	        	}
	        } catch (IOException e) {
        		throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0069, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0069, BOOTSTRAP_FILE_NAME));
	        } finally {
		        try {
		        	if (bootstrapPropStream != null) {
		        		bootstrapPropStream.close();
		        	}
		        } catch (IOException e ) {
		        }
	        }
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

    /**
     * Returns the instance of <code>CofigurationBootMgr</code> to use to
     * get configuration information.
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
    public final void verifyBootstrapProperties() throws ConfigurationException {
    	getReader();
    }
    	
    synchronized CurrentConfigurationReader getReader() throws ConfigurationException {
    	if (reader == null) {
            // Get the default bootstrap properties from the System properties ...
			Properties bootstrap = getBootStrapProperties();
			
            String readerClassName = bootstrap.getProperty( CONFIGURATION_READER_CLASS_PROPERTY_NAME, PropertiesConfigurationReader.class.getName() );

        	CurrentConfigurationReader tempReader;
        	try {
                Class readerClass = Class.forName(readerClassName);
                tempReader = (CurrentConfigurationReader) readerClass.newInstance();
        	} catch (Exception e) {
        		throw new ConfigurationException(e);
			}
        	tempReader.connect(bootstrap);
            reader = tempReader;
    	}

    	return reader;
    }
}
