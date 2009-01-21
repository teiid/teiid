/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.ResourceModel;
import com.metamatrix.common.config.api.SharedResource;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.bootstrap.CurrentConfigBootstrap;
import com.metamatrix.common.config.bootstrap.PropertyFileCurrentConfigBootstrap;
import com.metamatrix.common.config.bootstrap.SystemCurrentConfigBootstrap;
import com.metamatrix.common.config.reader.CurrentConfigurationReader;
import com.metamatrix.common.config.reader.SystemCurrentConfigurationReader;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.common.util.NetUtils;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.core.util.StringUtil;

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

    private static final String DOT_STR = "."; //$NON-NLS-1$
    
	private static CurrentConfigurationReader READER = null;
    private static Properties BOOTSTRAP_PROPERTIES=null;
    
    private static Properties SYSTEM_BOOTSTRAP_PROPERTIES=null;
    private static Properties STARTUP_PROPERTIES= null;

    private static boolean SHUTDOWN_REQUESTED = false;
    private static boolean performedBootStrap = false;
    
    /**
     * Private constructor that prevents instantiation.
     */
    private CurrentConfiguration() {
    }
    
    public static String getSystemName() throws ConfigurationException {
        
        return getConfigurationModel().getSystemName();
    }

    /**
     * Get the value for the property with the specified name.  First, this method obtains the
     * value from the property in the configuration.  If no such property exists,
     * then this method obtains the value from the property in the bootstrap
     * properties.  If no such method exists, this method returns null.
     * @param name the name of the property
     * @return the value for the property; null if there is
     * no property with the specified name or if the configuration
     * bootstrap information could not be accessed.
     */
    public static String getProperty( String name, boolean forceRefresh) throws ConfigurationException {
        if ( name == null ) {
            return null;
        }

        if (!forceRefresh) {
            return getProperty(name);
        }
        
        check();
        

        return READER.getComponentPropertyValue(Configuration.NEXT_STARTUP_ID, name);

    }   

    
    /**
     * Get all of the startup configuration properties.  The properties
     * @param forceRefresh forces the refreshing of the cache from the datasource.
     * @return the immutable properties; never null
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
    public synchronized static Properties getStartupProperties() throws ConfigurationException {
        check();
        Properties result = READER.getStartupConfigurationProperties();
        Properties copyResult = PropertiesUtils.clone(result, getBootStrapProperties(), false, true);
        if (!(copyResult instanceof UnmodifiableProperties)) {
            copyResult = new UnmodifiableProperties(copyResult);
        }

        return copyResult;
    }
    
    
    /**
     * Get the value for the property with the specified name.  First, this method obtains the
     * value from the property in the configuration.  If no such property exists,
     * then this method obtains the value from the property in the bootstrap
     * properties.  If no such method exists, this method returns null.
     * @param name the name of the property
     * @return the value for the property; null if there is
     * no property with the specified name or if the configuration
     * bootstrap information could not be accessed.
     */
    public synchronized static String getStartupProperty(String name) {

        if (name == null) {
            return null;
        }

        try {
            check();
            if (STARTUP_PROPERTIES == null) {
                STARTUP_PROPERTIES = READER.getStartupConfigurationProperties();
            }
        } catch (ConfigurationException e) {
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$
            System.err.println("** ERROR: Unable to read/obtain the configuration property from Startup Configuration. **"); //$NON-NLS-1$
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$
            return null;
        } catch (Throwable e) {
            e.printStackTrace();
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$
            System.err
                      .println("** ERROR: Unknown error during reading of the configuration properties from Startup Configuration. **"); //$NON-NLS-1$
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$
            return null;
        }

        String result = null;
        if (STARTUP_PROPERTIES == null) {
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$
            System.err.println("** ERROR: Startup Configuration properties are null - call MetaMatrix Support. **"); //$NON-NLS-1$
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$

            return null;
        }
        result = STARTUP_PROPERTIES.getProperty(name);

        if (result == null || result.trim().length() == 0) {
            result = getBootStrapProperty(name);
        }

        return result;
    }
    
    public static String getProperty( String name, String defaultValue ) {
        String value = getProperty(name);
        return value == null ? defaultValue: value;
    }
    
    /**
     * Get the value for the property with the specified name.  First, this method obtains the
     * value from the property in the configuration.  If no such property exists,
     * then this method obtains the value from the property in the bootstrap
     * properties.  If no such method exists, this method returns null.
     * @param name the name of the property
     * @return the value for the property; null if there is
     * no property with the specified name or if the configuration
     * bootstrap information could not be accessed.
     */
    public static String getProperty( String name ) {

        if ( name == null ) {
            return null;
        }

       Properties configProps;
       try {
            check();
            configProps = READER.getConfigurationProperties();
        } catch ( ConfigurationException e ) {
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$
            System.err.println("** ERROR: Unable to read/obtain the configuration property for the current configuration. **"); //$NON-NLS-1$
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$
            return null;
        } catch ( Throwable e ) {
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$
            System.err.println("** ERROR: Unknown error during reading of the configuration properties for the current configuration. **"); //$NON-NLS-1$
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$
            return null;
       }




        String result = null;
        if ( configProps == null ) {
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$
            System.err.println("** ERROR: Configuration properties are null - call MetaMatrix Support. **"); //$NON-NLS-1$
            System.err.println("**********************************************************************************************"); //$NON-NLS-1$

            return null;
        }
        result = configProps.getProperty(name);
        
        if (result == null || result.trim().length() == 0) {
            result = getBootStrapProperty(name);
        }

        return result;
    }
    
    public synchronized static String getBootStrapProperty(String key) {
		try {
	        return getBootStrapProperties().getProperty(key);
		} catch (ConfigurationException e) {
			System.err.println("*************************************************************************************"); //$NON-NLS-1$
			System.err.println("** ERROR: Unable to obtain the bootstrap properties for the current configuration. **"); //$NON-NLS-1$
			System.err.println("*************************************************************************************"); //$NON-NLS-1$
			e.printStackTrace(System.err);
		}
        return null;
    }

    /**
     * Get all of the configuration properties.  The properties
     * that are returned have default properties that are the bootstrap properties.
     * @return the immutable properties; never null
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
    public static Properties getProperties() throws ConfigurationException {
        return getProperties(false);
    }

    /**
     * Get all of the configuration properties.  The properties
     * that are returned have default properties that are the bootstrap properties.
     * @param forceRefresh forces the refreshing of the cache from the datasource.
     * @return the immutable properties; never null
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
    public synchronized static Properties getProperties(boolean forceRefresh) throws ConfigurationException {
		check();
        Properties result = READER.getConfigurationProperties();
        Properties copyResult = PropertiesUtils.clone(result,getBootStrapProperties(),false,true);
        if ( !(copyResult instanceof UnmodifiableProperties) ) {
                    copyResult = new UnmodifiableProperties(copyResult);
        }

        return copyResult;
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
    public synchronized static Properties getResourceProperties(String resourceName) throws ConfigurationException {
        check();

        Properties result;

        SharedResource sr = READER.getResource(resourceName);

        Properties props;
        if (sr != null) {
            props = ResourceModel.getDefaultProperties(resourceName);
            props.putAll(sr.getProperties());
//        	props = PropertiesUtils.clone(sr.getProperties(), false);

        } else {
            props = new Properties();

        }

        // allow the system properties to override
        PropertiesUtils.setOverrideProperies(props, getSystemBootStrapProperties());

        result =  new UnmodifiableProperties(props);

        return result;
    }

    /**
     * Get the startup configuration that was used for deployment.
     * @return the Configuration that was used for deployment.
     * @throws ConfigurationException if the startup configuration could not be obtained
     */
    public synchronized static Configuration getStartupConfiguration() throws ConfigurationException {
        check();
        Configuration config = READER.getStartupConfiguration();
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
    public static Configuration getConfiguration() throws ConfigurationException {
        return getConfiguration(false);
    }

    /**
     * Get the current configuration that is to be used for deployment.
     * @param forceRefresh forces the refreshing of the cache from the datasource.
     * @return the Configuration used for deployment
     * @throws ConfigurationException if the current configuration could not be obtained
     */
    public synchronized static Configuration getConfiguration(boolean forceRefresh) throws ConfigurationException {
	   check();
       Configuration config = READER.getNextStartupConfiguration();
       if ( config == null ) {
              throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0021, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0021));
       }

       return config;
    }


    /**
     * Get the current configuration that is to be used for deployment.
     * @param forceRefresh forces the refreshing of the cache from the datasource.
     * @return the Configuration used for deployment
     * @throws ConfigurationException if the current configuration could not be obtained
     */
    public synchronized static ConfigurationModelContainer getConfigurationModel() throws ConfigurationException {
	   check();

        ConfigurationModelContainer config = null;
        try {
            config = READER.getConfigurationModel();
        } catch (UnsupportedOperationException e) {
        }

        if (config == null) {
            throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0022,
                                             CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0022));
        }

        return config;
    }
    
    /**
     * Get the current configuration that is to be used for deployment.
     * @param forceRefresh forces the refreshing of the cache from the datasource.
     * @return the Configuration used for deployment
     * @throws ConfigurationException if the current configuration could not be obtained
     */
    public synchronized static ConfigurationModelContainer getStartupConfigurationModel() throws ConfigurationException {
       check();
       
       ConfigurationModelContainer config = null;
       try {
           config = READER.getStartupConfigurationModel();
       } catch (UnsupportedOperationException e) {
       }
           
       if (config == null) {
           throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0022, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0022));
       }

       return config;
    }    


    /**
     * Returns a <code>Collection</code> of type <code>ComponentType</code> .
     * that are flagged as being monitored.  A component of this type is considered
     * to be available for monitoring statistics.
     * @param includeDeprecated true if class names that have been deprecated should be
     *    included in the returned list, or false if only non-deprecated constants should be returned.
     * @return List of type <code>ComponentType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see com.metamatrix.common.api.ComponentType
     */

    public static Collection getMonitoredComponentTypes(boolean includeDeprecated) throws ConfigurationException {
       throw new UnsupportedOperationException("Method getMonitoredComponent is not implemented on CurrentConfiguration."); //$NON-NLS-1$
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
    public static Collection getComponentTypes(boolean includeDeprecated) throws ConfigurationException {
        check();
        return READER.getComponentTypes(includeDeprecated);
    }
    
    /**
     * Returns a <code>Collection</code> of type <code>ProductType</code> that represents
     * all the ComponentTypes defined.
     * @return List of type <code>ProductType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ProductType
     */
    public static Collection getProductTypes() throws ConfigurationException {
        
        check();
        return READER.getProductTypes();
       
        
    }
      
     /**
     * Returns the full Host impl for a HostID.  <i>Optional method.</i>
     * This will be implemented server-side, but not necessarily client-side.
     * @param hostID ID of the Host that is wanted
     * @return the full Host object
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     * @throws UnsupportedOperationException if this method is not implemented
     * by the {@link com.metamatrix.common.config.reader.CurrentConfigurationReader}
     * implementation
     */
    public static Host getHost(HostID hostID) throws ConfigurationException{
        check();
        return READER.getHost(hostID);
    }

    /**
     * Returns the Host for the specified Host name.  <i>Optional method.</i>
     * This will be implemented server-side, but not necessarily client-side.
     * @param host name of the Host that is wanted
     * @return the Host 
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     * @throws UnsupportedOperationException if this method is not implemented
     * by the {@link com.metamatrix.common.config.reader.CurrentConfigurationReader}
     * implementation
     */
    public static Host getHost(String name) throws ConfigurationException{
        check();
        return READER.getConfigurationModel().getHost(name);
    } 
    
    /**
     * Call to find a host.  The hostName passed in can be any of the
     * the following:
     * <li>The fully qualified host name</li>
     * <li>The short name of the fully qualified host name</li>
     * <li>The IP address of box</li>
     * <li>The bind address defined in the host properties</li>
     * <li>The general reference of localhost</li>
     * 
     * The order of resolution will be as follows:
     * <li>hostName matches to configured host name</li>
     * <li>resolve hostName to an InetAddress and use its' full host name to match configured host(s)</li>
     * <li>resolve hostName to an InetAddress and use its' short host name to match configured host(s)</li>
     * <li>In cases where the <code>hostName</code> represents the short name and will not resolve to a longer name, 
     *     convert the <code>Host</code> full name to the short name to try to match.</li>
     * <li>match hostname to the physical address for a configurated host</li>  
     * <li>match hostname to the bindaddress for a configurated host</li>  
     * 
     * @param hostName
     * @return Host
     * @throws ConfigurationException 
     * @throws Exception
     * @since 4.3
     */
    public static Host findHost(String hostName) throws ConfigurationException  {
        check();
        Host h = null;
        
        try {
            // first try to match the host by what was passed before
            // substituting something else
            h = getHost(hostName);
            if (h != null) {
                return h;
            }
            
            
            if( hostName.equalsIgnoreCase("localhost")) { //$NON-NLS-1$
                hostName = InetAddress.getLocalHost().getHostName();
                h = getHost(hostName);
                if (h != null) {
                    return h;
                } 
            }
            
                            
            // the hostName could be an IP address that we'll use to try to 
            // resolve back to the actuall host name
            InetAddress inetAddress = InetAddress.getByName(hostName);           

            // try using the fully qualified host name
            
            h = getHost(inetAddress.getCanonicalHostName());
            if (h != null) {
                return h;
            }
            
            h = getHost(inetAddress.getHostName());            
            if (h != null) {
                return h;
            }
            
            // get short name
            String shortName = StringUtil.getFirstToken(inetAddress.getCanonicalHostName(),DOT_STR);
            h = getHost(shortName);
            if (h != null) {
                return h;
            }           
            // try the address
              h = getHost(inetAddress.getHostAddress());
              
              if (h != null) {
                  return h;
              }            
            
        } catch (Exception e) {
            // do nothing
        }
        
        Collection hosts = getConfigurationModel().getHosts();
        
        // 2nd try to match 
        try {

            // if the host name passed in is the short name,
            // then try to match to the Host short name
            
            Iterator hi = hosts.iterator(); 
            while (hi.hasNext()) {
                h = (Host) hi.next();
                String shortname = StringUtil.getFirstToken(h.getFullName(), DOT_STR);
                if (shortname.equalsIgnoreCase(hostName)) {
                    return h;
                }
    
            }     
            
            String shortName = StringUtil.getFirstToken(NetUtils.getInstance().getInetAddress().getCanonicalHostName(), DOT_STR); 
            h = getHost(shortName);
            if (h != null) {
                return h;
            }            
        } catch (Exception e) {
            // do nothing
        }
        
        // try to match based on the host physical or the bind address
        Iterator hostIter = hosts.iterator(); 
         
        while (hostIter.hasNext()) {
            h = (Host) hostIter.next();
             
            if (h.getHostAddress() != null && h.getHostAddress().equalsIgnoreCase(hostName)) {
                return h;
            } else if (h.getBindAddress() != null && h.getBindAddress().equalsIgnoreCase(hostName)) {
                return h;
            }

        }
        
        
        return null;

    }    
    
    /**
     * Returns the Host based on the current running machine. 
     * @return the full Host object
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     * @throws UnknownHostException if the NetUtils cannot derive the current host name
     * @throws UnsupportedOperationException if this method is not implemented
     * by the {@link com.metamatrix.common.config.reader.CurrentConfigurationReader}
     * implementation
     */
    public static Host getHost() throws ConfigurationException, UnknownHostException{
        check();
        // use the logicalHostName firstt, because this would be set if this
        // is called within a running VM (i.e., hostcontroller, vmcontroller)
        // otherwise use localhost
        Host host = findHost(VMNaming.getConfigName());
        if (host == null) {
            host = findHost("localhost");//$NON-NLS-1$
        }
        return host;
    }  
    
    /**
     * Returns the VM based on the current process thats running. 
     * @return the full Host object
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service
     * @throws UnknownHostException if the NetUtils cannot derive the current host name
     * @throws UnsupportedOperationException if this method is not implemented
     * by the {@link com.metamatrix.common.config.reader.CurrentConfigurationReader}
     * implementation
     */
    public static VMComponentDefn getVM() throws ConfigurationException, UnknownHostException{
        check();
        // use the logicalHostName first, because this would be set if this
        // is called within a running VM (i.e., hostcontroller, vmcontroller)
        Host host = getHost();
        if (host == null) return null;
        
        VMComponentDefn vm = 
                READER.getConfigurationModel().
                    getConfiguration().
                    getVMForHost(host.getFullName(), VMNaming.getVMName());
        
        if (vm != null) return vm;
        
        // if not found based on the actual vm name, 
        // then if the host only has 1 vm, then return it
        Collection vms = READER.getConfigurationModel().
                                getConfiguration().getVMsForHost(host.getFullName());
        
        if (vms != null && vms.size() == 1) {
            return (VMComponentDefn) vms.iterator().next();
        }
        
        return null;
    }      

    // the primary method used to perform all the checking
    // if data is preloaded.
    protected synchronized static final void check() throws ConfigurationException {
        verifyBootstrapProperties();
    }

    public synchronized static final void refresh() throws ConfigurationException {
        // during initialization or shutdown, do not allow a refresh to occur
        if (SHUTDOWN_REQUESTED) {
            return;
        }


        clear();
    }

    /**
     * Reset causes not just a refresh, but the bootstrapping process
     * to occurr again.
     */
    public synchronized static final void reset() throws ConfigurationException {
        // during initialization or shutdown, do not allow a refresh to occur
        if (SHUTDOWN_REQUESTED) {
            return;
        }

        clear();

    }

    private static final void clear() {

        READER = null;
        BOOTSTRAP_PROPERTIES = null;
        SYSTEM_BOOTSTRAP_PROPERTIES = null;
        // force a rebootstrap
        performedBootStrap = false;

    }

    public synchronized static final void shutdown() {
        SHUTDOWN_REQUESTED = true;


        if ( READER != null ) {
            try {
                READER.close();
                clear();
            } catch ( Exception e ) {
                System.err.println(CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0023));
                e.printStackTrace(System.err);
            }

        }
    }

    /**
     * This method should be called <i>only</i> by
     * {@link StartupStateController}, which is used by
     * MetaMatrixController to initialize the system configurations during bootstrapping.
     * Once bootstrap properties are verified, this method will use
     * the {@link #READER} to attempt to put the system state into
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
    public static synchronized final void performSystemInitialization(boolean forceInitialization) throws StartupStateException, ConfigurationException {

        try {
            check();

            // this only initializes the persistence of the configuration
            // (i.e., copying NEXTSTARTUP to OPERATIONAL, etc)
            READER.getInitializer().performSystemInitialization(forceInitialization);

        } catch (StartupStateException se) {
            throw se;
        } catch (ConfigurationException ce) {
            throw ce;
        }

		// perform reset to reload configuration information
        reset();


    }


    /**
     * This method should be called <i>only</i> by
     * {@link StartupStateController}, which is used by
     * MetaMatrixController to initialize the system configurations during bootstrapping.
     * Once bootstrap properties are verified, this method will use
     * the {@link #READER} to attempt to put the system state into
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
    public static final void beginSystemInitialization(boolean forceInitialization) throws StartupStateException, ConfigurationException {
		throw new UnsupportedOperationException("beginSystemInitialization is no longer supported."); //$NON-NLS-1$

    }

    /**
     * If the {@link #beginSystemInitialization} method executes without
     * an exception, StartupStateController should call this method
     * after any startup work is done.  This will put the system into a
     * state of {@link #STATE_STARTED}.  StartupStateController should <i>only</i>
     * call this method if the {@link #beginSystemInitialization} method
     * executed without exception.  This method will throw an exception if
     * the system is not currently in a state of {@link #STATE_STARTING}, but
     * that will not prevent the wrong client code from calling this method
     * if a different StartupStateController had successfully called
     * {@link #beginSystemInitialization}.  This method is package-level so
     * that only StartupStateController can access it.
     * @throws StartupStateException if the system is
     * not in a state in which initialization can be finished, namely
     * {@link #STATE_STARTING}.  This exception will indicate the
     * current system state.
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
//JBoss fix - method made public to get around a AccessDenied problem with Jboss30

    public static synchronized final void finishSystemInitialization() throws StartupStateException, ConfigurationException {
		throw new UnsupportedOperationException("finishSystemInitialization is no longer supported."); //$NON-NLS-1$

    }

    /**
     * This will put the system into a state of {@link #STATE_STOPPED}.
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
//JBoss fix - method made public to get around a AccessDenied problem with Jboss30
    public static final void indicateSystemShutdown() throws ConfigurationException {
        check();
        READER.getInitializer().indicateSystemShutdown();
    }

	synchronized static Properties getBootStrapProperties() throws ConfigurationException {
		if (BOOTSTRAP_PROPERTIES == null) {
			CurrentConfigBootstrap bootstrapStrategy = null;
			Properties systemBootStrapProps = getSystemBootStrapProperties();
			boolean useSystemProperties = systemBootStrapProps.getProperty(SystemCurrentConfigBootstrap.NO_CONFIGURATION) != null;
			try {
			
					bootstrapStrategy = new PropertyFileCurrentConfigBootstrap(useSystemProperties);
					BOOTSTRAP_PROPERTIES = bootstrapStrategy.getBootstrapProperties(systemBootStrapProps);
	
	        } catch (ConfigurationException ce) {
	            throw ce;
			} catch (Exception e) {
				throw new ConfigurationException();
			}
		}
		return BOOTSTRAP_PROPERTIES;
	}

    
    public static final Properties getSystemBootStrapProperties() throws ConfigurationException {
        if (SYSTEM_BOOTSTRAP_PROPERTIES != null) {
            return PropertiesUtils.clone(SYSTEM_BOOTSTRAP_PROPERTIES, false);
        }
        CurrentConfigBootstrap bootstrapStrategy = null;
        try {
            bootstrapStrategy = new SystemCurrentConfigBootstrap();
            SYSTEM_BOOTSTRAP_PROPERTIES = bootstrapStrategy.getBootstrapProperties();
        } catch (Exception e) {
            throw new ConfigurationException(e.getMessage());
        }
        return PropertiesUtils.clone(SYSTEM_BOOTSTRAP_PROPERTIES, false);
    }

    /**
     * Returns the instance of <code>CofigurationBootMgr</code> to use to
     * get configuration information.
     * @throws ConfigurationException if the current configuration and/or
     * bootstrap properties could not be obtained
     */
    public synchronized static final void verifyBootstrapProperties() throws ConfigurationException {

        if (performedBootStrap) {
            return;
        }

		try {

        	if ( READER == null ) {

                // Get the default bootstrap properties from the System properties ...
                boolean useSystemProperties = false;
				Properties bootstrap = getBootStrapProperties();
				
                useSystemProperties = bootstrap.getProperty(SystemCurrentConfigBootstrap.NO_CONFIGURATION) != null;

                String readerClassName = bootstrap.getProperty( CurrentConfigBootstrap.CONFIGURATION_READER_CLASS_PROPERTY_NAME );

                if ( readerClassName == null ) {
					if (useSystemProperties) {
	       				READER = new SystemCurrentConfigurationReader();
					} else {
						throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0024, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0024, CurrentConfigBootstrap.CONFIGURATION_READER_CLASS_PROPERTY_NAME));
					}
                } else {
                    Class readerClass = Class.forName(readerClassName);
                    CurrentConfigurationReader tempReader = (CurrentConfigurationReader) readerClass.newInstance();
                    tempReader.connect(bootstrap);
                    READER = tempReader;
                }
              
                performedBootStrap = true;
			}
		} catch ( ConfigurationException e ) {
                System.err.println("*************************************************************************************"); //$NON-NLS-1$
                System.err.println("** ERROR: Unable to obtain the bootstrap properties for the current configuration. **"); //$NON-NLS-1$
                System.err.println("*************************************************************************************"); //$NON-NLS-1$
 				e.printStackTrace(System.err);
                throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_ERR_0025, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0025));

           } catch ( Throwable e ) {
                System.err.println("*************************************************************************************"); //$NON-NLS-1$
                System.err.println("** ERROR: Unable to obtain the bootstrap properties for the current configuration. **"); //$NON-NLS-1$
                System.err.println("*************************************************************************************"); //$NON-NLS-1$
 				e.printStackTrace(System.err);

                throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0025, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0025));
           }


    }
}






