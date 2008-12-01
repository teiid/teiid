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

package com.metamatrix.common.config.reader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.StartupStateException;
import com.metamatrix.common.config.api.*;
import com.metamatrix.common.config.api.exceptions.ConfigurationConnectionException;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.BasicConfigurationObjectEditor;
import com.metamatrix.common.properties.UnmodifiableProperties;
import com.metamatrix.common.util.ErrorMessageKeys;

/**
 * <p>
 * This class implements a self-contained reader for the current configuration,
 * and should be used <i>only</i> by the {@link com.metamatrix.common.config.CurrentConfiguration CurrentConfiguration}
 * framework.  As such, this is an extremely low-level implementation that may
 * <i>not</i> use anything but <code>com.metamatrix.common.util</code> components
 * and only components that do not use {@link com.metamatrix.common.logging.LogManager LogManager}.
 * </p>
 * <p>
 * Each class that implements this interface must supply a no-arg constructor.
 * </p>
 */
public class FileCurrentConfigurationReader implements CurrentConfigurationReader, CurrentConfigurationInitializer{

    private InputStream inputStream;
    private String filename;
    private Configuration currentConfiguration = null;
    private Properties globalProperties = null;

    /**
     * The environment property name for the property file that contains the configuration.
     * This property is required.
     */
    public static final String FILENAME = "metamatrix.config.readerFile"; //$NON-NLS-1$

    /**
     * Default, no-arg constructor
     */
    public FileCurrentConfigurationReader(){
    }

    protected void finalize() {
        try {
            close();
        } catch ( Exception e ) {
            // Should never happen, but ...
            e.printStackTrace(System.err);
        }
    }

    /**
     * This method should connect to the repository that holds the current
     * configuration, using the specified properties.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @param env the environment properties that define the information
     * @throws ConfigurationConnectionException if there is an error establishing the connection.
     */
    public void connect( Properties env ) throws ConfigurationConnectionException{
        // Get the JDBC properties ...
        this.filename = env.getProperty(FILENAME);

	    // Verify required items
	    if (filename == null || filename.trim().length() == 0) {
	        throw new ConfigurationConnectionException(ErrorMessageKeys.CONFIG_ERR_0062, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0062));
	    }

        // Create the connection ...
        try {
            this.inputStream = this.getClass().getClassLoader().getResourceAsStream(this.filename);
        } catch ( Exception e ) {
            throw new ConfigurationConnectionException(e, ErrorMessageKeys.CONFIG_ERR_0063, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0063, filename));
        }
        
        if( this.inputStream == null ) {
            try {
                this.inputStream = new FileInputStream(this.filename);
            } catch (FileNotFoundException err) {
                throw new ConfigurationConnectionException(ErrorMessageKeys.CONFIG_ERR_0064, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0064, filename));
            }
        }
        
        if ( this.inputStream == null ) {
            throw new ConfigurationConnectionException(ErrorMessageKeys.CONFIG_ERR_0064, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0064, filename));
        }
        


        globalProperties = env;
    }

    /**
     * This method should close the connection to the repository that holds the current
     * configuration.  The implementation may <i>not</i> use logging but
     * instead should rely upon returning an exception in the case of any errors.
     * @throws Exception if there is an error establishing the connection.
     */
    public void close() throws Exception{
        if ( this.inputStream != null ) {
            try{
                this.inputStream.close();
            } catch (IOException e) {
                throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_ERR_0065, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0065, filename));
            }
            this.inputStream = null;
        }
    }

    // ------------------------------------------------------------------------------------
    //                     C O N F I G U R A T I O N   I N F O R M A T I O N
    // ------------------------------------------------------------------------------------

    /**
     * Obtain the properties for the current configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the properties
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    public Properties getConfigurationProperties() throws ConfigurationException{
        if ( this.currentConfiguration == null ) {
            buildConfiguration();
        }
        return this.currentConfiguration.getProperties();
    }
    
    

    /** 
     * @see com.metamatrix.common.config.reader.CurrentConfigurationReader#getStartupConfigurationProperties()
     * @since 4.2.1
     */
    public Properties getStartupConfigurationProperties() throws ConfigurationException {
        if ( this.currentConfiguration == null ) {
            buildConfiguration();
        }
        return this.currentConfiguration.getProperties();
    }
    
    /**
    * Returns the name of a property value for the specified configuration object.
    * <p>
    * The <code>getComponentPropertyValue</code> in the <code>FileCurrentConfigurationReader</code>
    * class <i>always</i> re-reads the entire file and rebuilds the Configuration object
    * from the file contents.
    * @param configID the ID of the component to read from; if not equivalent to the
    * ID of the Configuration (i.e., the filename), this method always returns null.
    * @return property value, or null if there was no value for the specified
    * property name on the specified configuration object.
    * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
    * @see #ComponentType
    */
    public String getComponentPropertyValue(ConfigurationID configID, String propertyName) throws ConfigurationException {
        if ( this.currentConfiguration == null ) {
            buildConfiguration();
        }
        String result = null;
        if ( propertyName != null && configID.equals( this.currentConfiguration.getID() ) ) {
            result = this.currentConfiguration.getProperties().getProperty(propertyName);
        }
        return result;
    }

    /**
     * Obtain the current configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
//    public Configuration getConfiguration() throws ConfigurationException{
//        if ( this.currentConfiguration == null ) {
//            buildConfiguration();
//        }
//        return this.currentConfiguration;
//    }

    /**
     * Obtain the next startup configuration.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     *
     * This implementation will perform the same as calling getConfiguration.
     *
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */

    public Configuration getNextStartupConfiguration() throws ConfigurationException {
        if ( this.currentConfiguration == null ) {
            buildConfiguration();
        }
        return this.currentConfiguration;

    }

    /**
     * Obtain the next startup configuration model.  The implementation
     * may <i>not</i> use logging but instead should rely upon returning
     * an exception in the case of any errors.
     * @return the serializable Configuration instance
     * @throws ConfigurationException if an error occurred within or during
     * communication with the repository.
     */
    public ConfigurationModelContainer getConfigurationModel() throws ConfigurationException {

        throw new UnsupportedOperationException("Method getConfigurationModel is not implemented in FileCurrentConfigurationReader"); //$NON-NLS-1$

    }


    /** 
     * @see com.metamatrix.common.config.reader.CurrentConfigurationReader#getStartupConfiguration()
     * @since 4.2
     */
    public Configuration getStartupConfiguration() throws ConfigurationException {
        throw new UnsupportedOperationException("Method getStartupConfigurationModel is not implemented in FileCurrentConfigurationReader"); //$NON-NLS-1$
    }
    /** 
     * @see com.metamatrix.common.config.reader.CurrentConfigurationReader#getStartupConfigurationModel()
     * @since 4.2
     */
    public ConfigurationModelContainer getStartupConfigurationModel() throws ConfigurationException {

        throw new UnsupportedOperationException("Method getStartupConfigurationModel is not implemented in FileCurrentConfigurationReader"); //$NON-NLS-1$
    }
    /**
    * Returns a <code>Collection</code> of type <code>ComponentType</code> that represents
    * all the ComponentTypes defined.
    * <p>
    * The <code>getAllComponentTypes</code> in the <code>FileCurrentConfigurationReader</code>
    * class <i>always</i> returns an empty list, since it is not currently possible
    * to read anything but the Configuration's properties from a file.
    * @param includeDeprecated true if class names that have been deprecated should be
    *    included in the returned list, or false if only non-deprecated constants should be returned.
    * @return List of type <code>ComponentType</code>
    * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
    * @see #ComponentType
    */
    public Collection getComponentTypes(boolean includeDeprecated) throws ConfigurationException {
        if ( this.inputStream == null ) {
            throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0063, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0063));
        }
        Collection result = Collections.EMPTY_LIST;
        return result;
    }
    
    /**
     * Returns a <code>Collection</code> of type <code>ProductType</code> that represents
     * all the ComponentTypes defined.
     * @return List of type <code>ProductType</code>
     * @throws ConfigurationException if an error occurred within or during communication with the Configuration Service.
     * @see #ProductType
     */
   public Collection getProductTypes() throws ConfigurationException {
       if ( this.inputStream == null ) {
           throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0063, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0063));
       }
       Collection result = Collections.EMPTY_LIST;
       return result;
   }      
       

    private void buildConfiguration() throws ConfigurationException{
        if ( this.inputStream == null ) {
            throw new ConfigurationException(ErrorMessageKeys.CONFIG_ERR_0063, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0063));
        }

        // Read the properties from the file ...
        Properties configProps = new Properties(globalProperties);
        if (this.inputStream != null) {
            try {
                configProps.load(this.inputStream);
            } catch ( IOException e ) {
                throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_ERR_0067, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0067, this.filename));
            }
        }

        configProps = new UnmodifiableProperties(configProps);

        // Use the properties from the file to create a new Configuration object ...
        ConfigurationObjectEditor coe = new BasicConfigurationObjectEditor(false);
        this.currentConfiguration = coe.createConfiguration(this.filename);
        try {
            this.currentConfiguration = (Configuration)
                coe.modifyProperties(this.currentConfiguration, configProps, ConfigurationObjectEditor.SET);
        } catch ( Exception e ) {
            e.printStackTrace(System.err);
            throw new ConfigurationException(e,ErrorMessageKeys.CONFIG_ERR_0068, CommonPlugin.Util.getString(ErrorMessageKeys.CONFIG_ERR_0068, this.filename));
        }
    }

    // ------------------------------------------------------------------------------------
    //             O P T I O N A L    S U P P O R T
    // ------------------------------------------------------------------------------------

    /**
     * Returns the full Host impl for a HostID.  This method is not implemented,
     * since it is currently only possible to read configuration properties
     * from a file.
     * @param hostID ID of the Host that is wanted
     * @return the full Host object
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     * @throws UnsupportedOperationException if this method is not implemented
     */
    public Host getHost(HostID hostID) throws ConfigurationException{
        throw new UnsupportedOperationException("Method getHost is not implemented in FileCurrentConfigurationReader"); //$NON-NLS-1$
    }

    /**
     * Should Return an appropriate initializer for the system configuration(s).
     * This method is not implemented, since it is currently only possible
     * to read configuration properties from a file.
     * @return an appropriate initializer for the system configuration(s)
     * @throws ConfigurationException if an error occurred within or during
     * communication with the Configuration Service, or if there is no object
     * for the given ID.
     * @throws UnsupportedOperationException if this method is not implemented
     */
    public CurrentConfigurationInitializer getInitializer() throws ConfigurationException{
    	return this;
    }

    public Map getResourceProperties() throws ConfigurationException {
        return new HashMap();
    }


    public SharedResource getResource(String resourceName ) throws ConfigurationException {
		return null;

    }

	@Override
	public void beginSystemInitialization(boolean forceInitialization)
			throws StartupStateException, ConfigurationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void finishSystemInitialization() throws StartupStateException,
			ConfigurationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void indicateSystemShutdown() throws ConfigurationException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void performSystemInitialization(boolean forceInitialization)
			throws StartupStateException, ConfigurationException {
		// TODO Auto-generated method stub
		
	}


}

