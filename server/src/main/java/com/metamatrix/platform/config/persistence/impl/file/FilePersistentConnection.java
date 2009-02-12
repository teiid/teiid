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

package com.metamatrix.platform.config.persistence.impl.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

import com.metamatrix.common.config.StartupStateController;
import com.metamatrix.common.config.StartupStateException;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.ConfigurationID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.config.model.ConfigurationModelContainerAdapter;
import com.metamatrix.common.util.CommonPropertyNames;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.persistence.api.PersistentConnection;
import com.metamatrix.platform.util.ErrorMessageKeys;


public class FilePersistentConnection implements PersistentConnection {
	
    /**
     * Defines the configuration file override to be used as NextStartup
     */
    public static final String CONFIG_NS_FILE_NAME_PROPERTY = "metamatrix.config.ns.filename"; //$NON-NLS-1$

    /**
     * Defines the path location for the configuration files.  If null then
     * no path will be used and it assumed the file is in the classpath.
     */
    public static final String CONFIG_FILE_PATH_PROPERTY = CommonPropertyNames.CONFIG_MODELS_DIRECTORY;

    /**
     * The actual name of the NextStart Configuration.  If the user provides an
     * override @see {#CONFIG_NS_FILE_NAME_PROPERTY}, then it will be
     * copied to this name.
     */
    public static final String NEXT_STARTUP_FILE_NAME = "config_ns.xml"; //$NON-NLS-1$

	private int state = StartupStateController.STATE_STOPPED;

	private Date startupTime = null;

    private String path;

    private String ns_full_path;

    private String ns_override;

    private ConfigurationModelContainerAdapter adapter;
    private boolean closed = true;


    public FilePersistentConnection(Properties props, ConfigurationModelContainerAdapter adapter) {
        this.adapter = adapter;

        path = props.getProperty(CONFIG_FILE_PATH_PROPERTY, ""); //$NON-NLS-1$
        if (props.getProperty(CONFIG_NS_FILE_NAME_PROPERTY, null) != null) {
        	String file = props.getProperty(CONFIG_NS_FILE_NAME_PROPERTY);
        	if (!file.equals(NEXT_STARTUP_FILE_NAME)) {
        		this.ns_override = getFullPath(path, file);
        	}
        }
        
        this.ns_full_path = getFullPath(path, NEXT_STARTUP_FILE_NAME);
    }

    private void init() throws  ConfigurationException {

    	File configFile = new File(ns_full_path);
    	if (this.ns_override != null && !configFile.exists()) {
    		copyFile(ns_override, ns_full_path);
    	}

        closed = false;
    }

    /**
     * call close when the connection is no longer needed.  This resource
     * will be cleaned up.
     * @throws ConfigurationException
     */
    public void close() {
        closed=true;
    }
    
    /**
     * call to determine is the connection is still available.
     * @return boolean true if the connection is available
     * @return
     */
    public boolean isClosed() {
        return closed;
    }
    

    public synchronized ConfigurationModelContainer read(ConfigurationID configID) throws ConfigurationException {
        if(configID == null){
            Assertion.isNotNull(configID, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0018));
        }

		init();

        InputStream is = readConfigurationFromFile(getFileName(configID));

        ConfigurationModelContainer model = this.adapter.readConfigurationModel(is, configID);
        try {
	        is.close();
        } catch (Exception e) {
        	throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0018, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0018, configID));
        }
        return model;

    }


	/**
	 * Call to set the startup state to @see {StartupStateController.STARTING Starting}.
	 * The server must be in the STOPPED state in order for this to work.  Otherwise,
	 * a StartpStateException will be thrown.
	 * @throws StartupStateException is thrown if the server state is not currently
	 * set to STOPPED.
	 */
    public synchronized void setServerStarting() throws StartupStateException, ConfigurationException {
		if (this.state != StartupStateController.STATE_STOPPED) {
			throw new StartupStateException(StartupStateController.STATE_STARTING, this.state);
		}


		this.state = StartupStateController.STATE_STARTING;

    }

	/**
	 * Call to forcibly set the startup state to @see {StartupStateController.STARTING Starting},
	 * regardless of the current state of the server.
	 * @throws StartupStateException is thrown if the server state cannot be set.
	 */
    public synchronized void setServerStarting( boolean force) throws StartupStateException, ConfigurationException {
		this.state = StartupStateController.STATE_STARTING;

    }

	/**
	 * Call to set the startup state to @see {StartupStateController.STARTED Started}.
	 * The server must be in the STARTING state in order for this to work.  Otherwise,
	 * a StartpStateException will be thrown.
	 * @throws StartupStateException is thrown if the server state cannot be set.
	 */
    public synchronized void setServerStarted( ) throws StartupStateException, ConfigurationException {
		if (this.state != StartupStateController.STATE_STARTING) {
			throw new StartupStateException(StartupStateController.STATE_STARTED, this.state);
		}

		this.state = StartupStateController.STATE_STARTED;
		this.startupTime = new Date();

    }


	/**
	 * Call to set the startup state to @see {StartupStateController.STOPPED Stopped}.
	 * This is normally called when the system is shutdown.
	 * @throws StartupStateException is thrown if the server state cannot be set.
	 */
    public synchronized void setServerStopped() throws StartupStateException, ConfigurationException {
    	state = StartupStateController.STATE_STOPPED;
    }



	/**
	 * Call to get the current state
	 * @return int state @see {StartupStateController Controller}
	 */
	public int getServerState() throws ConfigurationException {
		return this.state;
	}

	/**
	 * Call to get the current state
	 * @return int state @see {StartupStateController Controller}
	 */

	public java.util.Date getStartupTime() throws ConfigurationException {
		if (getServerState() == StartupStateController.STATE_STARTED) {
			return startupTime;
		}
		// go ahead and return a date, even though its not correct
		return new Date();
	}



    private InputStream readConfigurationFromFile(String fileName) throws ConfigurationException {
        InputStream inputStream = null;

        File configFile = new File(fileName);
        try {
        	if (configFile.exists()) {
	            inputStream = new FileInputStream(configFile);
	            BufferedInputStream bis = new BufferedInputStream(inputStream);
				return bis;
        	}
        } catch (IOException io) {
            throw new ConfigurationException(io, ErrorMessageKeys.CONFIG_0021, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0021, fileName ));
        }
        
        try {
            inputStream = this.getClass().getClassLoader().getResourceAsStream(configFile.getName());
        } catch ( Exception e ) {
            throw new ConfigurationException(e,"Unable to access the configuration file \"" + fileName + "\"");
        }

        if ( inputStream == null ) {
            throw new ConfigurationException(ErrorMessageKeys.CONFIG_0020, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0020, fileName ));
        }

        return inputStream;
    }

    public synchronized void write(ConfigurationModelContainer model, String principal) throws ConfigurationException {
       if(model == null){
            Assertion.isNotNull(model, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0022));
       }
       
       if(principal == null){
            Assertion.isNotNull(principal, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0023));
       }

       init();

        String fileName = "NotDefined"; //$NON-NLS-1$

		ConfigurationID id = (ConfigurationID) model.getConfiguration().getID();

		fileName = getFileName(id);

		write(model, fileName, principal);
    }

    public synchronized void write(ConfigurationModelContainer model, String fileName,  String principal) throws ConfigurationException {
        try {
            File configFile = new File(fileName);

            FileOutputStream stream = new FileOutputStream(configFile);
            BufferedOutputStream bos = new BufferedOutputStream(stream);

            adapter.writeConfigurationModel(bos, model, principal);

            bos.flush();
			bos.close();
			stream.close();


        } catch(Exception ioe) {
            ioe.printStackTrace();
            throw new ConfigurationException(ioe, ErrorMessageKeys.CONFIG_0024, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0024, fileName));
        }


    }

    public synchronized void delete(ConfigurationID configID, String principal) throws ConfigurationException {

        String fileName = "NotDefined"; //$NON-NLS-1$
        try {

            fileName = getFileName(configID);

         	deleteFile(fileName);

        } catch(Exception ioe) {
            ioe.printStackTrace();
            throw new ConfigurationException(ioe, ErrorMessageKeys.CONFIG_0025, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0025, fileName));
        }

    }


    private String getFileName(ConfigurationID id) throws ConfigurationException {

        if (id.equals(Configuration.NEXT_STARTUP_ID)) {
            return ns_full_path;
        } else if (id.equals(Configuration.STARTUP_ID)) {
        	return ns_full_path;
        } else {
           throw new ConfigurationException(ErrorMessageKeys.CONFIG_0026, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0026, id));
        }
    }

    private void copyFile(String fromFileName, String toFileName) throws ConfigurationException {
        InputStream in  = null;
        try {
         	in = readConfigurationFromFile(fromFileName);

         	deleteFile(toFileName);

         	FileUtils.write(in, toFileName);
		} catch(Exception e) {
			throw new ConfigurationException(e, ErrorMessageKeys.CONFIG_0027, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0027, fromFileName, toFileName));
		} finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ioe) {
                    throw new ConfigurationException(ioe, ErrorMessageKeys.CONFIG_0027, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0027, fromFileName, toFileName));
 
                }
            }
		}

    }

    private void deleteFile(String fileToDelete) throws ConfigurationException {

        try {

            File configFile = new File(fileToDelete);


	        if (configFile.exists() ) {
	        	configFile.delete();
	        }

        } catch(Exception ioe) {
            ioe.printStackTrace();
            throw new ConfigurationException(ioe, ErrorMessageKeys.CONFIG_0028, PlatformPlugin.Util.getString(ErrorMessageKeys.CONFIG_0028, fileToDelete ));
        }

    }



    private String getFullPath(String path, String fileName) {
		if (path == null || path.trim().length() == 0) {
			return fileName;
		}
       	File configFile = new File(path, fileName);
       	return configFile.getPath();
    }

	@Override
	public void beginTransaction() throws ConfigurationException {
	}

	@Override
	public void commit() throws ConfigurationException {
	}

	@Override
	public void rollback() throws ConfigurationException {
	}

}
