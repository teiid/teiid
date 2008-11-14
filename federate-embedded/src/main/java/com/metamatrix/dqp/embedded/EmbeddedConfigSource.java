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

package com.metamatrix.dqp.embedded;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.CoreConstants;
import com.metamatrix.dqp.config.DQPConfigSource;
import com.metamatrix.dqp.embedded.services.EmbeddedBufferService;
import com.metamatrix.dqp.embedded.services.EmbeddedDQPServiceRegistry;
import com.metamatrix.dqp.embedded.services.EmbeddedDataService;
import com.metamatrix.dqp.embedded.services.EmbeddedMetadataService;
import com.metamatrix.dqp.embedded.services.EmbeddedTrackingService;
import com.metamatrix.dqp.embedded.services.EmbeddedTransactionService;
import com.metamatrix.dqp.embedded.services.EmbeddedVDBService;
import com.metamatrix.dqp.embedded.services.EmbeddedConfigurationService;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.service.DQPServiceRegistry;
import com.metamatrix.dqp.service.DataService;
import com.metamatrix.dqp.service.MetadataService;
import com.metamatrix.dqp.service.VDBService;

/**
 * This class is main hook point for the Embedded DQP configuration. This classe's
 * responsibility is to encapsulate the knowedge of creating of the various application
 * services used the DQP.
 * 
 */
public class EmbeddedConfigSource implements DQPConfigSource {
    
    private Map services = new HashMap();
    private EmbeddedDQPServiceRegistry svcRegistry = new EmbeddedDQPServiceRegistry();
    
    /**  
    * Based the configuration file load the DQP services
    * @param configFile
    * @throws ApplicationInitializationException
    */    
    public EmbeddedConfigSource(URL dqpURL, Properties connectionProperties) throws ApplicationInitializationException {
        System.getProperties().setProperty(CoreConstants.NO_CONFIGURATION, Boolean.TRUE.toString());        
        String dqpURLString = dqpURL.toString(); 
        try {
            dqpURL = URLHelper.buildURL(dqpURLString);
            InputStream in = dqpURL.openStream();
            if (in != null) {

                // Load the "dqp.properties" file.
                Properties props = new Properties();
                props.load(in);
                in.close();

                // Merge any user properties with the mm.properties
                if (connectionProperties != null) {
                    props.putAll(connectionProperties);
                }
                
                // this will resolve any nested properties in the properties
                // file; this created for testing purpose
                props = PropertiesUtils.resolveNestedProperties(props);
                
                // create a unique identity number for this DQP
                props.put(DQPEmbeddedProperties.DQP_IDENTITY, getDQPIdentity());
                
                // create a workspace directory for the DQP
                props.put(DQPEmbeddedProperties.DQP_TMPDIR, getWorkspaceDirectory());
                
                // This is context of where the dqp.properties loaded, VDB are defined relative to
                // this path.
                props.put(DQPEmbeddedProperties.DQP_BOOTSTRAP_PROPERTIES_FILE, dqpURL);
                
                // First configure logging..
                configureLogging(dqpURL, props);
                
                // Load the Configuration service, so that we know we have started this first.
                createConfigurationService(props);                        
            }
            else {
                throw new ApplicationInitializationException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigSource.Can_not_load_config_file_2", dqpURL)); //$NON-NLS-1$
            }
        } catch (IOException e) {
            throw new ApplicationInitializationException(e);
        }        
    }  

    /**
     * create an identity for the DQP instance in this JVM 
     * @return int a unique number for this JVM
     */
    String getDQPIdentity() {
        String id = System.getProperty(DQPEmbeddedProperties.DQP_IDENTITY, "0"); //$NON-NLS-1$
        return id;
    }
    
    String getWorkspaceDirectory() {
        return System.getProperty(DQPEmbeddedProperties.DQP_TMPDIR, System.getProperty("java.io.tmpdir")); //$NON-NLS-1$ 
    }

    /**
     * Load the service using reflection. 
     * @param serviceName
     * @param serviceType
     * @return
     * @throws ApplicationInitializationException
     * @since 4.3
     */
    ApplicationService reflectivelyLoadService(String serviceName, Class serviceType) throws ApplicationInitializationException {
        if(serviceName != null) {
            try {
                Class serviceClass = Class.forName(serviceName);                
                if(! serviceType.isAssignableFrom(serviceClass) ) {
                    throw new ApplicationInitializationException(DQPEmbeddedPlugin.Util.getString("EmbeddedConfigSource.Service_class__4", serviceName, serviceType.getName()));    //$NON-NLS-1$
                }
                Constructor c= serviceClass.getConstructor(new Class[] {DQPServiceRegistry.class});
                return (ApplicationService) c.newInstance(new Object[] {svcRegistry});
            } catch(ClassNotFoundException e) {
                throw new ApplicationInitializationException(e, DQPEmbeddedPlugin.Util.getString("EmbeddedConfigSource.Unable_to_find_service_class_6", serviceName)); //$NON-NLS-1$
            } catch(NoSuchMethodException e) {
                throw new ApplicationInitializationException(e, DQPEmbeddedPlugin.Util.getString("EmbeddedConfigSource.Unable_to_instantiate_service_class_7", serviceName)); //$NON-NLS-1$
            } catch(InstantiationException e) {
                throw new ApplicationInitializationException(e, DQPEmbeddedPlugin.Util.getString("EmbeddedConfigSource.Unable_to_instantiate_service_class_7", serviceName)); //$NON-NLS-1$
            } catch(IllegalAccessException e) {
                throw new ApplicationInitializationException(e, DQPEmbeddedPlugin.Util.getString("EmbeddedConfigSource.Unable_to_instantiate_service_class_due_to_security_error_8", serviceName)); //$NON-NLS-1$
            } catch(InvocationTargetException e) {
                throw new ApplicationInitializationException(e, DQPEmbeddedPlugin.Util.getString("EmbeddedConfigSource.Unable_to_instantiate_service_class_7", serviceName)); //$NON-NLS-1$                
            }
        }   
        return null;
    }
    
    /* 
     * @see com.metamatrix.dqp.config.DQPConfigSource#getService(java.lang.String)
     */
    public ApplicationService getService(String serviceName) throws ApplicationInitializationException {
        ApplicationService svc = (ApplicationService) this.services.get(serviceName);
        if(svc != null) { 
            return svc;
        }
        
        // Create services as necessary
        if(serviceName.equalsIgnoreCase(DQPServiceNames.BUFFER_SERVICE)) {
            svc = createBufferService();
        } else if(serviceName.equalsIgnoreCase(DQPServiceNames.DATA_SERVICE)) {
            svc = createDataService();
        } else if(serviceName.equalsIgnoreCase(DQPServiceNames.METADATA_SERVICE)) {
            svc = createMetadataService();
        } else if(serviceName.equalsIgnoreCase(DQPServiceNames.VDB_SERVICE)) {
            svc = createVDBService();
        } else if(serviceName.equals(DQPServiceNames.TRACKING_SERVICE)) {
            svc =  createTrackingService();
        } else if(serviceName.equals(DQPServiceNames.CONFIGURATION_SERVICE)) {
            // this service will be started first
            svc =  getConfigurationService();
        } else if(serviceName.equals(DQPServiceNames.TRANSACTION_SERVICE)) {
            svc = createTransactionService();
        }
        
        this.services.put(serviceName, svc);
        return svc;
    }
    
    ApplicationService createTransactionService() throws ApplicationInitializationException {
        try {
            ConfigurationService configSvc = getConfigurationService();
            Properties props = configSvc.getSystemProperties();
            
            String enabled = props.getProperty(EmbeddedTransactionService.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
            
            if (!Boolean.valueOf(enabled).booleanValue()) {
                return null;
            }
            
            ApplicationService svc = new EmbeddedTransactionService(svcRegistry);
            svc.initialize(props);
            return svc;
        } catch (MetaMatrixComponentException e) {
            throw new ApplicationInitializationException(e);
        }
    }
        
    ApplicationService createBufferService() throws ApplicationInitializationException {
        try {
            ConfigurationService configSvc = getConfigurationService();
            Properties props = configSvc.getSystemProperties();
            
            ApplicationService svc = new EmbeddedBufferService(svcRegistry);
            svc.initialize(props);
            return svc;
        } catch (MetaMatrixComponentException e) {
            throw new ApplicationInitializationException(e);
        }
    }
    

    ApplicationService createDataService() throws ApplicationInitializationException {     
        try {
            ApplicationService svc = null;
            
            // this for testing only
            String className = overloadedClass(DQPServiceNames.DATA_SERVICE);
            if (className != null && className.length() > 0) {
                svc = reflectivelyLoadService(className, DataService.class);
            }
            else {
                svc = new EmbeddedDataService(svcRegistry);    
            }
                                
            svc.initialize(null);
            return svc;
        } catch (MetaMatrixComponentException e) {
            throw new ApplicationInitializationException(e); 
        } 
    }

    ApplicationService createTrackingService() throws ApplicationInitializationException {
        try {
            ApplicationService svc = new EmbeddedTrackingService(svcRegistry);        
            svc.initialize(null);
            return svc;
        } catch (MetaMatrixComponentException e) {
            throw new ApplicationInitializationException(e); 
        } 
    }

    ApplicationService createVDBService() throws ApplicationInitializationException {
        try {
            ApplicationService svc = null;
            
            // this for testing only
            String className = overloadedClass(DQPServiceNames.VDB_SERVICE);
            if (className != null && className.length() > 0) {
                svc = reflectivelyLoadService(className, VDBService.class);
            }
            else {
                svc = new EmbeddedVDBService(svcRegistry);
            }
            
            svc.initialize(null);
            return svc;
        } catch (MetaMatrixComponentException e) {
            throw new ApplicationInitializationException(e); 
        } 
    }

    ApplicationService createMetadataService() throws ApplicationInitializationException {        
        try {
            ApplicationService svc = null;
            
            // this for testing only
            String className = overloadedClass(DQPServiceNames.METADATA_SERVICE);
            if (className != null && className.length() > 0) {
                svc = reflectivelyLoadService(className, MetadataService.class);
            }
            else {
                svc = new EmbeddedMetadataService(svcRegistry);                                       
            }
            svc.initialize(null);
            return svc;
        } catch (MetaMatrixComponentException e) {
            throw new ApplicationInitializationException(e);
        }         
    }

    ApplicationService createConfigurationService(Properties props) throws ApplicationInitializationException {                
        try {
            ApplicationService svc = null;
                
            String className = overloadedClass(props, DQPServiceNames.CONFIGURATION_SERVICE);
            if (className != null && className.length() > 0) {
                svc = reflectivelyLoadService(className, ConfigurationService.class);
            }
            else {
                svc = new EmbeddedConfigurationService(this.svcRegistry);
            }                
            svc.initialize(props);
            
            // Add the DQP Services
            this.services.put(DQPServiceNames.CONFIGURATION_SERVICE, svc);
            return svc;            
        } catch(MetaMatrixComponentException e) {
            throw new ApplicationInitializationException(e);
        }
    }
    
    /** 
     * @see com.metamatrix.dqp.config.DQPConfigSource#getProperties()
     */
    public Properties getProperties() throws ApplicationInitializationException {
        try {
            return getConfigurationService().getSystemProperties();
        } catch (MetaMatrixComponentException e) {
            throw new ApplicationInitializationException(e);
        }
    }
    
    ConfigurationService getConfigurationService() throws ApplicationInitializationException{
        return (ConfigurationService)getService(DQPServiceNames.CONFIGURATION_SERVICE);
    }
    
    private String overloadedClass(String svcName) throws ApplicationInitializationException{
        return overloadedClass(getProperties(), svcName);
    }      
    
    private String overloadedClass(Properties props, String svcName) throws ApplicationInitializationException{
        return props.getProperty("service."+svcName+".classname"); //$NON-NLS-1$ //$NON-NLS-2$
    }     
    
    /**
     * Configure the logging for the DQP 
     * @throws MetaMatrixComponentException
     * @since 4.3
     */
    void configureLogging(URL dqpURL, Properties props) throws ApplicationInitializationException{
        boolean captureSystemStreams = Boolean.valueOf(props.getProperty(DQPEmbeddedProperties.DQP_CAPTURE_SYSTEM_PRINTSTREAMS, "false")).booleanValue(); //$NON-NLS-1$
        String logLevel = props.getProperty(DQPEmbeddedProperties.DQP_LOGLEVEL);
        String logFile = props.getProperty(DQPEmbeddedProperties.DQP_LOGFILE);
        String classpath = props.getProperty(DQPEmbeddedProperties.DQP_CLASSPATH);
        boolean unifiedClassLoader = !(classpath != null && classpath.length()>0);
        String instanceId = props.getProperty(DQPEmbeddedProperties.DQP_IDENTITY);        
        
        try {
            // Configure Logging            
            try {
                if (logFile != null && !logFile.equalsIgnoreCase(EmbeddedConfigUtil.STDOUT)) {
                    String modifiedLogFileName = logFile;                    
                    int dotIndex = logFile.lastIndexOf('.');
                    if (dotIndex != -1) {
                        modifiedLogFileName = logFile.substring(0,dotIndex)+"_"+instanceId+"."+logFile.substring(dotIndex+1); //$NON-NLS-1$ //$NON-NLS-2$
                    }
                    else {
                        modifiedLogFileName = logFile+"_"+instanceId; //$NON-NLS-1$
                    }
                    URL logURL = URLHelper.buildURL(dqpURL, modifiedLogFileName);
                    logFile = logURL.getPath();
                }
            } catch (MalformedURLException e) {
                // we may have absolute source, this is just for notification to somewhere.
                e.printStackTrace();                                
            }
            EmbeddedConfigUtil.configureLogger(logFile, logLevel, captureSystemStreams, unifiedClassLoader);
        } catch (MetaMatrixComponentException e) {
            throw new ApplicationInitializationException(e);
        }
    }    
}
