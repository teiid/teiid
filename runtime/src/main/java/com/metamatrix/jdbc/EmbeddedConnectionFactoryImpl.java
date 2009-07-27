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

package com.metamatrix.jdbc;

import static org.teiid.dqp.internal.process.Util.convertStats;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.URL;
import java.util.Date;
import java.util.Properties;

import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.dqp.internal.process.DQPCore;
import org.teiid.transport.AdminAuthorizationInterceptor;
import org.teiid.transport.LocalServerConnection;
import org.teiid.transport.LogonImpl;
import org.teiid.transport.SocketTransport;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.metamatrix.admin.objects.MMProcess;
import com.metamatrix.admin.objects.MMQueueWorkerPool;
import com.metamatrix.common.application.ApplicationService;
import com.metamatrix.common.application.exception.ApplicationInitializationException;
import com.metamatrix.common.application.exception.ApplicationLifecycleException;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.protocol.URLHelper;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.util.JMXUtil;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.MixinProxy;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.embedded.DQPEmbeddedPlugin;
import com.metamatrix.dqp.embedded.DQPEmbeddedProperties;
import com.metamatrix.dqp.embedded.admin.DQPConfigAdminImpl;
import com.metamatrix.dqp.embedded.admin.DQPMonitoringAdminImpl;
import com.metamatrix.dqp.embedded.admin.DQPRuntimeStateAdminImpl;
import com.metamatrix.dqp.embedded.admin.DQPSecurityAdminImpl;
import com.metamatrix.dqp.service.AuthorizationService;
import com.metamatrix.dqp.service.ConfigurationService;
import com.metamatrix.dqp.service.DQPServiceNames;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.vm.controller.SocketListenerStats;


/** 
 * A factory class which creates the connections to the embedded DQP instance.
 * This is also responsible for initializing the DQP if the DQP instance is not
 * already alive.
 */
public class EmbeddedConnectionFactoryImpl implements ServerConnectionFactory {
    private volatile boolean shutdownInProgress = false;
    private DQPCore dqp;
    private long starttime = -1L;
    private Thread shutdownThread;
    private ClientServiceRegistry clientServices;
    private String workspaceDirectory;
    private boolean init = false;
    private SocketTransport socketTransport;
    private JMXUtil jmxServer;
    private boolean restart = false;
    
	@Override
	public ServerConnection createConnection(Properties connectionProperties) throws CommunicationException, ConnectionException {
    	try {
    		initialize(connectionProperties);
            return new LocalServerConnection(connectionProperties, this.clientServices);
    	} catch (ApplicationInitializationException e) {
            throw new ConnectionException(e, e.getMessage());
		}
	}
    
        
    /**
     * When the DQP is restarted using the admin API, it only shuts it down, it gets
     * restarted when the next time connection is made, however this factory may be
     * holding on to a previous transport handler, so we need to check if the DQP is 
     * still alive and create a new one if necessary. 
     * @param props
     * @throws ApplicationInitializationException 
     * @since 4.3
     */
     public synchronized void initialize(Properties info) throws ApplicationInitializationException {
    	if (this.init) {
    		return;
    	}
        
        URL bootstrapURL = null;
        Properties props = new Properties(System.getProperties());
        String processName = "embedded"; //$NON-NLS-1$
        
        try {
			bootstrapURL = URLHelper.buildURL(info.getProperty(DQPEmbeddedProperties.BOOTURL));
			props.putAll(PropertiesUtils.loadFromURL(bootstrapURL));
			props.putAll(info);
			props = PropertiesUtils.resolveNestedProperties(props);
			
			processName = props.getProperty(DQPEmbeddedProperties.PROCESSNAME, processName); 
			props.setProperty(DQPEmbeddedProperties.PROCESSNAME, processName);
			
	        // Create a temporary workspace directory
			String teiidHome = info.getProperty(DQPEmbeddedProperties.TEIID_HOME);
	        this.workspaceDirectory = createWorkspace(teiidHome, props.getProperty(DQPEmbeddedProperties.DQP_WORKDIR, "work"), processName); //$NON-NLS-1$
	        props.setProperty(DQPEmbeddedProperties.DQP_WORKDIR, this.workspaceDirectory);
	        
	        // create the deploy directories
	        File deployDirectory = new File(teiidHome, props.getProperty(DQPEmbeddedProperties.DQP_DEPLOYDIR, "deploy")); //$NON-NLS-1$
	        props.setProperty(DQPEmbeddedProperties.DQP_DEPLOYDIR, deployDirectory.getCanonicalPath());
	        deployDirectory.mkdirs();
	        
	        // if there is no separate vdb-definitions specified then use the deploy directory as the location of the vdb
	        String vdbDefinitions = props.getProperty(DQPEmbeddedProperties.VDB_DEFINITION);
	        if (vdbDefinitions == null) {
	        	props.setProperty(DQPEmbeddedProperties.VDB_DEFINITION, deployDirectory.getCanonicalPath());
	        }
	        
	        // create log directory
	        File logDirectory = new File(teiidHome, props.getProperty(DQPEmbeddedProperties.DQP_LOGDIR, "log")); //$NON-NLS-1$
	        props.setProperty(DQPEmbeddedProperties.DQP_LOGDIR, logDirectory.getCanonicalPath());
	        deployDirectory.mkdirs();
	    	        
		} catch (IOException e) {
			throw new ApplicationInitializationException(e);
		}
		
		this.jmxServer = new JMXUtil(processName);
		
        EmbeddedGuiceModule config = new EmbeddedGuiceModule(bootstrapURL, props, this.jmxServer);
		Injector injector = Guice.createInjector(config);
		ResourceFinder.setInjector(injector);
		config.setInjector(injector);
		
		// start the DQP
		this.dqp = injector.getInstance(DQPCore.class);
		this.dqp.start(config);
		
		// make the configuration service listen for the connection life-cycle events
		// used during VDB delete
        ConfigurationService configService = (ConfigurationService)findService(DQPServiceNames.CONFIGURATION_SERVICE);
        
    	SessionServiceInterface sessionService = (SessionServiceInterface)this.dqp.getEnvironment().findService(DQPServiceNames.SESSION_SERVICE);
    	sessionService.register(configService.getSessionListener());
    	
        //in new class loader - all of these should be created lazily and held locally
        this.clientServices = createClientServices(configService);
                
    	// start socket transport
        boolean enableSocketTransport = PropertiesUtils.getBooleanProperty(props, DQPEmbeddedProperties.ENABLE_SOCKETS, false);
        if (enableSocketTransport) {
	        this.socketTransport = new SocketTransport(props, this.clientServices, (SessionServiceInterface)findService(DQPServiceNames.SESSION_SERVICE));
	        this.socketTransport.start();
        }
        
    	this.shutdownThread = new ShutdownWork();
    	Runtime.getRuntime().addShutdownHook(this.shutdownThread);
        
        this.starttime = System.currentTimeMillis();
        this.init = true;
        DQPEmbeddedPlugin.logInfo("DQPEmbeddedManager.start_dqp", new Object[] {new Date(System.currentTimeMillis()).toString()}); //$NON-NLS-1$
    }
    
	private ClientServiceRegistry createClientServices(ConfigurationService configService) {
    	ClientServiceRegistry services  = new ClientServiceRegistry();
    
    	SessionServiceInterface sessionService = (SessionServiceInterface)this.dqp.getEnvironment().findService(DQPServiceNames.SESSION_SERVICE);
    	services.registerClientService(ILogon.class, new LogonImpl(sessionService, configService.getClusterName()), com.metamatrix.common.util.LogConstants.CTX_SERVER);
    	
		Admin roleCheckedServerAdmin = wrapAdminService(Admin.class, getAdminAPI());
		services.registerClientService(Admin.class, roleCheckedServerAdmin, com.metamatrix.common.util.LogConstants.CTX_ADMIN);
    	
    	services.registerClientService(ClientSideDQP.class, this.dqp, LogConstants.CTX_QUERY_SERVICE);
    	
		return services;
	}
    
    /**
     * Create the temporary workspace directory for the dqp  
     * @param identity - identity of the dqp
     * @throws MMSQLException 
     */
    private String createWorkspace(String teiidHome, String baseDir, String identity) throws IOException {
		File baseFile = new File(teiidHome, baseDir);
		
        File f = new File(baseFile, identity);
        
        System.setProperty(DQPEmbeddedProperties.DQP_TMPDIR, f.getCanonicalPath() + "/temp"); //$NON-NLS-1$S

        // If directory already exists then try to delete it; because we may have
        // failed to delete at end of last run (JVM holds lock on jar files)
        if (f.exists()) {
            delete(f);
        }
        
        // since we may have cleaned it up now , create the directory again
        if (!f.exists()) {
            f.mkdirs();
        }  
        return f.getCanonicalPath();
    }
    
    /**
     * delete the any directory including sub-trees 
     * @param file
     */
    private void delete(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                delete(files[i]);                    
            } // for
        }

        // for saftey purpose only delete the jar files
        if (file.getName().endsWith(".jar")) { //$NON-NLS-1$
            file.delete();
        }
    }    
    
	@SuppressWarnings("unchecked")
	private <T> T wrapAdminService(Class<T> iface, T impl) {
		AuthorizationService authService = (AuthorizationService)this.dqp.getEnvironment().findService(DQPServiceNames.AUTHORIZATION_SERVICE);
		return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {iface}, new AdminAuthorizationInterceptor(authService, impl));
	}
    
	class ShutdownWork extends Thread {
    	ShutdownWork(){
    		super("embedded-shudown-thread"); //$NON-NLS-1$
    	}
    	
		@Override
		public void run() {
			shutdown(false, false);
		}
    }
   
    public synchronized boolean isAlive() {
        return (dqp != null);
    }
    
    public long getStartTime() {
    	return this.starttime;
    }
    
    public Properties getProperties() {
        if (isAlive()) {
            return ((ConfigurationService)findService(DQPServiceNames.CONFIGURATION_SERVICE)).getSystemProperties();
        }
        return null;
    }
        
    public synchronized DQPCore getDQP() {
        if (!isAlive()) {
            throw new MetaMatrixRuntimeException(JDBCPlugin.Util.getString("LocalTransportHandler.Transport_shutdown")); //$NON-NLS-1$
        }
        return this.dqp;
    }    
        
    public ApplicationService findService(String type) {
    	return getDQP().getEnvironment().findService(type);
    }
    
    /**  
     * A shutdown could happen when somebody calls stop/restart on DQP
     * or in normal course of process, we may have just closed the last connection
     * in both cases we want to handle the situation of graceful/proper shutdown.
     * 
     * @see com.metamatrix.jdbc.EmbeddedConnectionFactory#shutdown()
     */
    public void shutdown(boolean restart) {
    	shutdown(true, restart);
    }
     
    private synchronized void shutdown(boolean undoShutdownHook, boolean restart) {
    	
    	if (!isAlive()) {
    		return;
    	}
    	
    	if (undoShutdownHook) {
    		Runtime.getRuntime().removeShutdownHook(this.shutdownThread);
    	}
    	
        // Make sure shutdown is not already in progress; as call to shutdown will close
        // connections; and after the last connection closes, the listener also calls shutdown
        // for normal route.
        if (!this.shutdownInProgress) {

            // this will by pass, and only let shutdown called once.
            this.shutdownInProgress = true;
            
        	try {
				this.dqp.stop();
			} catch (ApplicationLifecycleException e) {
				LogManager.logWarning(LogConstants.CTX_DQP, e, e.getMessage());
			}
            
            // remove any artifacts which are not cleaned-up
            if (this.workspaceDirectory != null) {
                File file = new File(this.workspaceDirectory);
                if (file.exists()) {
                    delete(file);
                }
            }
            
            this.dqp = null;
            
            // shutdown the socket transport.
            if (this.socketTransport != null) {
            	this.socketTransport.stop();
            	this.socketTransport = null;
            }
            
            // shutdown the cache.
            ResourceFinder.getCacheFactory().destroy();
            
            this.shutdownInProgress = false;
            
            this.init = false;
            
            this.restart = restart;
        }    	
    }

    private Admin getAdminAPI() {
        
        InvocationHandler handler = new MixinProxy(new Object[] {
                new DQPConfigAdminImpl(this),                    
                new DQPMonitoringAdminImpl(this), 
                new DQPRuntimeStateAdminImpl(this), 
                new DQPSecurityAdminImpl(this)
        }) {
            
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                // We we perform any DQP functions check if the DQP is still alive
                if (!isAlive()) {
                    throw new AdminProcessingException(JDBCPlugin.Util.getString("EmbeddedConnection.DQP_shutDown")); //$NON-NLS-1$
                }
                
                ClassLoader callingClassLoader = Thread.currentThread().getContextClassLoader();
                try {
                    // Set the class loader to current class classloader so that the this classe's class loader gets used
                    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                    return super.invoke(proxy, method, args);
                }
                finally {
                    Thread.currentThread().setContextClassLoader(callingClassLoader);
                }
            }            
        };
        return (Admin) Proxy.newProxyInstance(this.getClass().getClassLoader(),new Class[] {Admin.class}, handler);
    }
	
	public JMXUtil getJMXServer() {
		return this.jmxServer;
	}
	
	public MMProcess getProcess() {
		
		Properties props = getProperties();
		
		String hostName = ((InetAddress)props.get(DQPEmbeddedProperties.HOST_ADDRESS)).getHostName();
		String processName = props.getProperty(DQPEmbeddedProperties.PROCESSNAME);

		String[] identifierParts = new String[] {hostName, processName};
		MMProcess process = new MMProcess(identifierParts);
        
		Runtime rt = Runtime.getRuntime();
		
		process.setEnabled(true);
		process.setCreated(new Date(getStartTime()));
		process.setInetAddress((InetAddress)props.get(DQPEmbeddedProperties.HOST_ADDRESS));
		process.setFreeMemory(rt.freeMemory());
		process.setTotalMemory(rt.totalMemory());
		process.setProperties(PropertiesUtils.clone(props));
		
		if (this.socketTransport != null) {
	        SocketListenerStats socketStats = this.socketTransport.getStats();
	        if (socketStats != null) {
	            process.setSockets(socketStats.sockets);
	            process.setMaxSockets(socketStats.maxSockets);
	            process.setObjectsRead(socketStats.objectsRead);
	            process.setObjectsWritten(socketStats.objectsWritten);
	        }
	        
	        WorkerPoolStats workerStats = this.socketTransport.getProcessPoolStats();
	        if (workerStats != null) {
	            MMQueueWorkerPool workerPool = convertStats(workerStats, hostName, processName, workerStats.name);
	            
	            process.setQueueWorkerPool(workerPool);
	        }	        
	        
	        process.setPort(this.socketTransport.getPort());
		}
		return process;
	}
	
	public boolean shouldRestart(){
		return this.restart;
	}
}
