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

package com.metamatrix.platform.vm.controller;

import java.io.File;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EventObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.teiid.adminapi.AdminException;
import org.teiid.transport.AdminAuthorizationInterceptor;
import org.teiid.transport.LogonImpl;

import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.admin.server.ServerAdminImpl;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MultipleException;
import com.metamatrix.common.application.AbstractClassLoaderManager;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.ComponentTypeID;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.DeployedComponentID;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.HostType;
import com.metamatrix.common.config.api.ServiceComponentDefn;
import com.metamatrix.common.config.api.ServiceComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnID;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.extensionmodule.ExtensionModuleEvent;
import com.metamatrix.common.extensionmodule.protocol.URLFactory;
import com.metamatrix.common.id.dbid.DBIDGenerator;
import com.metamatrix.common.id.dbid.DBIDGeneratorException;
import com.metamatrix.common.log.LogConfiguration;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.LogContextsUtil.PlatformAdminConstants;
import com.metamatrix.core.event.EventObjectListener;
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.ZipFileUtil;
import com.metamatrix.metadata.runtime.RuntimeMetadataCatalog;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.admin.api.AuthorizationAdminAPI;
import com.metamatrix.platform.admin.api.ConfigurationAdminAPI;
import com.metamatrix.platform.admin.api.ExtensionSourceAdminAPI;
import com.metamatrix.platform.admin.api.RuntimeStateAdminAPI;
import com.metamatrix.platform.admin.apiimpl.AuthorizationAdminAPIImpl;
import com.metamatrix.platform.admin.apiimpl.ConfigurationAdminAPIImpl;
import com.metamatrix.platform.admin.apiimpl.ExtensionSourceAdminAPIImpl;
import com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIImpl;
import com.metamatrix.platform.config.api.service.ConfigurationServiceInterface;
import com.metamatrix.platform.config.event.ConfigurationChangeEvent;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.ProcessRegistryBinding;
import com.metamatrix.platform.registry.ResourceNotBoundException;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.service.AuthorizationServiceInterface;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.service.api.ServiceState;
import com.metamatrix.platform.service.api.exception.ServiceException;
import com.metamatrix.platform.service.controller.ServiceData;
import com.metamatrix.platform.service.controller.ServicePropertyNames;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;
import com.metamatrix.platform.util.LogPlatformConstants;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;
import com.metamatrix.server.HostManagement;
import com.metamatrix.server.ResourceFinder;
import com.metamatrix.server.admin.api.RuntimeMetadataAdminAPI;
import com.metamatrix.server.admin.apiimpl.RuntimeMetadataAdminAPIImpl;
import com.metamatrix.server.util.ServerPropertyNames;

/**
 * This class is used to start up a server process and start all services that are 
 * configured under this server
 */
public abstract class ProcessController extends AbstractClassLoaderManager implements ProcessManagement {
	
    public final static String SERVICE_ID = "Service"; //$NON-NLS-1$

    public static final String STARTER_MAX_THREADS = "vm.starter.maxThreads"; //$NON-NLS-1$
    
    /**Interval to check the state of services (ms)*/    
    public static final String SERVICE_MONITOR_INTERVAL = "metamatrix.server.serviceMonitorInterval"; //$NON-NLS-1$
        
    // this is a 4.2.2 property used for command line setting
    private static final String STOP_DELAY_TIME = "metamatrix.vm.stop.delay.sec"; //$NON-NLS-1$
    
    private static final int DEFAULT_FORCE_SHUTDOWN_TIME = 30;
    public static final int DEFAULT_STARTER_MAX_THREADS = 15; 
    public static final int DEFAULT_STARTER_TIMETOLIVE = 15000;
    
    protected Host host;
    protected String processName;
    
	private long startTime;
	private Properties vmProps;
	VMComponentDefn vmComponentDefn;

    private boolean shuttingDown = false;

    //WorkerPool used for starting services asynchronously
    private WorkerPool startServicePool;    
    
    protected ClusteredRegistryState registry;
    
    private MessageBus messageBus;
    
    // Server events that are being generated
    ServerEvents events;
    
    protected ClientServiceRegistry clientServices;
    private Map<ComponentTypeID, Properties> defaultPropertiesCache = new HashMap<ComponentTypeID, Properties>();
    private String commonExtensionClasspath;

    private int force_shutdown_time = DEFAULT_FORCE_SHUTDOWN_TIME;

    /**
     * Create a new instance of VMController.
     *
     * @param processName Name of Process
     * @param startDeployedServices If true all services that are deployed to this vm are started.
     * @param standalone If true indicates that VMController is running in its own vm.
     * @throws Exception if an error occurs initializing vmController
     */
    public ProcessController(Host host, String processname, ClusteredRegistryState registry, ServerEvents serverEvents, MessageBus bus, HostManagement hostManagement) throws Exception {
    	super(Thread.currentThread().getContextClassLoader(), PropertiesUtils.getBooleanProperty(CurrentConfiguration.getInstance().getProperties(), ServerPropertyNames.CACHE_CLASS_LOADERS, true), false);
    	this.host = host;
    	this.processName = processname;
    	
    	this.registry = registry;
    	this.events = serverEvents;
    	this.messageBus = bus;
    	    	
        Properties configProps = CurrentConfiguration.getInstance().getProperties(); 
        int maxThreads = PropertiesUtils.getIntProperty(configProps, STARTER_MAX_THREADS, DEFAULT_STARTER_MAX_THREADS);
    	
        this.startServicePool = WorkerPoolFactory.newWorkerPool("StartServiceQueue", maxThreads); //$NON-NLS-1$
        
		initVMProperties();
		
        this.clientServices = new ClientServiceRegistry();

        RuntimeMetadataCatalog.getInstance().init(CurrentConfiguration.getInstance().getProperties(), ResourceFinder.getMessageBus(), ResourceFinder.getCacheFactory());
        
        this.registerILogonAPI();
        this.registerAdmin(hostManagement);
        
        this.registerSubSystemAdminAPIs(hostManagement);
        
        manageClassloaders();
    }

    @Override
    public URL parseURL(String url) throws MalformedURLException {
		return URLFactory.parseURL(null, url);
    }
    
    @Override
    public String getCommonExtensionClassPath() {
		return CurrentConfiguration.getInstance().getProperties().getProperty(ServerPropertyNames.COMMON_EXTENSION_CLASPATH, ""); //$NON-NLS-1$
    }
    	
    /**
     * Register the ServiceInterceptors for the SubSystemAdminAPIs 
     * @throws MetaMatrixComponentException
     */
    private void registerSubSystemAdminAPIs(HostManagement hostManagement) throws MetaMatrixComponentException {
        this.clientServices.registerClientService(ConfigurationAdminAPI.class, wrapAdminService(ConfigurationAdminAPI.class, ConfigurationAdminAPIImpl.getInstance(this.registry)), PlatformAdminConstants.CTX_CONFIGURATION_ADMIN_API);
        this.clientServices.registerClientService(RuntimeStateAdminAPI.class, wrapAdminService(RuntimeStateAdminAPI.class, RuntimeStateAdminAPIImpl.getInstance(this.registry, hostManagement)), PlatformAdminConstants.CTX_RUNTIME_STATE_ADMIN_API);
        this.clientServices.registerClientService(AuthorizationAdminAPI.class, wrapAdminService(AuthorizationAdminAPI.class, AuthorizationAdminAPIImpl.getInstance()), PlatformAdminConstants.CTX_AUTHORIZATION_ADMIN_API);
        this.clientServices.registerClientService(ExtensionSourceAdminAPI.class, wrapAdminService(ExtensionSourceAdminAPI.class, ExtensionSourceAdminAPIImpl.getInstance()), PlatformAdminConstants.CTX_EXTENSION_SOURCE_ADMIN_API);
        this.clientServices.registerClientService(RuntimeMetadataAdminAPI.class, wrapAdminService(RuntimeMetadataAdminAPI.class, RuntimeMetadataAdminAPIImpl.getInstance()), PlatformAdminConstants.CTX_RUNTIME_METADATA_ADMIN_API);
    }	
    
    /**
     * Register a ServiceInterceptor for the new Admin API, so that the client can access it via messaging.
     * @throws AdminException 
     * 
     * @throws MetaMatrixComponentException
     */
    private void registerAdmin(HostManagement hostManagement) throws AdminException {
    	ServerAdminImpl serverAdminImpl = new ServerAdminImpl(this.registry, hostManagement);
    	ServerAdmin roleCheckedServerAdmin = wrapAdminService(ServerAdmin.class, serverAdminImpl);
    	this.clientServices.registerClientService(ServerAdmin.class, roleCheckedServerAdmin, PlatformAdminConstants.CTX_ADMIN);
    }


	@SuppressWarnings("unchecked")
	private <T> T wrapAdminService(Class<T> iface, T impl) {
		//return (T)Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[] {iface}, new AdminAuthorizationInterceptor(PlatformProxyHelper.getAuthorizationServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL), impl));
		return impl;
	}
    
    /** 
     * Register ILogonAPI's ServiceInterceptor
     * @throws ServiceException 
     * @throws ConfigurationException 
     */
    private void registerILogonAPI() throws ConfigurationException, ServiceException {
    	this.clientServices.registerClientService(ILogon.class, new LogonImpl(PlatformProxyHelper.getSessionServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL), CurrentConfiguration.getInstance().getClusterName()), LogCommonConstants.CTX_LOGON);
    }    
    
    /**
     * Lazily get Current Configuration
     */
    ConfigurationModelContainer getConfigurationModel() throws ConfigurationException {
        return CurrentConfiguration.getInstance().getConfigurationModel();
    }    

    /**
     * Initialize vm properties.
     * This method initializes the following global attributes.
     *
     *  - hostID
     *  - vmComponentDefnID
     *  - vmProps
     */
	private void initVMProperties() throws Exception {
		String hostname = host.getFullName();
        ConfigurationModelContainer config = getConfigurationModel();
        
        VMComponentDefn deployedVM = config.getConfiguration().getVMForHost(hostname, processName);

    	this.vmComponentDefn = deployedVM;
    	
    	vmProps = new Properties(config.getConfiguration().getProperties());
    	PropertiesUtils.putAll(vmProps, host.getProperties());
    	PropertiesUtils.putAll(vmProps, config.getDefaultPropertyValues(deployedVM.getComponentTypeID()));
    	PropertiesUtils.putAll(vmProps, config.getConfiguration().getAllPropertiesForComponent(deployedVM.getID()));
       
        // this system property setting will override the setting in the VM
	    // this is done because the command line argument
	    force_shutdown_time = PropertiesUtils.getIntProperty(System.getProperties(), STOP_DELAY_TIME, DEFAULT_FORCE_SHUTDOWN_TIME);
	    if (DEFAULT_FORCE_SHUTDOWN_TIME == force_shutdown_time) {
	    	force_shutdown_time = PropertiesUtils.getIntProperty(vmProps, VMComponentDefnType.FORCED_SHUTDOWN_TIME, DEFAULT_FORCE_SHUTDOWN_TIME);
	    }
	   
	    logMessage(PlatformPlugin.Util.getString("VMController.VM_Force_Shutdown_Time", force_shutdown_time)); //$NON-NLS-1$
	   
	    // add the vm to registry
	    ProcessRegistryBinding binding = new ProcessRegistryBinding(host.getFullName(), this.processName, deployedVM, this, this.messageBus);        
	    this.startTime = binding.getStartTime();
	    this.events.processAdded(binding);
	}

    protected void logMessage(String s) {
        LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, s);
    }

	public void start() {
    			
        logMessage(PlatformPlugin.Util.getString(LogMessageKeys.VM_0013));
        VMComponentDefnID vmComponentDefnID = (VMComponentDefnID)this.vmComponentDefn.getID();

        try {
            ConfigurationModelContainer configuration = getConfigurationModel();

            Collection deployedServices = configuration.getConfiguration().getDeployedServicesForVM(this.vmComponentDefn);
            
            logMessage(PlatformPlugin.Util.getString(LogMessageKeys.VM_0014, new Integer(deployedServices.size()), vmComponentDefnID.getName()));

            ArrayList essentialServices = new ArrayList();
            ArrayList otherServices = new ArrayList();
            
            // create a list of platform services and other services
            Iterator servicesIterator = deployedServices.iterator();
            while (servicesIterator.hasNext()) {
                DeployedComponent depComp = (DeployedComponent) servicesIterator.next();

                ServiceComponentDefn scd = (ServiceComponentDefn) depComp.getDeployedComponentDefn(configuration.getConfiguration());

                if (scd.isEssential()) {
                    essentialServices.add(depComp);
                } else if (depComp.isEnabled()) {
                    otherServices.add(depComp);
                }
            }

            // start platform services first (synchronously) 
            boolean errored = false;
            servicesIterator = essentialServices.iterator();
            while (servicesIterator.hasNext()) {
                DeployedComponent depComp = (DeployedComponent) servicesIterator.next();
                try {
                    startDeployedService(depComp, null, configuration, true);
                } catch (ServiceException e) {
                    errored = true;
                    logException(e, e.getMessage());
                }
            }
            
            if (errored) return;
            

            // now start the rest of the services asynchronously
            servicesIterator = otherServices.iterator();
            while (servicesIterator.hasNext()) {
                DeployedComponent depComp = (DeployedComponent) servicesIterator.next();
                try {
                    startDeployedService(depComp, null, configuration, false);
                } catch (ServiceException e) {
                	logException(e, e.getMessage());
                }
            }
            logMessage(PlatformPlugin.Util.getString(LogMessageKeys.VM_0016, new Integer(deployedServices.size()),  vmComponentDefnID.getName()));

        } catch (ConfigurationException e) {
            logException(e, PlatformPlugin.Util.getString(LogMessageKeys.VM_0017, vmComponentDefnID.getName(), host.getID().getName()));
        }
    }

    public void startService(ServiceID serviceID) {
        logMessage(PlatformPlugin.Util.getString(LogMessageKeys.VM_0018, serviceID));

        ServiceRegistryBinding binding = null;

        try {
            binding = this.registry.getServiceBinding(serviceID.getHostName(), serviceID.getProcessName(), serviceID);
        } catch (ResourceNotBoundException e) {
            String msg = PlatformPlugin.Util.getString(LogMessageKeys.VM_0019, serviceID);
            throw new ServiceException(e, msg);
        } catch (Exception e) {
            String msg = PlatformPlugin.Util.getString(LogMessageKeys.VM_0020, serviceID);
            throw new ServiceException(e, msg);
        }

        if (!binding.isServiceBad()) {
            throw new ServiceException(PlatformPlugin.Util.getString(LogMessageKeys.VM_0021, serviceID));
        }

        try {
            ConfigurationModelContainer configuration = getConfigurationModel();
            startDeployedService(binding.getDeployedComponent(), serviceID, configuration, true);
        } catch (ConfigurationException e) {
            throw new ServiceException(e, PlatformPlugin.Util.getString(LogMessageKeys.VM_0022, serviceID));
        }
    }

    public void startDeployedService(ServiceComponentDefnID id) {
        logMessage(PlatformPlugin.Util.getString(LogMessageKeys.VM_0023, id));
        startDeployedService(id, null);
    }

	private void startDeployedService(ServiceComponentDefnID defnID, ServiceID serviceID) {
        try {
            ConfigurationModelContainer configuration = getConfigurationModel();
            VMComponentDefnID vmComponentDefnID = (VMComponentDefnID)this.vmComponentDefn.getID();
            DeployedComponent deployedService = configuration.getConfiguration().getDeployedServiceForVM(defnID, vmComponentDefnID, (HostID) getConfigHost().getID());
            startDeployedService(deployedService, serviceID, configuration, true);
        } catch (ConfigurationException e) {
            throw new ServiceException(e, PlatformPlugin.Util.getString(LogMessageKeys.VM_0024, defnID));
        }
    }

	/**
	 *  Start the service identified by the DeployedComponentID
	 *  If synch is true then wait for service to start before returning.
	 *  Any exceptions will then be thrown to the caller.
	 *  If synch is false then start service asynchronously.
	 * @throws ConfigurationException 
	 */
	private void startDeployedService(DeployedComponent deployedService, ServiceID serviceID, ConfigurationModelContainer configModel, boolean synch) {
        Properties defaultProps = null;
        synchronized (this) {
            defaultProps = defaultPropertiesCache.get(deployedService.getComponentTypeID());
            
            if (defaultProps == null) {
                defaultProps = new Properties(vmProps);
                PropertiesUtils.putAll(defaultProps, configModel.getDefaultPropertyValues(deployedService.getComponentTypeID()));
                defaultPropertiesCache.put(deployedService.getComponentTypeID(), defaultProps);
            }
        }
        Properties serviceProps = new Properties(defaultProps);
        Properties props = configModel.getConfiguration().getAllPropertiesForComponent(deployedService.getID());
        PropertiesUtils.putAll(serviceProps, props);
        PropertiesUtils.setOverrideProperies(serviceProps, vmProps);
        
 //       ProductServiceConfigID pscID = deployedService.getProductServiceConfigID();
        String serviceClassName = serviceProps.getProperty( ServicePropertyNames.SERVICE_CLASS_NAME );

        if (serviceClassName != null && serviceClassName.length() > 0) {
            logMessage( PlatformPlugin.Util.getString(LogMessageKeys.VM_0025, deployedService.getServiceComponentDefnID().getName(), processName, host.getID().getName()));

            serviceProps.put(ServicePropertyNames.INSTANCE_NAME, deployedService.getName());
            serviceProps.put(ServicePropertyNames.SERVICE_NAME, deployedService.getServiceComponentDefnID().getName());
            serviceProps.put(ServicePropertyNames.COMPONENT_TYPE_NAME, deployedService.getComponentTypeID().getFullName());

            // get routing id.
            if (!deployedService.isDeployedConnector()) {
                serviceProps.put(ServicePropertyNames.SERVICE_ROUTING_ID, deployedService.getComponentTypeID().getFullName());
            } else {
                ServiceComponentDefn scd = (ServiceComponentDefn) deployedService.getDeployedComponentDefn(configModel.getConfiguration());
                String routingID = scd.getRoutingUUID();
                serviceProps.put(ServicePropertyNames.SERVICE_ROUTING_ID, routingID);
            }
            startService(this.clientServices, serviceClassName, serviceID, deployedService, serviceProps, synch );

        } else {
            String msg = PlatformPlugin.Util.getString(LogMessageKeys.VM_0026, new Object[] {ServicePropertyNames.SERVICE_CLASS_NAME, deployedService.getServiceComponentDefnID().getName(), processName, host.getID().getName()});
            throw new ServiceException(msg);
        }

    }
	
    /**
     * Shut down all services waiting for work to complete.
     * Essential services will also be shutdown.
     */
	public synchronized void shutdown(boolean now) {
		logMessage(PlatformPlugin.Util.getString(LogMessageKeys.VM_0041));
		
		try {
            stopServices(now, true);
        } catch (MultipleException e) {
        	logException(e, e.getMessage());
        } catch (ServiceException e) {
            logException(e, e.getMessage());
        } 
	
        // unregister VMController
        events.processRemoved(host.getFullName(), this.processName);
        
        this.shuttingDown = true;

        notifyAll();        
        
		this.startServicePool.execute(new Runnable() {
            public void run() {
                // Wait before killing the VM.
                try {
                    Thread.sleep(force_shutdown_time * 1000);
                } catch (Exception e) {}
                // And exit.
                System.exit(1);
            }
        });
    }


	/**
	 * Kill service once work is complete
	 */
	public void stopService(ServiceID id, boolean now, boolean shutdown) {
        try {
			logMessage(PlatformPlugin.Util.getString(LogMessageKeys.VM_0035, id));
			validateServiceID(id);
			ServiceRegistryBinding binding = this.registry.getServiceBinding(id.getHostName(), id.getProcessName(), id);
			stopService(binding, now, shutdown);
		} catch (ResourceNotBoundException e) {
			throw new ServiceException(e);
		}
    }

    /**
     * Set the current log configuration for this vm.
     */
    public void setCurrentLogConfiguration(LogConfiguration logConfiguration) {
        logMessage(PlatformPlugin.Util.getString(LogMessageKeys.VM_0046, logConfiguration));
        LogManager.setLogConfiguration(logConfiguration);
    }

	/**
	 * Get the time the VM was initialized.
	 */
    public long getStartTime() {
        return this.startTime;
    }

    
    public Host getConfigHost() {
        return this.host;
    }
       
    /**
	 * Method called from registries to determine if VMController is alive.
	 */
	public void ping() {
	}

	public synchronized boolean isShuttingDown() {
		return shuttingDown;
	}

    /**
	 * Return information about VM. totalMemory, freeMemory, threadCount
	 */
    public ProcessStatistics getVMStatistics() {
        ProcessStatistics vmStats = new ProcessStatistics();
        Runtime rt = Runtime.getRuntime();
        vmStats.freeMemory = rt.freeMemory();
        vmStats.totalMemory = rt.totalMemory();

        // get total thread count.
        ThreadGroup root, tg;
        root = Thread.currentThread().getThreadGroup();
        while ((tg = root.getParent()) != null) {
            root = tg;
        }
        vmStats.threadCount = root.activeCount();
        vmStats.name = processName;
        
        
        vmStats.processPoolStats = getProcessPoolStats(); 
        vmStats.socketListenerStats = getSocketListenerStats();
        
        return vmStats;
    }

    /**
     * Prints thread information to a log file.
     * Does not include the stacktrace - that is not available in 1.4.
     */
    public String dumpThreads() {

        ThreadGroup root, tg;
        root = Thread.currentThread().getThreadGroup();
        while ((tg = root.getParent()) != null) {
            root = tg;
        }
        StringBuffer sb = new StringBuffer();
        listThreads(root, 0, sb);
        return sb.toString();
    }

    /**
     * Print information about the specified thread group.
     */
    private void listThreads(ThreadGroup tg, int indent, StringBuffer sb) {

        for (int i = 0; i < indent; i++) {
            sb.append("    "); //$NON-NLS-1$
        }
        sb.append(tg).append("\n"); //$NON-NLS-1$
        indent++;

        //Recursively print information threads in this group
        int cnt = tg.activeCount();
        Thread[] threads = new Thread[cnt];
        tg.enumerate(threads, false);
        for (int i = 0; i < cnt; i++) {
            if (threads[i] != null) {
                for (int j = 0; j < indent; j++) {
                	sb.append("    "); //$NON-NLS-1$
                }
                sb.append(threads[i]).append("\n"); //$NON-NLS-1$
            }
        }

        //Recursively print information about child thread groups
        cnt = tg.activeGroupCount();
        ThreadGroup[] groups = new ThreadGroup[cnt];
        tg.enumerate(groups);
        for (int i = 0; i < cnt; i++) {
            listThreads(groups[i], indent, sb);
        }
    }

    /**
     * Private helper method that verifies service belongs to this vm
     */
    private void validateServiceID(ServiceID serviceID) {
        if (!(serviceID.getHostName().equalsIgnoreCase(this.host.getFullName()) && serviceID.getProcessName().equalsIgnoreCase(this.processName))) {
            throw new ServiceException(PlatformPlugin.Util.getString(LogMessageKeys.VM_0047, serviceID, this.host.getFullName(), this.processName));
        }
    }
        
    /** 
     * @see com.metamatrix.platform.vm.api.controller.ProcessManagement#exportLogs()
     * @since 4.3
     */
    public byte[] exportLogs() {
        File tmpFile = null;
        try {
            tmpFile = File.createTempFile("logs", ".zip");  //$NON-NLS-1$ //$NON-NLS-2$
            tmpFile.deleteOnExit();
            
            String mainLogDirectory = host.getLogDirectory();
            ZipFileUtil.addAll(tmpFile, mainLogDirectory, "log"); //$NON-NLS-1$ 
            
            String hostLogDirectory = host.getProperty(HostType.HOST_DIRECTORY) + File.separator + "log"; //$NON-NLS-1$ 
            ZipFileUtil.addAll(tmpFile, hostLogDirectory, "hosts_"+host.getName()+"_log"); //$NON-NLS-1$//$NON-NLS-2$
            
            String servletLogDirectory = host.getProperty(HostType.HOST_DIRECTORY) + File.separator + "servletengine" + File.separator + "logs"; //$NON-NLS-1$ //$NON-NLS-2$
            ZipFileUtil.addAll(tmpFile, servletLogDirectory, "hosts_"+host.getName()+"_servletengine_log"); //$NON-NLS-1$//$NON-NLS-2$
            
            return new FileUtil(tmpFile.getAbsolutePath()).readBytes();
            
        } catch (Exception e) {
            throw new ServiceException(e);
        } finally {
            try {
                tmpFile.delete();
            } catch (Exception e) {
            }
        }
    }
    
    protected abstract SocketListenerStats getSocketListenerStats();
    protected abstract WorkerPoolStats getProcessPoolStats();

    
    /**
     * Start a service in this VM. If synch flag is true then what for the service to start
     * before returning. Starting a service in this manner is typically done from the console so
     * any errors can be reported. Starting a service asynchronously (synch == false) is intended for
     * starting up the server. This prevents any race conditions due to service dependencies.
     *
     * @param ClientServiceRegistry Class to registry ServerListener
     * @param serviceClassName Class to instantiate as a service.
     * @param serviceID - null if a new service, if non-null then we are re-starting a service.
     * @param serviceProps Properties required to start service.
     * @param synch Flag to indicate if service should be started synchronously are asynchronously.
     */
    private void startService(final ClientServiceRegistry clientServiceRegistry, final String serviceClassName, final ServiceID serviceID, final DeployedComponent deployedComponent, final Properties serviceProps, boolean synch) {

        if (!synch) {
            //add work to the pool
            startServicePool.execute( new Runnable() {
				public void run() {
					try {
						startService(clientServiceRegistry, serviceID, deployedComponent,serviceClassName, serviceProps);
					} catch (ServiceException e) {
						logException(e, e.getMessage());
					}					
				}
            });
        } else {
            //start synchronously
            startService(clientServiceRegistry, serviceID, deployedComponent,serviceClassName, serviceProps);
        }
    }
    
    /**
     * Throw away the common extension class loader when there is change in the Extension modules or has a changed extension
     * classpath.
     */
    private void manageClassloaders() throws MessagingException {
    	this.commonExtensionClasspath = getCommonExtensionClassPath();
    	this.messageBus.addListener(ExtensionModuleEvent.class, new EventObjectListener() {
			@Override
			public void processEvent(EventObject obj) {
				if (obj instanceof ExtensionModuleEvent) {
					clearCache();
				}
			}
    	});
    	
    	this.messageBus.addListener(ConfigurationChangeEvent.class, new EventObjectListener() {
			@Override
			public void processEvent(EventObject obj) {
				if (obj instanceof ConfigurationChangeEvent) {
					String extensionClasspath = getCommonExtensionClassPath();
					if (!extensionClasspath.equals(ProcessController.this.commonExtensionClasspath)) {
						commonExtensionClasspath = extensionClasspath;
						clearCache();	
					}
				}
			}
    	});    	
    }
    
    private void startService(ClientServiceRegistry serverListenerRegistry, ServiceID serviceID, DeployedComponent deployedComponent,final String serviceClass,Properties serviceProps) {
        String serviceInstanceName = null;

        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        try {
    		ClassLoader commonExtensionClassLoader = getCommonClassLoader(null); 
    		if (commonExtensionClassLoader != null) {
    			Thread.currentThread().setContextClassLoader(commonExtensionClassLoader);
    		}
        	
        	if (serviceID == null) {
        		serviceID = this.createServiceID();        		
        	}
        	
            serviceInstanceName = serviceProps.getProperty(ServicePropertyNames.INSTANCE_NAME);
            String serviceType = serviceProps.getProperty(ServicePropertyNames.SERVICE_NAME);
            boolean essential = PropertiesUtils.getBooleanProperty(serviceProps, ServicePropertyNames.SERVICE_ESSENTIAL, false);

            // Create an instance of serviceClass
            final ServiceInterface service  = (ServiceInterface) Thread.currentThread().getContextClassLoader().loadClass(serviceClass).newInstance();

            ServiceData serviceData = service.getServiceData();
            serviceData.setId(serviceID);
			serviceData.setServiceType(serviceProps.getProperty(ServicePropertyNames.COMPONENT_TYPE_NAME));
            serviceData.setRoutingId(serviceProps.getProperty(ServicePropertyNames.SERVICE_ROUTING_ID));
            serviceData.setInstanceName(serviceProps.getProperty(ServicePropertyNames.INSTANCE_NAME));

            // Create ServiceRegistryBinding and register
            final ServiceRegistryBinding binding = new ServiceRegistryBinding(service, host.getFullName(), deployedComponent, essential, this.messageBus);
            
            logMessage(PlatformPlugin.Util.getString("ServiceController.0",serviceInstanceName)); //$NON-NLS-1$
            
            events.serviceAdded(binding);

            // Initialize service
            final Object[] param1 = new Object[] { serviceID };
            DeployedComponentID deployedComponentID = (DeployedComponentID) deployedComponent.getID();
            logMessage(PlatformPlugin.Util.getString("ServiceController.1",param1)); //$NON-NLS-1$
            
            service.init(serviceID, deployedComponentID, serviceProps, serverListenerRegistry, this); 
            logMessage(PlatformPlugin.Util.getString("ServiceController.2",param1)); //$NON-NLS-1$
            logMessage(PlatformPlugin.Util.getString("ServiceController.3",param1)); //$NON-NLS-1$                
                               
            logMessage(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0009, serviceType, serviceInstanceName));

        } catch (ClassNotFoundException e) {
        	throw new ServiceException(e, PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0028, serviceInstanceName));
        } catch (IllegalAccessException e) {
        	throw new ServiceException(e, PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0028, serviceInstanceName));
        } catch (InstantiationException e){
        	throw new ServiceException(e, PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0028, serviceInstanceName));
        } finally {
        	Thread.currentThread().setContextClassLoader(currentClassLoader);
        }
    }    
    

    /**
     * Stop all the services in this vm.
     *
     * @param now If true do not wait for work to complete in services.
     * @throws ServiceException if an error occurs stopping 1 or more services.
     */
    private void stopServices(boolean now, boolean shutdown) throws MultipleException {

        List<ServiceRegistryBinding> bindings = this.registry.getServiceBindings(host.getFullName(), this.processName);
        
        List<ServiceException> exceptions = new LinkedList<ServiceException>();
        
        for (ServiceRegistryBinding binding:bindings) {
            try {
                stopService(binding, now, shutdown);
            } catch (ServiceException se) {
            	exceptions.add(se);
            }
        }

        int numExceptions = exceptions.size();
        if (numExceptions == 1) {
            //if there is one ServiceException, throw it
            throw exceptions.get(0);
        } else if (numExceptions > 1) {
            //if there are many, throw the MultipleException containing all of them
            throw new MultipleException(exceptions, "Multiple errors stopping services"); //$NON-NLS-1$
        }
    }

    /**
     * Stop service, if now flag is true then stop service immediately, do not wait for work to complete.
     *
     * @param serviceID Identifies service to be stopped
     * @param now If true, stop service without waiting for work to complete
     * @throws ServiceException if an error occurs while stopping service or if attempting shutdown the last essential service.
     */
    private synchronized void stopService(ServiceRegistryBinding binding, boolean now, boolean shutdown) {
        ServiceInterface service = null;
        int currentState;

        if (!shutdown && !canServiceBeShutdown(binding)) {
            throw new ServiceException(ErrorMessageKeys.SERVICE_0017, PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0017, binding.getServiceID()));
        }

        try {
            service = binding.getService();
            // check for null service in case the service was stopped.  When servcie is stopped,
            // the binding service reference is nulled out.
            if (service == null) {
                return;
            }
            currentState = binding.getCurrentState();
        } catch (Exception e) {
            throw new ServiceException(e, ErrorMessageKeys.SERVICE_0018, PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0018, binding.getServiceID()));
        }

        // Do not kill a service that was not intialized. May throw meaningless exception
        if (currentState != ServiceState.STATE_INIT_FAILED &&
            currentState != ServiceState.STATE_CLOSED) {
            if (!now) {
                service.die(); // throws ServiceException
            } else {
                service.dieNow(); // do not wait for work to complete.
            }
        }
        
        // Leave binding in registry but remove service instance.
        if (shutdown) {
        	events.serviceRemoved(binding.getServiceID());
        }
        else {
        	events.serviceUpdated(binding);
        }
    }
    
    
    /**
     * Check a service, and updates the state in some cases.  
     * Catches any ServiceExceptions, so that the caller doesn't have to deal with them.
     * @param serviceID Identifies service to be stopped
     */
    public void checkService(ServiceID serviceID) {
        logMessage(PlatformPlugin.Util.getString(LogMessageKeys.VM_0054, serviceID));
        validateServiceID(serviceID);
    	
        ServiceInterface service = null;
        int currentState = 0;

        try {
            ServiceRegistryBinding binding = this.registry.getServiceBinding(serviceID.getHostName(), serviceID.getProcessName(), serviceID);
            service = binding.getService();
            // check for null service in case the service was stopped.  When service is stopped,
            // the binding service reference is nulled out.
            
            if (service != null) {
                currentState = binding.getCurrentState();
            } else {
                return;
            }
        } catch (Exception e) {
            logException(e, PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0068, serviceID));
            return;
        }

        try {
            //Only check state of OPEN and DATA_SOURCE_UNAVAILABLE services
            if (currentState == ServiceState.STATE_OPEN ||
                currentState == ServiceState.STATE_DATA_SOURCE_UNAVAILABLE) {
                service.checkState(); // throws ServiceException
            }
        } catch (ServiceException e) {
            logException(e, PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0068, serviceID)); 
        } 
    }

    protected Properties getProperties() {
    	return this.vmProps;
    }
   
    /**
     * Return TRUE if the system's services are started; i.e. at least one of every essential services
     * in a product is running. Authorization, Configuration, Membership and Session
     * services are considered to be essential.
     *
     * @param callerSessionID ID of the caller's current session.
     * @return Boolean - TRUE if system is started, FALSE if not.
     * @throws MetaMatrixComponentException if an error occurred in communicating with a component.
     */
    protected boolean isStarted() {

        boolean servicesStarted = false;
        List authServices = this.registry.getActiveServiceBindings(null, null, AuthorizationServiceInterface.NAME);
        List sessionServices = this.registry.getActiveServiceBindings(null, null,SessionServiceInterface.NAME);
        List membershipServices = this.registry.getActiveServiceBindings(null, null,MembershipServiceInterface.NAME);
        List configurationServices = this.registry.getActiveServiceBindings(null, null,ConfigurationServiceInterface.NAME);
        
        if ( (authServices.size() > 0) &&
             (sessionServices.size()) > 0 &&
             (membershipServices.size() > 0) &&
             (configurationServices.size() > 0)
             ) {

            servicesStarted = true;
        }
        return servicesStarted;
    }    
    
    /**
     * Create a globally unique ServiceID
     *
     * @return ServiceID
     */
    private ServiceID createServiceID() {
        try {
            return new ServiceID(DBIDGenerator.getInstance().getID(SERVICE_ID), host.getFullName(), this.processName);
        } catch (DBIDGeneratorException e) {
            throw new ServiceException(e, ErrorMessageKeys.SERVICE_0025, PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0025));
        }
    }

    /**
     * True indicates service can be
     */
    private boolean canServiceBeShutdown(ServiceRegistryBinding binding) {

        boolean shutdown = true;
        try {
            if (binding.isEssential()) {
                List services = this.registry.getActiveServiceBindings(null, null, binding.getServiceType());
                if (services.size() < 2) {
                    shutdown = false;
                }
            }
        } catch (Exception e) {
            logException(e, PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0026, binding.getServiceID()));
        }
        return shutdown;
    }

    // ---------------------------------------------------------------------
    // Logging helpers
    // ---------------------------------------------------------------------

    private void logException(Throwable e, String msg) {
        LogManager.logError(LogPlatformConstants.CTX_SERVICE_CONTROLLER, e, msg);
    }


	public InetAddress getAddress() {
		return CurrentConfiguration.getInstance().getHostAddress();
	}


	public String getName() {
		return processName;
	}
	
}
