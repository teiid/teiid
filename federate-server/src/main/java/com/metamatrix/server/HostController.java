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

package com.metamatrix.server;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.StartupStateController;
import com.metamatrix.common.config.StartupStateException;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostType;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.NetUtils;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.HostControllerRegistryBinding;
import com.metamatrix.platform.registry.HostMonitor;
import com.metamatrix.platform.registry.VMRegistryBinding;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;
import com.metamatrix.platform.vm.util.VMUtils;

@Singleton
public class HostController implements HostManagement {

    private static final String DEFAULT_JAVA_MAIN = "com.metamatrix.server.Main"; //$NON-NLS-1$
 
    private Host host;
    
    private Map processMap = new HashMap(3);
        
    private ClusteredRegistryState registry;
    
    private HostMonitor monitor;
    
    private MessageBus messageBus;

    @Inject
    public HostController(@Named(Configuration.HOST)Host host, ClusteredRegistryState registry, HostMonitor hostMonitor, MessageBus bus) throws Exception {
        this.host = host;
        this.registry = registry;
        this.monitor = hostMonitor;
        this.messageBus = bus;
    }
    
    public void run(boolean startProcesses) throws ConfigurationException, IOException, StartupStateException {
    	
    	if (isHostRunning()) {
    		System.err.println(PlatformPlugin.Util.getString("HostController.Host_is_already_running_startprocesses", host.getFullName())); //$NON-NLS-1$
    		System.exit(-1);
    	}
    	    	
    	// normal startup.
    	StartupStateController.performSystemInitialization(true);
    	
        startLogging();
        
		Runtime.getRuntime().addShutdownHook(new ShutdownThread());        
		
		this.monitor.hostAdded(new HostControllerRegistryBinding(this.host.getFullName(), this, this.messageBus));
		
        if (startProcesses ) {
            try {
				startServers(host.getFullName());
			} catch (MetaMatrixComponentException e) {
				LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, e.getMessage());
			}
        }    	
    }
    
    private void shutdown() {
		if (isHostRunning()) {
			HostControllerRegistryBinding prevHost = getRunningHost();
			try {
				prevHost.getHostController().shutdown(prevHost.getHostName());
			} catch (MetaMatrixComponentException e) {
			}
		}
		else {
			System.err.println("Did not find previous instance of host controller to shutdown"); //$NON-NLS-1$
		}
		System.exit(-2);    	
    }
    
    private boolean isHostRunning() {
    	List<HostControllerRegistryBinding> hosts = this.registry.getHosts();
    	for(HostControllerRegistryBinding host:hosts) {
    		if (host.getHostName().equalsIgnoreCase(this.host.getFullName())) {
    			return true;
    		}
    	}
    	return false;
    }
    
    private HostControllerRegistryBinding getRunningHost() {
    	List<HostControllerRegistryBinding> hosts = this.registry.getHosts();
    	for(HostControllerRegistryBinding host:hosts) {
    		if (host.getHostName().equalsIgnoreCase(this.host.getFullName())) {
    			return host;
    		}
    	}
    	return null;
    }
    
    private void startLogging() throws ConfigurationException, IOException {

        VMNaming.setLogicalHostName(host.getFullName());
        VMNaming.setHostAddress(host.getHostAddress());
        VMNaming.setBindAddress(host.getBindAddress());
        
        // setup the log file
        String hostFileName = StringUtil.replaceAll(host.getFullName(), ".", "_"); //$NON-NLS-1$ //$NON-NLS-2$
        
        VMUtils.startLogFile(host.getLogDirectory(), hostFileName + "_hc.log"); //$NON-NLS-1$
        
        // If the java-i-tmp directory doesn't exist, it needs to be created
        // because extension jars class loading will fail because
        // java internals can' write to a non-existent directory.
        String temp_dir = host.getTempDirectory();
        File tempDir = new File(temp_dir);
        if (tempDir.exists()) {           
            FileUtils.removeDirectoryAndChildren(tempDir);
        }
        
        String dataDir = host.getDataDirectory();
        File dataF = new File(dataDir);
        if (dataF.exists()) {           
            FileUtils.removeDirectoryAndChildren(dataF);
            dataF.mkdirs();
        }
        
        if (!tempDir.exists()) {           
            tempDir.mkdirs();
        }
    }

    
    private void startVM(VMComponentDefn deployedVM, String hostname, ConfigurationModelContainer currentConfig) {
        // Get all of the properties for the deployed VM plus all of the
        // properties inherited from parents and the configuration.
        // The properties defined on this VM override inherited properties.
        Properties vmPropsAndConfigProps = new Properties();
        Properties props = currentConfig.getDefaultPropertyValues(deployedVM.getComponentTypeID());
        Properties vmProps = currentConfig.getConfiguration().getAllPropertiesForComponent(deployedVM.getID());
        vmPropsAndConfigProps.putAll(props);
        vmPropsAndConfigProps.putAll(this.host.getProperties());                    
        vmPropsAndConfigProps.putAll(vmProps);
		String vmName = deployedVM.getID().getName();

        // pass the instance name and its properties
        String name = vmName.toUpperCase();
		if (processMap.containsKey(name)) {
			LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, PlatformPlugin.Util.getString(LogMessageKeys.HOST_0011,vmName));
		    try {
				killServer(hostname, vmName, true);
			} catch (MetaMatrixComponentException e) {
				LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, e.getMessage());
			}
		}
		
		if (deployedVM.isEnabled()) {
		    processMap.put(name, startDeployVM(vmName, hostname, vmPropsAndConfigProps));
		} else {
			LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, PlatformPlugin.Util.getString("HostController.VM_is_not_enabled_to_start", vmName));//$NON-NLS-1$
		}
    }


    private class ShutdownThread extends Thread {
        public ShutdownThread() {
            super("ShutdownTread"); //$NON-NLS-1$
        }
        public void run() {
        	try {
				killServers(host.getFullName(), true);
			} catch (MetaMatrixComponentException e) {
				LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, e.getMessage());
			}
        }
    }

    private static void printUsage() {
        String msg = "java com.metamatrix.platform.host.HostController [-config hostName] [-noprocesses] [-shutdown] [-help]" + //$NON-NLS-1$
                     "\nWhere:" + //$NON-NLS-1$
                     "\n                  -help" + //$NON-NLS-1$
                     "\n                  -config hostName indicates specific host configuration to use" + //$NON-NLS-1$
                     "\n                  -noprocesses indicates to not start the processes" + //$NON-NLS-1$
                     "\n                  -shutdown shutdown the host controller and processes"; //$NON-NLS-1$

        System.out.println(msg);
    }
    
    /**
     * Optional arg; portNum
     * if exists then hostController listens on portNum
     * else hostController reads in hostPort from CurrentConfiguration.
     */
    public static void main(String args[]) {
        String hostname = null;
        boolean startProcesses = true;
        boolean shutdown = false;
        
        int parmIndex = args.length;
        for (int i = 0; i < parmIndex; i++) {
            String command = args[i];
            if (command.equalsIgnoreCase("-config") ) { //$NON-NLS-1$ 
                i++;
                if (i == parmIndex) {
                    printUsage();
                    System.exit(-1);
                } else {
                    hostname = args[i];
                }
                
            } else if (command.equalsIgnoreCase("-noprocesses")) { //$NON-NLS-1$
                startProcesses = false;
                
            } else if (command.equalsIgnoreCase("-help")) { //$NON-NLS-1$
                printUsage();
                System.exit(-1);
            }  else if (command.equalsIgnoreCase("-shutdown")) { //$NON-NLS-1$
            	shutdown = true;
            }
        }

		String logMsg = "startserver "; //$NON-NLS-1$
		for (int i = 0; i < args.length; i++) {
			logMsg += args[i] + " "; //$NON-NLS-1$
		}

        try {
            // if the hostname was not passed, then add the hostname
            // to the logmsg to let the user know which hostname
            // is being used, for informational purposes
            if (hostname == null) {
                hostname = NetUtils.getHostname();
                logMsg += "resolved host " + hostname;  //$NON-NLS-1$
            } 
            
            LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER,logMsg);
           
            Host host = null;
            try {
    			host = CurrentConfiguration.findHost(hostname);        
    		} catch (ConfigurationException e) {
    		}

            if (host == null) {
            	LogManager.logError(LogCommonConstants.CTX_CONTROLLER,"ERROR " + PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0001, hostname)); //$NON-NLS-1$
                System.exit(-1);
            }            
            
            HostController hostController = loadHostcontroller(host);
            if (!shutdown) {
            	hostController.run(startProcesses);
            	Thread.sleep(Integer.MAX_VALUE);
            }
            else {
            	hostController.shutdown();
            }
            
        } catch (Throwable e) {
            LogManager.logError(LogCommonConstants.CTX_CONTROLLER,"ERROR " + PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0011));//$NON-NLS-1$
            System.exit(1);
        }
    }
    
	private static HostController loadHostcontroller(Host host) {
		Injector injector = Guice.createInjector(new HostControllerGuiceModule(host));
		ResourceFinder.setInjector(injector); 
		return injector.getInstance(HostController.class);
	}    
    
   private Process startDeployVM( String vmName, String hostName, Properties vmprops) {
	   LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, "Start deploy VM " + vmName + "on host"+ hostName); //$NON-NLS-1$ //$NON-NLS-2$
       String command = buildVMCommand(vmprops);
       return execCommand(command);
   }

   private String buildVMCommand(Properties vmprops) {
	   String java = vmprops.getProperty(HostType.JAVA_EXEC, "java"); //$NON-NLS-1$
	   String java_opts = vmprops.getProperty(VMComponentDefnType.JAVA_OPTS, System.getProperty(VMComponentDefnType.JAVA_OPTS)); 
	   String java_main = vmprops.getProperty(VMComponentDefnType.JAVA_MAIN, DEFAULT_JAVA_MAIN);
	   String java_args = vmprops.getProperty(VMComponentDefnType.JAVA_ARGS, ""); //$NON-NLS-1$
	   
	   java = replaceToken(java, vmprops);
	   java_opts = replaceToken(java_opts, vmprops);
	   java_main = replaceToken(java_main, vmprops);
	   java_args = replaceToken(java_args, vmprops);
   
	   String cmd = java + " " +java_opts+ " " +java_main + " " +java_args; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
       return cmd;
   }
   
   /*
    * Replace any defined ${...} properties with associated properties
    * that exist in the vmprops
    */
   private String replaceToken(String value, Properties props) {
	   String rtn = value;
	   while (true) {
		   int startidx = rtn.indexOf("${"); //$NON-NLS-1$
		   if (startidx == -1) return rtn;
		   
		   int endidx = rtn.indexOf("}"); //$NON-NLS-1$
		   if (endidx < startidx)  return rtn;
		   
		   String tokenprop = rtn.substring(startidx + 2, endidx);
		   String tokenvalue = props.getProperty(tokenprop);
		   StringBuffer buf = new StringBuffer(rtn);
		   rtn = buf.replace(startidx, endidx + 1, tokenvalue).toString();
	   }
   }


   /**
    * Execute the specified command. The command executed MUST be an executable.
    */
   private Process execCommand(String cmd) {
       Runtime rt = Runtime.getRuntime();
       Process process = null;
       try {
    	   LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, PlatformPlugin.Util.getString(LogMessageKeys.VM_0049, cmd));
           process = rt.exec(cmd);
           LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER,PlatformPlugin.Util.getString(LogMessageKeys.VM_0050));
       } catch (Exception e) {
           LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, PlatformPlugin.Util.getString(LogMessageKeys.VM_0051, cmd));
       }
       return process;
   }

	@Override
	public void killServer(String hostName, String vmName, boolean stopNow) throws MetaMatrixComponentException {
		
		if (isRootHost(hostName)) {
	    	LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, "KillVM " + vmName); //$NON-NLS-1$
	        
	        // find the vm and shut it down.
	        List<VMRegistryBinding> vms = HostController.this.registry.getVMs(null);
	        for (VMRegistryBinding vm:vms) {
	            if (vm.getHostName().equalsIgnoreCase(this.host.getFullName())) {
	                if (vm.getVMName().equalsIgnoreCase(vmName)) {
	                    try {
	                    	if (stopNow)
	                    		vm.getVMController().shutdownNow();
	                    	else
	                    		vm.getVMController().shutdown();
	                    } catch (Exception e) {
	                        // ignore
	                    }
	                    break;
	                }
	            }
	        }
	        
	        Process process = (Process) processMap.get(vmName.toUpperCase());
	        if (process != null) {
	            processMap.remove(vmName.toUpperCase());
	            process.destroy();
	        }
		}
		else {
			HostManagement remoteHost = getRemoteHost(hostName);
			if (remoteHost != null) {
				remoteHost.killServer(hostName, vmName, stopNow);
			}
			else {
				throw new MetaMatrixComponentException("HostController for host = " + hostName + " can not be reached"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
	}
	
	@Override
	public void killServers(String hostName, boolean stopNow) throws MetaMatrixComponentException {
		
		if (isRootHost(hostName)) {
	    	LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, "KillAllVMs"); //$NON-NLS-1$
	        
	        // must copy the map so that when
	        // doKillVM tries to remove it from the map
	        // while its being iterated over.
	        Map copyMap = new HashMap();
	        copyMap.putAll(processMap);
	
	        Iterator processes = copyMap.keySet().iterator();
	        while (processes.hasNext()) {
	            String vmName = (String) processes.next();
	            killServer(hostName, vmName, stopNow);
	        }
	        processMap.clear();
		}
		else {
			HostManagement remoteHost = getRemoteHost(hostName);
			if (remoteHost != null) {
				remoteHost.killServers(hostName,stopNow);
			}
			else {
				throw new MetaMatrixComponentException("HostController for host = " + hostName + " can not be reached"); //$NON-NLS-1$ //$NON-NLS-2$
			}			
		}
	}
	
	private HostManagement getRemoteHost(String hostName) {
		HostControllerRegistryBinding hostBinding =  this.registry.getHost(hostName);
		if (hostBinding != null) {
			return hostBinding.getHostController();
		}		
		return null;
	}
	
	private boolean isRootHost(String hostName) {
		if (this.host.getFullName().equalsIgnoreCase(hostName)) {
			return true;
		}
		return false;
	}
	
	@Override
	public boolean ping(String hostName) {
		if (isRootHost(hostName)){
			return true;
		}
		HostManagement remoteHost = getRemoteHost(hostName);
		if (remoteHost != null) {
			return remoteHost.ping(hostName);
		}
		return false;
	}

	
	@Override
	public boolean pingServer(String hostName, String vmName) {
		return true;
	}
	
	@Override
	public void shutdown(String hostName) throws MetaMatrixComponentException{
		if (isRootHost(hostName)){
	        try {
				killServers(host.getFullName(), true);
			} catch (MetaMatrixComponentException e) {
				LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, e.getMessage());
			}
	        this.monitor.hostRemoved(this.host.getFullName());
	        System.exit(0);
		}
		else {
			HostManagement remoteHost = getRemoteHost(hostName);
			if (remoteHost != null) {
				remoteHost.shutdown(hostName);
			}
			else {
				throw new MetaMatrixComponentException("HostController for host = " + hostName + " can not be reached"); //$NON-NLS-1$ //$NON-NLS-2$
			}			
		}
	}
	
	@Override
	public void shutdownCluster() throws MetaMatrixComponentException{
		List<HostControllerRegistryBinding> allhostss = this.registry.getHosts();
		for(HostControllerRegistryBinding mhost:allhostss) {
			try {
				mhost.getHostController().shutdown(mhost.getHostName());
			} catch (MetaMatrixComponentException e) {
				LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Failed to shutdown servers on "+ mhost.getHostName()); //$NON-NLS-1$
			}
		}		
	}
	
	@Override
	public void startServer(String hostName, String vmName) throws MetaMatrixComponentException {
		if (isRootHost(hostName)) {
	    	try {
				LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, "StartVM " + vmName); //$NON-NLS-1$
				CurrentConfiguration.verifyBootstrapProperties();
				ConfigurationModelContainer currentConfig = CurrentConfiguration.getConfigurationModel();
	
				VMComponentDefn deployedVM = currentConfig.getConfiguration().getVMForHost(this.host.getFullName(), vmName);
	
				if (deployedVM != null) {
					startVM(deployedVM, this.host.getFullName(), currentConfig);
				}
			} catch (ConfigurationException e) {
				LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Error starting the vm = "+vmName); //$NON-NLS-1$
			} 
		}
		else {
			HostManagement remoteHost = getRemoteHost(hostName);
			if (remoteHost != null) {
				remoteHost.startServer(hostName, vmName);
			}
			else {
				throw new MetaMatrixComponentException("HostController for host = " + hostName + " can not be reached"); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
        
	}
	
	@Override
	public void startServers(String hostName) throws MetaMatrixComponentException {
       	
       	if (isRootHost(hostName)) {
	        try {
	        	hostName = this.host.getFullName();
	        	LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER,"StartAllVMs on Host " + hostName); //$NON-NLS-1$
				CurrentConfiguration.verifyBootstrapProperties();
				ConfigurationModelContainer currentConfig = CurrentConfiguration.getConfigurationModel();
				Collection deployedVMs = currentConfig.getConfiguration().getVMsForHost(hostName);
	
				if ( deployedVMs != null && deployedVMs.size() > 0) {
				    Iterator vmIterator = deployedVMs.iterator();
	
				    while ( vmIterator.hasNext() ) {
				        VMComponentDefn deployedVM = (VMComponentDefn) vmIterator.next();
				        startVM(deployedVM, hostName, currentConfig);
				    }
				} else {
				    String msg = "Unable to start VM's on host \"" + hostName +	 //$NON-NLS-1$
				                "\" due to the configuration has no VM's deployed to this host."; //$NON-NLS-1$
				    LogManager.logInfo(LogCommonConstants.CTX_CONTROLLER, msg);
				}
			} catch (ConfigurationException e) {
				LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Error starting the vms"); //$NON-NLS-1$
			}	
       	}
       	else {
			HostManagement remoteHost = getRemoteHost(hostName);
			if (remoteHost != null) {
				remoteHost.startServers(hostName);
			}
			else {
				throw new MetaMatrixComponentException("HostController for host = " + hostName + " can not be reached"); //$NON-NLS-1$ //$NON-NLS-2$
			}       		
       	}
	}

	@Override
	public void bounceAllServersInCluster() {
		killAllServersInCluster();
		startAllServersInCluster();
	}

	@Override
	public void bounceServers(String hostName) throws MetaMatrixComponentException {
		if (isRootHost(hostName)) {
			
			killServers(hostName, false);
			startServers(hostName);
			
		}
		else {
			HostManagement remoteHost = getRemoteHost(hostName);
			if (remoteHost != null) {
				remoteHost.bounceServers(hostName);
			}
			else {
				throw new MetaMatrixComponentException("HostController for host = " + hostName + " can not be reached"); //$NON-NLS-1$ //$NON-NLS-2$
			}			
		}
		
	}

	@Override
	public void killAllServersInCluster() {
		List<HostControllerRegistryBinding> allhostss = this.registry.getHosts();
		for(HostControllerRegistryBinding mhost:allhostss) {
			try {
				mhost.getHostController().killServers(mhost.getHostName(), false);
			} catch (MetaMatrixComponentException e) {
				LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Failed to start servers on "+ mhost.getHostName()); //$NON-NLS-1$
			}
		}		
	}

	@Override
	public void startAllServersInCluster() {
		
		List<HostControllerRegistryBinding> allhostss = this.registry.getHosts();
		for(HostControllerRegistryBinding mhost:allhostss) {
			try {
				mhost.getHostController().startServers(mhost.getHostName());
			} catch (MetaMatrixComponentException e) {
				LogManager.logError(LogCommonConstants.CTX_CONTROLLER, e, "Failed to start servers on "+ mhost.getHostName()); //$NON-NLS-1$
			}
		}
	}
   
}

