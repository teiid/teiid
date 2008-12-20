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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.StartupStateController;
import com.metamatrix.common.config.api.ConfigurationModelContainer;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostType;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.net.SocketHelper;
import com.metamatrix.common.util.NetUtils;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.VMRegistryBinding;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;
import com.metamatrix.platform.util.MetaMatrixController;
import com.metamatrix.platform.util.VMResources;
import com.metamatrix.platform.vm.util.VMUtils;

public class HostController extends Thread {

    private final static int COMMAND_START_ALL_VMS       = 0; // Start all deployed vms for this host.
    private final static int COMMAND_START_VM            = 1; // Start deployed vm for this host.
    private final static int COMMAND_KILL_ALL_VMS        = 2; // kill all vms.
    private final static int COMMAND_KILL_VM             = 3; // kill vm
    private final static int COMMAND_PING                = 4;
    private final static int COMMAND_EXIT                = 5; // Kill all vm's and die.
    private final static int COMMAND_INVALID             = 6;

    public final static String START_ALL_VMS = "StartAllVMs"; //$NON-NLS-1$
    public final static String START_VM = "StartVM"; //$NON-NLS-1$
    public final static String KILL_ALL_VMS = "KillAllVMs"; //$NON-NLS-1$
    public final static String KILL_VM = "KillVM"; //$NON-NLS-1$
    public final static String EXIT = "Exit"; //$NON-NLS-1$
    public final static String PING = "Ping"; //$NON-NLS-1$

    public final static int DEFAULT_WAIT_TIME = 1 * 60; // 1 minute
    public final static int DEFAULT_PORT = 15001;
    private int port = DEFAULT_PORT;

    private final static String[] commands = {"StartAllVMs", "StartVM", "KillAllVMs", "KillVM", "Ping", "Exit" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
    
    private static final String DEFAULT_JAVA_MAIN = "com.metamatrix.server.Main"; //$NON-NLS-1$
 
    private Host host;
    
    private Map processMap = new HashMap(3);
    private ServerSocket serverSocket;
    boolean hcKilled = false;
    private boolean isListening = false;
    private StarterThread starterThread = null;
        
    private ClusteredRegistryState registry;

    // Initialized  I18N namespaces
    static {
		VMResources.initResourceBundles();
    }


    /**
     * Construct HostController
     * @param host HostName
     * @param port Port to listen on. If -1 then get port from operational configuration
     * @param startMetaMatrix - if true same as startMM, nextStartup config is copied to operational and all processes are started on all hosts
     * @param startProcess - if true local process are started (overridden by startMetaMatrix
     * @param wait - time in seconds to wait for appServer to startup before attempting to start processes.
     */
    public HostController(Host host, boolean startProcesses, int wait) throws Exception {
        super("HostControllerThread"); //$NON-NLS-1$
        
        this.host = host;
        this.port = Integer.parseInt(host.getPort());
        
        init();
                
		Injector injector = Guice.createInjector(new HostControllerGuiceModule(host, "hostControllerProcess")); //$NON-NLS-1$
		this.registry = injector.getInstance(ClusteredRegistryState.class);
        
        addShutdown();        
        
        if (startProcesses ) {
            starterThread = new StarterThread(wait);
            starterThread.start();
        }
    }
    
    private void init() {

    	try {
            VMNaming.setLogicalHostName(host.getFullName());
            VMNaming.setHostAddress(host.getHostAddress());
            VMNaming.setBindAddress(host.getBindAddress());
            
            // setup the log file
            String hostFileName = StringUtil.replaceAll(host.getFullName(), ".", "_"); //$NON-NLS-1$ //$NON-NLS-2$
            
            VMUtils.startLogFile(host.getLogDirectory(), hostFileName + "_hc.log"); //$NON-NLS-1$
            
            // only clean the data directory if the vm(s) are not running
            if (!isAProcessRunning()) {
                
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
        } catch (Exception e) {
            logMessage(PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0001, host.getFullName()));
        }
                
    }    
         

    private void addShutdown() {
        try {
            logInfo(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0001));
            Runtime.getRuntime().addShutdownHook(new ShutdownThread(this));
        } catch (Exception e) {
            logInfo(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0002));
            // If running as an NT service, we cannot add a shutdown hook.
            // -Xrs flag is required in java command line.
            // This prevents service from being terminated when user logs off.
        }

    }    

    void closeServerSocket() {
        try {
            logInfo(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0003));
            serverSocket.close();
        } catch (Exception e) {
            logCritical("ERROR " + PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0002));//$NON-NLS-1$
        }
    }

    boolean isListening() {
        return this.isListening;
    }

    String getHostname() {
        return host.getFullName();
    }
    
    Host getHost() {
        return host;
    }
    
    boolean isAProcessRunning() {
        try {
            List<VMRegistryBinding> vms = this.registry.getVMs(null);
            for (VMRegistryBinding vm:vms) {
                if (getHostname().equalsIgnoreCase(vm.getHostName())) {
                    try {
                        return true;
                    } catch (Exception e) {
                        //Ignore, we tried to gracefully shutdown, vm will get killed during MetaMatrixController.startserver()
                    }
                }
            }
        } catch (Throwable err) {
            //Ignore we are 
        }
        return false;
    }
    
    public void run() {

        logInfo(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0004, getHostname()));
        try {
            // the listener should use the bindaddress
			InetAddress inet = InetAddress.getByName(this.host.getBindAddress());
			logMessage(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0012, Integer.toString(port), this.host.getBindAddress()));
            serverSocket = SocketHelper.getInternalServerSocket(port, 50, inet);
        } catch (Exception e) {
            logCritical("ERROR " + PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0003,  port));//$NON-NLS-1$
            System.exit(1);
        }
        
        this.isListening = true;
        logMessage(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0005, port));
        
        try {
            while (true) {
                Socket socket = serverSocket.accept();
                ServerThread thread = new ServerThread(this, socket);
                thread.start();
            }
        } catch (Exception e) {
            if (!hcKilled) { // if ^C was entered then this exception is expected.
                logCritical("ERROR " + PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0004));//$NON-NLS-1$
                e.printStackTrace();
            }
        }
    }

    public static void logInfo(String msg) {
        System.out.println(new Date(System.currentTimeMillis()) + " : " + msg); //$NON-NLS-1$
    }

    public static void logCritical(String msg) {
        System.out.println("-------------------------------------------------------\n"); //$NON-NLS-1$
        System.out.println(new Date(System.currentTimeMillis()) + " : " + msg); //$NON-NLS-1$
        System.out.println("-------------------------------------------------------\n"); //$NON-NLS-1$
    }
    
    void doStartAllVMs() {
        logMessage("StartAllVMs"); //$NON-NLS-1$

        try {
            String hostname = getHostname();
            CurrentConfiguration.verifyBootstrapProperties();
            ConfigurationModelContainer currentConfig = CurrentConfiguration.getConfigurationModel();
            Collection deployedVMs = currentConfig.getConfiguration().getVMsForHost(hostname);

            if ( deployedVMs != null && deployedVMs.size() > 0) {
                Iterator vmIterator = deployedVMs.iterator();

                while ( vmIterator.hasNext() ) {
                    VMComponentDefn deployedVM = (VMComponentDefn) vmIterator.next();
                    startVM(deployedVM, hostname, currentConfig);
                }
            } else {
                String msg = "Unable to start VM's on host \"" + hostname +	 //$NON-NLS-1$
                            "\" due to the configuration has no VM's deployed to this host."; //$NON-NLS-1$
                logMessage(msg);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            logMessage(e.getMessage());
        }
    }

    private void doStartVM(String vmName) {

        logMessage("StartVM " + vmName); //$NON-NLS-1$

        try {
            Host h = getHost();
            CurrentConfiguration.verifyBootstrapProperties();
            ConfigurationModelContainer currentConfig = CurrentConfiguration.getConfigurationModel();

            VMComponentDefn deployedVM = currentConfig.getConfiguration().getVMForHost(h.getFullName(), vmName);

            if (deployedVM != null) {
            	startVM(deployedVM, h.getFullName(), currentConfig);
            } 
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void startVM(VMComponentDefn deployedVM, String hostname, ConfigurationModelContainer currentConfig) throws Exception {
        // Get all of the properties for the deployed VM plus all of the
        // properties inherited from parents and the configuration.
        // The properties defined on this VM override inherited properties.
        Properties vmPropsAndConfigProps = new Properties();
        Properties props = currentConfig.getDefaultPropertyValues(deployedVM.getComponentTypeID());
        Properties vmProps = currentConfig.getConfiguration().getAllPropertiesForComponent(deployedVM.getID());
        vmPropsAndConfigProps.putAll(props);
        vmPropsAndConfigProps.putAll(getHost().getProperties());                    
        vmPropsAndConfigProps.putAll(vmProps);

        // pass the instance name and its properties
        startVM( deployedVM.getID().getName(), hostname, deployedVM.isEnabled(), vmPropsAndConfigProps );
  	
    }

    private void doKillAllVMs() {

        logMessage("KillAllVMs"); //$NON-NLS-1$
        
        // must copy the map so that when
        // doKillVM tries to remove it from the map
        // while its being iterated over.
        Map copyMap = new HashMap();
        copyMap.putAll(processMap);

        Iterator processes = copyMap.keySet().iterator();
        while (processes.hasNext()) {
            String vmName = (String) processes.next();
            doKillVM(vmName);
        }
        processMap.clear();
    }

    private void doKillVM(String vmName) {

        logMessage("KillVM " + vmName); //$NON-NLS-1$
        
        // find the vm and shut it down.
        List<VMRegistryBinding> vms = HostController.this.registry.getVMs(null);
        for (VMRegistryBinding vm:vms) {
            if (vm.getHostName().equalsIgnoreCase(getHostname())) {
                if (vm.getVMName().equalsIgnoreCase(vmName)) {
                    try {
                        vm.getVMController().shutdownNow();
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

    private void doPing() {
    }

    private void doExit() {
        doKillAllVMs();
        System.exit(0);
    }
         

    private void startVM(String vmName, String hostName, boolean enabled, Properties props) throws Exception {

        String name = vmName.toUpperCase();
        if (processMap.containsKey(name)) {
            logMessage(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0011,vmName));
            doKillVM(vmName);
        }
        
        if (enabled) {
            processMap.put(name, startDeployVM(vmName, hostName, props));
        } else {
            logMessage(PlatformPlugin.Util.getString("HostController.VM_is_not_enabled_to_start", vmName));//$NON-NLS-1$
        }
    }    


    class StarterThread extends Thread {
        private int wait;

        public StarterThread(int wait) {
            super("StarterThread");  //$NON-NLS-1$
            this.wait = wait;
        }

        public void run() {
            // wait for host controller to start listening.
            while (!isListening()) {
                try {
                    Thread.sleep(wait);
                } catch (Exception e) {}
            }
            try {
                // start processes on this host using operational configuration
                logInfo(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0008, host));
                doStartAllVMs();

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0007));
            }
        }
    }

    private class ShutdownThread extends Thread {

        private HostController hostController;

        public ShutdownThread(HostController hc) {
            super("ShutdownTread"); //$NON-NLS-1$
            hostController = hc;
        }

        public void run() {
            hostController.hcKilled = true;
            hostController.closeServerSocket();
            
            // loop through and shutdown all vms for this host
            try {
                List<VMRegistryBinding> vms = HostController.this.registry.getVMs(null);
                for (VMRegistryBinding vm:vms) {
                    if (hostController.getHostname().equalsIgnoreCase(vm.getHostName())) {
                        try {
                            vm.getVMController().shutdown();
                        } catch (Exception e) {
                            //Ignore, we tried to gracefully shutdown, vm will get killed during MetaMatrixController.startserver()
                        }
                    }
                }
            } catch (Throwable err) {
                //Ignore we are 
            }

            Iterator processes = processMap.keySet().iterator();
            while (processes.hasNext()) {
                String vmName = (String) processes.next();
                logMessage(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0009, vmName));
                Process process = (Process) processMap.get(vmName);
                process.destroy();
                process = null;
            }
            processMap.clear();
        }
    }

    private class ServerThread extends Thread {

        private BufferedReader in;
        private Socket socket;
        private HostController hc;

        public ServerThread(HostController hostController, Socket socket) throws Exception {
            this.socket = socket;
            this.hc = hostController;
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        }
        
        public void run() {
            boolean done = false;
            while (!done) {
                String command = readCommand();
                if (command != null) {
                    processCommand(command);
                } else {
                    done = true;
                }
            }
            try {
                socket.close();
            } catch (Exception e) {
            }
        }

        private String readCommand() {

            try {
                String line = in.readLine();
                return line;
            } catch (IOException e) {
                e.printStackTrace();
                logMessage(PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0008));
                // Defect 16329 - return null so that this thread does not continue to read from the inputstream.
                return null;
            }
        }

        private void processCommand( String commandLine ) {

            List parsedCommand = StringUtil.split(commandLine, " \t"); //$NON-NLS-1$
            int numTokens = parsedCommand.size();

            if(numTokens == 0) {
                return;
            }

            // Pull command string out
            String command = parsedCommand.get(0).toString().toLowerCase();

            int commandType = COMMAND_INVALID;
            for(int i = 0; i < COMMAND_INVALID; i++) {
                if(command.equalsIgnoreCase(commands[i])) {
                    commandType = i;
                    break;
                }
            }

            switch (commandType) {

                case COMMAND_START_ALL_VMS:
                    hc.doStartAllVMs();
                    break;

                case COMMAND_START_VM:
                    if (numTokens < 2) {
                        logMessage(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0010));
                    } else {
                        String vmName = (String) parsedCommand.get(1);
                        hc.doStartVM(vmName);
                    }
                    break;

                case COMMAND_KILL_ALL_VMS:
                    hc.doKillAllVMs();
                    break;

                case COMMAND_KILL_VM:
                    if (numTokens < 2) {
                        logMessage(PlatformPlugin.Util.getString(LogMessageKeys.HOST_0010));
                    } else {
                        String vmName = (String) parsedCommand.get(1);
                        hc.doKillVM(vmName);
                    }
                    break;

                case COMMAND_PING:
                    hc.doPing();
                    break;

                case COMMAND_EXIT:
                    hc.doExit();
                    break;

                case COMMAND_INVALID:
                    break;
            }
        }

  
    }

    private void logMessage(String msg) {
        System.out.println(new Date(System.currentTimeMillis()) + " : " + msg); //$NON-NLS-1$
    }

    private static void printUsage() {

        String msg = "java com.metamatrix.platform.host.HostController [-port portNum] [-config hostName] [-noprocesses] [-help]" + //$NON-NLS-1$
                     "\nWhere:" + //$NON-NLS-1$
                     "\n                  -help" + //$NON-NLS-1$
                     "\n                  -config hostName indicates specific host configuration to use" + //$NON-NLS-1$
                     "\n                  -noprocesses indicates to not start the processes" ; //$NON-NLS-1$

        System.out.println(msg);
    }
    
    /**
     * Optional arg; portNum
     * if exists then hostController listens on portNum
     * else hostController reads in hostPort from CurrentConfiguration.
     */
    public static void main(String args[]) {
        HostController controller = null;
        String hostname = null;
        int wait = DEFAULT_WAIT_TIME;
        boolean startProcesses = true;
            

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
            
            logCritical(logMsg);
           
            // find the host in the last used configuration 
            Host host = CurrentConfiguration.findHost(hostname);
            
            if (host != null) {
                if (isAHostRunning(host)) {                            
                   logCritical(PlatformPlugin.Util.getString("HostController.Host_is_already_running_startprocesses", host.getFullName())); //$NON-NLS-1$ 
                    MetaMatrixController.startHost(host.getName());
                    System.exit(1);
                }
            }                       
            
            checkToPromoteNextStartupConfig();
            
            // if the host was not resolved,
            // then try after the configuration was updated
            if (host == null) {
                host = CurrentConfiguration.findHost(hostname);
                if (host == null) {
                    logCritical("ERROR " + PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0001, hostname)); //$NON-NLS-1$
                    System.exit(-1);
                }
            }           

            controller = new HostController(host, startProcesses, wait);
            controller.start();
        } catch (Exception e) {
            logCritical("ERROR " + PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0011));//$NON-NLS-1$
            System.exit(1);
        }
    }
    
    private static boolean checkToPromoteNextStartupConfig(){
      // if another host is not running, then it must be assumed that this is the first
      // and that the configuration must be initialized
		try {
			Collection<Host> hosts = CurrentConfiguration.getConfigurationModel().getHosts();
			for(Host h:hosts) {
				if (!isAHostRunning(h)) {
					StartupStateController.performSystemInitialization(true);
					return true;
				}
			}
		} catch (Exception e) {
			// ignore..
		}
		return false;
    }
       
   private static boolean isAHostRunning(Host host) {
        return MetaMatrixController.pingHost(host);
    }      
    
   
   private Process startDeployVM( String vmName, String hostName, Properties vmprops) {
	   logInfo("Start deploy VM " + vmName + "on host"+ hostName); //$NON-NLS-1$ //$NON-NLS-2$
       String command = buildVMCommand(vmprops);
       return execCommand(command);
   }

   private String buildVMCommand(Properties vmprops) {
	   String java = vmprops.getProperty(HostType.JAVA_EXEC, "java"); //$NON-NLS-1$
	   String java_opts = vmprops.getProperty(VMComponentDefnType.JAVA_OPTS, ""); //$NON-NLS-1$
	   String java_main = vmprops.getProperty(VMComponentDefnType.JAVA_MAIN, DEFAULT_JAVA_MAIN);
	   String java_args = vmprops.getProperty(VMComponentDefnType.JAVA_ARGS, ""); //$NON-NLS-1$
	   
	   java = replaceToken(java, vmprops);
	   java_opts = replaceToken(java_opts, vmprops);
	   java_main = replaceToken(java_main, vmprops);
	   java_args = replaceToken(java_args, vmprops);
   
	   String cmd = java + " " + java_main +" " +java_args; //$NON-NLS-1$ //$NON-NLS-2$ 
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
           logInfo( PlatformPlugin.Util.getString(LogMessageKeys.VM_0049, cmd));
           process = rt.exec(cmd);
           logInfo(PlatformPlugin.Util.getString(LogMessageKeys.VM_0050));
       } catch (Exception e) {
           System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.VM_0051, cmd));
           logCritical(e.getMessage());
       }
       return process;
   }
   
}

