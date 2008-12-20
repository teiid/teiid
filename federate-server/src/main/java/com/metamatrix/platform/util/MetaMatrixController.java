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

package com.metamatrix.platform.util;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.StartupStateController;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.HostType;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.net.SocketHelper;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.NetUtils;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.metadata.runtime.StartupVDBDeleteUtility;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentConnection;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentUtil;
import com.metamatrix.server.HostController;

/**
 *  Utility class used to start the MetaMatrix Server
 */
public class MetaMatrixController {

    private static Configuration currentConfig;
    private static boolean loggingEnabled = false;
    public static final String LOGON_API  = "com.metamatrix.platform.security.api.LogonAPIHome"; //$NON-NLS-1$
    
    static {
        try {
            MetaMatrixController.refreshConfiguration();
        } catch (Exception e) {
            
        }
    }
    /**
     * Start all deployed services in all hosts/process in the system.
     * <br>This method employes default behavior that runs session cleanup -
     * All active sessions will be terminated.  Thus, it should only be
     * called when the MetaMatrix system is initially comming up.</br>
     * @param forceStartup Force the configuration next start up.
     */
    public static void startServer(boolean forceStartup) throws Exception {
        boolean cleanupSessions = true;
        doStartServer(forceStartup, cleanupSessions);
    }

    /**
     * Start all deployed services in all hosts/process in the system.
     * <br>This method allows the choice of whether or not to terminate
     * active sessions. </br>
     * @param forceStartup Force the configuration next start up.
     * @param cleanupSessions If <code>true</code>, terminate any active
     * sessions found in the session table.
     */
    public static void startServer(boolean forceStartup, boolean cleanupSessions) throws Exception {
        doStartServer(forceStartup, cleanupSessions);
    }

    /**
     * Start all deployed services in all hosts/process in the system.
     */
    private synchronized static void doStartServer(boolean forceStartup, boolean cleanupSessions) throws Exception {

        String command = HostController.START_ALL_VMS + "\n"; //$NON-NLS-1$
        String error = null;
        
        // Set system State to STARTING
        log("Copying NextStartup configuration to Startup configuration.", true); //$NON-NLS-1$
        StartupStateController.performSystemInitialization(forceStartup);
        
        // get updated ino.
        
        refreshConfiguration();
        
        // save the configuration to the local file system
		saveCurrentConfigurationToFile();

        if ( cleanupSessions ) {
            // Kill zombie sessions
            cleanupSessions();
        }

        Iterator hostIter = getCurrentConfiguration().getHosts().iterator();
        while (hostIter.hasNext()) {
            Host host = (Host) hostIter.next();
                try {
                    canHostBestarted(host);
                    log("Starting up processes on " + host.getName(), true); //$NON-NLS-1$
    
                    sendCommand(host, command);
                } catch (Exception e) {
                    if (error == null) {
                        error = "System may not have started correctly, error connecting to the following host(s): "; //$NON-NLS-1$
                    }
                    error = error + host.getName() + " "; //$NON-NLS-1$
                }
        }

        // Set system state to STARTED
       // [vah] 102202 - with changes to configuration, the initialization
       // is done in one step.
//        StartupStateController.finishSystemInitialization();

        if (error != null) {
            log(error, true);
        }
    }

    /**
     * Kill all deployed VMs on all hosts in the system.
     */
    public synchronized static void killServer() throws Exception {

        String command = HostController.KILL_ALL_VMS + "\n"; //$NON-NLS-1$
        String error = null;

        // get updated ino.
         refreshConfiguration();

		// get the host prior to indicating system shutdown
        Iterator hostIter = getCurrentConfiguration().getHosts().iterator();
        
        // Set the system state to STOPPED
        StartupStateController.indicateSystemShutdown();
         
        while (hostIter.hasNext()) {
            Host host = (Host) hostIter.next();

            try {
                if (pingHost(host)) {

                    log("Killing all processes on " + host.getHostAddress()); //$NON-NLS-1$
                
                    sendCommand(host, command);
                } else {
                    log("Host " + host.getHostAddress() + " is not alive to kill processes.");//$NON-NLS-1$ //$NON-NLS-2$
                }
            } catch (Exception e) {
                if (error == null) {
                    error = "System may not have shutdown correctly, error connecting to the following host(s): "; //$NON-NLS-1$
                }
                error = error + host.getHostAddress() + " "; //$NON-NLS-1$
            }
        }

        if (error != null) {
            log(error,true);
        }
    }

    /**
     * Start process on specified host.
     */
    public synchronized static void startProcess(String hostName, String process) throws Exception {

        if (hostName == null || hostName.length() == 0) {
            throw new IllegalArgumentException("hostName must not be null or empty"); //$NON-NLS-1$
        }

        if (process == null || process.length() == 0) {
            throw new IllegalArgumentException("process must not be null or empty"); //$NON-NLS-1$
        }

        // get updated ino.
        refreshConfiguration();
        
        Host host = getHost(hostName); 
        //currentConfig.getHost(hostName);

        String command = HostController.START_VM + " " + process + "\n"; //$NON-NLS-1$ //$NON-NLS-2$

        // get host to connect to.
        try {
            
            Collection vms = currentConfig.getVMsForHost((HostID) host.getID());

            Iterator vmIter = vms.iterator(); 
            VMComponentDefn vmDefn = null;
            while (vmIter.hasNext()) {
                vmDefn = (VMComponentDefn) vmIter.next();
                if (vmDefn.getName().equalsIgnoreCase(process)) {
                    break;
                }
                vmDefn=null;
            }
            if (vmDefn == null) {
                String msg = "Process " + process + "on host " + hostName + " not found: Must be deployed to start."; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                throw new Exception(msg);
            }
        } catch (Exception e) {
            log("Error reading configuration."); //$NON-NLS-1$
            throw e;
        }

        log("Starting process " + process + " on " + host); //$NON-NLS-1$ //$NON-NLS-2$
        sendCommand(host,command);
    }

    /**
     * Kill process on specified host.
     */
    public synchronized static void killProcess(String hostName, String process) throws Exception {

        if (hostName == null || hostName.length() == 0) {
            throw new IllegalArgumentException("hostName must not be null or empty"); //$NON-NLS-1$
        }

        if (process == null || process.length() == 0) {
            throw new IllegalArgumentException("process must not be null or empty"); //$NON-NLS-1$
        }

        // get updated ino.
        refreshConfiguration();
        
        Host host = getHost(hostName);      

        String command = HostController.KILL_VM + " " + process + "\n"; //$NON-NLS-1$ //$NON-NLS-2$

        log("Killing process " + process + " on " + hostName); //$NON-NLS-1$ //$NON-NLS-2$
        sendCommand(host,command);
    }


    /**
     * Start all processes on the host.
     */
    public synchronized static void startHost(String host) throws Exception {

        // get updated ino.
        refreshConfiguration();
        
        Host h = getHost(host); 
        

        canHostBestarted(h);
        
        String command = HostController.START_ALL_VMS + "\n"; //$NON-NLS-1$
        log("Starting all processes on " + host); //$NON-NLS-1$
        sendCommand(h, command);
        
    }
    
    private static final int WAIT_TIME = 5000; // 5 seconds
    protected static void canHostBestarted(Host host) throws Exception {
        final int retries = 5;
        
        if (host == null) {            
            String msg = PlatformPlugin.Util.getString("MetaMatrixController.Host_not_defined_in_configuration", "Unknown Host");//$NON-NLS-1$ //$NON-NLS-2$
            throw new MetaMatrixException(msg);
        }

        int cnt =0;
        do {
            // Verify the host is running, must be able to ping it
			boolean isAlive;
			try {
				isAlive = pingHostController(host.getFullName());
			} catch (UnknownHostException e) {
				LogManager.logError(LogCommonConstants.CTX_CONTROLLER, "Unable to ping bindaddress: " + host.getHostAddress()); //$NON-NLS-1$
				continue;
			}
            
            if (isAlive) {
                // host is running
                // Verify the host has at least one VM deployed to it, otherwise
                // cant start it
                Collection vms = currentConfig.getVMsForHost((HostID) host.getID());
                if (vms == null || vms.size() == 0) {
                    String msg = PlatformPlugin.Util.getString("MetaMatrixController.No_VMS_setup_for_host", host.getFullName());//$NON-NLS-1$
                    throw new MetaMatrixException(msg);
                }
                return;
                
            }
            
            // wait, just in case the hostcontroller is just coming up
            Thread.sleep(WAIT_TIME);
            ++cnt;
        
        } while (cnt <= retries);

        String msg = PlatformPlugin.Util.getString("MetaMatrixController.Host_is_not_running", new Object[] {host.getFullName(), host.getPort()});//$NON-NLS-1$
        LogManager.logWarning(LogCommonConstants.CTX_CONTROLLER, msg);
        throw new MetaMatrixException(msg);

        
    }

    /**
     * Kill all processes on the host.
     */
    public synchronized static void killHost(String host) throws Exception {

        // get updated ino.
        refreshConfiguration();

        Host h = getHost(host);       

        String command = HostController.KILL_ALL_VMS + "\n"; //$NON-NLS-1$
        log("Stopping all processes on " + host); //$NON-NLS-1$
        sendCommand(h, command);
    }

    /**
     * Kill Host controller on host.
     */
    public synchronized static void killHostController(String host) throws Exception {
        Host h = getHost(host); 

        
        String command = HostController.EXIT + "\n"; //$NON-NLS-1$
        log("Killing HostController on " + host); //$NON-NLS-1$
        sendCommand(h, command);
    }

    /**
     * Ping host controller. Return false if ping fails.
     */
    public synchronized static boolean pingHostController(String host) throws Exception {
        // get updated ino.
        refreshConfiguration();
        
        Host h = getHost(host); 

        return pingHost(h);
    }

	public static boolean pingHost(Host h) {
        log("Pinging " + h.getFullName()); //$NON-NLS-1$
		try {
            sendCommand(h, HostController.PING + "\n"); //$NON-NLS-1$
            log(h.getFullName() + " is alive at address " + h.getHostAddress()); //$NON-NLS-1$
            return true;
        } catch (Exception e) {
        	log(h.getFullName() + " is not listening at address " + h.getHostAddress()); //$NON-NLS-1$
            return false;
        }
	}

    /**
     * Cleans the session table of zombie entries.  A zombie entry is one that's marked ACTIVE when
     * no client is using it. Since we're starting the system now, we're sure that any session
     * entries that are marked ACTIVE are zombies.
     */
    private static void cleanupSessions() {
        //Delete VDB versions marked for deletion if no sessions are logged in
        //using them, or if this is the last session.
        try {
            VDBDeleteUtility vdbDeleter = new StartupVDBDeleteUtility();
            vdbDeleter.deleteVDBsMarkedForDelete();
        } catch (Exception e) {
            log(e, "Warning: failed to delete VDBs that were marked for deletion."); //$NON-NLS-1$
        }
        
    }
    
    

    // insure we are getting latest config.
    private synchronized static void refreshConfiguration() throws Exception{
        currentConfig = CurrentConfiguration.getConfiguration(true);
    }

	private synchronized static Configuration getCurrentConfiguration() throws Exception {
		if (currentConfig == null) {
            CurrentConfiguration.verifyBootstrapProperties();
      	    currentConfig = CurrentConfiguration.getConfiguration();
		}
		return currentConfig;
	}
	
	
    private static void saveCurrentConfigurationToFile() throws Exception {
        
        
        String installDir = null;
        Host host = CurrentConfiguration.getHost();
        if (host != null) {
            installDir = host.getProperty(HostType.INSTALL_DIR);
        }
        if (installDir == null || installDir.length() == 0) {
            installDir = "./";//$NON-NLS-1$
        }
         
        installDir = FileUtils.buildDirectoryPath(new String[] {installDir, "config"} ); //$NON-NLS-1$;
        
        File f = new File(installDir);
        if (!f.exists()) {
            installDir = "." + File.separator; //$NON-NLS-1$
        }
        
        FilePersistentUtil.writeModel(FilePersistentConnection.NEXT_STARTUP_FILE_NAME, installDir, 
        CurrentConfiguration.getConfigurationModel(), 
                        "MetaMatrixController"); //$NON-NLS-1$

        
    }

    
    private static void sendCommand(Host host, String command) throws IOException {

        log("Sending command to " + host.getHostAddress() + " command: " + command); //$NON-NLS-1$ //$NON-NLS-2$


        Socket socket = null;
        DataOutputStream out = null;
        try {
            InetAddress inetAddress = InetAddress.getByName(host.getHostAddress());
            int port = Integer.parseInt(host.getPort()); 
 
            socket = SocketHelper.getInternalClientSocket(inetAddress, port);
            out = new DataOutputStream(socket.getOutputStream());
            out.write(command.getBytes());
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (socket != null) {
                    socket.close();
                }
            } catch (Exception e) {
            }
        }
    }    
    
    private static void log(String msg, boolean enabled) {

        if (enabled) {
            System.out.println(new Date(System.currentTimeMillis()) + " : " + msg); //$NON-NLS-1$
        }
    }

    private static void log(String msg) {

        log(msg, loggingEnabled);
    }

    private static void log(Exception e, String msg) {

        if (loggingEnabled) {
            System.out.println(new Date(System.currentTimeMillis()) + " : " + msg); //$NON-NLS-1$
            e.printStackTrace();
        }
    }

    private static void enableLogging() {
        System.setErr(System.out);
        loggingEnabled = true;
    }

    private static void disableLogging() {
        loggingEnabled = false;
        System.setErr(System.err);
    }
    
    private static Host getHost(String hostName) throws Exception {
        
        Host host = CurrentConfiguration.findHost(hostName); 

        if (host == null) {            
            String msg = PlatformPlugin.Util.getString("MetaMatrixController.Host_not_defined_in_configuration", hostName);//$NON-NLS-1$ 
            throw new MetaMatrixException(msg);
        }  
        
        return host;
    }


    private static void printUsage() {

        String msg = "java com.metamatrix.platform.util.MetaMatrixController <command> [param] [param]" + //$NON-NLS-1$
                     "\nWhere command can be:" + //$NON-NLS-1$
                     "\n                  startServer <-force>" + //$NON-NLS-1$
                     "\n                  startHost -config <hostName>" + //$NON-NLS-1$
                     "\n                  startHost <hostName> (deprecated)" + //$NON-NLS-1$
                     "\n                  startProcess -config <hostName> <vmName>" + //$NON-NLS-1$
                     "\n                  startProcess <hostName> <vmName> (deprecated)" + //$NON-NLS-1$
                     "\n                  killServer" + //$NON-NLS-1$
                     "\n                  killHost <hostName> (deprecated)" + //$NON-NLS-1$
                     "\n                  killHost -config <hostName>" + //$NON-NLS-1$
                     "\n                  killHostController -config <hostName>" + //$NON-NLS-1$
                     "\n                  killHostController <hostName> (deprecated)" + //$NON-NLS-1$
                     "\n                  killProcess -config <hostName> <vmName>" + //$NON-NLS-1$
                     "\n                  killProcess <hostName> <vmName> (deprecated)" + //$NON-NLS-1$
                     "\n                  ping -config <hostName>" + //$NON-NLS-1$
                     "\n                  ping <hostName> (deprecated)" + //$NON-NLS-1$
                     "\n\n Note: -force indicates server should be started regardless of current state"; //$NON-NLS-1$

        System.out.println(msg);
    }

    public static void main(String[] vars) {


        String command = null;
		String host = null;
		
		try {
			host = NetUtils.getHostname();
		} catch (UnknownHostException e) {
			host = "localhost";//$NON-NLS-1$
		}
		
		
		String process = null;
		int idx = 0;
		if (vars.length == 0) {
			printUsage();
			System.exit(1);
		}

        if (vars.length > idx) {
            command = vars[idx];
            idx++;
            if (vars.length > idx) {
                String value = vars[idx];

                idx++;                
                if (value.equalsIgnoreCase("-config") ) { //$NON-NLS-1$ 
                	if (vars.length == idx) {
                        printUsage();
                        System.exit(1);
                    }
                	host = vars[idx];
                    idx++;                    
                } else {
                
                    host = value;
                }
                
                if (vars.length > idx) {
                    process = vars[idx];
                }
            }
        }

     

        enableLogging();
        try {

            if (command.equalsIgnoreCase("killServer")) { //$NON-NLS-1$
                MetaMatrixController.killServer();
                System.exit(0);
            }

            if (command.equalsIgnoreCase("startServer")) { //$NON-NLS-1$
                boolean force = false;
                if (host != null && host.equalsIgnoreCase("-force")) { //$NON-NLS-1$
                    force = true;
                }
                MetaMatrixController.startServer(force);
                System.exit(0);
            }

            if (host == null) {
                printUsage();
                System.exit(1);
            }
                        
            
            if (command.equalsIgnoreCase("killHost")) { //$NON-NLS-1$
                MetaMatrixController.killHost(host);
                System.exit(0);
            }

            if (command.equalsIgnoreCase("killHostController")) { //$NON-NLS-1$
                MetaMatrixController.killHostController(host);
                System.exit(0);
            }

            if (command.equalsIgnoreCase("startHost")) { //$NON-NLS-1$
                MetaMatrixController.startHost(host);
                System.exit(0);
            }

            if (command.equalsIgnoreCase("ping")) { //$NON-NLS-1$
                MetaMatrixController.pingHostController(host);
                
                System.exit(0);
            }

            if (process == null) {
                printUsage();
                System.exit(1);
            }

            if (command.equalsIgnoreCase("killProcess")) { //$NON-NLS-1$
                MetaMatrixController.killProcess(host, process);
                System.exit(0);
            }

            if (command.equalsIgnoreCase("startHost")) { //$NON-NLS-1$
                MetaMatrixController.startProcess(host, process);
                System.exit(0);
            }

            printUsage();
            System.exit(1);

        } catch (Exception e) {
            log(e, e.getMessage());
            System.exit(1);
        } finally {
            disableLogging();
        }
        System.exit(0);
    }
}


