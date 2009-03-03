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

package com.metamatrix.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Configuration;
import com.metamatrix.common.config.api.DeployedComponent;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.HostID;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.admin.apiimpl.RuntimeStateAdminAPIHelper;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.registry.HostControllerRegistryBinding;
import com.metamatrix.platform.registry.ServiceRegistryBinding;
import com.metamatrix.platform.registry.VMRegistryBinding;
import com.metamatrix.platform.service.api.CacheAdmin;
import com.metamatrix.platform.service.api.ServiceID;
import com.metamatrix.platform.service.api.ServiceInterface;
import com.metamatrix.platform.util.ErrorMessageKeys;
import com.metamatrix.platform.util.LogMessageKeys;
import com.metamatrix.platform.vm.api.controller.VMControllerInterface;
import com.metamatrix.platform.vm.controller.VMStatistics;

/**
 *  Utility class that allows users to view the state of the services.
 */
public class ServiceManager {

    private final static int COMMAND_LIST_VMS                = 0;
    private final static int COMMAND_LIST_SERVICES           = 1;
    private final static int COMMAND_START_VM                = 2;
    private final static int COMMAND_STOP_SERVICE            = 3;
    private final static int COMMAND_STOP_VM                 = 4;
    private final static int COMMAND_GET_SERVICE_STATUS      = 5;
    private final static int COMMAND_LIST_DEPLOYED_VMS       = 6;
    private final static int COMMAND_LIST_DEPLOYED_SERVICES  = 7;
    private final static int COMMAND_START_SERVER            = 8;
    private final static int COMMAND_KILL_ALL_VMS            = 9;
    private final static int COMMAND_LIST_DEPLOYED_HOSTS     = 10;
    private final static int COMMAND_MARK_SERVICE_AS_BAD     = 11;
    private final static int COMMAND_LIST_VM_PROPERTIES      = 12;
    private final static int COMMAND_LIST_SERVICE_PROPERTIES = 13;
    private final static int COMMAND_SHUTDOWN_SERVER         = 14;
    private final static int COMMAND_SHUTDOWN_VM             = 17;
    private final static int COMMAND_START_SERVICE           = 19;
    private final static int COMMAND_EXPERT_MODE_ON          = 20;
    private final static int COMMAND_EXPERT_MODE_OFF         = 21;
    private final static int COMMAND_GET_SERVICE_QUEUES      = 22;
    private final static int COMMAND_GET_VM_STATS            = 24;
    private final static int COMMAND_DUMP_THREADS            = 25;
    private final static int COMMAND_SYNCH_SERVER            = 26;
    private final static int COMMAND_KILL_ALL_HCS            = 27;
    private final static int COMMAND_KILL_HC                 = 28;
    private final static int COMMAND_BOUNCE_SERVICE          = 29;
    private final static int COMMAND_CLEAR_CODE_TABLE_CACHES = 30;
    private final static int COMMAND_CLEAR_PREPARED_STMT_CACHES = 31;
    private final static int COMMAND_EXIT                    = 33;
    private final static int COMMAND_HELP                    = 34;
    private final static int COMMAND_INVALID                 = 35;


    private final static String[] commands = {"ListProcesses", //$NON-NLS-1$
                                              "ListServices", //$NON-NLS-1$
                                              "StartProcess", //$NON-NLS-1$
                                              "StopService", //$NON-NLS-1$
                                              "StopProcess", //$NON-NLS-1$
                                              "GetServiceStatus", //$NON-NLS-1$
                                              "ListDeployedProcesses", //$NON-NLS-1$
                                              "ListDeployedServices", //$NON-NLS-1$
                                              "StartServer", //$NON-NLS-1$
                                              "KillAllProcesses", //$NON-NLS-1$
                                              "ListDeployedHosts", //$NON-NLS-1$
                                              "MarkServiceAsBad", //$NON-NLS-1$
                                              "ListProcessProps", //$NON-NLS-1$
                                              "ListServiceProps", //$NON-NLS-1$
                                              "ShutdownServer", //$NON-NLS-1$
                                              "ShutdownProcess", //$NON-NLS-1$
                                              "RestartService", //$NON-NLS-1$
                                              "ExpertModeOn", //$NON-NLS-1$
                                              "ExpertModeOff", //$NON-NLS-1$
                                              "GetServiceQueues", //$NON-NLS-1$
                                              "GetProcessStats", //$NON-NLS-1$
                                              "DumpThreads", //$NON-NLS-1$
                                              "Synch", //$NON-NLS-1$
                                              "KillAllHostControllers", //$NON-NLS-1$
                                              "KillHostController", //$NON-NLS-1$
											  "BounceService", //$NON-NLS-1$
											  "ClearCodeTableCaches", //$NON-NLS-1$
											  "ClearPreparedStatementCaches", //$NON-NLS-1$
                                              "Exit", //$NON-NLS-1$
                                              "Help" }; //$NON-NLS-1$

    private final static String[] shortCommands = {"lv", //$NON-NLS-1$
                                                   "ls", //$NON-NLS-1$
                                                   "StartProcess", //$NON-NLS-1$
                                                   "StopService", //$NON-NLS-1$
                                                   "StopProcess", //$NON-NLS-1$
                                                   "GetServiceStatus", //$NON-NLS-1$
                                                   "ListDeployedProcesses", //$NON-NLS-1$
                                                   "ListDeployedServices", //$NON-NLS-1$
                                                   "StartServer", //$NON-NLS-1$
                                                   "KillAllProcesses", //$NON-NLS-1$
                                                   "ListDeployedHosts", //$NON-NLS-1$
                                                   "MarkServiceAsBad", //$NON-NLS-1$
                                                   "ListProcessProps", //$NON-NLS-1$
                                                   "ListServiceProps", //$NON-NLS-1$
                                                   "ShutdownServer", //$NON-NLS-1$
                                                   "ShutdownProcess", //$NON-NLS-1$
                                                   "RestartService", //$NON-NLS-1$
                                                   "ExpertModeOn", //$NON-NLS-1$
                                                   "ExpertModeOff", //$NON-NLS-1$
                                                   "GetServiceQueues", //$NON-NLS-1$
                                                   "GetProcessStats", //$NON-NLS-1$
                                                   "DumpThreads", //$NON-NLS-1$
                                                   "Synch", //$NON-NLS-1$
                                                   "KillAllHCs", //$NON-NLS-1$
                                                   "KillHC", //$NON-NLS-1$
												   "BounceService", //$NON-NLS-1$
											  	   "ClearCodeTableCaches", //$NON-NLS-1$
											       "ClearPreparedStatementCaches", //$NON-NLS-1$
                                                   "Exit", //$NON-NLS-1$
                                                   "Help" }; //$NON-NLS-1$

    private final static String[] stateStrings = {"Not_Initialized", //$NON-NLS-1$
                                                  "Running", //$NON-NLS-1$
                                                  "Closed", //$NON-NLS-1$
                                                  "Failed", //$NON-NLS-1$
                                                  "Init_Failed", //$NON-NLS-1$
                                                  "Not_Registered",  //$NON-NLS-1$
                                                  "Data_Source_Unavailable"}; //$NON-NLS-1$

    @Inject
    private HostManagement hostManager;

    @Inject
    private ClusteredRegistryState registry;
    
    @Inject
	MessageBus messageBus;
    
    @Inject
    @Named(com.metamatrix.server.Configuration.HOST)
    private Host host;

    private Configuration currentConfig;

    private boolean expertMode = false;
    
    public ServiceManager() throws Exception {
    	this.currentConfig = CurrentConfiguration.getInstance().getConfiguration();
    }

    public void run (String command, boolean exit) {

    	if(!isHostRunning()) {
    		System.out.println("HostController is not running; You can not run Service manager without HostController running!"); //$NON-NLS-1$
    		System.out.println("Exiting.."); //$NON-NLS-1$
    		command = "Exit"; //$NON-NLS-1$
    	}
    	
        try {
            if (command != null && command.trim().length() > 0) {
            	System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0010, command));
                processCommand(command);
                if (exit) {
					processCommand("Exit"); //$NON-NLS-1$
                }
            }
            this.printUsage();
            this.startInteractiveMode();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
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
    
    private void startInteractiveMode() {
    	BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            processCommand( readCommandLine(in) );
        }
    }

    private String readCommandLine(BufferedReader in) {
        try {
            System.out.print(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0011) + " "); //$NON-NLS-1$
            String line = in.readLine();
            return line;
        } catch (IOException e) {
            System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0012));
            return ""; //$NON-NLS-1$
        }
    }

    private void processCommand( String commandLine ) {

        if (commandLine == null) {
            return;
        }
        List parsedCommand = StringUtil.splitPreservingQuotedSubstring(commandLine, " \t"); //$NON-NLS-1$
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

        if (commandType == COMMAND_INVALID) {
            for(int i = 0; i < COMMAND_INVALID; i++) {
                if(command.equalsIgnoreCase(shortCommands[i])) {
                    commandType = i;
                    break;
                }
            }
        }


        switch (commandType) {
            case COMMAND_LIST_VMS:
                doListVMs();
                break;

            case COMMAND_LIST_SERVICES:
                doListServices();
                break;

            case COMMAND_LIST_DEPLOYED_HOSTS:
                doListDeployedHosts();
                break;

            case COMMAND_LIST_DEPLOYED_VMS:
                doListDeployedVMs();
                break;

            case COMMAND_LIST_DEPLOYED_SERVICES:
                doListDeployedServices();
                break;

            case COMMAND_START_VM:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0013));
                } else {
                    String vmName = (String) parsedCommand.get(1);
                    doStartVM(vmName);
                }
                break;

            case COMMAND_STOP_VM:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0013));
                } else {
                    String vmName = (String) parsedCommand.get(1);
                    doStopVM(vmName);
                }
                break;

            case COMMAND_START_SERVER:
                doStartServer();
                break;

            case COMMAND_KILL_HC:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0014));
                } else {
                    String host = (String) parsedCommand.get(1);
                    doShutdownHC(host);
                }
                break;

            case COMMAND_KILL_ALL_HCS:
                doShutdownAllHCs();
                break;

            case COMMAND_KILL_ALL_VMS:
                doKillAllVMs();
                break;

            case COMMAND_STOP_SERVICE:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0015));
                } else {
                    String id = (String) parsedCommand.get(1);
                    long value;
                    try {
                        value = Long.parseLong(id);
                    } catch (Exception e) {
                        System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0016));
                        break;
                    }
                    doStopService(new ServiceID(value, null));
                }
                break;

            case COMMAND_GET_SERVICE_STATUS:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0015));
                } else {
                    String id = (String) parsedCommand.get(1);
                    long value;
                    try {
                        value = Long.parseLong(id);
                    } catch (Exception e) {
                        System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0016));
                        break;
                    }
                    doGetServiceStatus(new ServiceID(value, null));
                }
                break;

            case COMMAND_MARK_SERVICE_AS_BAD:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0015));
                } else {
                    String id = (String) parsedCommand.get(1);
                    long value;
                    try {
                        value = Long.parseLong(id);
                    } catch (Exception e) {
                        System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0016));
                        break;
                    }
                    doMarkServiceAsBad(new ServiceID(value, null));
                }
                break;

            case COMMAND_LIST_VM_PROPERTIES:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0013));
                } else {
                    String vmName = (String) parsedCommand.get(1);
                    doListVMProps(vmName);
                }
                break;

            case COMMAND_LIST_SERVICE_PROPERTIES:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0017));
                } else {
                    String serviceName = (String) parsedCommand.get(1);
                    doListServiceProps(serviceName);
                }
                break;

            case COMMAND_SHUTDOWN_VM:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0013));
                } else {
                    String vmName = (String) parsedCommand.get(1);
                    doShutdownVM(vmName, false);
                }
                break;

            case COMMAND_BOUNCE_SERVICE:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0031));
                } else {
                    String name = (String) parsedCommand.get(1);
                    doBounceService(name);
                }
                break;

            case COMMAND_SHUTDOWN_SERVER:
                doShutdownServer();
                break;

            case COMMAND_EXPERT_MODE_ON:
                this.expertMode = true;
                printUsage();
                break;

            case COMMAND_EXPERT_MODE_OFF:
                this.expertMode = false;
                printUsage();
                break;

            case COMMAND_GET_VM_STATS:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0013));
                } else {
                    String vmName = (String) parsedCommand.get(1);
                    doGetVMStats(vmName);
                }
                break;

            case COMMAND_DUMP_THREADS:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0013));
                } else {
                    String vmName = (String) parsedCommand.get(1);
                    doDumpThreads(vmName);
                }
                break;

            case COMMAND_GET_SERVICE_QUEUES:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0015));
                } else {
                    String id = (String) parsedCommand.get(1);
                    long value;
                    try {
                        value = Long.parseLong(id);
                    } catch (Exception e) {
                        System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0016));
                        break;
                    }
                    String queueName = null;
                    if (numTokens > 2) {
                        queueName = (String) parsedCommand.get(2);
                    }
                    doGetServiceQueues(new ServiceID(value, null), queueName);
                }
                break;

            case COMMAND_SYNCH_SERVER:
                this.doSynchronize();
                break;

            case COMMAND_EXIT:
                this.doExit();
                System.exit(0);
                break;

            case COMMAND_INVALID:
                System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0018, commandLine));
                break;

            case COMMAND_HELP:
                printUsage();
                break;

            case COMMAND_CLEAR_CODE_TABLE_CACHES:
				doClearCodeTableCaches();
                break;

            case COMMAND_CLEAR_PREPARED_STMT_CACHES:
				doClearPreparedStatementCaches();
                break;

            case COMMAND_START_SERVICE:
                if (numTokens < 2) {
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0015));
                } else {
                    String id = (String) parsedCommand.get(1);
                    long value;
                    try {
                        value = Long.parseLong(id);
                    } catch (Exception e) {
                        System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0016));
                        break;
                    }
                    doStartService(new ServiceID(value, null));
                }
                break;
        }
    }


	public void doBounceService(String name) {

    try {
		System.out.println("Bouncing service: " + name); //$NON-NLS-1$
		String serviceName = name.trim();
		
		List<ServiceRegistryBinding> bindings = this.registry.getServiceBindings(null, null);		
        if (bindings.isEmpty()) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0036));
            return;
        }

        for (ServiceRegistryBinding binding:bindings) {
			if (binding.getInstanceName().trim().equalsIgnoreCase(serviceName)) {
				try {
					System.out.println("Killing " + binding.getServiceID()); //$NON-NLS-1$
					binding.getService().dieNow();
				} catch (Exception se) {
	            	System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0063, binding.getServiceID()));
	            	se.printStackTrace();
				}
				try {
		            VMRegistryBinding vmBinding = registry.getVM(binding.getHostName(), binding.getServiceID().getVMControllerID().toString());
		            if (vmBinding != null) {
		            	vmBinding.getVMController().startService(binding.getServiceID());
		            	System.out.println("Starting " + binding.getServiceID()); //$NON-NLS-1$
		            }
		            else {
		            	System.out.println("VM not found in registry"); //$NON-NLS-1$
		            }
					
				} catch (Exception e) {
	            	System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0064, binding.getServiceID()));
		            e.printStackTrace();
				}
			}
        }
    } catch (Exception e) {
        System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0065));
        e.printStackTrace();
    }
}

	/**
	 * Method doClearPreparedStatementCaches.
	 */
	private void doClearPreparedStatementCaches() {
		doClearCaches(CacheAdmin.PREPARED_PLAN_CACHE);
	}

	/**
	 * Method doClearCodeTableCaches.
	 */
	private void doClearCodeTableCaches() {
		doClearCaches(CacheAdmin.CODE_TABLE_CACHE);
	}


	/**
	 * Method doClearPreparedStatementCaches.
	 */
	private void doClearCaches(String type) {
        try {
    		List<ServiceRegistryBinding> bindings = this.registry.getServiceBindings(null, null);		
            if (bindings.isEmpty()) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0036));
                return;
            }

            for (ServiceRegistryBinding binding:bindings) {
					try {
						ServiceInterface service = binding.getService();
						if (service instanceof CacheAdmin) {
							CacheAdmin admin = (CacheAdmin) service;
							Map caches = admin.getCaches();   // key = cache name, value = cache type
							if(caches != null) {
							    Iterator cacheIter = caches.keySet().iterator();
							    while(cacheIter.hasNext()) {
									String cacheName = (String) cacheIter.next();
									String cacheType = (String) caches.get(cacheName);
									if(cacheType.equals(type)) {
								        admin.clearCache(cacheName, null);		// properties not currently used
									}
						    	}
					  		}
						}
					} catch (Exception e) {
			            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0066, binding.getServiceID()));
            			e.printStackTrace();
					}
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0035));
            e.printStackTrace();
        }
	}

    private void printUsage() {
        if (this.expertMode) {
            printExpertUsage();
        } else {
            printNoviceUsage();
        }
    }

    private static void printExpertUsage() {

        System.out.println("Usage: svcmgr [-config hostname] <Command> [options]"); //$NON-NLS-1$
        System.out.println();
        System.out.println("BounceService <ServiceName>                 Stop and Start all services with name"); //$NON-NLS-1$
        System.out.println("ClearCodeTableCaches                        Clear code table caches"); //$NON-NLS-1$
        System.out.println("ClearPreparedStatementCaches                Clear prepared statement caches"); //$NON-NLS-1$
        System.out.println("DumpJNDI                                    Display contents of JNDI Registry"); //$NON-NLS-1$
        System.out.println("GetServiceQueues <ServiceID>                Display service queue stats"); //$NON-NLS-1$
        System.out.println("GetServiceStatus <ServiceID>                Get status of service"); //$NON-NLS-1$
        System.out.println("GetProcessStats <Process Name>              Displays stats for Process"); //$NON-NLS-1$
        System.out.println("DumpThreads <Process Name>                  Lists all running threads in the log file"); //$NON-NLS-1$
        System.out.println("ListDeployedHosts                           List all deployed hosts"); //$NON-NLS-1$
        System.out.println("ListDeployedServices                        Display all deployed services"); //$NON-NLS-1$
        System.out.println("ListDeployedProcesses                       List all deployed Processes"); //$NON-NLS-1$
        System.out.println("ListServices                                List all running Services"); //$NON-NLS-1$
        System.out.println("ListServiceProps <Name>                     List properties of service"); //$NON-NLS-1$
        System.out.println("ListProcessProps <Process Name>             List properties of process."); //$NON-NLS-1$
        System.out.println("ListProcesses                               List all running Processes"); //$NON-NLS-1$
        System.out.println("KillAllHostControllers                      Kill all HC's running in the system"); //$NON-NLS-1$
        System.out.println("KillHostController <host>                   Kill HostController running on host"); //$NON-NLS-1$
        System.out.println("KillAllProcesses                            Kill all processes running in the system"); //$NON-NLS-1$
        System.out.println("MarkServiceAsBad <ServiceID>                Mark service as bad"); //$NON-NLS-1$
        System.out.println("RestartService <ServiceID>                  Restart Service"); //$NON-NLS-1$
        System.out.println("RunGC <Process Name>                        Runs garbage collector on Process"); //$NON-NLS-1$
        System.out.println("ShutdownServer                              Gracefully shutdown all processes"); //$NON-NLS-1$
        System.out.println("ShutdownService <ServiceID>                 Shutdown Service"); //$NON-NLS-1$
        System.out.println("ShutdownServiceNow <ServiceID>              Shutdown Service Now"); //$NON-NLS-1$
        System.out.println("ShutdownProcess <Process Name>              Shutdown Process"); //$NON-NLS-1$
        System.out.println("ShutdownProcessNow <Process Name>           Shutdown Process Now"); //$NON-NLS-1$
        System.out.println("StartServer                                 Start all deployed Processes"); //$NON-NLS-1$
        System.out.println("StartProcess <Process Name>                 Start deployed Process"); //$NON-NLS-1$
        System.out.println("StopService <ServiceID>                     Stop Service"); //$NON-NLS-1$
        System.out.println("StopProcess <Process Name>                  Kill Process"); //$NON-NLS-1$
        System.out.println("Synch                                       Synchronize Registries"); //$NON-NLS-1$
        System.out.println("Exit"); //$NON-NLS-1$
        System.out.println("Help"); //$NON-NLS-1$
    }

    private static void printNoviceUsage() {

        System.out.println("Usage: svcmgr [-config hostname] <Command> [options]"); //$NON-NLS-1$
        System.out.println();
        System.out.println("ClearCodeTableCaches                        Clear code table caches"); //$NON-NLS-1$
        System.out.println("ClearPreparedStatementCaches                Clear prepared statement caches"); //$NON-NLS-1$
        System.out.println("GetServiceQueues <ServiceID>                Display service queue stats"); //$NON-NLS-1$
        System.out.println("GetProcessStats <Process Name>              Displays stats for Process"); //$NON-NLS-1$
        System.out.println("KillAllHostControllers                      Kill all HC's running in the system"); //$NON-NLS-1$
        System.out.println("KillHostController <host>                   Kill HostController running on host"); //$NON-NLS-1$
        System.out.println("ListServices                                List all running Services"); //$NON-NLS-1$
        System.out.println("ListProcesses                               List all running Processes"); //$NON-NLS-1$
        System.out.println("ShutdownServer                              Gracefully shutdown all processes"); //$NON-NLS-1$
        System.out.println("ShutdownProcess <Process Name>              Shutdown Process"); //$NON-NLS-1$
        System.out.println("StartServer                                 Start all deployed processes"); //$NON-NLS-1$
        System.out.println("StartProcess <Process Name>                 Start deployed Process"); //$NON-NLS-1$
        System.out.println("Exit"); //$NON-NLS-1$
        System.out.println("Help"); //$NON-NLS-1$
    }

    private synchronized void doExit() {
    	try {
			this.messageBus.shutdown();
		} catch (MessagingException e) {
			e.printStackTrace();
		}
    }

    private void doSynchronize() {

		// Sleep for a second to allow registry to synch up. 
		try {
			Thread.sleep(1000);
		} catch (Exception e) {
		}

		try {
			RuntimeStateAdminAPIHelper.getInstance(this.registry, this.hostManager).synchronizeServer();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    private void doGetVMStats(String vmName) {
        VMControllerInterface vm = getVMController(vmName);
        if (vm != null) {
            try {
                VMStatistics stats = vm.getVMStatistics();
                System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0019, stats.name));
                System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0020, stats.totalMemory));
                System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0021, stats.freeMemory));
                System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0022, stats.threadCount));
            } catch (Exception e) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0031, vmName));
                e.printStackTrace();
            }

        } else {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0030, vmName));
        }
    }

    private void doDumpThreads(String vmName) {
        VMControllerInterface vm = getVMController(vmName);
        if (vm != null) {
            try {
                System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0023));
                vm.dumpThreads();
                System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0024, vmName));
            } catch (Exception e) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0031, vmName));
                e.printStackTrace();
            }
        } else {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0030, vmName));
        }
    }

    private VMControllerInterface getVMController(String vmName) {

        // find vm
        try {
            Iterator vmIter = registry.getVMs(null).iterator();
            while (vmIter.hasNext()) {
                VMRegistryBinding vmBinding = (VMRegistryBinding) vmIter.next();
                if (vmBinding.getVMName().equalsIgnoreCase(vmName)) {
                    return vmBinding.getVMController();
                }
            }
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void doStartServer() {

        try {
            this.hostManager.startServers(this.host.getFullName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void doKillAllVMs() {

        try {
        	this.hostManager.killServers(this.host.getFullName(), false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void doStartVM(String vmName) {

        String host = null;
        // get host to connect to.
        try {
            Iterator vmIter = getDeployedVMs().iterator();
            while (vmIter.hasNext()) {
                VMComponentDefn vmDefn = (VMComponentDefn) vmIter.next();
                if (vmDefn.getName().equalsIgnoreCase(vmName)) {
                    host = vmDefn.getHostID().getName();
                    break;
                }
            }
            if (host == null) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0032));
                return;
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0033));
            return;
        }

        try {
        	this.hostManager.startServer(host, vmName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void doListVMProps(String vmName) {

        String host = null;
        // get host to connect to.
        try {
            Iterator vmIter = getDeployedVMs().iterator();
            while (vmIter.hasNext()) {
                VMComponentDefn vmDefn = (VMComponentDefn) vmIter.next();
                if (vmDefn.getName().equalsIgnoreCase(vmName)) {
                    host = vmDefn.getHostID().getName();
                    Properties vmPropsAndConfigProps = currentConfig.getAllPropertiesForComponent(vmDefn.getID());
                    System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0025, vmName));
                    Iterator iter = vmPropsAndConfigProps.keySet().iterator();
                    while (iter.hasNext()) {
                        String key = (String) iter.next();
                        String value = (String) vmPropsAndConfigProps.get(key);
                        System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0026, key, value));
                    }

                    break;
                }
            }
            if (host == null) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0032));
                return;
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0033));
            return;
        }
    }


    public void doListServiceProps(String serviceName) {
        try {
            Iterator hostIter = this.currentConfig.getHostIDs().iterator();
            while (hostIter.hasNext()) {
                HostID hostId = (HostID) hostIter.next();
                Iterator vmIter = currentConfig.getVMsForHost(hostId).iterator();
                while (vmIter.hasNext()) {
                    VMComponentDefn vmDefn = (VMComponentDefn) vmIter.next();
                    Iterator servicesIter = currentConfig.getDeployedServicesForVM(vmDefn).iterator();
                    while (servicesIter.hasNext()) {
                        DeployedComponent deployedService = (DeployedComponent) servicesIter.next();
                        if (deployedService.getName().equalsIgnoreCase(serviceName)) {
                            Properties serviceProps = currentConfig.getAllPropertiesForComponent(deployedService.getID());

                            System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0027, deployedService.getFullName()));
                            Iterator iter = serviceProps.keySet().iterator();
                            while (iter.hasNext()) {
                                String key = (String) iter.next();
                                String value = (String) serviceProps.get(key);
                                System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0026, key, value));
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0033));
            e.printStackTrace();
        }
    }

    public void doStopVM(String vmName) {

        // find vm
        try {
            Iterator vmIter = registry.getVMs(null).iterator();
            VMRegistryBinding vmBinding = null;
            while (vmIter.hasNext()) {
                vmBinding = (VMRegistryBinding) vmIter.next();
                if (vmBinding.getVMName().equalsIgnoreCase(vmName)) {
                    this.hostManager.killServer(vmBinding.getHostName(),vmBinding.getVMName(), false);
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0034, vmName));
            System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0028));
            killVM(vmName);
        }
    }

    private void killVM(String vmName) {

        String host = null;
        // get host to connect to.
        try {
            Iterator vmIter = getDeployedVMs().iterator();
            while (vmIter.hasNext()) {
                VMComponentDefn vmDefn = (VMComponentDefn) vmIter.next();
                if (vmDefn.getName().equalsIgnoreCase(vmName)) {
                    host = vmDefn.getHostID().getName();
                    break;
                }
            }
            if (host == null) {
                System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0029));
                return;
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0033));
            return;
        }

        try {
        	this.hostManager.killServer(host, vmName, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void doShutdownVM(String vmName, boolean now) {
        try {
            Iterator vmIter = registry.getVMs(null).iterator();
            while (vmIter.hasNext()) {
                VMRegistryBinding vmBinding = (VMRegistryBinding) vmIter.next();
                if (vmBinding.getVMName().equalsIgnoreCase(vmName)) {
                    this.hostManager.killServer(vmBinding.getHostName(), vmName, now);
                    return;
                }
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0034, vmName));
            e.printStackTrace();
        }
    }

    public void doListVMs() {
        try {

            Iterator vmIter = registry.getVMs(null).iterator();
            while (vmIter.hasNext()) {
                System.out.println(vmIter.next());
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0035));
            e.printStackTrace();
        }
    }

    public void doListServices() {
        try {
        	List<ServiceRegistryBinding> services = this.registry.getServiceBindings(null, null);
            if (services.isEmpty()) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0036));
                return;
            }

            for(ServiceRegistryBinding serviceBinding: services) {
                System.out.println(serviceBinding.getServiceID() + " " + serviceBinding.getInstanceName() + "\t"+ServiceManager.stateStrings[serviceBinding.getCurrentState()]); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0035));
            e.printStackTrace();
        }
    }

    public void doListDeployedHosts() {
        try {
            Iterator hostIter = this.currentConfig.getHostIDs().iterator();
            while (hostIter.hasNext()) {
                HostID hostId = (HostID) hostIter.next();
                System.out.println(hostId);
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0037));
            e.printStackTrace();
        }
    }

    public void doListDeployedVMs() {
        try {
            Iterator vmIter = getDeployedVMs().iterator();
            while (vmIter.hasNext()) {
                VMComponentDefn vmDefn = (VMComponentDefn) vmIter.next();
                System.out.println(vmDefn);
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0038));
        }
    }

    public List getDeployedVMs() throws Exception {

        List vms = new ArrayList();

        try {
            Collection vmDefns = this.currentConfig.getVMComponentDefns();
            
            vms.addAll( vmDefns );
            
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0033));
            e.printStackTrace();
            throw e;
        }
        return vms;
    }

    public void doListDeployedServices() {
        try {
            Iterator hostIter = this.currentConfig.getHostIDs().iterator();
            while (hostIter.hasNext()) {
                HostID hostId = (HostID) hostIter.next();
                Iterator vmIter = currentConfig.getVMsForHost(hostId).iterator();
                while (vmIter.hasNext()) {
                    VMComponentDefn vmDefn = (VMComponentDefn) vmIter.next();
                    Iterator servicesIter =currentConfig.getDeployedServicesForVM(vmDefn).iterator();
                    while (servicesIter.hasNext()) {
                        System.out.println(servicesIter.next());
                    }
                }
            }

        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0033));
            e.printStackTrace();
        }
    }

    public void doStopService(ServiceID serviceID) {

        ServiceID id = null;
        try {
            id = getServiceID(serviceID);
            if (id == null) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0039));
                return;
            }
            VMRegistryBinding vmBinding = registry.getVM(id.getHostName(), id.getVMControllerID().toString());
            if (vmBinding != null) {
            	vmBinding.getVMController().stopService(id, false, false);
            }
            else {
            	System.out.println("No VM found on host="+id.getHostName()+" with id="+id.getVMControllerID().toString()); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0015, id));
            e.printStackTrace();
        }
    }

    public void doShutdownAllHCs() {
    	System.out.println("Cluster is being shutdown"); //$NON-NLS-1$
    	try {
			this.hostManager.shutdownCluster();
		} catch (MetaMatrixComponentException e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0043));
            e.printStackTrace();			
		}
    }

    public void doShutdownHC(String host) {
        try {
            System.out.println(PlatformPlugin.Util.getString(LogMessageKeys.SERVICE_0030, host));
            this.hostManager.shutdown(host);
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0040, host));
            e.printStackTrace();
        }
    }

    public void doShutdownServer() {
	    System.out.println("All the servers in Cluster are being shutdown"); //$NON-NLS-1$
    	this.hostManager.killAllServersInCluster();
    }

    private void doGetServiceStatus(ServiceID serviceID) {
        ServiceID id = null;
        try {
            id = getServiceID(serviceID);
            if (id == null) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0039));
                return;
            }
            System.out.println(registry.getServiceBinding(id.getHostName(), id.getVMControllerID().toString(), id));
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0043));
            e.printStackTrace();
        }
    }

    private void doGetServiceQueues(ServiceID serviceID, String queueName) {

        ServiceID id = null;
        try {
            id = getServiceID(serviceID);
            if (id == null) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0039));
                return;
            }
            ServiceRegistryBinding serviceBinding = this.registry.getServiceBinding(id.getHostName(), id.getVMControllerID().toString(), id);
            
            ServiceInterface service = null;
            if (serviceBinding != null) {
            	service = serviceBinding.getService();
            }

            if (service == null ) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0046, id));
                return;                
            }
            if (queueName == null) {                
                Collection queues = service.getQueueStatistics();
                if (queues == null) {
                    System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0044));
                    return;
                }
                Iterator iter = queues.iterator();
                while (iter.hasNext()) {
                    WorkerPoolStats qs = (WorkerPoolStats) iter.next();
                    printQueueStats(qs);
                }
            } else {
                WorkerPoolStats qs = service.getQueueStatistics(queueName);
                if (qs == null) {
                    System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0044, queueName ));
                    return;
                }
                printQueueStats(qs);
            }
        } catch (NullPointerException npe) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0046, id));
                        
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0046, id));
            e.printStackTrace();
        }
    }

    private void printQueueStats(WorkerPoolStats qs) {
        System.out.println(qs);
    }


    private void doStartService(ServiceID serviceID) {
        ServiceID id = null;
        try {
            id = getServiceID(serviceID);
            if (id == null) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0039));
                return;
            }
            VMRegistryBinding vmBinding = registry.getVM(id.getHostName(), id.getVMControllerID().toString());
            if (vmBinding != null) {
            	vmBinding.getVMController().startService(id);
            }
            else {
            	System.out.println("No VM found on host="+id.getHostName()+" with id="+id.getVMControllerID().toString());   //$NON-NLS-1$ //$NON-NLS-2$         	
            }            
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0047));
            e.printStackTrace();
        }
    }

    private void doMarkServiceAsBad(ServiceID serviceID) {
        ServiceID id = null;
        try {
            id = getServiceID(serviceID);
            if (id == null) {
                System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0039));
                return;
            }
            ServiceRegistryBinding binding = this.registry.getServiceBinding(serviceID.getHostName(), serviceID.getVMControllerID().toString(), serviceID);
            if (binding != null) {
            	binding.markServiceAsBad();
            }
        } catch (Exception e) {
            System.out.println(PlatformPlugin.Util.getString(ErrorMessageKeys.SERVICE_0047));
            e.printStackTrace();
        }
    }

    private ServiceID getServiceID(ServiceID serviceID) {
    	List<ServiceRegistryBinding> bindings = this.registry.getServiceBindings(null, null);
        for (ServiceRegistryBinding binding: bindings) {
            if (serviceID.getID() == binding.getServiceID().getID()) {
                return binding.getServiceID();
            }
        }
        return null;
    }

    
	private static ServiceManager loadServiceManager(Host host) {
		Injector injector = Guice.createInjector(new ServiceManagerGuiceModule(host));
		ResourceFinder.setInjector(injector); 
		return injector.getInstance(ServiceManager.class);
	}    
   

    public static void main(String[] args) throws Exception {

        String command = ""; //$NON-NLS-1$ 
		boolean exit = false;

        for (int i = 0; i < args.length; i++) {
        	exit = true;
        	command = command + args[i] + " "; //$NON-NLS-1$
        }

        Host host = null;
        try {
			host = CurrentConfiguration.getInstance().getDefaultHost();      
		} catch (ConfigurationException e) {
		}

        if (host == null) {
        	LogManager.logError(LogCommonConstants.CTX_CONTROLLER,"ERROR " + PlatformPlugin.Util.getString(ErrorMessageKeys.HOST_0001)); //$NON-NLS-1$
            System.exit(-1);
        }          
        
        VMNaming.setup(host.getFullName(), host.getHostAddress(), host.getBindAddress());
        
        try {
			ServiceManager manager = loadServiceManager(host);
			manager.run(command,exit);
		} catch (Exception e) {
			System.out.println("Please make sure the HostController is running on this host"); //$NON-NLS-1$
			System.exit(-1);
		}
    }
}


