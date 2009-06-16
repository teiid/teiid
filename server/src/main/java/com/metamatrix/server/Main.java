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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.JDBCConnectionPoolHelper;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.platform.config.persistence.impl.file.FilePersistentUtil;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;

/**
 * This is main server starter class.
 */
public class Main {
	
	private static final String CONFIG_PREFIX = "config-"; //$NON-NLS-1$

	@Inject
	MessageBus messageBus;
	
	@Inject
	ProcessManagement vmController;
	
	@Inject
	LogListener logListener;
	
	@Inject	
	ClusteredRegistryState registry;
	
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
		    System.out.println("Usage: java com.metamatrix.server.Main <vm_name>"); //$NON-NLS-1$
		    System.exit(1);        	
		}

		final String processName = args[0];

		CurrentConfiguration.getInstance().setProcessName(processName);

		final Host host = CurrentConfiguration.getInstance().getDefaultHost();        
		
		Thread t = new Thread("Main Info Thread") { //$NON-NLS-1$
			@Override
			public void run() {
				try {
					saveCurrentConfigurationToFile(host, processName);
				} catch (ConfigurationException e) {
					System.out.print("Could not archive start up configuration"); //$NON-NLS-1$
				}
				// write info log
				writeInfoLog(host, processName);
			}
		};
		t.start();
		        
		createTempDirectory();                    
		
		// wire up guice modules
		Main main = loadMain(host, processName);
		
		// launch the server
		main.launchServer();
	}
	
	private static Main loadMain(Host host, String processName) {
		Injector injector = Guice.createInjector(new ServerGuiceModule(host, processName));
		// Until we get the all the DI working we have to resort to this kind of stuff..
		ResourceFinder.setInjectorAndCompleteInitialization(injector); 
		return injector.getInstance(Main.class);
	}


	private void launchServer() {
        
        try {          
            Runtime.getRuntime().addShutdownHook(new ShutdownWork());
            
            // start the VM
            this.vmController.start();
            
            synchronized (this.vmController) {
                while(!this.vmController.isShuttingDown()) {
                	this.vmController.wait(1000);
                }
			}
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


	private static void createTempDirectory() {
		// If the java-io-tmp directory doesn't exist, it needs to be created
		// because extension jars class loading will fail because
		// java internals can' write to a non-existent directory.
		// It's added here, in addition to the host controller because if the
		// vm starter command is changed and the vm is bounced, the hostcontroller
		// would never ensure this tmp directory is created.
		if (FileUtils.TEMP_DIRECTORY != null) {
		    File tf = new File(FileUtils.TEMP_DIRECTORY);
		    if (!tf.exists()) {
		        tf.mkdirs();
		    }
		}
	}		

	private static String buildPrefix(String hostName, String processName){
	    String hostFileName = StringUtil.replaceAll(hostName, ".", "_"); //$NON-NLS-1$ //$NON-NLS-2$
	    return hostFileName + "_" + processName; //$NON-NLS-1$
	}   

    private static void saveCurrentConfigurationToFile(Host host, String processName) throws ConfigurationException {
    	String configDir = host.getConfigDirectory();
    	File f = new File(configDir);
    	f.mkdirs();
        FilePersistentUtil.writeModel(CONFIG_PREFIX+new Date().toString()+".xml", configDir,  //$NON-NLS-1$ 
        CurrentConfiguration.getInstance().getConfigurationModel(), 
                        processName);
        //remove old instances
        String[] result = f.list(new FilenameFilter() {
        	@Override
        	public boolean accept(File dir, String name) {
        		return name.startsWith(CONFIG_PREFIX);
        	}
        });
        if (result.length > 10) {
        	Arrays.sort(result);
        	try {
				FileUtils.remove(result[0]);
			} catch (IOException e) {
				System.out.println("Error removing archived config"); //$NON-NLS-1$
			}
        }
    }

    private static void writeInfoLog(Host host, String processName) {
        // trigger the logging of the current application info to a log file for debugging        
        LogApplicationInfo logApplInfo = new LogApplicationInfo(host.getFullName(), processName, host.getLogDirectory(), buildPrefix(host.getFullName(), processName) + "_info.log"); //$NON-NLS-1$
        logApplInfo.run();
    }
    
    /**
     * All work to be done during shutdown
     */
    class ShutdownWork extends Thread {

		@Override
		public void run() {

			registry.shutdown();
			
			try {
				messageBus.shutdown();
			} catch (MessagingException e) {
				e.printStackTrace();
			}
            
			// shutdown cache
            ResourceFinder.getCacheFactory().destroy();
     
            // shutdown logging
            logListener.shutdown();
     
            // close the connection pool to the DB
            JDBCConnectionPoolHelper.getInstance().shutDown();            
		}
    }
}
