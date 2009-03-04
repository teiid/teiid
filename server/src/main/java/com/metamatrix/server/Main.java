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

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.VMComponentDefn;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.messaging.MessagingException;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.core.log.LogListener;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.dqp.ResourceFinder;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.vm.api.controller.ProcessManagement;

/**
 * This is main server starter class.
 */
public class Main {

	@Inject
	MessageBus messageBus;
	
	@Inject
	ProcessManagement vmController;
	
	@Inject
	LogListener logListener;
	
	public static void main(String[] args) {
        
		try {
			if (args.length != 1) {
			    System.out.println("Usage: java com.metamatrix.server.Main <vm_name>"); //$NON-NLS-1$
			    System.exit(1);        	
			}

			String processName = args[0];

			Host host = null;
			try {
				host = CurrentConfiguration.getInstance().getDefaultHost();        
			} catch (ConfigurationException e) {
			}
			
			if (host == null) {
			    System.err.println(PlatformPlugin.Util.getString("SocketVMController.5")); //$NON-NLS-1$
			    System.exit(-1);
			}
			
			VMComponentDefn deployedVM = CurrentConfiguration.getInstance().getConfiguration().getVMForHost(host.getName(), processName);
			String bindAddress = deployedVM.getBindAddress();
			
			VMNaming.setProcessName(processName);
			VMNaming.setup(host.getFullName(), host.getHostAddress(), bindAddress);
			
			// write info log
			writeInfoLog(host, processName);
			        
			createTempDirectory();                    
			
			// wire up guice modules
			Main main = loadMain(host, processName);
			
			// launch the server
			
			main.launchServer();
		} catch (Throwable e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} 
	}
	
	
	private static Main loadMain(Host host, String processName) {
		Injector injector = Guice.createInjector(new ServerGuiceModule(host, processName));
		// Until we get the all the DI working we have to resort to this kind of stuff..
		ResourceFinder.setInjector(injector); 
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


    private static void writeInfoLog(Host host, String processName) {
        // trigger the logging of the current application info to a log file for debugging        
        LogApplicationInfo logApplInfo = new LogApplicationInfo(host.getFullName(), processName, host.getLogDirectory(), buildPrefix(host.getFullName(), processName) + "_info.log"); //$NON-NLS-1$
        logApplInfo.start();        	
    }
    
    /**
     * All work to be done during shutdown
     */
    class ShutdownWork extends Thread {

		@Override
		public void run() {

			try {
				messageBus.shutdown();
			} catch (MessagingException e) {
				e.printStackTrace();
			}
            
			// shutdown cache
            ResourceFinder.getCacheFactory().destroy();
     
            // shutdown logging
            logListener.shutdown();
		}
    }
}
