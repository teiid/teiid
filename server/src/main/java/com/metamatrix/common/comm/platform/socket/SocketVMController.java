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

package com.metamatrix.common.comm.platform.socket;

import java.util.Properties;

import org.teiid.transport.SSLConfiguration;
import org.teiid.transport.SocketListener;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.util.LogCommonConstants;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.platform.PlatformPlugin;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.util.PlatformProxyHelper;
import com.metamatrix.platform.vm.controller.ProcessController;
import com.metamatrix.platform.vm.controller.ServerEvents;
import com.metamatrix.platform.vm.controller.SocketListenerStats;
import com.metamatrix.server.Configuration;
import com.metamatrix.server.HostManagement;
import com.metamatrix.server.Main;

/**
 * Main class for a server process.
 */
@Singleton
public class SocketVMController extends ProcessController {
	
	public static final String SOCKET_CONTEXT = "ServerSocket"; //$NON-NLS-1$
	
    private static final String SERVER_PORT = VMComponentDefnType.SERVER_PORT; 
    private static final String MAX_THREADS = VMComponentDefnType.MAX_THREADS; 
    private static final String INPUT_BUFFER_SIZE = VMComponentDefnType.INPUT_BUFFER_SIZE;       
    private static final String OUTPUT_BUFFER_SIZE = VMComponentDefnType.OUTPUT_BUFFER_SIZE;       
    
    private static final int DEFAULT_SERVER_PORT = 31000;
    private static final int DEFAULT_MAX_THREADS = 15;
    private static final long DEFAULT_WAITFORSERVICES = 500;
    private static final int DEFAULT_INPUT_BUFFER_SIZE = 0;
    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 0;

    private SocketListener listener; 
        
    @Inject
    public SocketVMController(@Named(Configuration.HOST) Host host, @Named(Configuration.PROCESSNAME) String processName, ClusteredRegistryState registry, ServerEvents serverEvents, MessageBus bus, HostManagement hostManagement) throws Exception {
        super(host, processName, registry, serverEvents, bus, hostManagement);
    }

    @Override
	public void start() {
		super.start();
		
		waitForServices();
        
        startSocketListener();		
	}
    
    
    // extend the VMController method to close the socket at the start of the stop process
    // so that the port can be made available sooner on bounces.
    @Override
    public void shutdown(boolean now) {
        if (listener != null) {
            try {
                listener.stop();
            } catch (Exception e) {
                // ignore
            } finally {
                listener = null;
            }
        }

        // call the base class.
        super.shutdown(now);
    }        
    
    /** 
     * Get VM Properties and Start the Socket Listener
     * 
     * @since 4.2
     */
    private void startSocketListener() {
        Properties props = getProperties();
        int socketPort = PropertiesUtils.getIntProperty(props, SERVER_PORT, DEFAULT_SERVER_PORT);
        int maxThreads = PropertiesUtils.getIntProperty(props, MAX_THREADS, DEFAULT_MAX_THREADS);
        int inputBufferSize = PropertiesUtils.getIntProperty(props, INPUT_BUFFER_SIZE, DEFAULT_INPUT_BUFFER_SIZE);
        int outputBufferSize = PropertiesUtils.getIntProperty(props, OUTPUT_BUFFER_SIZE, DEFAULT_OUTPUT_BUFFER_SIZE);
        String bindaddress = CurrentConfiguration.getInstance().getBindAddress();
        
        final Object[] param = new Object[] {
            this.processName, bindaddress, String.valueOf(socketPort)
        };
        
        logMessage(PlatformPlugin.Util.getString("SocketVMController.1", param)); //$NON-NLS-1$
        SSLConfiguration helper = new SSLConfiguration();
        try {
	        helper.init(props);
	        listener = new SocketListener(socketPort, bindaddress, this.clientServices, inputBufferSize, outputBufferSize, maxThreads, helper.getServerSSLEngine(), helper.isClientEncryptionEnabled(), PlatformProxyHelper.getSessionServiceProxy(PlatformProxyHelper.ROUND_ROBIN_LOCAL));
        } catch (Exception e) {
        	LogManager.logCritical(LogCommonConstants.CTX_CONTROLLER, e, PlatformPlugin.Util.getString("SocketVMController.2",param)); //$NON-NLS-1$
            System.exit(1); 
        }
    }

    /** 
     * 
     * @throws MetaMatrixComponentException
     * @throws RegistryException
     * @since 4.2
     */
    private void waitForServices() {
        boolean isReady = false;
        while( !isReady ) {
            try {
                Thread.sleep(DEFAULT_WAITFORSERVICES);
            } catch (InterruptedException err) {   
            }
            isReady =  isStarted();
        }
    }
    
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("SocketVMController:  "); //$NON-NLS-1$
        buffer.append(" socketHost:").append(this.host.getFullName()); //$NON-NLS-1$
        buffer.append(" socketPort:").append(this.listener.getPort()); //$NON-NLS-1$
        return buffer.toString();
    }

    protected SocketListenerStats getSocketListenerStats() {
        if (listener == null) {
            return null;
        }
        return listener.getStats();
    }    

    protected WorkerPoolStats getProcessPoolStats() {
    	return this.listener.getProcessPoolStats();
    }
    
    @Deprecated
    public static void main(String[] args) throws Exception{
    	Main.main(args);
    }

}