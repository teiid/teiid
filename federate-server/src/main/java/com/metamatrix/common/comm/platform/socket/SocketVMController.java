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

package com.metamatrix.common.comm.platform.socket;

import java.io.IOException;
import java.util.Properties;

import javax.xml.registry.RegistryException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.server.SocketListener;
import com.metamatrix.common.config.api.Host;
import com.metamatrix.common.config.api.VMComponentDefnType;
import com.metamatrix.common.messaging.MessageBus;
import com.metamatrix.common.net.SocketHelper;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.common.util.VMNaming;
import com.metamatrix.platform.registry.ClusteredRegistryState;
import com.metamatrix.platform.vm.controller.ServerEvents;
import com.metamatrix.platform.vm.controller.SocketListenerStats;
import com.metamatrix.platform.vm.controller.VMController;
import com.metamatrix.platform.vm.controller.VMControllerID;
import com.metamatrix.server.Configuration;
import com.metamatrix.server.Main;

/**
 * Main class for a server process.
 */
@Singleton
public class SocketVMController extends VMController {
	
	public static final String SOCKET_CONTEXT = "ServerSocket"; //$NON-NLS-1$
	
    private static final String SERVER_PORT = VMComponentDefnType.SERVER_PORT; 
    private static final String MAX_THREADS = VMComponentDefnType.MAX_THREADS; 
    private static final String TIMETOLIVE = VMComponentDefnType.TIMETOLIVE;      
    private static final String INPUT_BUFFER_SIZE = VMComponentDefnType.INPUT_BUFFER_SIZE;       
    private static final String OUTPUT_BUFFER_SIZE = VMComponentDefnType.OUTPUT_BUFFER_SIZE;       
    
    private static final int DEFAULT_SERVER_PORT = 31000;
    private static final int DEFAULT_MAX_THREADS = 15;
    private static final long DEFAULT_TIMETOLIVE = 15000;
    private static final long DEFAULT_WAITFORSERVICES = 500;
    private static final int DEFAULT_INPUT_BUFFER_SIZE = 102400;
    private static final int DEFAULT_OUTPUT_BUFFER_SIZE = 102400;

    private static final String SOCKET_WORKER_POOL_NAME = "SocketWorkerQueue"; //$NON-NLS-1$
    private SocketListener listener; 
    private WorkerPool workerPool;
        
    @Inject
    public SocketVMController(@Named(Configuration.HOST) Host host, @Named(Configuration.VMNAME) String vmName, VMControllerID vmId, ClusteredRegistryState registry, ServerEvents serverEvents, MessageBus bus) throws Exception {
        super(host, vmName, vmId, registry, serverEvents, bus);
    }

	public void startVM() {
		super.startVM();
		
		waitForServices();
        
        startSocketListener();		
	}
    
    
    // extend the VMController method to close the socket at the start of the stop process
    // so that the port can be made available sooner on bounces.
    protected void doStopVM(boolean now, boolean shutdown) {
        if (workerPool != null) {
            try {
                workerPool.shutdown();                
            } catch (Exception e) {
                //ignore
            } finally {
                workerPool = null;
            }
           
        }
        
        if (listener != null) {
            try {
                listener.stop();
            } catch (Exception e) {
                // ignore
            } finally {
                listener = null;
            }
        }
        
        super.doStopVM(now, shutdown);

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
        long timeToLive = PropertiesUtils.getLongProperty(props, TIMETOLIVE, DEFAULT_TIMETOLIVE);
        int inputBufferSize = PropertiesUtils.getIntProperty(props, INPUT_BUFFER_SIZE, DEFAULT_INPUT_BUFFER_SIZE);
        int outputBufferSize = PropertiesUtils.getIntProperty(props, OUTPUT_BUFFER_SIZE, DEFAULT_OUTPUT_BUFFER_SIZE);
        String hostaddress = VMNaming.getHostAddress();
        String bindaddress =  VMNaming.getBindAddress();
        
        final Object[] param = new Object[] {
            this.vmName, bindaddress, String.valueOf(socketPort)
        };
        
        logMessage(CommPlatformPlugin.Util.getString("SocketVMController.1", param)); //$NON-NLS-1$
        workerPool = WorkerPoolFactory.newWorkerPool(SOCKET_WORKER_POOL_NAME, maxThreads, timeToLive);
        try {
            listener = new SocketListener(socketPort, hostaddress, bindaddress, this.clientServices, inputBufferSize, outputBufferSize, workerPool, SocketHelper.getServerSSLEngine());
        } catch (IOException e) {
            logError(CommPlatformPlugin.Util.getString("SocketVMController.2",param)); //$NON-NLS-1$
            e.printStackTrace();
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

    public SocketListenerStats getSocketListenerStats() {
        if (listener == null) {
            return null;
        }
        return listener.getStats();
    }    

    public WorkerPoolStats getProcessPoolStats() {
        if (workerPool == null) {
            return null;
        }
        return workerPool.getStats();
    }
    
    @Deprecated
    public static void main(String[] args) throws Exception{
    	Main.main(args);
    }

}