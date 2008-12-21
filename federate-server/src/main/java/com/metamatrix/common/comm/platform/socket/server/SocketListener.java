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

package com.metamatrix.common.comm.platform.socket.server;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.platform.socket.ObjectChannel;
import com.metamatrix.common.comm.platform.socket.SSLAwareChannelHandler;
import com.metamatrix.common.comm.platform.socket.SocketLog;
import com.metamatrix.common.comm.platform.socket.SocketVMController;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListener;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListenerFactory;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.core.util.MetaMatrixProductVersion;
import com.metamatrix.platform.vm.controller.SocketListenerStats;

/**
 * Server-side class to listen for new connection requests and create a SocketClientConnection for each connection request.
 */
public class SocketListener implements ChannelListenerFactory {
    private HostInfo hostInfo;
    private ClientServiceRegistry server;
    private WorkerPool workerPool;
    private String bindAddress;
    private SSLAwareChannelHandler channelHandler;
    private Channel serverChanel;
    
    /**
     * 
     * @param port
     * @param hostaddress
     * @param bindaddress
     * @param server
     * @param inputBufferSize
     * @param outputBufferSize
     * @param workerPool
     * @param engine null if SSL is disabled
     */
    public SocketListener(int port, String hostaddress, String bindaddress,
			ClientServiceRegistry server, int inputBufferSize,
			int outputBufferSize, WorkerPool workerPool, SSLEngine engine) {
    	InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getByName(hostaddress);
        } catch (UnknownHostException err) {
        }
        this.hostInfo = new HostInfo(hostaddress,port, inetAddress);
        if (bindaddress == null) {
        	this.bindAddress = this.hostInfo.getHostName();
        } else {
        	this.bindAddress = bindaddress;
        }
        this.server = server;
        this.workerPool = workerPool;
        if (LogManager.isMessageToBeRecorded(SocketVMController.SOCKET_CONTEXT, SocketLog.DETAIL)) { 
            LogManager.logDetail(SocketVMController.SOCKET_CONTEXT, "server = " + this.server + "binding to port:" + hostInfo.getPortNumber()); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		ThreadPoolExecutor executor = new ThreadPoolExecutor(0,
				Integer.MAX_VALUE, 2, TimeUnit.MINUTES,
				new SynchronousQueue<Runnable>(),
				new WorkerPoolFactory.DefaultThreadFactory("ServerNio")); //$NON-NLS-1$
        
        ChannelFactory factory =
            new NioServerSocketChannelFactory(executor, executor, Runtime.getRuntime().availableProcessors() * 2);
        
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        this.channelHandler = new SSLAwareChannelHandler(this, engine, Thread.currentThread().getContextClassLoader());
        bootstrap.setPipelineFactory(channelHandler);
        bootstrap.setOption("receiveBufferSize", new Integer(inputBufferSize)); //$NON-NLS-1$
        bootstrap.setOption("sendBufferSize", new Integer(outputBufferSize)); //$NON-NLS-1$
        bootstrap.setOption("keepAlive", Boolean.TRUE); //$NON-NLS-1$
        
        this.serverChanel = bootstrap.bind(new InetSocketAddress(this.bindAddress, hostInfo.getPortNumber()));
    }
    
    public int getPort() {
    	return ((InetSocketAddress)this.serverChanel.getLocalAddress()).getPort();
    }
    
    static String getVersionInfo() {
        return MetaMatrixProductVersion.VERSION_NUMBER;
    }
    
    public void stop() {
    	this.serverChanel.close();
    }
   
    public SocketListenerStats getStats() {
        SocketListenerStats stats = new SocketListenerStats();             
        stats.objectsRead = this.channelHandler.getObjectsRead();
        stats.objectsWritten = this.channelHandler.getObjectsWritten();
        stats.sockets = this.channelHandler.getConnectedChannels();
        stats.maxSockets = this.channelHandler.getMaxConnectedChannels();
        return stats;
    }

	public ChannelListener createChannelListener(ObjectChannel channel) {
		return new SocketClientInstance(channel, this.workerPool, this.server);
	}

}