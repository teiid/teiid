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

package com.metamatrix.common.comm.platform.socket.server;

import java.net.InetSocketAddress;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.platform.socket.ObjectChannel;
import com.metamatrix.common.comm.platform.socket.SSLAwareChannelHandler;
import com.metamatrix.common.comm.platform.socket.SocketVMController;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListener;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListenerFactory;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.MetaMatrixProductVersion;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.vm.controller.SocketListenerStats;

/**
 * Server-side class to listen for new connection requests and create a SocketClientConnection for each connection request.
 */
public class SocketListener implements ChannelListenerFactory {
    private ClientServiceRegistry server;
    private WorkerPool workerPool;
    private SSLAwareChannelHandler channelHandler;
    private Channel serverChanel;
    private boolean isClientEncryptionEnabled;
    private SessionServiceInterface sessionService;
    
    /**
     * 
     * @param port
     * @param bindaddress
     * @param server
     * @param inputBufferSize
     * @param outputBufferSize
     * @param workerPool
     * @param engine null if SSL is disabled
     */
    public SocketListener(int port, String bindAddress,
			ClientServiceRegistry server, int inputBufferSize,
			int outputBufferSize, WorkerPool workerPool, SSLEngine engine, boolean isClientEncryptionEnabled, SessionServiceInterface sessionService) {
    	this.isClientEncryptionEnabled = isClientEncryptionEnabled;
    	this.sessionService = sessionService;
    	if (port < 0 || port > 0xFFFF) {
            throw new IllegalArgumentException("port out of range:" + port); //$NON-NLS-1$
        }

       	this.server = server;
        this.workerPool = workerPool;
        if (LogManager.isMessageToBeRecorded(SocketVMController.SOCKET_CONTEXT, MessageLevel.DETAIL)) { 
            LogManager.logDetail(SocketVMController.SOCKET_CONTEXT, "server = " + this.server + "binding to port:" + port); //$NON-NLS-1$ //$NON-NLS-2$
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
        
        this.serverChanel = bootstrap.bind(new InetSocketAddress(bindAddress, port));
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
		return new SocketClientInstance(channel, this.workerPool, this.server, this.isClientEncryptionEnabled, this.sessionService);
	}

}