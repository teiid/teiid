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

package org.teiid.transport;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLEngine;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.teiid.transport.ChannelListener.ChannelListenerFactory;

import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.platform.socket.ObjectChannel;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.queue.WorkerPool;
import com.metamatrix.common.queue.WorkerPoolFactory;
import com.metamatrix.common.queue.WorkerPoolStats;
import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.vm.controller.SocketListenerStats;

/**
 * Server-side class to listen for new connection requests and create a SocketClientConnection for each connection request.
 */
public class SocketListener implements ChannelListenerFactory {
    private ClientServiceRegistry server;
    private SSLAwareChannelHandler channelHandler;
    private Channel serverChanel;
    private boolean isClientEncryptionEnabled;
    private SessionServiceInterface sessionService;
    private WorkerPool workerPool;
    private ExecutorService nettyPool;
    
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
    public SocketListener(int port, String bindAddress, ClientServiceRegistry server, int inputBufferSize,
			int outputBufferSize, int maxWorkers, SSLEngine engine, boolean isClientEncryptionEnabled, SessionServiceInterface sessionService) {
    	this.isClientEncryptionEnabled = isClientEncryptionEnabled;
    	this.sessionService = sessionService;
    	if (port < 0 || port > 0xFFFF) {
            throw new IllegalArgumentException("port out of range:" + port); //$NON-NLS-1$
        }

       	this.server = server;
        this.workerPool = WorkerPoolFactory.newWorkerPool("SocketWorker", maxWorkers); //$NON-NLS-1$
        this.nettyPool = Executors.newCachedThreadPool();
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_SERVER, MessageLevel.DETAIL)) { 
            LogManager.logDetail(LogConstants.CTX_SERVER, "server = " + bindAddress + "binding to port:" + port); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
        ChannelFactory factory = new NioServerSocketChannelFactory(nettyPool, nettyPool, Math.min(Runtime.getRuntime().availableProcessors(), maxWorkers));
        
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        this.channelHandler = new SSLAwareChannelHandler(this, engine, Thread.currentThread().getContextClassLoader());
        bootstrap.setPipelineFactory(channelHandler);
        if (inputBufferSize != 0) {
        	bootstrap.setOption("receiveBufferSize", new Integer(inputBufferSize)); //$NON-NLS-1$
        }
        if (outputBufferSize != 0) {
        	bootstrap.setOption("sendBufferSize", new Integer(outputBufferSize)); //$NON-NLS-1$
        }
        bootstrap.setOption("keepAlive", Boolean.TRUE); //$NON-NLS-1$
        
        this.serverChanel = bootstrap.bind(new InetSocketAddress(bindAddress, port));
    }
    
    public WorkerPoolStats getProcessPoolStats() {
    	return this.workerPool.getStats();
    }
    
    public int getPort() {
    	return ((InetSocketAddress)this.serverChanel.getLocalAddress()).getPort();
    }
    
    static String getVersionInfo() {
        return ApplicationInfo.getInstance().getMajorReleaseNumber();
    }
    
    public void stop() {
    	this.serverChanel.close();
    	this.workerPool.shutdownNow();
    	this.nettyPool.shutdownNow();
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