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

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.util.NamedThreadFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.net.socket.ObjectChannel;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.transport.ChannelListener.ChannelListenerFactory;


/**
 * Server-side class to listen for new connection requests and create a SocketClientConnection for each connection request.
 */
public class SocketListener implements ChannelListenerFactory {
	private SSLAwareChannelHandler channelHandler;
    private Channel serverChanel;
    private boolean isClientEncryptionEnabled;
    private ExecutorService nettyPool;
    private ClientServiceRegistryImpl csr;
    
    public SocketListener(SocketConfiguration config, ClientServiceRegistryImpl csr, StorageManager storageManager, int portOffset) {
		this(config.getPortNumber()+portOffset, config.getHostAddress().getHostAddress(), config.getInputBufferSize(), config.getOutputBufferSize(), config.getMaxSocketThreads(), config.getSSLConfiguration(), csr, storageManager);
        
		LogManager.logDetail(LogConstants.CTX_TRANSPORT, RuntimePlugin.Util.getString("SocketTransport.1", new Object[] {config.getHostAddress().getHostAddress(), String.valueOf(config.getPortNumber())})); //$NON-NLS-1$
    }
    
    /**
     * 
     * @param port
     * @param inputBufferSize
     * @param outputBufferSize
     * @param engine null if SSL is disabled
     * @param bindaddress
     * @param server
     */
    public SocketListener(int port, String bindAddress, int inputBufferSize,
			int outputBufferSize, int maxWorkers, SSLConfiguration config, ClientServiceRegistryImpl csr, StorageManager storageManager) {
    	this.isClientEncryptionEnabled = config.isClientEncryptionEnabled();
    	this.csr = csr;
    	if (port < 0 || port > 0xFFFF) {
            throw new IllegalArgumentException("port out of range:" + port); //$NON-NLS-1$
        }

    	this.nettyPool = Executors.newCachedThreadPool(new NamedThreadFactory("NIO")); //$NON-NLS-1$
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)) { 
            LogManager.logDetail(LogConstants.CTX_TRANSPORT, "server = " + bindAddress + "binding to port:" + port); //$NON-NLS-1$ //$NON-NLS-2$
		}
        
        if (maxWorkers == 0) {
        	maxWorkers = Runtime.getRuntime().availableProcessors();
        }
		
        ChannelFactory factory = new NioServerSocketChannelFactory(this.nettyPool, this.nettyPool, maxWorkers);
        
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        this.channelHandler = createChannelPipelineFactory(config, storageManager);
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
    
    public int getPort() {
    	return ((InetSocketAddress)this.serverChanel.getLocalAddress()).getPort();
    }
    
    public void stop() {
    	this.serverChanel.close();
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

    protected SSLAwareChannelHandler createChannelPipelineFactory(SSLConfiguration config, StorageManager storageManager) {
    	return new SSLAwareChannelHandler(this, config, Thread.currentThread().getContextClassLoader(), storageManager);
    }
    
	public ChannelListener createChannelListener(ObjectChannel channel) {
		return new SocketClientInstance(channel, csr, this.isClientEncryptionEnabled);
	}
	
}