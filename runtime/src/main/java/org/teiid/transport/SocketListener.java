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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.SSLEngine;

import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.NamedThreadFactory;
import org.teiid.core.util.PropertiesUtils;
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
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 1 << 21;

    protected SSLAwareChannelHandler channelHandler;
    private ChannelFuture serverChannel;
    private boolean isClientEncryptionEnabled;
    private ThreadFactory nettyPool;
    private ClientServiceRegistryImpl csr;
	private ServerBootstrap bootstrap;
    
    private int maxMessageSize = PropertiesUtils.getIntProperty(System.getProperties(), "org.teiid.maxMessageSize", DEFAULT_MAX_MESSAGE_SIZE); //$NON-NLS-1$
    private long maxLobSize = PropertiesUtils.getLongProperty(System.getProperties(), "org.teiid.maxStreamingLobSize", ObjectDecoder.MAX_LOB_SIZE); //$NON-NLS-1$
	
    public SocketListener(InetSocketAddress address, SocketConfiguration config, ClientServiceRegistryImpl csr, StorageManager storageManager) {
		this(address, config.getInputBufferSize(), config.getOutputBufferSize(), config.getMaxSocketThreads(), config.getSSLConfiguration(), csr, storageManager);
		LogManager.logDetail(LogConstants.CTX_TRANSPORT, RuntimePlugin.Util.getString("SocketTransport.1", new Object[] {address.getHostName(), String.valueOf(config.getPortNumber())})); //$NON-NLS-1$
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
    public SocketListener(final InetSocketAddress address, final int inputBufferSize,
            final int outputBufferSize, int maxWorkers,
            final SSLConfiguration config, final ClientServiceRegistryImpl csr,
            final StorageManager storageManager) {

        if (config != null) {
    		this.isClientEncryptionEnabled = config.isClientEncryptionEnabled();
    	}
    	this.csr = csr;

    	this.nettyPool = new NamedThreadFactory("NIO"); //$NON-NLS-1$
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)) { 
            LogManager.logDetail(LogConstants.CTX_TRANSPORT, "server = " + address.getAddress() + "binding to port:" + address.getPort()); //$NON-NLS-1$ //$NON-NLS-2$
		}
        
        if (maxWorkers == 0) {
        	maxWorkers = Math.max(4, 2*Runtime.getRuntime().availableProcessors());
        }
        EventLoopGroup workers = new NioEventLoopGroup(maxWorkers, this.nettyPool); 
        
        bootstrap = new ServerBootstrap();
        bootstrap.group(workers).channel(NioServerSocketChannel.class);
        this.channelHandler = createChannelHandler();
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                configureChannelPipeline(pipeline, config, storageManager);
            }
        });
        if (inputBufferSize != 0) {
        	bootstrap.option(ChannelOption.SO_RCVBUF, new Integer(inputBufferSize)); //$NON-NLS-1$
        }
        if (outputBufferSize != 0) {
        	bootstrap.option(ChannelOption.SO_SNDBUF, new Integer(outputBufferSize)); //$NON-NLS-1$
        }
        bootstrap.option(ChannelOption.TCP_NODELAY, Boolean.TRUE); //$NON-NLS-1$
        bootstrap.option(ChannelOption.SO_KEEPALIVE, Boolean.TRUE); //$NON-NLS-1$
        
        this.serverChannel = bootstrap.bind(address);
        this.serverChannel.syncUninterruptibly();
    }
    
    protected void configureChannelPipeline(ChannelPipeline pipeline,
            SSLConfiguration config, StorageManager storageManager) throws Exception {
        if (config != null) {
            SSLEngine engine = config.getServerSSLEngine();
            if (engine != null) {
                pipeline.addLast("ssl", new SslHandler(engine)); //$NON-NLS-1$
            }
        }
        pipeline.addLast("decoder", new ObjectDecoder(maxMessageSize, 
                maxLobSize, 
                Thread.currentThread().getContextClassLoader(), 
                storageManager)); //$NON-NLS-1$
        pipeline.addLast("chunker", new ChunkedWriteHandler()); //$NON-NLS-1$
        pipeline.addLast("encoder", new ObjectEncoder()); //$NON-NLS-1$        
        pipeline.addLast("handler", this.channelHandler); //$NON-NLS-1$                
    }
    
    public int getPort() {
    	return ((InetSocketAddress)this.serverChannel.channel().localAddress()).getPort();
    }
    
    public void stop() {
    	ChannelFuture future = this.serverChannel.channel().closeFuture();
    	if (this.bootstrap != null) {
        	bootstrap.group().shutdownGracefully();
        	bootstrap = null;
    	}
    	try {
			future.sync();
		} catch (InterruptedException e) {
			throw new TeiidRuntimeException(e);
		}
    }
   
    public SocketListenerStats getStats() {
        SocketListenerStats stats = new SocketListenerStats();      
        if (this.channelHandler != null) {
            stats.objectsRead = this.channelHandler.getObjectsRead();
            stats.objectsWritten = this.channelHandler.getObjectsWritten();
            stats.sockets = this.channelHandler.getConnectedChannels();
            stats.maxSockets = this.channelHandler.getMaxConnectedChannels();
        }
        return stats;
    }

    protected SSLAwareChannelHandler createChannelHandler() {
    	return new SSLAwareChannelHandler(this);
    }
    
	public ChannelListener createChannelListener(ObjectChannel channel) {
		return new SocketClientInstance(channel, csr, this.isClientEncryptionEnabled);
	}
	
	SSLAwareChannelHandler getChannelHandler() {
		return channelHandler;
	}
	
    public int getMaxMessageSize() {
        return maxMessageSize;
    }
    
    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }
    
    public void setMaxLobSize(long maxLobSize) {
        this.maxLobSize = maxLobSize;
    }	
}