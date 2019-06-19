/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.transport;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
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
import io.netty.util.concurrent.Future;


/**
 * Server-side class to listen for new connection requests and create a SocketClientConnection for each connection request.
 */
public class SocketListener implements ChannelListenerFactory {
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 1 << 21;

    protected SSLAwareChannelHandler channelHandler;
    private Channel serverChannel;
    private boolean isClientEncryptionEnabled;
    private ClientServiceRegistryImpl csr;
    private ServerBootstrap bootstrap;

    private int maxMessageSize = PropertiesUtils.getHierarchicalProperty("org.teiid.maxMessageSize", DEFAULT_MAX_MESSAGE_SIZE, Integer.class); //$NON-NLS-1$
    private long maxLobSize = PropertiesUtils.getHierarchicalProperty("org.teiid.maxStreamingLobSize", ObjectDecoder.MAX_LOB_SIZE, Long.class); //$NON-NLS-1$

    public SocketListener(InetSocketAddress address, SocketConfiguration config, ClientServiceRegistryImpl csr, StorageManager storageManager) {
        this(address, config.getInputBufferSize(), config.getOutputBufferSize(), config.getMaxSocketThreads(), config.getSSLConfiguration(), csr, storageManager);
        LogManager.logDetail(LogConstants.CTX_TRANSPORT, RuntimePlugin.Util.getString("SocketTransport.1", new Object[] {address.getHostName(), String.valueOf(config.getPortNumber())})); //$NON-NLS-1$
    }

    public SocketListener(final InetSocketAddress address, final int inputBufferSize,
            final int outputBufferSize, int maxWorkers,
            final SSLConfiguration config, final ClientServiceRegistryImpl csr,
            final StorageManager storageManager) {

        if (config != null) {
            this.isClientEncryptionEnabled = config.isClientEncryptionEnabled();
        }
        this.csr = csr;

        NamedThreadFactory nettyPool = new NamedThreadFactory("NIO"); //$NON-NLS-1$
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_TRANSPORT, "server = " + address.getAddress() + "binding to port:" + address.getPort()); //$NON-NLS-1$ //$NON-NLS-2$
        }

        if (maxWorkers == 0) {
            maxWorkers = Math.max(4, PropertiesUtils.getIntProperty(System.getProperties(), "io.netty.eventLoopThreads", 2*Runtime.getRuntime().availableProcessors())); //$NON-NLS-1$
        }
        EventLoopGroup workers = new NioEventLoopGroup(maxWorkers, nettyPool);

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
            bootstrap.childOption(ChannelOption.SO_RCVBUF, new Integer(inputBufferSize));
        }
        if (outputBufferSize != 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, new Integer(outputBufferSize));
        }
        bootstrap.childOption(ChannelOption.TCP_NODELAY, Boolean.TRUE);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, Boolean.TRUE);
        ChannelFuture future = bootstrap.bind(address);
        future.syncUninterruptibly();
        this.serverChannel = future.channel();
    }

    protected void configureChannelPipeline(ChannelPipeline pipeline,
            SSLConfiguration config, StorageManager storageManager) throws Exception {
        if (config != null) {
            SSLEngine engine = config.getServerSSLEngine();
            if (engine != null) {
                pipeline.addLast("ssl", new SslHandler(engine)); //$NON-NLS-1$
            }
        }
        pipeline.addLast("decoder", new ObjectDecoder(maxMessageSize, //$NON-NLS-1$
                maxLobSize,
                Thread.currentThread().getContextClassLoader(),
                storageManager));
        pipeline.addLast("chunker", new ChunkedWriteHandler()); //$NON-NLS-1$
        pipeline.addLast("encoder", new ObjectEncoder()); //$NON-NLS-1$
        pipeline.addLast("handler", this.channelHandler); //$NON-NLS-1$
    }

    public int getPort() {
        return ((InetSocketAddress)this.serverChannel.localAddress()).getPort();
    }

    /**
     * Stops the {@link SocketListener}
     * @return a Future if the transport was started successfully
     * that can notify of successfully killing all clients
     */
    public Future<?> stop() {
        ChannelFuture future = this.serverChannel.closeFuture();
        Future<?> shutdown = null;
        if (this.bootstrap != null) {
            shutdown = bootstrap.config().group().shutdownGracefully(0, 0, TimeUnit.SECONDS);
            bootstrap = null;
        }
        try {
            future.await();
        } catch (InterruptedException e) {
            throw new TeiidRuntimeException(e);
        }
        return shutdown;
    }

    public SocketListenerStats getStats() {
        SocketListenerStats stats = new SocketListenerStats();
        stats.objectsRead = this.channelHandler.getObjectsRead();
        stats.objectsWritten = this.channelHandler.getObjectsWritten();
        stats.sockets = this.channelHandler.getConnectedChannels();
        stats.maxSockets = this.channelHandler.getMaxConnectedChannels();
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