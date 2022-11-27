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

/**
 *
 */
package org.teiid.transport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.net.socket.ObjectChannel;
import org.teiid.runtime.RuntimePlugin;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GenericFutureListener;


/**
 * Main class for creating Netty Nio Channels
 */

@Sharable
public class SSLAwareChannelHandler extends ChannelDuplexHandler {

    public class ObjectChannelImpl implements ObjectChannel {
        private final Channel channel;

        public ObjectChannelImpl(Channel channel) {
            this.channel = channel;
        }

        public void close() {
            channel.close();
        }

        public boolean isOpen() {
            return channel.isOpen();
        }

        public SocketAddress getRemoteAddress() {
            return channel.remoteAddress();
        }

        @Override
        public InetAddress getLocalAddress() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object read() throws IOException,
                ClassNotFoundException {
            throw new UnsupportedOperationException();
        }

        public synchronized Future<?> write(Object msg) {
            //see https://github.com/netty/netty/issues/3887
            //    https://issues.jboss.org/browse/TEIID-5658
            //submit directly to the event loop to maintain ordering
            return channel.eventLoop().submit(new Runnable() {

                @Override
                public void run() {
                    final ChannelFuture future = channel.writeAndFlush(msg);
                    future.addListener(completionListener);
                }
            });
        }
    }

    private final ChannelListener.ChannelListenerFactory listenerFactory;
    private Map<Channel, ChannelListener> listeners = new ConcurrentHashMap<Channel, ChannelListener>();
    private AtomicLong objectsRead = new AtomicLong(0);
    private AtomicLong objectsWritten = new AtomicLong(0);
    private volatile int maxChannels;

    private ChannelFutureListener completionListener = new ChannelFutureListener() {

        @Override
        public void operationComplete(ChannelFuture arg0)
                throws Exception {
            if (arg0.isSuccess()) {
                objectsWritten.getAndIncrement();
            } else if (arg0.cause() != null) {
                writeExceptionCaught(arg0.channel(), arg0.cause());
            }
        }

    };

    public SSLAwareChannelHandler(ChannelListener.ChannelListenerFactory listenerFactory) {
        this.listenerFactory = listenerFactory;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        ChannelListener listener = this.listenerFactory.createChannelListener(new ObjectChannelImpl(ctx.channel()));
        this.listeners.put(ctx.channel(), listener);
        maxChannels = Math.max(maxChannels, this.listeners.size());
        SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
        if (sslHandler != null) {
            sslHandler.handshakeFuture().addListener(new GenericFutureListener<DefaultPromise<Channel>>() {
                @Override
                public void operationComplete(DefaultPromise<Channel> future)
                        throws Exception {
                    onConnection(ctx.channel());

                }
            });
        } else {
            onConnection(ctx.channel());
        }
    }

    private void onConnection(Channel channel) throws Exception {
        ChannelListener listener = this.listeners.get(channel);
        if (listener != null) {
            SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
            listener.onConnection(sslHandler != null?sslHandler.engine():null );
        }
    }


    private void writeExceptionCaught(Channel channel,
            Throwable cause) {
        ChannelListener listener = this.listeners.get(channel);
        if (listener != null) {
            listener.exceptionOccurred(cause);
        } else {
            int level = SocketClientInstance.getLevel(cause);
            LogManager.log(level, LogConstants.CTX_TRANSPORT, LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)||level<MessageLevel.WARNING?cause:null, RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40114, cause.getMessage()));
            channel.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
            Throwable cause) throws Exception {
        writeExceptionCaught(ctx.channel(), cause);
    }

    public void messageReceived(ChannelHandlerContext ctx,
            Object msg) throws Exception {
        objectsRead.getAndIncrement();
        ChannelListener listener = this.listeners.get(ctx.channel());
        if (listener != null) {
            listener.receivedMessage(msg);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        messageReceived(ctx, msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ChannelListener listener = this.listeners.remove(ctx.channel());
        if (listener != null) {
            LogManager.logDetail(LogConstants.CTX_TRANSPORT,
                    RuntimePlugin.Util.getString("SSLAwareChannelHandler.channel_closed")); //$NON-NLS-1$
            listener.disconnected();
        }
    }

    public long getObjectsRead() {
        return this.objectsRead.get();
    }

    public long getObjectsWritten() {
        return this.objectsWritten.get();
    }

    public int getConnectedChannels() {
        return this.listeners.size();
    }

    public int getMaxConnectedChannels() {
        return this.maxChannels;
    }
}