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

package com.metamatrix.common.comm.platform.socket.client;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.SingleInstanceCommunicationException;
import com.metamatrix.common.comm.platform.socket.SSLAwareChannelHandler;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListenerFactory;
import com.metamatrix.core.util.NamedThreadFactory;

public class NioObjectChannelFactory implements ObjectChannelFactory {
	
	private int inputBufferSize;
	private int outputBufferSize;
	private boolean conserveBandwidth;
	private ClassLoader classLoader;
	private ChannelFactory channlFactory;
	
	public NioObjectChannelFactory(boolean conserveBandwidth, int inputBufferSize, int outputBufferSize, ClassLoader classLoader, int workerCount) {
		this.conserveBandwidth = conserveBandwidth;
		this.inputBufferSize = inputBufferSize;
		this.outputBufferSize = outputBufferSize;
		this.classLoader = classLoader;
		ThreadPoolExecutor executor = new ThreadPoolExecutor(0,
				Integer.MAX_VALUE, 2, TimeUnit.MINUTES,
				new SynchronousQueue<Runnable>(),
				new NamedThreadFactory("Nio")); //$NON-NLS-1$
		this.channlFactory = new NioClientSocketChannelFactory(executor, executor, workerCount);
	}

	public void createObjectChannel(SocketAddress address, SSLEngine engine,
			ChannelListenerFactory listener) throws IOException,
			CommunicationException {
		ClientBootstrap bootstrap = new ClientBootstrap(channlFactory);

        final SSLAwareChannelHandler handler = new SSLAwareChannelHandler(listener, engine, this.classLoader);
        
        bootstrap.setPipelineFactory(handler);
        
        if (!conserveBandwidth) {
        	bootstrap.setOption("tcpNoDelay", Boolean.TRUE); //$NON-NLS-1$
        }
        bootstrap.setOption("receiveBufferSize", new Integer(inputBufferSize)); //$NON-NLS-1$
        bootstrap.setOption("sendBufferSize", new Integer(outputBufferSize)); //$NON-NLS-1$
        bootstrap.setOption("keepAlive", Boolean.TRUE); //$NON-NLS-1$
               
        ChannelFuture future = bootstrap.connect(address);
        
        //connections have no timeout
        future.awaitUninterruptibly();
        
        if (!future.isSuccess()) {
        	if (future.getCause() instanceof IOException) {
        		throw (IOException)future.getCause();
        	}
        	throw new SingleInstanceCommunicationException(future.getCause());
        }
	}
}
