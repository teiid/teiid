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

package org.teiid.net.socket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import org.teiid.client.util.ResultsFuture;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.net.CommunicationException;
import org.teiid.net.socket.SocketUtil.SSLSocketFactory;
import org.teiid.netty.handler.codec.serialization.ObjectDecoderInputStream;
import org.teiid.netty.handler.codec.serialization.ObjectEncoderOutputStream;


public final class OioOjbectChannelFactory implements ObjectChannelFactory {
	
	private final static int STREAM_BUFFER_SIZE = 1<<15;
	private final static int MAX_OBJECT_SIZE = 1 << 25;
	
	private static Logger log = Logger.getLogger("org.teiid.client.sockets"); //$NON-NLS-1$
	
	final static class OioObjectChannel implements ObjectChannel {
		private final Socket socket;
		private ObjectOutputStream outputStream;
		private ObjectInputStream inputStream;
		private Object readLock = new Object();

		private OioObjectChannel(Socket socket) throws IOException {
			log.fine("creating new OioObjectChannel"); //$NON-NLS-1$
			this.socket = socket;
            BufferedOutputStream bos = new BufferedOutputStream( socket.getOutputStream(), STREAM_BUFFER_SIZE);
            outputStream = new ObjectEncoderOutputStream( new DataOutputStream(bos), 512);
            //The output stream must be flushed on creation in order to write some initialization data
            //through the buffered stream to the input stream on the other side
            outputStream.flush();
            final ClassLoader cl = this.getClass().getClassLoader();
            BufferedInputStream bis = new BufferedInputStream(socket.getInputStream(), STREAM_BUFFER_SIZE);
            inputStream = new ObjectDecoderInputStream(new DataInputStream(bis), cl, MAX_OBJECT_SIZE);
		}

		@Override
		public void close() {
			log.finer("closing socket"); //$NON-NLS-1$
			try {
		        outputStream.flush();
		    } catch (IOException e) {
		        // ignore
		    }
		    try {
		        outputStream.close();
		    } catch (IOException e) {
		        // ignore
		    }
		    try {
		        inputStream.close();
		    } catch (IOException e) {
		        // ignore
		    }
		    try {
		        socket.close();
		    } catch (IOException e) {
		        // ignore
		    }
		}

		@Override
		public SocketAddress getRemoteAddress() {
			return socket.getRemoteSocketAddress();
		}

		@Override
		public boolean isOpen() {
			return !socket.isClosed();
		}

		@Override
		public Object read() throws IOException, ClassNotFoundException {
			log.finer("reading message from socket"); //$NON-NLS-1$
			synchronized (readLock) {
				try {
					return inputStream.readObject();
				} catch (SocketTimeoutException e) {
					throw e;
		        } catch (IOException e) {
		            close();
		            throw e;
				}
			}
		}

		@Override
		public synchronized Future<?> write(Object msg) {
			log.finer("writing message to socket"); //$NON-NLS-1$
		    ResultsFuture<Void> result = new ResultsFuture<Void>();
		    try {
		        outputStream.writeObject(msg);
		        outputStream.flush();     
		        outputStream.reset();
		    	result.getResultsReceiver().receiveResults(null);
		    } catch (IOException e) {
		        close();
		    	result.getResultsReceiver().exceptionOccurred(e);
		    }
		    return result;
		}
	}

	private Properties props;
	private int receiveBufferSize = 0;
	private int sendBufferSize = 0;
	private boolean conserveBandwidth;
	private int soTimeout = 3000;
	private volatile SSLSocketFactory sslSocketFactory;

	public OioOjbectChannelFactory(Properties props) {
		this.props = props;
		PropertiesUtils.setBeanProperties(this, props, "org.teiid.sockets"); //$NON-NLS-1$
	}

	@Override
	public ObjectChannel createObjectChannel(SocketAddress address, boolean ssl) throws IOException,
			CommunicationException {
		final Socket socket;
		if (ssl) {
			if (this.sslSocketFactory == null) {
				try {
					sslSocketFactory = SocketUtil.getSSLSocketFactory(props);
				} catch (GeneralSecurityException e) {
					throw new CommunicationException(e);
				}
			}
			socket = sslSocketFactory.getSocket();
		} else {
			socket = new Socket();
		}
		if (receiveBufferSize > 0) {
			socket.setReceiveBufferSize(receiveBufferSize);
		}
		if (sendBufferSize > 0) {
			socket.setSendBufferSize(sendBufferSize);
		}
	    socket.setTcpNoDelay(!conserveBandwidth); // enable Nagle's algorithm to conserve bandwidth
	    socket.connect(address);
	    socket.setSoTimeout(soTimeout);
	    return new OioObjectChannel(socket);
	}
	
	public int getSendBufferSize() {
		return sendBufferSize;
	}

	public void setSendBufferSize(int sendBufferSize) {
		this.sendBufferSize = sendBufferSize;
	}

	public int getReceiveBufferSize() {
		return receiveBufferSize;
	}

	public void setReceiveBufferSize(int receiveBufferSize) {
		this.receiveBufferSize = receiveBufferSize;
	}

	public boolean isConserveBandwidth() {
		return conserveBandwidth;
	}

	public void setConserveBandwidth(boolean conserveBandwidth) {
		this.conserveBandwidth = conserveBandwidth;
	}
	
	public void setSoTimeout(int soTimeout) {
		this.soTimeout = soTimeout;
	}

	public int getSoTimeout() {
		return soTimeout;
	}
}