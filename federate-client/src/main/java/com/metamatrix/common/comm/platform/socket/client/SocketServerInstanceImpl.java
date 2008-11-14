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

package com.metamatrix.common.comm.platform.socket.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import javax.net.ssl.SSLEngine;

import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.api.MessageListener;
import com.metamatrix.common.comm.api.ServerInstanceContext;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.SingleInstanceCommunicationException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.Handshake;
import com.metamatrix.common.comm.platform.socket.MessagePacket;
import com.metamatrix.common.comm.platform.socket.ObjectChannel;
import com.metamatrix.common.comm.platform.socket.SocketLog;
import com.metamatrix.common.comm.platform.socket.SocketUtil;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListener;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListenerFactory;
import com.metamatrix.common.util.ApplicationInfo;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.Cryptor;
import com.metamatrix.common.util.crypto.DhKeyGenerator;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.core.util.MetaMatrixProductVersion;

/**
 * TODO: the design of this class is not good its functionality should be merged
 * above (SocketServerConnection) and below (
 */
public class SocketServerInstanceImpl implements ChannelListener, SocketServerInstance {
	
    static String RELEASE_NUMBER;
	
	private HostInfo hostInfo;
    private ObjectChannel socketChannel;
    private SocketLog log;
    private Cryptor cryptor;
    
    private Map<Serializable, MessageListener> asynchronousListeners = new ConcurrentHashMap<Serializable, MessageListener>();
    
    private volatile boolean handshakeCompleted;
    private volatile CommunicationException handshakeError;
	private boolean ssl;
    
    /**
     * SocketServerInstance
     * @param host
     * @param workerPool
     * @param synchronousSendTimeout
     * @param log
     * @param classLoader
     * @param objectSocketFactory Factory to use when creating underlying
     *            ObjectSocket.
     * @throws IOException 
     */
    SocketServerInstanceImpl(final HostInfo host, boolean ssl, SocketLog log, ObjectChannelFactory channelFactory)
        throws CommunicationException, IOException {

        this.hostInfo = host;
        this.log = log;
        this.ssl = ssl;

        InetSocketAddress address = null;
        if (hostInfo.getInetAddress() != null) {
            address = new InetSocketAddress(hostInfo.getInetAddress(), hostInfo.getPortNumber());
        } else {
            address = new InetSocketAddress(hostInfo.getHostName(), hostInfo.getPortNumber());
        }
        SSLEngine engine = null;
        if (ssl) {
        	try {
				engine = SocketUtil.getClientSSLEngine();
			} catch (NoSuchAlgorithmException e) {
				throw new CommunicationException(e);
			} catch (IOException e) {
				throw new CommunicationException(e);
			}
        }
		channelFactory.createObjectChannel(address, engine, new ChannelListenerFactory() {

			public ChannelListener createChannelListener(
					ObjectChannel channel) {
				SocketServerInstanceImpl.this.socketChannel = channel;
				return SocketServerInstanceImpl.this;
			}
			
		});
		synchronized (this) {
			long endTime = System.currentTimeMillis() + Handshake.HANDSHAKE_TIMEOUT;
			while (!this.handshakeCompleted && this.handshakeError == null) {
				long remainingTimeout = endTime - System.currentTimeMillis();
				if (remainingTimeout <= 0) {
					break;
				}
				try {
					this.wait(remainingTimeout);
				} catch (InterruptedException e) {
					throw new CommunicationException(CommPlatformPlugin.Util.getString("SocketServerInstanceImpl.handshake_error")); //$NON-NLS-1$
				}
			}
			if (this.handshakeError != null) {
				throw this.handshakeError;
			}
			if (!this.handshakeCompleted) {
				if (this.socketChannel != null) {
					this.socketChannel.close();
				}
				throw new CommunicationException(CommPlatformPlugin.Util.getString("SocketServerInstanceImpl.handshake_timeout")); //$NON-NLS-1$
			}
		}
    }
    
    /**
     * Return identifier of the server VM this ServerInstance is associated with. 
     */
    public HostInfo getHostInfo() {
        return this.hostInfo;
    }
    
    static String getVersionInfo() {
        if (RELEASE_NUMBER == null) {
        	RELEASE_NUMBER = MetaMatrixProductVersion.VERSION_NUMBER;
            try {
                ApplicationInfo info = ApplicationInfo.getInstance();
                ApplicationInfo.Component component = info.getMainComponent();
                if (component != null) {
                	RELEASE_NUMBER = component.getReleaseNumber();
                } 
            } catch (Throwable t) {
                //Ignore default to Unknown
            }
        }
        return RELEASE_NUMBER;
    }
    
    private void receivedHahdshake(Handshake handshake) {
        try {
            /*if (handshake.getVersion().indexOf(getVersionInfo()) == -1) {
                throw new CommunicationException(CommPlatformPlugin.Util.getString("SocketServerInstanceImpl.version_mismatch", getVersionInfo(), handshake.getVersion())); //$NON-NLS-1$
            }*/
            
            handshake.setVersion(getVersionInfo());
            
            byte[] serverPublicKey = handshake.getPublicKey();
            
            if (serverPublicKey != null) {
            	DhKeyGenerator keyGen = new DhKeyGenerator();
            	byte[] publicKey = keyGen.createPublicKey();
                this.cryptor = keyGen.getSymmetricCryptor(serverPublicKey);
                handshake.setPublicKey(publicKey);
            } else {
                this.cryptor = new NullCryptor();
            }
            
            this.socketChannel.write(handshake);
            this.handshakeCompleted = true;
        } catch (CryptoException err) {
        	this.handshakeError = new CommunicationException(err);
        } finally {
        	synchronized (this) {
				this.notify();
			}
        }
    }

    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    private void encryptMessage(Message message) throws CommunicationException {
        if (message.secure) {
            try {
            	message.setContents(cryptor.sealObject(message.getContents()));
            } catch (CryptoException err) {
                throw new CommunicationException(err);
            }
        }
    }
    
    private void decryptMessage(Message message) throws CommunicationException {
        try {
        	message.setContents(cryptor.unsealObject(message.getContents()));
        } catch (CryptoException err) {
            throw new CommunicationException(err);
        }
    }

    public void send(Message message, MessageListener listener, Serializable messageKey)
        throws CommunicationException {
	    if (listener != null) {
	        asynchronousListeners.put(messageKey, listener);
	    }
	    try {
	        encryptMessage(message);
	        Future<?> writeFuture = socketChannel.write(new MessagePacket(messageKey, message));
	        writeFuture.get(); //client writes are blocking to ensure proper failure handling
	    } catch (Throwable e) {
	        asynchronousListeners.remove(messageKey);
	        throw new SingleInstanceCommunicationException(e);
	    }
    }

    /**
     * Send an exception to all clients that are currently waiting for a
     * response.
     */
	public void exceptionOccurred(Throwable e) {
    	if (e instanceof CommunicationException) {
	        if (e.getCause() instanceof InvalidClassException) {
	            log.logError("SocketServerInstance.read", e, "Unknown class or incorrect class version:"); //$NON-NLS-1$ //$NON-NLS-2$
	        } else {
	            log.logDetail("SocketServerInstance.read", e, "Unable to read: socket was already closed."); //$NON-NLS-1$ //$NON-NLS-2$
	        }
    	} else if (e instanceof EOFException) {
    		e = new CommunicationException(e);
    		log.logDetail("SocketServerInstance.read", e, "Unable to read: socket was already closed."); //$NON-NLS-1$ //$NON-NLS-2$
    	} else {
    		log.logDetail("SocketServerInstance.read", e, "Unable to read: unexpected exception"); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	
    	if (!handshakeCompleted) {
    		synchronized (this) {
				this.handshakeError = new CommunicationException(e, CommPlatformPlugin.Util.getString(ssl?"SocketServerInstanceImpl.secure_error_during_handshake":"SocketServerInstanceImpl.error_during_handshake")); //$NON-NLS-1$ //$NON-NLS-2$  
				this.notify();
			}
    	}
        Message messageHolder = new Message();
        messageHolder.setContents(e);

        Set<Map.Entry<Serializable, MessageListener>> entries = this.asynchronousListeners.entrySet();
        for (Iterator iterator = entries.iterator(); iterator.hasNext();) {
			Map.Entry<String, MessageListener> entry = (Map.Entry<String, MessageListener>) iterator.next();
			iterator.remove();
			deliverAsynchronousResponse(entry.getKey(), messageHolder, entry.getValue());
		}
    }

	public void receivedMessage(Object packet) {
        log.logDetail("SocketServerInstance.read", "reading"); //$NON-NLS-1$ //$NON-NLS-2$
        if (log.isLogged("SocketServerInstance.read", SocketLog.DETAIL)) { //$NON-NLS-1$
            log.logDetail("SocketServerInstance.read", "read:" + packet); //$NON-NLS-1$ //$NON-NLS-2$
        }
        try {
	        if (packet instanceof MessagePacket) {
	        	MessagePacket messagePacket = (MessagePacket)packet;
	            decryptMessage(messagePacket.message);
                processAsynchronousPacket(messagePacket);
	        } else if (packet instanceof Handshake) {
	        	receivedHahdshake((Handshake)packet);
	        } else {
	            if (log.isLogged("SocketServerInstance.read", SocketLog.DETAIL)) { //$NON-NLS-1$
	                log.logDetail("SocketServerInstance.read", "packet ignored:" + packet); //$NON-NLS-1$ //$NON-NLS-2$
	            }
	        }
        } catch (CommunicationException e) {
        	this.exceptionOccurred(e);
        }
    }

    private void processAsynchronousPacket(MessagePacket packet) {
        Serializable messageKey = packet.messageKey;
        Message message = packet.message;
        if (log.isLogged("SocketServerInstance.read", SocketLog.DETAIL)) { //$NON-NLS-1$
            log.logDetail("SocketServerInstance.read", "read asynch message:" + message); //$NON-NLS-1$ //$NON-NLS-2$
        }
        /* Defect 20272 - Removing the listener must happen before the response is delivered,
         * otherwise there the potential for a race condition in which a new request is registered
         * with the same message key and this thread removes the new listener instead.
         */
        MessageListener listener = asynchronousListeners.remove(messageKey);
        if (listener != null) {
            deliverAsynchronousResponse(messageKey, message, listener);
        }
    }

    private void deliverAsynchronousResponse(Serializable messageKey, Message message, MessageListener listener) {
        listener.deliverMessage(message, messageKey);
        log.logDetail("SocketServerInstanceImpl.deliverMessage", "message delivered"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void shutdown() {
    	socketChannel.close();
    }

    public ServerInstanceContext getContext() {
        return new SocketServerInstanceContext(hostInfo.getHostName(), hostInfo
				.getPortNumber(), this.ssl);
    }
    
    /** 
     * @return Returns the cryptor.
     */
    public Cryptor getCryptor() {
        return this.cryptor;
    }

	public void onConnection() {
		
	}

}