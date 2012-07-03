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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.teiid.client.security.ILogon;
import org.teiid.core.crypto.CryptoException;
import org.teiid.core.crypto.Cryptor;
import org.teiid.core.crypto.DhKeyGenerator;
import org.teiid.core.crypto.NullCryptor;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.internal.process.DQPWorkContext.Version;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.net.CommunicationException;
import org.teiid.net.socket.Handshake;
import org.teiid.net.socket.Message;
import org.teiid.net.socket.ObjectChannel;
import org.teiid.runtime.RuntimePlugin;


/**
 * Sockets implementation of the communication framework class representing the server's view of a client connection.
 * Implements the server-side of the sockets messaging protocol.  
 * The client side of the protocol is implemented in SocketServerInstance.
 * Users of this class are expected to provide a WorkerPool for processing incoming messages.  Users must also call read().
 * Users also provide a ServerListener implementation.  The ServerListener is the application level object 
 * processing the application level messages.
 */
public class SocketClientInstance implements ChannelListener, ClientInstance {
	
	private final ObjectChannel objectSocket;
    private Cryptor cryptor;
    private ClientServiceRegistryImpl csr;
    private boolean usingEncryption; 
    private DhKeyGenerator keyGen;
    private DQPWorkContext workContext = new DQPWorkContext();
        
    public SocketClientInstance(ObjectChannel objectSocket, ClientServiceRegistryImpl csr, boolean isClientEncryptionEnabled) {
        this.objectSocket = objectSocket;
        this.csr = csr;
        this.workContext.setSecurityHelper(csr.getSecurityHelper());
        this.usingEncryption = isClientEncryptionEnabled;
        SocketAddress address = this.objectSocket.getRemoteAddress();
        if (address instanceof InetSocketAddress) {
        	InetSocketAddress addr = (InetSocketAddress)address;
        	this.workContext.setClientAddress(addr.getAddress().getHostAddress());
        	this.workContext.setClientHostname(addr.getHostName());
        }
    }
    
    public void send(Message message, Serializable messageKey) {
    	message.setMessageKey(messageKey);
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_TRANSPORT, "send message: " + message); //$NON-NLS-1$
        }
    	objectSocket.write(message);
    }
    
    /** 
     * @return Returns the cryptor.
     */
    public Cryptor getCryptor() {
        return this.cryptor;
    }

	public void exceptionOccurred(Throwable t) {
		LogManager.log(t instanceof IOException?MessageLevel.DETAIL:MessageLevel.ERROR, LogConstants.CTX_TRANSPORT, t, "Unhandled exception, closing client instance"); //$NON-NLS-1$
	}

	public void onConnection() throws CommunicationException {
        Handshake handshake = new Handshake();
        handshake.setAuthType(csr.getAuthenticationType());
        if (usingEncryption) {
            keyGen = new DhKeyGenerator();
            byte[] publicKey;
			try {
				publicKey = keyGen.createPublicKey();
			} catch (CryptoException e) {
				throw new CommunicationException(e);
			}
            handshake.setPublicKey(publicKey);
        } 
        this.objectSocket.write(handshake);
	}
	
	@Override
	public void disconnected() {
		if (workContext.getSessionId() != null) {
			workContext.runInContext(new Runnable() {
				@Override
				public void run() {
					try {
						csr.getClientService(ILogon.class).logoff();
					} catch (Exception e) {
						LogManager.logDetail(LogConstants.CTX_TRANSPORT, e, "Exception closing client instance"); //$NON-NLS-1$
					}
				}
			});
		}
	}

	private void receivedHahdshake(Handshake handshake) throws CommunicationException {
		String clientVersion = handshake.getVersion();
		this.workContext.setClientVersion(Version.getVersion(clientVersion));
		if (usingEncryption) {
            byte[] returnedPublicKey = handshake.getPublicKey();
            
            //ensure the key information
            if (returnedPublicKey == null) {
                throw new CommunicationException(RuntimePlugin.Util.getString("SocketClientInstance.invalid_sessionkey")); //$NON-NLS-1$
            }
            
            try {
				this.cryptor = keyGen.getSymmetricCryptor(returnedPublicKey);
			} catch (CryptoException e) {
				throw new CommunicationException(e);
			}
            this.keyGen = null;
        } else {
            this.cryptor = new NullCryptor();
        }
	}

	public void receivedMessage(Object msg) throws CommunicationException {
        if (msg instanceof Message) {
            processMessagePacket((Message)msg);
        } else if (msg instanceof Handshake) {
        	receivedHahdshake((Handshake)msg);
        } 
	}

	private void processMessagePacket(Message packet) {
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)) { 
			LogManager.logDetail(LogConstants.CTX_TRANSPORT, "processing message:" + packet); //$NON-NLS-1$
        }
		if (this.workContext.getSecurityHelper() != null) {
			this.workContext.getSecurityHelper().clearSecurityContext();
		}
		final ServerWorkItem work = new ServerWorkItem(this, packet.getMessageKey(), packet, this.csr);
		this.workContext.runInContext(work);
	}

	public void shutdown() throws CommunicationException {
		this.objectSocket.close();
	}

	public DQPWorkContext getWorkContext() {
		return this.workContext;
	}
}
