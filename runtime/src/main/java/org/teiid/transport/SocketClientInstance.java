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

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.teiid.dqp.internal.process.DQPWorkContext;

import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.common.comm.platform.socket.Handshake;
import com.metamatrix.common.comm.platform.socket.ObjectChannel;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.LogConstants;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.Cryptor;
import com.metamatrix.common.util.crypto.DhKeyGenerator;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.core.log.MessageLevel;

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
    	if (LogManager.isMessageToBeRecorded(LogConstants.CTX_TRANSPORT, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_TRANSPORT, " message: " + message + " for request ID:" + messageKey); //$NON-NLS-1$ //$NON-NLS-2$
        }
    	message.setMessageKey(messageKey);
    	objectSocket.write(message);
    }
    
    /** 
     * @return Returns the cryptor.
     */
    public Cryptor getCryptor() {
        return this.cryptor;
    }

	public void exceptionOccurred(Throwable t) {
		LogManager.logDetail(LogConstants.CTX_TRANSPORT, t, "Unhandled exception, closing client instance"); //$NON-NLS-1$
	}

	public void onConnection() throws CommunicationException {
        Handshake handshake = new Handshake();
        handshake.setVersion(SocketListener.getVersionInfo());
        
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

	private void receivedHahdshake(Handshake handshake) throws CommunicationException {
		if (usingEncryption) {
            byte[] returnedPublicKey = handshake.getPublicKey();
            
            //ensure the key information
            if (returnedPublicKey == null) {
                throw new CommunicationException(CommPlatformPlugin.Util.getString("SocketClientInstance.invalid_sessionkey")); //$NON-NLS-1$
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
